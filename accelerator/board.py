# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2020-2024 Carl Drougge                                     #
# Modifications copyright (c) 2024 Anders Berkeman                         #
#                                                                          #
# Licensed under the Apache License, Version 2.0 (the "License");          #
# you may not use this file except in compliance with the License.         #
# You may obtain a copy of the License at                                  #
#                                                                          #
#  http://www.apache.org/licenses/LICENSE-2.0                              #
#                                                                          #
# Unless required by applicable law or agreed to in writing, software      #
# distributed under the License is distributed on an "AS IS" BASIS,        #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. #
# See the License for the specific language governing permissions and      #
# limitations under the License.                                           #
#                                                                          #
############################################################################

from __future__ import print_function

import bottle
import json
import sys
import os
import tarfile
import itertools
import collections
import functools
import mimetypes
import time
from stat import S_ISDIR, S_ISLNK

from accelerator.job import Job, JobWithFile
from accelerator.dataset import Dataset
from accelerator.unixhttp import call, WaitressServer
from accelerator.build import fmttime
from accelerator.configfile import resolve_listen
from accelerator.error import NoSuchWhateverError
from accelerator.metadata import insert_metadata
from accelerator.shell.parser import ArgumentParser, name2job, name2ds
from accelerator.shell.workdir import job_data, workdir_jids
from accelerator.compat import setproctitle, url_quote, urlencode
from accelerator.compat import FileNotFoundError, PermissionError
from accelerator import __version__ as ax_version
from accelerator import graph

# why wasn't Accept specified in a sane manner (like sending it in preference order)?
def get_best_accept(*want):
	d = {want[0]: -1} # fallback to first specified
	# {'a/*': 'a/exact'}, reversed() so earlier win
	want_short = {w.split('/', 1)[0] + '/*': w for w in reversed(want)}
	want = set(want)
	want.update(want_short)
	for accept in bottle.request.headers.get('Accept', '').split(','):
		accept = accept.split(';')
		mimetype = accept[0].strip()
		if mimetype in want:
			d[mimetype] = 1.0
			for p in accept[1:]:
				p = p.strip()
				if p.startswith('q='):
					try:
						d[mimetype] = float(p[2:])
					except ValueError:
						pass
	# include k in sort key as well so /* gets lower priority
	_, best = sorted(((v, k) for k, v in d.items()), reverse=True)[0]
	return want_short.get(best, best)

class JSONEncoderWithSet(json.JSONEncoder):
	__slots__ = ()
	def default(self, o):
		if isinstance(o, set):
			return list(o)
		return json.JSONEncoder.default(o)

json_enc = JSONEncoderWithSet(indent=4, ensure_ascii=False).encode


def ax_repr(o):
	res = []
	if isinstance(o, JobWithFile):
		link = '/job/' + bottle.html_escape(o.job)
		res.append('JobWithFile(<a href="%s">job=' % (link,))
		res.append(ax_repr(o.job))
		name = bottle.html_escape(o.name)
		if o.sliced:
			name += '.0'
		res.append('</a>, <a href="%s/%s">name=' % (link, name,))
		res.append(ax_repr(o.name))
		res.append('</a>, sliced=%s, extra=%s' % (ax_repr(o.sliced), ax_repr(o.extra),))
		res.append(')')
	elif isinstance(o, (list, tuple)):
		bra, ket = ('[', ']',) if isinstance(o, list) else ('(', ')',)
		res.append(bra)
		comma = ''
		for v in o:
			res.append(comma)
			res.append(ax_repr(v))
			comma = ', '
		res.append(ket)
	elif isinstance(o, dict):
		res.append('{')
		comma = ''
		for k, v in o.items():
			res.append(comma)
			res.append(ax_repr(k))
			res.append(': ')
			res.append(ax_repr(v))
			comma = ', '
		res.append('}')
	else:
		res.append(bottle.html_escape(repr(o)))
	return ''.join(res)

def ax_link(v):
	if isinstance(v, tuple):
		return '(%s)' % (', '.join(ax_link(vv) for vv in v),)
	elif isinstance(v, list):
		return '[%s]' % (', '.join(ax_link(vv) for vv in v),)
	elif v:
		ev = bottle.html_escape(v)
		if isinstance(v, Dataset):
			job = bottle.html_escape(v.job)
			name = bottle.html_escape(v.name)
			return '<a href="/job/%s">%s</a>/<a href="/dataset/%s">%s</a>' % (url_quote(v.job), job, url_quote(v), name,)
		elif isinstance(v, Job):
			return '<a href="/job/%s">%s</a>' % (url_quote(v), ev,)
		else:
			return ev
	else:
		return ''

_development = False

name2hashed = {}
hashed = {}

# Make contents-based names so that the files can be cached forever
def populate_hashed():
	from hashlib import sha1
	from base64 import b64encode
	dirname = os.path.join(os.path.dirname(__file__), 'board')
	for filename, ctype in [
		('style.css', 'text/css; charset=UTF-8'),
		('script.js', 'text/javascript; charset=UTF-8'),
		('graph.js', 'text/javascript; charset=UTF-8'),
	]:
		try:
			with open(os.path.join(dirname, filename), 'rb') as fh:
				data = fh.read()
			h = b64encode(sha1(data).digest(), b'_-').rstrip(b'=').decode('ascii')
			h_name = h + '/' + filename
			name2hashed[filename] = '/h/' + h_name
			hashed[h_name] = (data, ctype,)
		except OSError as e:
			name2hashed[filename] = '/h/ERROR'
			print(e, file=sys.stderr)


def js_quote(obj):
	# If the string contains '</script>' that will end the script, even if
	# it's in the middle of a string. Escape < to avoid problems like that.
	return json.dumps(obj).replace('<', '\\074')


def template(tpl_name, **kw):
	return bottle.template(
		tpl_name,
		ax_repr=ax_repr,
		ax_link=ax_link,
		ax_version=ax_version,
		js_quote=js_quote,
		name2hashed=name2hashed,
		template=template,
		url_quote=url_quote,
		**kw
	)


def view(name, subkey=None, prefer_ctype='application/json'):
	def view_decorator(func):
		@functools.wraps(func)
		def view_wrapper(**kw):
			if _development:
				populate_hashed()
			res = func(**kw)
			if isinstance(res, dict):
				accept = get_best_accept(prefer_ctype, 'application/json', 'text/json', 'text/html')
				if accept == 'text/html':
					return template(name, **res)
				elif prefer_ctype == accept == 'image/svg+xml':
					bottle.response.content_type = 'image/svg+xml; charset=UTF-8'
					return template(name, **res)
				else:
					bottle.response.content_type = accept + '; charset=UTF-8'
					if callable(subkey):
						res = subkey(res)
					elif subkey:
						res = res[subkey]
					return [json_enc(res), '\n']
			return res
		return view_wrapper
	return view_decorator

def fix_stacks(stacks, report_t):
	pid2pid = {}
	pid2jid = {}
	pid2part = {}
	job_pid = None
	for pid, indent, msg, t in stacks:
		if pid not in pid2pid and pid not in pid2jid:
			if msg.startswith('analysis('):
				pid2part[pid] = ''.join(c for c in msg if c.isdigit())
				pid2pid[pid] = job_pid
			else:
				pid2jid[pid] = msg.split(' ', 1)[0]
				job_pid = pid
		elif pid not in pid2part:
			pid2part[pid] = msg if msg in ('prepare', 'synthesis') else 'analysis'
		jobpid = pid
		while jobpid in pid2pid:
			jobpid = pid2pid[jobpid]
		jid = pid2jid[jobpid]
		if indent < 0:
			msg = msg.split('\n')
			start = len(msg) - 1
			while start and sum(map(bool, msg[start:])) < 5:
				start -= 1
			msg = [line.rstrip('\r') for line in msg[start:]]
			t = fmttime(report_t - t)
		else:
			t = fmttime(report_t - t, short=True)
		yield (jid, pid, indent, pid2part.get(pid), msg, t)

# datasets aren't dicts, so can't be usefully json encoded
def ds_json(d):
	ds = d['ds']
	keys = ('job', 'name', 'parent', 'filename', 'previous', 'hashlabel', 'lines')
	res = {k: getattr(ds, k) for k in keys}
	res['method'] = ds.job.method
	res['columns'] = {k: c.type for k, c in ds.columns.items()}
	return res

def main(argv, cfg):
	parser = ArgumentParser(prog=argv.pop(0), description='''runs a web server on listen_on (default localhost:8520, can be socket path) for displaying results (result_directory)''')
	parser.add_argument('--development', action='store_true', negation='not', help='enable tracebacks, disable caching, ...')
	parser.add_argument('listen_on', default='localhost:8520', nargs='?', help='host:port or path/to/socket')
	args = parser.parse_intermixed_args(argv)
	cfg.board_listen = resolve_listen(args.listen_on)[0]
	if isinstance(cfg.board_listen, str):
		# The listen path may be relative to the directory the user started us
		# from, but the reloader will exec us from the project directory, so we
		# have to be a little gross.
		cfg.board_listen = os.path.join(cfg.user_cwd, cfg.board_listen)
		argv[1:] = [cfg.board_listen]
	run(cfg, from_shell=True, development=args.development)

def run(cfg, from_shell=False, development=False):
	global _development
	_development = development

	project = os.path.split(cfg.project_directory)[1]
	setproctitle('ax board-server for %s on %s' % (project, cfg.board_listen,))

	# The default path filter (i.e. <something:path>) does not match newlines,
	# but we want it to do so (e.g. in case someone names a dataset with one).
	def pathfilter(config):
		return r'(?:.|\n)+?', None, None
	bottle.default_app[0].router.add_filter('path', pathfilter)

	populate_hashed()

	def call_s(*path, **kw):
		if kw:
			data = urlencode(kw).encode('utf-8')
		else:
			data = None
		return call(os.path.join(cfg.url, *map(url_quote, path)), data=data)

	def call_u(*path, **kw):
		url = os.path.join(cfg.urd, *map(url_quote, path))
		if kw:
			url = url + '?' + urlencode(kw)
		return call(url, server_name='urd')

	# yield chunks of a file, optionally inserting some bonus content somewhere
	def file_parts_iter(parts, clen, fh, start=0, end=None, chunksize=128 * 1024):
		try:
			if end is None:
				end = clen
			pos = 0
			for part, part_len in parts:
				if end <= pos:
					break
				if pos + part_len > start:
					if isinstance(part, bytes):
						yield part[max(0, start - pos):max(0, end - pos)]
					else:
						skip = max(0, start - pos)
						fh.seek(part + skip)
						z = part_len - skip - max(0, pos + part_len - end)
						while z > 0:
							want = min(z, chunksize)
							data = fh.read(want)
							yield data
							if len(data) != want:
								return # File shrunk? Nothing we can do.
							z -= chunksize
				pos += part_len
		finally:
			fh.close()

	# Based on the one in bottle but modified for our needs.
	def static_file(filename, root, job=None):
		root = os.path.abspath(root) + os.sep
		filename = os.path.abspath(os.path.join(root, filename))
		if not filename.startswith(root):
			return bottle.HTTPError(403, "Access denied.")
		try:
			fh = open(filename, 'rb')
		except PermissionError:
			return bottle.HTTPError(403, "Access denied.")
		except FileNotFoundError:
			return bottle.HTTPError(404, "File does not exist.")
		headers = {}
		mimetype, encoding = mimetypes.guess_type(filename)
		if mimetype:
			if mimetype.startswith('text/'):
				mimetype += '; charset=UTF-8'
			headers['Content-Type'] = mimetype
		if encoding:
			headers['Content-Encoding'] = encoding
		stats = os.fstat(fh.fileno())
		lm = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(stats.st_mtime))
		headers['Last-Modified'] = lm

		ims = bottle.request.environ.get('HTTP_IF_MODIFIED_SINCE')
		if ims:
			ims = bottle.parse_date(ims.split(';')[0].strip())
		if ims is not None and ims >= int(stats.st_mtime):
			headers['Date'] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
			fh.close()
			return bottle.HTTPResponse(status=304, **headers)

		file_parts = insert_metadata(filename, fh, job, stats.st_size)
		clen = sum(z for _, z in file_parts)
		headers['Content-Length'] = clen

		if bottle.request.method == 'HEAD':
			body = ''
			fh.close()
		else:
			body = fh

		headers['Accept-Ranges'] = 'bytes'
		if 'HTTP_RANGE' in bottle.request.environ:
			ranges = list(bottle.parse_range_header(bottle.request.environ['HTTP_RANGE'], clen))
			if not ranges:
				return bottle.HTTPError(416, "Requested Range Not Satisfiable")
			offset, end = ranges[0]
			headers['Content-Range'] = 'bytes %d-%d/%d' % (offset, end-1, clen)
			headers['Content-Length'] = str(end-offset)
			if body:
				body = file_parts_iter(file_parts, clen, body, offset, end)
			return bottle.HTTPResponse(body, status=206, **headers)
		if body:
			body = file_parts_iter(file_parts, clen, body)
		return bottle.HTTPResponse(body, **headers)

	@bottle.get('/')
	@view('main')
	def main_page(path='/results'):
		return dict(
			project=project,
			workdirs=cfg.workdirs,
			path=path,
			url_path=url_quote(path),
		)

	# Look for actual workdirs, so things like /workdirs/foo/foo-37/foo-1/bar
	# resolves to ('foo-37', 'foo-1/bar') and not ('foo-1', 'bar').
	path2wd = {v: k for k, v in cfg.workdirs.items()}
	def job_and_file(path, default_name):
		wd = ''
		path = iter(path.split('/'))
		for name in path:
			if not name:
				continue
			wd = wd + '/' + name
			if wd in path2wd:
				break
		else:
			return None, default_name
		try:
			jobid = Job(next(path))
		except (StopIteration, NoSuchWhateverError):
			return None, default_name
		return jobid, '/'.join(path) or default_name

	def results_contents(path):
		files = {}
		dirs = {}
		res = {'files': files, 'dirs': dirs}
		default_jobid = None
		default_prefix = ''
		prefix = cfg.result_directory
		for part in path.strip('/').split('/'):
			prefix = os.path.join(prefix, part)
			if not default_jobid:
				try:
					default_jobid, default_prefix = job_and_file(os.readlink(prefix), '')
					if default_jobid and default_prefix:
						default_prefix += '/'
				except OSError:
					pass
			elif default_prefix:
				default_prefix += part + '/'
		filenames = os.listdir(prefix)
		for fn in filenames:
			if fn.startswith('.') or fn.endswith('_'):
				continue
			ffn = os.path.join(prefix, fn)
			try:
				lstat = os.lstat(ffn)
				if S_ISLNK(lstat.st_mode):
					link_dest = os.readlink(ffn)
					stat = os.stat(link_dest)
					jobid, name = job_and_file(link_dest, fn)
				else:
					stat = lstat
					jobid = default_jobid
					name = default_prefix + fn
			except OSError:
				continue
			if S_ISDIR(stat.st_mode):
				dirs[fn] = os.path.join('/results', path, fn, '')
			else:
				files[fn] = dict(
					jobid=jobid,
					name=name,
					ts=lstat.st_mtime,
					size=stat.st_size,
				)
		if path:
			a, b = os.path.split(path)
			dirs['..'] = os.path.join('/results', a, '') if a else '/'
		return res

	@bottle.get('/results')
	@bottle.get('/results/')
	@bottle.get('/results/<path:path>')
	def results(path=''):
		path = path.strip('/')
		if os.path.isdir(os.path.join(cfg.result_directory, path)):
			accept = get_best_accept('text/html', 'application/json', 'text/json')
			if accept == 'text/html':
				return main_page(path=os.path.join('/results', path).rstrip('/'))
			else:
				bottle.response.content_type = accept + '; charset=UTF-8'
				bottle.response.set_header('Cache-Control', 'no-cache')
				return json.dumps(results_contents(path))
		elif path:
			try:
				link_dest = os.readlink(os.path.join(cfg.result_directory, path))
				job, _ = job_and_file(link_dest, None)
			except OSError:
				job = None
			return static_file(path, root=cfg.result_directory, job=job)
		else:
			return {'missing': 'result directory %r missing' % (cfg.result_directory,)}

	@bottle.get('/status')
	@view('status')
	def status():
		status = call_s('status/full')
		if 'short' in bottle.request.query:
			if status.idle:
				return 'idle'
			else:
				t, msg, _ = status.current
				return '%s (%s)' % (msg, fmttime(status.report_t - t, short=True),)
		else:
			status.tree = list(fix_stacks(status.pop('status_stacks', ()), status.report_t))
			return status

	@bottle.get('/last_error')
	@view('last_error')
	def last_error():
		return call_s('last_error')

	@bottle.get('/job/<jobid>/method.tar.gz/')
	@bottle.get('/job/<jobid>/method.tar.gz/<name:path>')
	def job_method(jobid, name=None):
		job = name2job(cfg, jobid)
		with tarfile.open(job.filename('method.tar.gz'), 'r:gz') as tar:
			if name:
				info = tar.getmember(name)
			else:
				members = [info for info in tar.getmembers() if info.isfile()]
				if len(members) == 1 and not name:
					info = members[0]
				else:
					return template('job_method_list', members=members, job=job)
			bottle.response.content_type = 'text/plain; charset=UTF-8'
			return tar.extractfile(info).read()

	@bottle.get('/job/<jobid>/<name:path>')
	def job_file(jobid, name):
		job = name2job(cfg, jobid)
		res = static_file(name, root=job.path, job=job)
		if not res.content_type and res.status_code < 400:
			# bottle default is text/html, which is probably wrong.
			res.content_type = 'text/plain'
		return res

	@bottle.get('/job/<jobid>')
	@bottle.get('/job/<jobid>/')
	@view('job')
	def job(jobid):
		job = name2job(cfg, jobid)
		try:
			post = job.post
		except IOError:
			post = None
		if post:
			aborted = False
			files = [fn for fn in job.files() if fn[0] != '/']
			jobs = list(post.subjobs)
			jobs.append(job)
			jobs = call_s('jobs_are_current', jobs='\0'.join(jobs))
			subjobs = [(Job(jobid), jobs[jobid]) for jobid in post.subjobs]
			current = jobs[job]
		else:
			aborted = True
			current = False
			files = None
			subjobs = None
		return dict(
			job=job,
			aborted=aborted,
			current=current,
			output=os.path.exists(job.filename('OUTPUT')),
			datasets=job.datasets,
			params=job.params,
			subjobs=subjobs,
			files=files,
		)

	@bottle.get('/dataset/<dsid:path>')
	@view('dataset', ds_json)
	def dataset(dsid):
		ds = name2ds(cfg, dsid.rstrip('/'))
		q = bottle.request.query
		if q.column:
			lines = int(q.lines or 10)
			it = ds.iterate(None, q.column)
			it = itertools.islice(it, lines)
			t = ds.columns[q.column].type
			if t in ('datetime', 'date', 'time',):
				it = map(str, it)
			elif t in ('bytes', 'pickle',):
				it = map(repr, it)
			res = list(it)
			bottle.response.content_type = 'application/json; charset=UTF-8'
			return json.dumps(res)
		else:
			return dict(ds=ds)

	@bottle.get('/graph/job/<jobid>')
	@view('rendergraph', prefer_ctype='image/svg+xml')
	def job_graph(jobid):
		job = name2job(cfg, jobid)
		ret = graph.graph(job, 'job')
		return ret

	@bottle.get('/graph/dataset/<dsid:path>')
	@view('rendergraph', prefer_ctype='image/svg+xml')
	def dataset_graph(dsid):
		ds = name2ds(cfg, dsid.rstrip('/'))
		ret = graph.graph(ds, 'dataset')
		return ret

	@bottle.get('/graph/urd/<user>/<build>/<ts>')
	@view('rendergraph', prefer_ctype='image/svg+xml')
	def urd_graph(user, build, ts):
		key = user + '/' + build + '/' + ts
		d = call_u(key)
		ret = graph.graph(d, 'urd')
		return ret

	def load_workdir(jobs, name):
		known = call_s('workdir', name)
		for jid in workdir_jids(cfg, name):
			jobs[jid] = job_data(known, jid)
		return jobs

	@bottle.get('/workdir/<name>')
	@view('workdir', 'jobs')
	def workdir(name):
		return dict(name=name, jobs=load_workdir(collections.OrderedDict(), name))

	@bottle.get('/workdir')
	@bottle.get('/workdir/')
	@view('workdir', 'jobs')
	def all_workdirs():
		jobs = collections.OrderedDict()
		for name in sorted(cfg.workdirs):
			load_workdir(jobs, name)
		return dict(name='ALL', jobs=jobs)

	@bottle.get('/methods')
	@view('methods')
	def methods():
		methods = call_s('methods')
		by_package = collections.defaultdict(list)
		for name, data in sorted(methods.items()):
			by_package[data.package].append(name)
		by_package.pop('accelerator.test_methods', None)
		return dict(methods=methods, by_package=by_package)

	@bottle.get('/method/<name>')
	@view('method', 'data')
	def method(name):
		methods = call_s('methods')
		if name not in methods:
			return bottle.HTTPError(404, 'Method %s not found' % (name,))
		return dict(name=name, data=methods[name], cfg=cfg)

	@bottle.get('/urd')
	@bottle.get('/urd/')
	@view('urd', 'lists')
	def urd():
		return dict(
			lists=call_u('list'),
			project=project,
		)

	@bottle.get('/urd/<user>/<build>')
	@bottle.get('/urd/<user>/<build>/')
	@view('urdlist', 'timestamps')
	def urdlist(user, build):
		key = user + '/' + build
		return dict(
			key=key,
			timestamps=call_u(key, 'since/0', captions=1),
		)

	@bottle.get('/urd/<user>/<build>/<ts>')
	@view('urditem', 'entry')
	def urditem(user, build, ts):
		key = user + '/' + build + '/' + ts
		d = call_u(key)
		return dict(key=key, entry=d)

	@bottle.get('/h/<name:path>')
	def hashed_file(name):
		if name not in hashed:
			return bottle.HTTPError(404, 'Not Found')
		data, ctype = hashed[name]
		bottle.response.content_type = ctype
		bottle.response.set_header('Cache-Control', 'max-age=604800, immutable')
		return data

	@bottle.get('/robots.txt')
	def robots_txt():
		bottle.response.content_type = 'text/plain'
		return b'User-agent: *\nDisallow: /\n'

	@bottle.error(500)
	def error(e):
		tpl = bottle.ERROR_PAGE_TEMPLATE
		if isinstance(e.exception, NoSuchWhateverError):
			e.body = str(e.exception)
		return bottle.template(tpl, e=e)

	bottle.TEMPLATE_PATH = [os.path.join(os.path.dirname(__file__), 'board')]
	kw = {}
	if development:
		kw['reloader'] = True
		kw['debug'] = True
	if not from_shell:
		kw['quiet'] = True
	kw['server'] = WaitressServer
	listen = cfg.board_listen
	if isinstance(listen, tuple):
		kw['host'], kw['port'] = listen
	else:
		from accelerator.server import check_socket
		check_socket(listen)
		kw['host'] = listen
		kw['port'] = 0
	bottle.run(**kw)
