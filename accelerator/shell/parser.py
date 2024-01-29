# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2024 Carl Drougge                       #
# Modifications copyright (c) 2019 Anders Berkeman                         #
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

# parsing of "job specs", including as part of a dataset name.
# handles jobids, paths and method names.

from __future__ import division, print_function

import argparse
import sys
from os.path import join, exists, realpath, split
from os import readlink, environ
import re

from accelerator.dataset import Dataset
from accelerator.job import WORKDIRS
from accelerator.job import Job
from accelerator.error import NoSuchJobError, NoSuchDatasetError, NoSuchWorkdirError, UrdError
from accelerator.unixhttp import call
from accelerator.compat import url_quote, urlencode, PY3

class JobNotFound(NoSuchJobError):
	pass

class DatasetNotFound(NoSuchDatasetError):
	pass

def _groups(tildes):
	def char_and_count(buf):
		char, count = re.match(r'([~+<>^]+)(\d*)$', ''.join(buf)).groups()
		count = int(count or 1) - 1
		return char[0], len(char) + count
	i = iter(tildes)
	buf = [next(i)]
	for c in i:
		if c in '~+<>^' and buf[-1] != c:
			yield char_and_count(buf)
			buf = [c]
		else:
			buf.append(c)
	yield char_and_count(buf)

# "foo~~^3" -> "foo", [("~", 2), ("^", 3)]
def split_tildes(n, allow_empty=False, extended=False):
	if extended:
		m = re.match(r'(.*?)([~+<>^][~+<>^\d]*)$', n)
	else:
		m = re.match(r'(.*?)([~^][~^\d]*)$', n)
	if m:
		n, tildes = m.groups()
		lst = list(_groups(tildes))
	else:
		lst = []
	assert n or allow_empty, "empty job id"
	return n, lst

def method2job(cfg, method, **kw):
	url ='%s/method2job/%s?%s' % (cfg.url, url_quote(method), urlencode(kw))
	found = call(url)
	if 'error' in found:
		raise JobNotFound(found.error)
	return Job(found.id)

# follow jobs.previous (or datasets.previous.job if that is unavailable) count times.
def job_up(job, count):
	err_job = job
	for ix in range(count):
		prev = job.params.jobs.get('previous')
		if not prev:
			prev = job.params.datasets.get('previous')
			if prev:
				prev = prev.job
		if not prev:
			raise JobNotFound('Tried to go %d up from %s, but only %d previous jobs available' % (count, err_job, ix,))
		job = prev
	return job

def urd_call_w_tildes(cfg, path, tildes):
	res = call(cfg.urd + '/' + path, server_name='urd', retries=0, quiet=True)
	if tildes:
		up = sum(count for char, count in tildes if char == '^')
		down = sum(count for char, count in tildes if char == '~')
		tildes = down - up
		if tildes:
			key = res.user + '/' + res.build
			timestamps = call(cfg.urd + '/' + key + '/since/0', server_name='urd', retries=0, quiet=True)
			pos = timestamps.index(res.timestamp) + tildes
			if pos < 0 or pos >= len(timestamps):
				return None
			res = call(cfg.urd + '/' + key + '/' + timestamps[pos], server_name='urd', retries=0, quiet=True)
	return res

def name2job(cfg, n, _want_ds=False):
	dotted = None
	if '.' in n:
		if n.startswith(':') and ':' in n[1:]: # :urd:-list
			prefix_len = n.index(':', 1) + 1
		else:
			prefix_len = 0
		# If workdir names have '.' in them we don't want to split there.
		for prefix in sorted((name + '-' for name in cfg.workdirs), key=lambda name: -len(name)):
			if n.startswith(prefix):
				prefix_len = len(prefix)
				break
		try:
			prefix_len = n.index('.', prefix_len)
			n, dotted = n[:prefix_len], iter(n[prefix_len + 1:].split('.'))
		except ValueError:
			pass
	def split(n, what):
		n, tildes = split_tildes(n, extended=True, allow_empty=True)
		if n.endswith('!'):
			current = True
			n = n[:-1]
		else:
			current = False
		assert n, "empty " + what
		return n, current, tildes
	n, current, tildes = split(n, "job id")
	job = _name2job(cfg, n, current)
	job = _name2job_do_tildes(cfg, job, current, tildes)
	ds = None
	if dotted:
		for n in dotted:
			n, current, tildes = split(n, "param")
			p = job.params
			k = None
			if n in ('jobs', 'datasets'):
				k = n
				if current or tildes:
					raise JobNotFound("Don't use !~+<>^ on .%s, put after .%s.foo(HERE)." % (k, k))
				try:
					n = next(dotted)
				except StopIteration:
					raise JobNotFound("%s.%s.what?" % (job, k,))
				n, current, tildes = split(n, k)
			elif n in p.jobs and n in p.datasets:
				raise JobNotFound("Job %s (%s) has %s in both .jobs and .datasets, please specify." % (job, job.method, n,))
			if k:
				if n not in p[k]:
					raise JobNotFound("Job %s (%s) does not have a %r." % (job, job.method, k + '.' + n,))
			else:
				if n in p.jobs:
					k = 'jobs'
				elif n in p.datasets:
					k = 'datasets'
				else:
					raise JobNotFound("Job %s (%s) does not have a %r." % (job, job.method, n,))
			if not p[k][n]:
				raise JobNotFound("%s.%s.%s is None" % (job, k, n,))
			job = p[k][n]
			if isinstance(job, list):
				if len(job) != 1:
					raise JobNotFound("Job %s (%s) has %d %s in %r." % (job, job.method, len(job), k, n,))
				job = job[0]
			if isinstance(job, Dataset):
				ds = job
				job = job.job
			else:
				ds = None
			if tildes:
				ds = None
			job = _name2job_do_tildes(cfg, job, current, tildes)
	if _want_ds and ds:
		return ds
	return job

def _name2job_do_tildes(cfg, job, current, tildes):
	for char, count in tildes:
		if char == '~':
			job = method2job(cfg, job.method, offset=-count, start_from=job, current=current)
		elif char == '+':
			job = method2job(cfg, job.method, offset=count, start_from=job, current=current)
		elif char == '^':
			job = job_up(job, count)
		elif char == '<':
			if count > job.number:
				raise JobNotFound('Tried to go %d jobs back from %s.' % (count, job,))
			job = Job._create(job.workdir, job.number - count)
		elif char == '>':
			job = Job._create(job.workdir, job.number + count)
		else:
			raise Exception("BUG: split_tildes should not give %r as a char" % (char,))
	if not exists(job.filename('setup.json')):
		raise JobNotFound('Job resolved to %r but that job does not exist' % (job,))
	return job

def _name2job(cfg, n, current):
	if n.startswith(':'):
		# resolve through urd
		assert cfg.urd, 'No urd configured'
		a = n[1:].rsplit(':', 1)
		if len(a) == 1:
			raise JobNotFound('looks like a partial :urdlist:[entry] spec')
		entry = a[1] or '-1'
		try:
			entry = int(entry, 10)
		except ValueError:
			pass
		path, tildes = split_tildes(a[0])
		path = path.split('/')
		if len(path) < 3:
			path.insert(0, environ.get('USER', 'NO-USER'))
		if len(path) < 3:
			path.append('latest')
		path = '/'.join(map(url_quote, path))
		try:
			urdres = urd_call_w_tildes(cfg, path, tildes)
		except UrdError as e:
			print(e, file=sys.stderr)
			urdres = None
		if not urdres:
			raise JobNotFound('urd list %r not found' % (a[0],))
		from accelerator.build import JobList
		joblist = JobList(Job(e[1], e[0]) for e in urdres.joblist)
		res = joblist.get(entry)
		if not res:
			raise JobNotFound('%r not found in %s' % (entry, path,))
		return res
	if re.match(r'[^/]+-\d+$', n):
		# Looks like a jobid
		return Job(n)
	m = re.match(r'([^/]+)-LATEST$', n)
	if m:
		# Looks like workdir-LATEST
		wd = m.group(1)
		if wd not in WORKDIRS:
			raise NoSuchWorkdirError('Not a valid workdir: "%s"' % (wd,))
		path = join(WORKDIRS[wd], n)
		try:
			n = readlink(path)
		except OSError as e:
			raise JobNotFound('Failed to read %s: %s' % (path, e,))
		return Job(n)
	if n not in ('.', '..') and '/' not in n:
		# Must be a method then
		return method2job(cfg, n, current=current)
	if exists(join(n, 'setup.json')):
		# Looks like the path to a jobdir
		path, jid = split(realpath(n))
		job = Job(jid)
		if WORKDIRS.get(job.workdir, path) != path:
			print("### Overriding workdir %s to %s" % (job.workdir, path,))
		WORKDIRS[job.workdir] = path
		return job
	raise JobNotFound("Don't know what to do with %r." % (n,))

def split_ds_dir(n):
	"""try to split a path at the jid/ds boundary"""
	orig_n = n
	jid_cand, name = n.split('/', 1)
	if re.match(r'.+-\d+(?:[~^][~^\d]*)?$', jid_cand):
		# looks like a JID, so assume it is. start with ./ to avoid this.
		return jid_cand, name
	name_bits = []
	while '/' in n and not exists(join(n, 'setup.json')):
		n, bit = n.rsplit('/', 1)
		name_bits.append(bit)
	while n.endswith('/') or n.endswith('/.'):
		n, bit = n.rsplit('/', 1)
		name_bits.append(bit)
	if not n:
		raise JobNotFound('No setup.json found in %r' % (orig_n,))
	if not name_bits:
		name_bits = ['default']
	return n, '/'.join(reversed(name_bits))

def name2ds(cfg, n):
	job = name = tildes = None
	if n.startswith(':'):
		colon2 = n.rfind(':', 1)
		if colon2 > 0:
			tailslash = n.find('/', colon2)
			if tailslash > 0:
				name = n[tailslash + 1:]
				n = n[:tailslash]
		job = name2job(cfg, n, _want_ds=name is None)
	elif '/' not in n:
		job = name2job(cfg, n, _want_ds=True)
	else:
		n, name = split_ds_dir(n)
		job = name2job(cfg, n)
		name, tildes = split_tildes(name, allow_empty=True)
	if isinstance(job, Dataset):
		ds = job
	else:
		ds = job.dataset(name)
	if tildes:
		def follow(key, motion):
			# follow ds.key count times
			res = ds
			for done in range(count):
				if not getattr(res, key):
					raise DatasetNotFound('Tried to go %d %s from %s, but only %d available (stopped on %s)' % (count, motion, ds, done, res,))
				res = getattr(res, key)
			return res
		for char, count in tildes:
			if char == '~':
				ds = follow('previous', 'back')
			else:
				ds = follow('parent', 'up')
	slices = ds.job.params.slices
	from accelerator import g
	if hasattr(g, 'slices'):
		assert g.slices == slices, "Dataset %s needs %d slices, by we are already using %d slices" % (ds, slices, g.slices)
	else:
		g.slices = slices
	return ds


class ArgumentParser(argparse.ArgumentParser):
	def __init__(self, *a, **kw):
		kw = dict(kw)
		kw['prefix_chars'] = '-+'
		if PY3:
			# allow_abbrev is 3.5+. it's not even available in the pypi backport of argparse.
			# it also regrettably disables -abc for -a -b -c until 3.8.
			kw['allow_abbrev'] = False
		return argparse.ArgumentParser.__init__(self, *a, **kw)

	def add_argument(self, *a, **kw):
		if kw.get('action') == 'store_true':
			# automatically add negated version of boolean args.
			# uses all negations (--no, --not and --dont) if none is specified.
			from argparse import SUPPRESS
			for name in a:
				if name.startswith('--'):
					dest = name[2:]
					break
			else:
				dest = a[0].lstrip('-')
			kw = dict(kw)
			dest = kw.get('dest', dest.replace('-', '_'))
			negation = kw.pop('negation')
			for name in a:
				if len(name) == 2:
					# short args negate with +, in traditional unix fashion
					neg_names = ['+' + name[1]]
				elif name.startswith('--no-'):
					neg_names = ['--' + negation + name[4:]]
				else:
					neg_names = ['--' + negation + name[1:]]
				for neg_name in neg_names:
					argparse.ArgumentParser.add_argument(self, neg_name, dest=dest, action='store_const', const=False, help=SUPPRESS)
		return argparse.ArgumentParser.add_argument(self, *a, **kw)

	# parse_intermixed_args is new in python 3.7
	if not hasattr(argparse.ArgumentParser, 'parse_intermixed_args'):
		parse_intermixed_args = argparse.ArgumentParser.parse_args

	def parse_args(self, *a, **kw):
		raise Exception("Don't use parse_args, use parse_intermixed_args")
