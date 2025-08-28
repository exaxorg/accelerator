# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2024 Carl Drougge                       #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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

from glob import glob
from collections import defaultdict
from bottle import route, request, auth_basic, abort
import bottle
from threading import Lock
import json
import re
from datetime import datetime
import operator
import os.path
from io import TextIOWrapper
import sys
import os
import signal

from accelerator.compat import iteritems, itervalues
from accelerator.colourwrapper import colour
from accelerator.shell.parser import ArgumentParser
from accelerator.unixhttp import WaitressServer

LOGFILEVERSION = '4'

lock = Lock()

def locked(func):
	def inner(*a, **kw):
		with lock:
			return func(*a, **kw)
	return inner


def joblistlike(jl):
	assert isinstance(jl, list)
	for v in jl:
		assert isinstance(v, (list, tuple)), v
		assert len(v) == 2, v
		for s in v:
			assert isinstance(s, str), s
	return True


class TimeStamp(str):
	"""Can be a string like 2019-12-09T12:19:04.123456 (day and time are
	optional, partial time ok) and/or an int >= 0.
	Integers without datetimes sort before all datetimes.
	Datetimes without integers sort before the same datetime with an integer.
	When both are specified they are separated by a +
	"""

	__slots__ = ('_ts', '_integer')

	def __new__(cls, ts):
		if isinstance(ts, TimeStamp):
			return ts
		try:
			integer = int(ts, 10)
			assert integer >= 0, f'Invalid timestamp {ts}'
			ts = None
		except ValueError:
			m = re.match(r'(\d{4}-\d{2}(?:-\d{2}(?:[T ]\d{2}(?::\d{2}(?::\d{2}(?:\.\d{1,6})?)?)?)?)?)(\+\d+)?$', ts)
			assert m, f'Invalid timestamp {ts}'
			ts, integer = m.groups()
			ts = ts.replace(' ', 'T')
			integer = int(integer[1:], 10) if integer else None
		assert ts is not None or integer is not None, f'Invalid timestamp {ts}'
		if ts:
			if integer is not None:
				strval = f'{ts}+{integer}'
			else:
				strval = ts
		else:
			strval = str(integer)
		obj = str.__new__(cls, strval)
		obj._ts = ts
		obj._integer = integer
		return obj

	__hash__ = str.__hash__

	def __eq__(self, other):
		if not isinstance(other, TimeStamp):
			other = TimeStamp(other)
		return self._ts == other._ts and self._integer == other._integer

	def __lt__(self, other):
		if not isinstance(other, TimeStamp):
			other = TimeStamp(other)
		if self._ts is not None:
			if other._ts is not None:
				if self._integer is not None:
					if self._ts == other._ts:
						if other._integer is not None:
							return self._integer < other._integer
						else:
							return False
					else:
						return self._ts < other._ts
				else:
					if other._integer is not None:
						return self._ts <= other._ts
					else:
						return self._ts < other._ts
			else:
				return False
		elif other._ts is not None:
			return True
		else:
			return self._integer < other._integer

	def __le__(self, other):
		if not isinstance(other, TimeStamp):
			other = TimeStamp(other)
		return self < other or self == other

	def __ge__(self, other):
		return not self < other

	def __gt__(self, other):
		return not self <= other


# Like a (non-ordered) DotDict that only compares some values when checking equality.
class Entry(dict):
	def __eq__(self, other):
		nonexistant = object()
		for k in ('timestamp', 'joblist', 'caption', 'user', 'build', 'deps', 'key',):
			if self.get(k, nonexistant) != other.get(k, nonexistant):
				return False
		return True

	def __ne__(self, other):
		return not (self == other)

	def __getattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		try:
			return self[name]
		except KeyError:
			pass # raise new error outside this except clause
		raise AttributeError(name)

	def __setattr__(self, name, value):
		if name[0] == "_":
			raise AttributeError(name)
		self[name] = value

	def __delattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		del self[name]


class DB:
	def __init__(self, path, verbose=True):
		self._initialised = False
		self.path = path
		self.db = defaultdict(dict)
		self.ghost_db = defaultdict(lambda: defaultdict(list))
		if os.path.isdir(path):
			files = glob(os.path.join(path, '*/*.urd'))
			self._parsed = {}
			stat = {}
			for fn in files:
				with open(fn) as fh:
					ix = 0
					for line in fh:
						self._parse(line)
						ix += 1
					stat[fn[len(path) + 1:-len('.urd')]] = ix
			self._playback_parsed()
			if verbose:
				print("urd-list                          lines     ghosts     active")
				for key, val in sorted(stat.items()):
					print(f"{key:30}  {val:7}    {len(self.ghost_db[key]):7}    {len(self.db[key]):7}")
				print()
		else:
			print(f"Creating directory \"{path}\".")
			os.makedirs(path)
		self._lasttime = None
		self._initialised = True

	def _parse(self, line):
		line = line.rstrip('\n')
		if line[:2] == '3|':
			line = line.split('|')
		else:
			line = line.split('\t')
		logfileversion, writets = line[:2]
		assert writets not in self._parsed
		if logfileversion == '3':
			if line[2] == 'add':
				line[-1] = json.dumps(line[-1]) # caption is json encoded in v4
		else:
			assert logfileversion == '4', logfileversion
		if line[2] == 'add' and len(line) < 10:
			line.append('null')
		self._parsed[writets] = line[2:]

	def _playback_parsed(self):
		for _writets, line in sorted(iteritems(self._parsed)):
			action = line.pop(0)
			assert action in ('add', 'truncate',)
			if action == 'add':
				self._parse_add(line)
			elif action == 'truncate':
				self._parse_truncate(line)
			else:
				assert "can't happen"

	def _parse_add(self, line):
		key = line[1]
		user, build = key.split('/')
		flags = line[4].split(',') if line[4] else []
		data = Entry(timestamp=line[0],
			user=user,
			build=build,
			deps=json.loads(line[2]),
			joblist=json.loads(line[3]),
			flags=flags,
			caption=json.loads(line[5]),
			build_job=json.loads(line[6])
		)
		self.add(data)

	def _parse_truncate(self, line):
		timestamp, key = line
		self.truncate(key, timestamp)

	def _validate_data(self, data, with_deps=True):
		if with_deps:
			assert set(data) == {'timestamp', 'joblist', 'caption', 'user', 'build', 'deps', 'flags', 'build_job',}
			assert isinstance(data.user, str)
			assert isinstance(data.build, str)
			assert isinstance(data.build_job, str)
			assert isinstance(data.deps, dict)
			for v in itervalues(data.deps):
				assert isinstance(v, dict)
				self._validate_data(Entry(v), False)
		else:
			assert set(data) == {'timestamp', 'joblist', 'caption',}
		assert joblistlike(data.joblist), data.joblist
		assert data.joblist
		assert isinstance(data.caption, str)
		data.timestamp = TimeStamp(data.timestamp)

	def _serialise(self, action, data):
		if action == 'add':
			self._validate_data(data)
			json_deps = json.dumps(data.deps)
			json_joblist = json.dumps(data.joblist)
			json_caption = json.dumps(data.caption)
			json_build_job = json.dumps(data.build_job)
			key = f'{data.user}/{data.build}'
			flags = ','.join(data.flags)
			for s in key, json_deps, json_joblist, json_caption, data.user, data.build, data.timestamp, flags:
				assert '\t' not in s, s
				assert '\n' not in s, s
			logdata = [json_deps, json_joblist, flags, json_caption, json_build_job,]
		elif action == 'truncate':
			key = data.key
			logdata = []
		else:
			assert "can't happen"
		data.timestamp = TimeStamp(data.timestamp)
		while True: # paranoia
			now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
			if now != self._lasttime: break
		self._lasttime = now
		s = '\t'.join([LOGFILEVERSION, now, action, data.timestamp, key,] + logdata)
		return s

	def _is_ghost(self, data):
		for key, data in iteritems(data.deps):
			db = self.db[key]
			ts = data['timestamp']
			if ts not in db:
				return True
			for k, v in iteritems(data):
				if db[ts].get(k) != v:
					return True
		return False

	@locked
	def add(self, data):
		key = f'{data.user}/{data.build}'
		flags = data.pop('flags', [])
		assert flags in ([], ['update']), f'Unknown flags: {flags!r}'
		new = False
		changed = False
		ghosted = 0
		data.timestamp = TimeStamp(data.timestamp)
		assert data.timestamp != '0', "Timestamp 0 is special, you can't add it."
		is_ghost = self._is_ghost(data)
		if is_ghost:
			db = self.ghost_db[key]
			if data.timestamp in db:
				new = False
				changed = (data not in db[data.timestamp])
			else:
				new = True
		else:
			db = self.db[key]
			if data.timestamp in db:
				new = False
				changed = (db[data.timestamp] != data)
			else:
				new = True
		if changed and 'update' not in flags:
			assert self._initialised, f'Log updates without update flag: {data!r}'
			bottle.response.status = 409
			return {'error': 'would update'}
		if new or changed:
			data.flags = flags
			self.log('add', data) # validates, too
			del data.flags
			if is_ghost:
				db[data.timestamp].append(data)
			else:
				if changed:
					ghost_data = db[data.timestamp]
					self.ghost_db[key][data.timestamp].append(ghost_data)
				db[data.timestamp] = data
				if changed:
					ghosted = self._update_ghosts()
		res = dict(new=new, changed=changed, is_ghost=is_ghost)
		if changed:
			res['deps'] = ghosted
		return res

	def _update_ghosts(self):
		def inner():
			count = 0
			for key, db in iteritems(self.db):
				for ts, data in sorted(db.items()):
					if self._is_ghost(data):
						count += 1
						del db[ts]
						self.ghost_db[key][ts].append(data)
			return count
		res = 0
		while True:
			count = inner()
			if not count:
				break
			res += count
		return res

	@locked
	def truncate(self, key, timestamp):
		old = self.db[key]
		new = {}
		ghost = {}
		timestamp = TimeStamp(timestamp)
		for ts, data in iteritems(old):
			if ts < timestamp:
				new[ts] = data
			else:
				ghost[ts] = data
		self.log('truncate', Entry(key=key, timestamp=timestamp))
		self.db[key] = new
		ghost_db = self.ghost_db[key]
		for ts, data in iteritems(ghost):
			ghost_db[ts].append(data)
		if ghost:
			deps = self._update_ghosts()
		else:
			deps = 0
		return {'count': len(ghost), 'deps': deps}

	def log(self, action, data):
		if self._initialised:
			if action == 'truncate':
				user, build = data.key.split('/')
			else:
				user, build = data.user, data.build
			assert '/' not in user
			assert '/' not in build
			path = os.path.join(self.path, user)
			if not os.path.isdir(path):
				os.makedirs(path)
			fn = os.path.join(path, build + '.urd')
			with open(fn, 'a') as fh:
				start_pos = fh.tell()
				try:
					fh.write(self._serialise(action, data) + '\n')
					fh.flush()
				except IOError as e:
					try:
						try:
							# Try to avoid leaving a partial line in the file.
							fh.truncate(start_pos)
							fh.close()
							extra = ''
						except:
							extra = "  Also failed to remove partially written data."
							stars = colour.red('****')
							extra2 = "  " + stars + " YOUR URD DB IS PROBABLY BROKEN NOW! " + stars
						msg = f"  Failed to write {fn}: {e}"
						brk = "#" * (max(len(msg), len(extra)) + 2)
						print("", file=sys.stderr)
						print(brk, file=sys.stderr)
						print(msg, file=sys.stderr)
						if extra:
							print(extra, file=sys.stderr)
							print(extra2, file=sys.stderr)
						print(brk, file=sys.stderr)
						print("", file=sys.stderr)
					finally:
						# This is a fatal error.
						os.killpg(os.getpgid(0), signal.SIGTERM)

	@locked
	def get(self, key, timestamp):
		db = self.db[key]
		return db.get(TimeStamp(timestamp))

	@locked
	def since(self, key, timestamp):
		timestamp = TimeStamp(timestamp)
		return sorted(k for k in self.db[key] if k > timestamp)

	@locked
	def limited_endpoint(self, key, timestamp, cmpfunc, minmaxfunc):
		db = self.db[key]
		try:
			k = minmaxfunc(k for k in db if cmpfunc(k, timestamp))
		except ValueError: # empty sequence
			return None
		return db[k]

	@locked
	def latest(self, key):
		db = self.db[key]
		if db:
			return db[max(db)]

	@locked
	def first(self, key):
		db = self.db[key]
		if db:
			return db[min(db)]

	def keys(self):
		return filter(self.db.get, self.db)


def auth(user, passphrase):
	return authdict.get(user) == passphrase or (allow_passwordless and user not in authdict)

def timestamp404(timestamp):
	try:
		return TimeStamp(timestamp)
	except AssertionError as e:
		raise bottle.HTTPError(404, str(e))

@route('/<user>/<build>/since/<timestamp>')
def since(user, build, timestamp):
	key = user + '/' + build
	timestamp = timestamp404(timestamp)
	res = db.since(key, timestamp)
	if 'captions' in request.query:
		res = [(ts, (db.get(key, ts) or {}).get('caption', ''),) for ts in res]
	return res

@route('/<user>/<build>/latest')
def latest(user, build):
	return db.latest(user + '/' + build)

@route('/<user>/<build>/first')
def first(user, build):
	return db.first(user + '/' + build)

@route('/<user>/<build>/<timestamp>')
def single(user, build, timestamp):
	key = user + '/' + build
	if len(timestamp) > 1 and timestamp[0] in '<>':
		if timestamp[0] == '<':
			minmaxfunc = max
			if timestamp[1] == '=':
				if '-' in timestamp:
					# we want 2014-04-10 <= 2014-04 to be True
					def cmpfunc(k, ts):
						return k <= ts or k.startswith(ts)
				else:
					cmpfunc = operator.le
				timestamp = timestamp[2:]
			else:
				cmpfunc = operator.lt
				timestamp = timestamp[1:]
		else:
			minmaxfunc = min
			if timestamp[1] == '=':
				cmpfunc = operator.ge
				timestamp = timestamp[2:]
			else:
				cmpfunc = operator.gt
				timestamp = timestamp[1:]
		timestamp = timestamp404(timestamp)
		return db.limited_endpoint(key, timestamp, cmpfunc, minmaxfunc)
	else:
		timestamp = timestamp404(timestamp)
		return db.get(key, timestamp)


@route('/add', method='POST')
@auth_basic(auth)
def add():
	body = TextIOWrapper(request.body, encoding='utf-8')
	data = Entry(json.load(body))
	if data.user != request.auth[0]:
		abort(401, "Error:  user does not match authentication!")
	result = db.add(data)
	return result


@route('/truncate/<user>/<build>/<timestamp>', method='POST')
@auth_basic(auth)
def truncate(user, build, timestamp):
	if user != request.auth[0]:
		abort(401, "Error:  user does not match authentication!")
	timestamp = timestamp404(timestamp)
	return db.truncate(user + '/' + build, timestamp)


@route('/test/<user>', method='POST')
@auth_basic(auth)
def test(user):
	if user != request.auth[0]:
		abort(401, "Error:  user does not match authentication!")


@route('/list')
def slash_list():
	return sorted(db.keys())


@bottle.error(401)
@bottle.error(404)
@bottle.error(409)
@bottle.error(500)
def error_handler(e):
	res = [e.body]
	if e.exception:
		res.append(repr(e.exception))
	if e.traceback:
		res.append(e.traceback)
	if isinstance(e.exception, AssertionError):
		# Just the message then
		res = [str(e.exception)]
	bottle.response.content_type = 'text/plain'
	return '\n'.join(res).encode('utf-8')


def readauth(filename):
	if not os.path.exists(filename):
		return {}
	d = {}
	with open(filename) as fh:
		for line in fh:
			line = line.strip()
			if not line or line.startswith('#'):  continue
			line = line.split(':')
			assert len(line) == 2, "Parse error in \"" + filename + "\" " +  ':'.join(line)
			d[line[0]] = line[1]
	return d


def jsonify(callback):
	def func(*a, **kw):
		res = callback(*a, **kw)
		if isinstance(res, (bottle.BaseResponse, bottle.BottleException)):
			return res
		bottle.response.content_type = 'text/json'
		return json.dumps(res)
	return func


def main(argv, cfg):
	global authdict, allow_passwordless, db

	parser = ArgumentParser(prog=argv.pop(0))
	parser.add_argument('--path', type=str, default='urd.db',
		help='database directory (can be relative to project directory) (default: urd.db)',
	)
	parser.add_argument('--listen', help='[host]:port or socket path, overrides config')
	parser.add_argument('--allow-passwordless', action='store_true', negation='dont', help='accept any pass for users not in passwd.')
	parser.add_argument('--quiet',              action='store_true', negation='not',  help='less chatty.')
	args = parser.parse_intermixed_args(argv)

	if args.listen is None:
		listen = cfg.urd_listen
		if not listen:
			raise Exception('urd not configured in this project')
	else:
		from accelerator.configfile import resolve_listen
		listen = resolve_listen(args.listen)[0]

	auth_fn = os.path.join(args.path, 'passwd')
	authdict = readauth(auth_fn)
	allow_passwordless = args.allow_passwordless
	if not authdict and not args.allow_passwordless:
		raise Exception(f'No users in {auth_fn!r} and --allow-passwordless not specified.')
	db = DB(args.path, not args.quiet)

	bottle.install(jsonify)

	kw = dict(debug=False, reloader=False, quiet=args.quiet, server=WaitressServer)
	if isinstance(listen, tuple):
		kw['host'], kw['port'] = listen
	else:
		from accelerator.server import check_socket
		check_socket(listen)
		kw['host'] = listen
		kw['port'] = 0
	bottle.run(**kw)
