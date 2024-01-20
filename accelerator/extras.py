# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
# Modifications copyright (c) 2019-2024 Carl Drougge                       #
# Modifications copyright (c) 2023 Pablo Correa Gómez                      #
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
from __future__ import division

import os
import datetime
import json
import pathlib
from traceback import print_exc
from collections import OrderedDict
from functools import partial
import sys

from accelerator.compat import PY2, PY3, pickle, izip, iteritems, first_value
from accelerator.compat import num_types, uni, unicode, str_types

from accelerator.error import AcceleratorError
from accelerator.job import Job, JobWithFile
from accelerator.statmsg import status

def _fn(filename, jobid, sliceno):
	if isinstance(filename, pathlib.Path):
		filename = str(filename)
	if filename.startswith('/'):
		assert not jobid, "Don't specify full path (%r) and jobid (%s)." % (filename, jobid,)
		assert not sliceno, "Don't specify full path (%r) and sliceno." % (filename,)
	elif jobid:
		filename = Job(jobid).filename(filename, sliceno)
	elif sliceno is not None:
		filename = '%s.%d' % (filename, sliceno,)
	return filename

def _type_list_or(v, t, falseval, listtype):
	if isinstance(v, list):
		return listtype(t(v) if v else falseval for v in v)
	elif v:
		return t(v)
	else:
		return falseval

def _job_params(jobid):
	from accelerator.setupfile import load_setup
	d = load_setup(jobid)
	_apply_typing(d.options, d.get('_typing', ()))
	return d


# _SavedFile isn't really intended to pickle safely, so only allow it
# in the automatic pickling of analysis results.
_SavedFile_allow_pickle = False

class _SavedFile(object):
	__slots__ = ('_filename', '_sliceno', '_loader',)

	def __init__(self, filename, sliceno, loader):
		if isinstance(filename, pathlib.Path):
			filename = str(filename)
		self._filename = filename
		self._sliceno = sliceno
		self._loader = loader

	def wait(self):
		pass

	def load(self):
		self.wait()
		return self._loader(self._filename, sliceno=self._sliceno)

	@property
	def filename(self):
		return _fn(self._filename, None, self._sliceno)

	@property
	def path(self):
		if self._filename.startswith('/'):
			job = None
		else:
			from accelerator import g
			job = g.job
		return _fn(self._filename, job, self._sliceno)

	def jobwithfile(self, extra=None):
		from accelerator import g
		return JobWithFile(g.job, self._filename, self._sliceno is not None, extra)

	def remove(self):
		# mark file as temp, so it will be deleted later (unless --keep-temp-files)
		saved_files[self.filename] = True

	def __getstate__(self):
		if _SavedFile_allow_pickle:
			return self._filename, self._sliceno, self._loader
		else:
			raise TypeError('Cannot pickle _SavedFile')

	def __setstate__(self, state):
		self._filename, self._sliceno, self._loader = state


_backgrounded = []

class _BackgroundSavedFile(_SavedFile):
	__slots__ = ('_ok', '_process',)

	def __init__(self, filename, sliceno, loader, saver, args, temp, hidden=False):
		_SavedFile.__init__(self, filename, sliceno, loader)
		if hidden:
			self._ok = bool # dummy function
		else:
			self._ok = partial(saved_files.__setitem__, self.filename, temp)
		from accelerator.mp import SimplifiedProcess
		self._process = SimplifiedProcess(target=self._run, args=(saver, args,))
		_backgrounded.append(self)

	def _run(self, func, args):
		from accelerator import g
		g.running = 'server' # Hack to disable status messages from this process
		func(*args)

	def wait(self):
		if self._process:
			with status("Waiting for background save of " + self.filename):
				self._wait()

	def _wait(self):
		if self._process:
			self._process.join()
			rc = self._process.exitcode
			self._process = None
			if rc:
				raise IOError('Failed to save ' + self.filename)
			self._ok()

	def __setstate__(self, state):
		_SavedFile.__setstate__(self, state)
		self._process = None

def _backgrounded_wait():
	if _backgrounded:
		with status("Waiting for background save(s)"):
			for bs in _backgrounded:
				bs._wait()
			_backgrounded[:] = ()


def job_params(jobid=None, default_empty=False):
	if default_empty and not jobid:
		return DotDict(
			options=DotDict(),
			datasets=DotDict(),
			jobs=DotDict(),
		)
	from accelerator.dataset import Dataset, NoDataset, DatasetList
	from accelerator.job import Job, NoJob
	from accelerator.build import JobList
	d = _job_params(jobid)
	d.datasets = DotDict({k: _type_list_or(v, Dataset, NoDataset, DatasetList) for k, v in d.datasets.items()})
	d.jobs = DotDict({k: _type_list_or(v, Job, NoJob, JobList) for k, v in d.jobs.items()})
	d.jobid = Job(d.jobid)
	return d

def job_post(jobid):
	job = Job(jobid)
	d = job.json_load('post.json')
	version = d.get('version', 0)
	if version == 0:
		prefix = job.path + '/'
		d.files = sorted(fn[len(prefix):] if fn.startswith(prefix) else fn for fn in d.files)
		version = 1
	if version != 1:
		raise AcceleratorError("Don't know how to load post.json version %d (in %s)" % (d.version, jobid,))
	return d

def _pickle_save(variable, filename, temp, _hidden):
	with FileWriteMove(filename, temp, _hidden=_hidden) as fh:
		# use protocol version 2 so python2 can read the pickles too.
		pickle.dump(variable, fh, 2)

def pickle_save(variable, filename='result.pickle', sliceno=None, temp=None, background=False, _hidden=False):
	args = (variable, _fn(filename, None, sliceno), temp, _hidden)
	if background:
		return _BackgroundSavedFile(filename, sliceno, pickle_load, _pickle_save, args, temp, _hidden)
	else:
		_pickle_save(*args)
		return _SavedFile(filename, sliceno, pickle_load)

# default to encoding='bytes' because datetime.* (and probably other types
# too) saved in python 2 fail to unpickle in python 3 otherwise. (Official
# default is 'ascii', which is pretty terrible too.)
def pickle_load(filename='result.pickle', jobid=None, sliceno=None, encoding='bytes'):
	filename = _fn(filename, jobid, sliceno)
	with status('Loading ' + filename):
		with open(filename, 'rb') as fh:
			if PY3:
				return pickle.load(fh, encoding=encoding)
			else:
				return pickle.load(fh)


def json_encode(variable, sort_keys=True, as_str=False):
	"""Return variable serialised as json bytes (or str with as_str=True).

	You can pass tuples and sets (saved as lists).
	On py2 you can also pass bytes that will be passed through compat.uni.

	If you set sort_keys=False you can use OrderedDict to get whatever
	order you like.
	"""
	# make sure to evaluate sort_keys only once, for test_job_save_background
	sort_keys = bool(sort_keys)
	if sort_keys:
		dict_type = dict
	else:
		dict_type = OrderedDict
	def typefix(e):
		if isinstance(e, dict):
			return dict_type((typefix(k), typefix(v)) for k, v in iteritems(e))
		elif isinstance(e, (list, tuple, set,)):
			return [typefix(v) for v in e]
		elif PY2 and isinstance(e, bytes):
			return uni(e)
		else:
			return e
	variable = typefix(variable)
	res = json.dumps(variable, indent=4, sort_keys=sort_keys)
	if PY3 and not as_str:
		res = res.encode('ascii')
	return res

def _json_save(variable, filename, sort_keys, _encoder, temp):
	with FileWriteMove(filename, temp) as fh:
		fh.write(_encoder(variable, sort_keys=sort_keys))
		fh.write(b'\n')

def json_save(variable, filename='result.json', sliceno=None, sort_keys=True, _encoder=json_encode, temp=False, background=False):
	args = (variable, _fn(filename, None, sliceno), sort_keys, _encoder, temp)
	if background:
		return _BackgroundSavedFile(filename, sliceno, json_load, _json_save, args, temp)
	else:
		_json_save(*args)
		return _SavedFile(filename, sliceno, json_load)

def _unicode_as_utf8bytes(obj):
	if isinstance(obj, unicode):
		return obj.encode('utf-8')
	elif isinstance(obj, dict):
		return DotDict((_unicode_as_utf8bytes(k), _unicode_as_utf8bytes(v)) for k, v in iteritems(obj))
	elif isinstance(obj, list):
		return [_unicode_as_utf8bytes(v) for v in obj]
	else:
		return obj

def json_decode(s, unicode_as_utf8bytes=PY2):
	if unicode_as_utf8bytes:
		return _unicode_as_utf8bytes(json.loads(s, object_pairs_hook=DotDict))
	else:
		return json.loads(s, object_pairs_hook=DotDict)

def json_load(filename='result.json', jobid=None, sliceno=None, unicode_as_utf8bytes=PY2):
	filename = _fn(filename, jobid, sliceno)
	if PY3:
		with open(filename, 'r', encoding='utf-8') as fh:
			data = fh.read()
	else:
		with open(filename, 'rb') as fh:
			data = fh.read()
	return json_decode(data, unicode_as_utf8bytes)


def quote(s):
	"""Quote s unless it looks fine without"""
	s = unicode(s)
	r = repr(s)
	if PY2:
		# remove leading u
		r = r[1:]
	if s and len(s) + 2 == len(r) and not any(c.isspace() for c in s):
		return s
	else:
		return r


def debug_print_options(options, title=''):
	print('-' * 53)
	if title:
		print('-', title)
		print('-' * 53)
	max_k = max(len(str(k)) for k in options)
	for key, val in sorted(options.items()):
		print("%s = %r" % (str(key).ljust(max_k), val))
	print('-' * 53)


def stackup():
	"""Returns (filename, lineno) for the first caller not in the accelerator dir."""

	from inspect import stack
	blacklist = os.path.dirname(__file__)
	for stk in stack()[1:]:
		if os.path.dirname(stk[1]) != blacklist:
			return stk[1], stk[2]
	return '?', -1

saved_files = {}

class FileWriteMove(object):
	"""with FileWriteMove(name) as fh: ...
	Opens file with a temp name and renames it in place on exit if no
	exception occured. Tries to remove temp file if exception occured.
	"""

	__slots__ = ('filename', 'tmp_filename', 'temp', '_hidden', '_status', 'close', '_open')

	def __init__(self, filename, temp=None, _hidden=False):
		self.filename = filename
		self.tmp_filename = '%s.%dtmp' % (filename, os.getpid(),)
		self.temp = temp
		self._hidden = _hidden

	def __enter__(self):
		self._status = status('Saving ' + self.filename)
		self._status.__enter__()
		# stupid python3 feels that w and x are exclusive, while python2 requires both.
		fh = getattr(self, '_open', open)(self.tmp_filename, 'xb' if PY3 else 'wbx')
		self.close = fh.close
		return fh
	def __exit__(self, e_type, e_value, e_tb):
		self._status.__exit__(None, None, None)
		self.close()
		if e_type is None:
			os.rename(self.tmp_filename, self.filename)
			if not self._hidden:
				saved_files[self.filename] = self.temp
		else:
			try:
				os.unlink(self.tmp_filename)
			except Exception:
				print_exc(file=sys.stderr)

class ResultIter(object):

	__slots__ = ('_slices', '_is_tupled', '_loaders', '_tupled')

	def __init__(self, slices):
		slices = range(slices)
		self._slices = iter(slices)
		tuple_len = pickle_load("Analysis.tuple")
		if tuple_len is False:
			self._is_tupled = False
		else:
			self._is_tupled = True
			self._loaders = [self._loader(ix, iter(slices)) for ix in range(tuple_len)]
			self._tupled = izip(*self._loaders)
	def __iter__(self):
		return self
	def _loader(self, ix, slices):
		for sliceno in slices:
			yield pickle_load("Analysis.%d." % (ix,), sliceno=sliceno)
	def __next__(self):
		if self._is_tupled:
			return next(self._tupled)
		else:
			return pickle_load("Analysis.", sliceno=next(self._slices))
	next = __next__

class ResultIterMagic(object):
	"""Wrap a ResultIter to give magic merging functionality,
	and so that you get an error if you attempt to use it after it is first
	exhausted. This is to avoid bugs, for example using analysis_res as if
	it was a list.
	"""

	__slots__ = ('_inner', '_reuse_msg', '_exc', '_done', '_started')

	def __init__(self, slices, reuse_msg="Attempted to iterate past end of iterator.", exc=Exception):
		self._inner = ResultIter(slices)
		self._reuse_msg = reuse_msg
		self._exc = exc
		self._done = False
		self._started = False

	def __iter__(self):
		return self

	def __next__(self):
		try:
			self._started = True
			item = next(self._inner)
		except StopIteration:
			if self._done:
				raise self._exc(self._reuse_msg)
			else:
				self._done = True
				raise
		return item
	next = __next__

	def merge_auto(self):
		"""Merge values from iterator using magic.
		Currenly supports data that has .update, .itervalues and .iteritems
		methods.
		If value has an .itervalues method the merge continues down to that
		level, otherwise the value will be overwritten by later slices.
		Don't try to use this if all your values don't have the same depth,
		or if you have empty dicts at the last level.
		"""
		if self._started:
			raise self._exc("Will not merge after iteration started")
		if self._inner._is_tupled:
			return (self._merge_auto_single(it, ix) for ix, it in enumerate(self._inner._loaders))
		else:
			return self._merge_auto_single(self, -1)

	def _merge_auto_single(self, it, ix):
		# find a non-empty one, so we can look at the data in it
		data = next(it)
		if isinstance(data, num_types):
			# special case for when you have something like (count, dict)
			return sum(it, data)
		if isinstance(data, list):
			for part in it:
				data.extend(part)
			return data
		while not data:
			try:
				data = next(it)
			except StopIteration:
				# All were empty, return last one
				return data
		depth = 0
		to_check = data
		while hasattr(to_check, "values"):
			if not to_check:
				raise self._exc("Empty value at depth %d (index %d)" % (depth, ix,))
			to_check = first_value(to_check)
			depth += 1
		if hasattr(to_check, "update"): # like a set
			depth += 1
		if not depth:
			raise self._exc("Top level has no .values (index %d)" % (ix,))
		def upd(aggregate, part, level):
			if level == depth:
				aggregate.update(part)
			else:
				for k, v in iteritems(part):
					if k in aggregate:
						upd(aggregate[k], v, level + 1)
					else:
						aggregate[k] = v
		for part in it:
			upd(data, part, 1)
		return data


class DotDict(OrderedDict):
	"""Like an OrderedDict, but with d.foo as well as d['foo'].
	(Names beginning with _ will have to use d['_foo'] syntax.)
	The normal dict.f (get, items, ...) still return the functions.
	"""

	__slots__ = () # all the .stuff is actually items, not attributes

	# this is a workaround for older versions not inheriting from OrderedDict.
	# without this, unpickling of pickles that were not ordered will fail.
	# it throws the arguments away, as they will be passed to __init__ too
	# anyway. (Or set as items, in the pickle case.)
	def __new__(cls, other=(), **kw):
		obj = OrderedDict.__new__(cls)
		OrderedDict.__init__(obj)
		return obj

	def __getattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		try:
			return self[name]
		except KeyError:
			pass # raise new error outside this except clause
		raise AttributeError(name)

	def __setattr__(self, name, value):
		# if using the python implementation of OrderedDict (as python2 does)
		# this is needed. don't worry about __slots__, it won't apply in that
		# case, and __getattr__ is not needed as it falls through automatically.
		if name.startswith('_OrderedDict__'):
			return OrderedDict.__setattr__(self, name, value)
		if name[0] == "_":
			raise AttributeError(name)
		self[name] = value

	def __delattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		del self[name]

class _ListTypePreserver(list):
	"""Base class to inherit from in list subclasses that want their custom type preserved when slicing."""

	__slots__ = ()

	def __getslice__(self, i, j):
		return self[slice(i, j)]

	def __getitem__(self, item):
		if isinstance(item, slice):
			return self.__class__(list.__getitem__(self, item))
		else:
			return list.__getitem__(self, item)

	def __add__(self, other):
		if not isinstance(other, list):
			return NotImplemented
		return self.__class__(list.__add__(self, other))

	def __radd__(self, other):
		if not isinstance(other, list):
			return NotImplemented
		return self.__class__(list.__add__(other, self))

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, list.__repr__(self))

class OptionEnumValue(str):

	if PY3: # python2 doesn't support slots on str subclasses
		__slots__ = ('_valid', '_prefixes')

	@staticmethod
	def _mktype(name, valid, prefixes):
		return type('OptionEnumValue' + name, (OptionEnumValue,), {'_valid': valid, '_prefixes': prefixes})

	# be picklable
	def __reduce__(self):
		return _OptionEnumValue_restore, (self.__class__.__name__[15:], str(self), self._valid, self._prefixes)

def _OptionEnumValue_restore(name, value, valid, prefixes):
	return OptionEnumValue._mktype(name, valid, prefixes)(value)

class OptionEnum(object):
	"""A little like Enum in python34, but string-like.
	(For JSONable method option enums.)

	>>> foo = OptionEnum('a b c*')
	>>> foo.a
	'a'
	>>> foo.a == 'a'
	True
	>>> foo.a == foo['a']
	True
	>>> isinstance(foo.a, OptionEnumValue)
	True
	>>> isinstance(foo['a'], OptionEnumValue)
	True
	>>> foo['cde'] == 'cde'
	True
	>>> foo['abc']
	Traceback (most recent call last):
	...
	KeyError: 'abc'

	Pass either foo (for a default of None) or one of the members
	as the value in options{}. You get a string back, which compares
	equal to the member of the same name.

	Set none_ok if you accept None as the value.

	If a value ends in * that matches all endings. You can only access
	these as foo['cde'] (for use in options{}).
	"""

	__slots__ = ('_values', '_valid', '_prefixes', '_sub')

	def __new__(cls, values, none_ok=False):
		if isinstance(values, str_types):
			values = values.replace(',', ' ').split()
		values = list(values)
		if PY2:
			values = [v.encode('utf-8') if isinstance(v, unicode) else v for v in values]
		valid = set(values)
		prefixes = []
		for v in values:
			if v.endswith('*'):
				prefixes.append(v[:-1])
		if none_ok:
			valid.add(None)
		name = ''.join(v.title() for v in values)
		sub = OptionEnumValue._mktype(name, valid, prefixes)
		d = {}
		for value in values:
			d[value] = sub(value)
		d['_values'] = values
		d['_valid'] = valid
		d['_prefixes'] = prefixes
		d['_sub'] = sub
		return object.__new__(type('OptionEnum' + name, (cls,), d))
	def __getitem__(self, name):
		try:
			return getattr(self, name)
		except AttributeError:
			for cand_prefix in self._prefixes:
				if name.startswith(cand_prefix):
					return self._sub(name)
			raise KeyError(name)
	# be picklable
	def __reduce__(self):
		return OptionEnum, (self._values, None in self._valid)

class _OptionString(str):
	"""Marker value to specify in options{} for requiring a non-empty string.
	You can use plain OptionString, or you can use OptionString('example'),
	without making 'example' the default.
	"""

	__slots__ = ()

	def __call__(self, example):
		return _OptionString(example)
	def __repr__(self):
		if self:
			return 'OptionString(%r)' % (str(self),)
		else:
			return 'OptionString'
OptionString = _OptionString('')

class RequiredOption(object):
	"""Specify that this option is mandatory (that the caller must specify a value).
	None is accepted as a specified value if you pass none_ok=True.
	"""

	__slots__ = ('value', 'none_ok')

	def __init__(self, value, none_ok=False):
		self.value = value
		self.none_ok = none_ok

class OptionDefault(object):
	"""Default selection for complexly typed options.
	foo={'bar': OptionEnum(...)} is a mandatory option.
	foo=OptionDefault({'bar': OptionEnum(...)}) isn't.
	(Default None unless specified.)
	"""

	__slots__ = ('value', 'default')

	def __init__(self, value, default=None):
		self.value = value
		self.default = default

typing_conv = dict(
	set=set,
	JobWithFile=lambda a: JobWithFile(*a),
	datetime=lambda a: datetime.datetime(*a),
	date=lambda a: datetime.date(*a[:3]),
	Path=lambda a: pathlib.PosixPath(a),
	PurePath=lambda a: pathlib.PurePosixPath(a),
	time=lambda a: datetime.time(*a[3:]),
	timedelta=lambda a: datetime.timedelta(seconds=a),
)

def _mklist(t):
	def make(lst):
		return [t(e) for e in lst]
	return make

def _apply_typing(options, tl):
	for k, t in tl:
		if t.startswith('['):
			assert t.endswith(']')
			t = _mklist(typing_conv[t[1:-1]])
		else:
			t = typing_conv[t]
		d = options
		k = k.split('/')
		for kk in k[:-1]:
			d = d[kk]
		k = k[-1]
		if k == '*':
			for k, v in d.items():
				d[k] = None if v is None else t(v)
		else:
			v = d[k]
			if v is not None:
				v = t(v)
			d[k] = v
