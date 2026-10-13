# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
# Modifications copyright (c) 2019-2026 Carl Drougge                       #
# Modifications copyright (c) 2023-2024 Pablo Correa Gómez                 #
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

import os
import datetime
import pathlib
from traceback import print_exc
from collections import Counter
import sys

from accelerator.compat import izip, iteritems, first_value
from accelerator.compat import num_types

from accelerator.blob import pickle_load
from accelerator.error import AcceleratorError
from accelerator.job import Job, JobWithFile
from accelerator.types import DotDict

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


def job_params(jobid=None, *, default_empty=False):
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

def quote(s):
	"""Quote s unless it looks fine without"""
	s = str(s)
	r = repr(s)
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

class ResultIterMagic(object):
	"""Wrap a ResultIter to give magic merging functionality,
	and so that you get an error if you attempt to use it after it is first
	exhausted. This is to avoid bugs, for example using analysis_res as if
	it was a list.
	"""

	__slots__ = ('_inner', '_reuse_msg', '_exc', '_done', '_started')

	def __init__(self, slices, *, reuse_msg="Attempted to iterate past end of iterator.", exc=Exception):
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

	def merge_auto(self, *, allow_overwrite=None):
		"""Merge values from iterator using magic.
		Currenly supports data that has .update, .itervalues and .iteritems
		methods.
		If value has an .itervalues method the merge continues down to that
		level, otherwise the value will be overwritten by later slices.
		Don't try to use this if all your values don't have the same depth,
		or if you have empty dicts at the last level.

		If allow_overwrite is set to True, when there are keys in dict-like
		objects in multiple slices, the merging might only take the values from
		one of them, without any kind of warranties. This was historical
		behavior, and has a slightly greater performance.

		If allow_overwrite is set to False, no leaf containers allow duplicate
		keys, even for types where it's normally ok (set and Counter).
		"""
		if self._started:
			raise self._exc("Will not merge after iteration started")
		if self._inner._is_tupled:
			return (self._merge_auto_single(it, ix, allow_overwrite) for ix, it in enumerate(self._inner._loaders))
		else:
			return self._merge_auto_single(self, -1, allow_overwrite)

	def _merge_auto_single(self, it, ix, allow_overwrite):
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
		to_check_parent = data
		while hasattr(to_check, "values"):
			if not to_check:
				raise self._exc("Empty value at depth %d (index %d)" % (depth, ix,))
			to_check_parent = to_check
			to_check = first_value(to_check)
			depth += 1
		if hasattr(to_check, "update"): # like a set
			depth += 1
		if allow_overwrite is None and (isinstance(to_check, set) or isinstance(to_check_parent, Counter)):
			allow_overwrite = True
		if not depth:
			raise self._exc("Top level has no .values (index %d)" % (ix,))
		def upd(aggregate, part, level):
			if level == depth:
				if not allow_overwrite:
					for k in part:
						if k in aggregate:
							raise self._exc("Duplicate key %r (index %d)" % (k, ix,))
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


# The modern python int() constructor accepts a lot of stuff we often don't want.
# (Like _ and ３.)
def ascii_int(s, *, whitespace_ok=False):
	if whitespace_ok:
		s = s.strip(' \t\n\r\f\v')
	if all(c in '0123456789-' for c in s):
		return int(s, 10)
	else:
		raise ValueError('invalid literal for int() with base 10: {s!r}')


def fmttime(t, short=False):
	if t is None:
		return None
	if short:
		fmts = ['%.0fs', '%.1fm', '%.2fh', '%.2fd']
	else:
		fmts = ['%.1f seconds', '%.1f minutes', '%.2f hours', '%.2f days']
	sizes = [60, 60, 24, 1]
	for fmt, size in zip(fmts, sizes):
		if t < size * 2:
			break
		t /= size
	res = fmt % (t,)
	if res == '1.0 seconds':
		return '1 second'
	return res
