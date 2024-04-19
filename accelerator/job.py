# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2024 Carl Drougge                       #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
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
import re

from collections import namedtuple, OrderedDict
from functools import wraps
from pathlib import Path

from accelerator.compat import unicode, PY2, PY3, open, iteritems, FileNotFoundError
from accelerator.error import NoSuchJobError, NoSuchWorkdirError, NoSuchDatasetError, AcceleratorError


# WORKDIRS should live in the Automata class, but only for callers
# (methods read it too, though hopefully only through the functions in this module)

WORKDIRS = {}


def dirnamematcher(name):
	return re.compile(re.escape(name) + r'-[0-9]+$').match


def _assert_is_normrelpath(path, dirtype):
	norm = os.path.normpath(path)
	if (norm != path and norm + '/' != path) or norm.startswith('/'):
		raise AcceleratorError('%r is not a normalised relative path' % (path,))
	if norm == '..' or norm.startswith('../'):
		raise AcceleratorError('%r is above the %s dir' % (path, dirtype))


def _cachedprop(meth):
	@property
	@wraps(meth)
	def wrapper(self):
		if meth.__name__ not in self._cache:
			self._cache[meth.__name__] = meth(self)
		return self._cache[meth.__name__]
	return wrapper

_cache = {}
_nodefault = object()

class Job(unicode):
	"""
	A string that is a jobid, but also has some extra properties:
	.method The job method (can be the "name" when from build or urd).
	.number The job number as an int.
	.workdir The workdir name (the part before -number in the jobid)
	.path The filesystem directory where the job is stored.
	.params setup.json from this job.
	.post post.json from this job.
	.datasets list of Datasets in this job.
	And some functions:
	.withfile a JobWithFile with this job.
	.filename to join .path with a filename.
	.load to load a pickle.
	.json_load to load a json file.
	.open to open a file (like standard open)
	.dataset to get a named Dataset.
	.output to get what the job printed.
	.link_result to put a link in result_directory that points to a file in this job.
	.link_to_here to expose a subjob result in its parent.

	Decays to a (unicode) string when pickled.
	"""

	__slots__ = ('workdir', 'number', '_cache')

	def __new__(cls, jobid, method=None):
		k = (jobid, method)
		if k in _cache:
			return _cache[k]
		obj = unicode.__new__(cls, jobid)
		try:
			obj.workdir, tmp = jobid.rsplit('-', 1)
			obj.number = int(tmp)
		except ValueError:
			raise NoSuchJobError('Not a valid jobid: "%s".' % (jobid,))
		obj._cache = {}
		if method:
			obj._cache['method'] = method
		_cache[k] = obj
		return obj

	@classmethod
	def _create(cls, name, number):
		return Job('%s-%d' % (name, number,))

	@_cachedprop
	def method(self):
		return self.params.method

	@_cachedprop
	def input_directory(self):
		return self.params.get('input_directory', None)

	@_cachedprop
	def is_build(self):
		return self.params.get('is_build', False)

	@_cachedprop
	def parent(self):
		parent = self.params.get('parent')
		return Job(parent) if parent else NoJob

	@_cachedprop
	def build_job(self):
		candidate = self.parent
		while candidate and not candidate.is_build:
			candidate = candidate.parent
		return candidate

	@property
	def path(self):
		if self.workdir not in WORKDIRS:
			raise NoSuchWorkdirError('Not a valid workdir: "%s"' % (self.workdir,))
		return os.path.join(WORKDIRS[self.workdir], self)

	def filename(self, filename, sliceno=None):
		if isinstance(filename, Path):
			filename = str(filename)
		if sliceno is not None:
			filename = '%s.%d' % (filename, sliceno,)
		return os.path.join(self.path, filename)

	def open(self, filename, mode='r', sliceno=None, encoding=None, errors=None):
		assert 'r' in mode, "Don't write to other jobs"
		if 'b' not in mode and encoding is None:
			encoding = 'utf-8'
		return open(self.filename(filename, sliceno), mode, encoding=encoding, errors=errors)

	def files(self, pattern='*'):
		from fnmatch import filter
		try:
			return set(filter(self.post.files, pattern))
		except FileNotFoundError:
			return set()

	def withfile(self, filename, sliced=False, extra=None):
		return JobWithFile(self, filename, sliced, extra)

	@property # usually cached
	def params(self):
		if 'params' in self._cache:
			return self._cache['params']
		from accelerator.extras import job_params
		res = job_params(self)
		if 'endtime' in res:
			# only cache it if the job is finished, so that board does
			# not get stuck with an incomplete job.
			self._cache['params'] = res
		return res

	@_cachedprop
	def version(self):
		# this is self.params.version, but without fully loading the params
		# (unless already loaded).
		if 'params' in self._cache:
			return self._cache['params'].version
		from accelerator.setupfile import load_setup
		return load_setup(self).version

	@_cachedprop
	def post(self):
		from accelerator.extras import job_post
		return job_post(self)

	def load(self, filename='result.pickle', sliceno=None, encoding='bytes', default=_nodefault):
		"""blob.load from this job"""
		from accelerator.extras import pickle_load
		try:
			return pickle_load(self.filename(filename, sliceno), encoding=encoding)
		except FileNotFoundError:
			if default is _nodefault:
				raise
			return default

	def json_load(self, filename='result.json', sliceno=None, unicode_as_utf8bytes=PY2, default=_nodefault):
		from accelerator.extras import json_load
		try:
			return json_load(self.filename(filename, sliceno), unicode_as_utf8bytes=unicode_as_utf8bytes)
		except FileNotFoundError:
			if default is _nodefault:
				raise
			return default

	def dataset(self, name='default'):
		from accelerator.dataset import Dataset
		return Dataset(self, name)

	@_cachedprop
	def datasets(self):
		from accelerator.dataset import job_datasets
		return job_datasets(self)

	def output(self, what=None):
		if what == 'parts':
			as_parts = True
			what = None
		else:
			as_parts = False
		if isinstance(what, int):
			fns = [what]
		else:
			assert what in (None, 'prepare', 'analysis', 'synthesis'), 'Unknown output %r' % (what,)
			if what in (None, 'analysis'):
				fns = list(range(self.params.slices))
				if what is None:
					fns = ['prepare'] + fns + ['synthesis']
			else:
				fns = [what]
		res = OrderedDict()
		for k in fns:
			fn = self.filename('OUTPUT/' + str(k))
			if os.path.exists(fn):
				with open(fn, 'rt', encoding='utf-8', errors='backslashreplace') as fh:
					res[k] = fh.read()
		if as_parts:
			return res
		else:
			return ''.join(res.values())

	def link_result(self, filename='result.pickle', linkname=None):
		"""Put a symlink to filename in result_directory
		Only use this in a build script."""
		from accelerator.g import running
		assert running == 'build', "Only link_result from a build script"
		from accelerator.shell import cfg
		if isinstance(filename, Path):
			filename = str(filename)
		if isinstance(linkname, Path):
			linkname = str(linkname)
		_assert_is_normrelpath(filename, 'job')
		if linkname is None:
			linkname = os.path.basename(filename.rstrip('/'))
		_assert_is_normrelpath(linkname, 'result')
		if linkname.endswith('/'):
			if filename.endswith('/'):
				linkname = linkname.rstrip('/')
			else:
				linkname += os.path.basename(filename)
		source_fn = os.path.join(self.path, filename)
		assert os.path.exists(source_fn), "Filename \"%s\" does not exist in jobdir \"%s\"!" % (filename, self.path)
		result_directory = cfg['result_directory']
		dest_fn = result_directory
		for part in linkname.split('/'):
			if not os.path.exists(dest_fn):
				os.mkdir(dest_fn)
			elif dest_fn != result_directory and os.path.islink(dest_fn):
				raise AcceleratorError("Refusing to create link %r: %r is a symlink" % (linkname, dest_fn))
			dest_fn = os.path.join(dest_fn, part)
		try:
			os.remove(dest_fn + '_')
		except OSError:
			pass
		os.symlink(source_fn, dest_fn + '_')
		os.rename(dest_fn + '_', dest_fn)

	def link_to_here(self, filename='result.pickle'):
		from accelerator.g import job
		src = self.filename(filename)
		assert os.path.exists(src)
		dst = job.filename(filename)
		os.symlink(src, dst)
		job.register_file(dst)

	def chain(self, length=-1, reverse=False, stop_job=None):
		"""Like Dataset.chain but for jobs."""
		if isinstance(stop_job, dict):
			assert len(stop_job) == 1, "Only pass a single stop_job={job: name}"
			stop_job, stop_name = next(iteritems(stop_job))
			if stop_job:
				stop_job = Job(stop_job).params.jobs.get(stop_name)
		chain = []
		current = self
		while length != len(chain) and current and current != stop_job:
			chain.append(current)
			current = current.params.jobs.get('previous')
		if not reverse:
			chain.reverse()
		return chain

	# Look like a string after pickling
	def __reduce__(self):
		return unicode, (unicode(self),)


class CurrentJob(Job):
	"""The currently running job (as passed to the method),
	with extra functions for writing data."""

	__slots__ = ('input_directory',)

	def __new__(cls, jobid, params):
		obj = Job.__new__(cls, jobid, params.method)
		obj._cache['params'] = params
		obj.input_directory = params.input_directory
		return obj

	def finish_early(self, result=None):
		"""Finish job (successfully) without running later stages"""
		from accelerator.launch import _FinishJob
		raise _FinishJob(result)

	def save(self, obj, filename='result.pickle', sliceno=None, temp=None, background=False):
		from accelerator.extras import pickle_save
		return pickle_save(obj, filename, sliceno, temp=temp, background=background)

	def json_save(self, obj, filename='result.json', sliceno=None, sort_keys=True, temp=None, background=False):
		from accelerator.extras import json_save
		return json_save(obj, filename, sliceno, sort_keys=sort_keys, temp=temp, background=background)

	def datasetwriter(self, columns={}, filename=None, hashlabel=None, hashlabel_override=False, caption=None, previous=None, name='default', parent=None, meta_only=False, for_single_slice=None, copy_mode=False, allow_missing_slices=False):
		from accelerator.dataset import DatasetWriter
		return DatasetWriter(columns=columns, filename=filename, hashlabel=hashlabel, hashlabel_override=hashlabel_override, caption=caption, previous=previous, name=name, parent=parent, meta_only=meta_only, for_single_slice=for_single_slice, copy_mode=copy_mode, allow_missing_slices=allow_missing_slices)

	def open(self, filename, mode='r', sliceno=None, encoding=None, errors=None, temp=None):
		"""Mostly like standard open with sliceno and temp,
		but you must use it as context manager
		with job.open(...) as fh:
		and the file will have a temp name until the with block ends.
		"""
		if 'r' in mode:
			return Job.open(self, filename, mode, sliceno, encoding, errors)
		if 'b' not in mode and encoding is None:
			encoding = 'utf-8'
		if PY3 and 'x' not in mode:
			mode = mode.replace('w', 'x')
		def _open(fn, _mode):
			# ignore the passed mode, use the one we have
			return open(fn, mode, encoding=encoding, errors=errors)
		from accelerator.extras import FileWriteMove
		fwm = FileWriteMove(self.filename(filename, sliceno), temp=temp)
		fwm._open = _open
		return fwm

	def register_file(self, filename):
		"""Record a file produced by this job. Normally you would use
		job.open to have this happen automatically, but if the file was
		produced in a way where that is not practical you can use this
		to register it."""
		filename = self.filename(filename)
		assert os.path.exists(filename)
		from accelerator.extras import saved_files
		saved_files[filename] = 0

	def register_files(self, pattern='**/*' if PY3 else '*'):
		"""Bulk register files matching a pattern.
		Tries to exclude internal files automatically.
		Does not register temp-files.
		The default pattern registers everything (recursively, unless python 2).
		Returns which files were registered.
		"""
		from accelerator.extras import saved_files
		from glob import iglob
		pattern = os.path.normpath(pattern)
		assert not pattern.startswith('/')
		assert not pattern.startswith('../')
		forbidden = ('setup.json', 'post.json', 'method.tar.gz',)
		res = set()
		if PY3:
			files = iglob(pattern, recursive=True)
		else:
			# No recursive support on python 2.
			files = iglob(pattern)
		for fn in files:
			if (
				fn in forbidden or
				fn.startswith('DS/') or
				fn.startswith('OUTPUT/') or
				fn.startswith('Analysis.') or
				not os.path.isfile(fn)
			):
				continue
			key = self.filename(fn)
			# Don't override temp-ness of already registered files.
			if key not in saved_files:
				saved_files[key] = False
				res.add(fn)
		return res

	def input_filename(self, *parts):
		return os.path.join(self.input_directory, *parts)

	def open_input(self, filename, mode='r', encoding=None, errors=None):
		assert 'r' in mode, "Don't write to input files"
		if 'b' not in mode and encoding is None:
			encoding = 'utf-8'
		return open(self.input_filename(filename), mode, encoding=encoding, errors=errors)


class NoJob(Job):
	"""
	A empty string that is used for unset job arguments, with some properties
	that may still make sense on an unset job.
	Also provides .load() and .load_json() methods that return None as long
	as no filename or sliceno was specified, and .files() that always returns
	an empty set.
	"""

	__slots__ = ()

	# functions you shouldn't call on this
	filename = link_result = open = params = path = post = withfile = None

	workdir = None
	method = output = ''
	number = version = -1

	def __new__(cls):
		return unicode.__new__(cls, '')

	def dataset(self, name='default'):
		raise NoSuchDatasetError('NoJob has no datasets')

	@property
	def datasets(self):
		from accelerator.dataset import DatasetList
		return DatasetList()

	def files(self, pattern='*'):
		return set()

	def load(self, filename=None, sliceno=None, encoding='bytes', default=_nodefault):
		if default is not _nodefault:
			return default
		if filename is not None or sliceno is not None:
			raise NoSuchJobError('Can not load named / sliced file on <NoJob>')
		return None

	def json_load(self, filename=None, sliceno=None, unicode_as_utf8bytes=PY2, default=_nodefault):
		return self.load(filename, sliceno, default=default)

	@property # so it can return the same instance as all other NoJob things
	def parent(self):
		return NoJob

NoJob = NoJob()


class JobWithFile(namedtuple('JobWithFile', 'job name sliced extra')):
	__slots__ = ()

	def __new__(cls, job, name, sliced=False, extra=None):
		if isinstance(name, Path):
			name = str(name)
		assert not name.startswith('/'), "Specify relative filenames to JobWithFile"
		return tuple.__new__(cls, (Job(job), name, bool(sliced), extra,))

	def filename(self, sliceno=None):
		if sliceno is None:
			assert not self.sliced, "A sliced file requires a sliceno"
		else:
			assert self.sliced, "An unsliced file can not have a sliceno"
		return self.job.filename(self.name, sliceno)

	def load(self, sliceno=None, encoding='bytes', default=_nodefault):
		"""blob.load this file"""
		from accelerator.extras import pickle_load
		try:
			return pickle_load(self.filename(sliceno), encoding=encoding)
		except FileNotFoundError:
			if default is _nodefault:
				raise
			return default

	def json_load(self, sliceno=None, unicode_as_utf8bytes=PY2, default=_nodefault):
		from accelerator.extras import json_load
		try:
			return json_load(self.filename(sliceno), unicode_as_utf8bytes=unicode_as_utf8bytes)
		except FileNotFoundError:
			if default is _nodefault:
				raise
			return default

	def open(self, mode='r', sliceno=None, encoding=None, errors=None):
		return self.job.open(self.name, mode, sliceno, encoding, errors)
