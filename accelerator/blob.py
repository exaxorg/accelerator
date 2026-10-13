# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2024 Carl Drougge                       #
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

import json
import os
import pathlib
from functools import partial

from accelerator.compat import pickle, iteritems
from accelerator.job import Job, JobWithFile
from accelerator.statmsg import status
from accelerator.types import DotDict, OrderedDict

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
			_backgrounded.clear()


def _pickle_save(variable, filename, temp, _hidden):
	with FileWriteMove(filename, temp, _hidden=_hidden) as fh:
		# use protocol version 4 so all supported versions can read the pickles.
		pickle.dump(variable, fh, 4)

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
def pickle_load(filename='result.pickle', jobid=None, sliceno=None, *, encoding='bytes'):
	filename = _fn(filename, jobid, sliceno)
	with status('Loading ' + filename):
		with open(filename, 'rb') as fh:
			return pickle.load(fh, encoding=encoding)


def json_encode(variable, *, sort_keys=True, as_str=False):
	"""Return variable serialised as json bytes (or str with as_str=True).

	You can pass tuples and sets (saved as lists).

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
		else:
			return e
	variable = typefix(variable)
	res = json.dumps(variable, indent=4, sort_keys=sort_keys)
	if not as_str:
		res = res.encode('ascii')
	return res

def _json_save(variable, filename, sort_keys, _encoder, temp):
	with FileWriteMove(filename, temp) as fh:
		fh.write(_encoder(variable, sort_keys=sort_keys))
		fh.write(b'\n')

def json_save(variable, filename='result.json', sliceno=None, *, sort_keys=True, _encoder=json_encode, temp=False, background=False):
	args = (variable, _fn(filename, None, sliceno), sort_keys, _encoder, temp)
	if background:
		return _BackgroundSavedFile(filename, sliceno, json_load, _json_save, args, temp)
	else:
		_json_save(*args)
		return _SavedFile(filename, sliceno, json_load)

def json_decode(s):
	return json.loads(s, object_pairs_hook=DotDict)

def json_load(filename='result.json', *, jobid=None, sliceno=None):
	filename = _fn(filename, jobid, sliceno)
	with open(filename, 'r', encoding='utf-8') as fh:
		data = fh.read()
	return json_decode(data)


saved_files = {}


class FileWriteMove(object):
	"""with FileWriteMove(name) as fh: ...
	Opens file with a temp name and renames it in place on exit if no
	exception occured. Tries to remove temp file if exception occured.
	"""

	__slots__ = ('filename', 'tmp_filename', 'temp', '_hidden', '_status', 'close', '_open')

	def __init__(self, filename, temp=None, *, _hidden=False):
		from accelerator import g
		job = getattr(g, 'job', None)
		if job: # This is also used outside jobs
			# Normalise filename so it always contains the job path (unless already absolute)
			filename = job.filename(filename)
		self.filename = filename
		self.tmp_filename = '%s.%dtmp' % (filename, os.getpid(),)
		self.temp = temp
		self._hidden = _hidden

	def __enter__(self):
		self._status = status('Saving ' + self.filename)
		self._status.__enter__()
		fh = getattr(self, '_open', open)(self.tmp_filename, 'xb')
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


# Backwards-compatibility
load = pickle_load
save = pickle_save
