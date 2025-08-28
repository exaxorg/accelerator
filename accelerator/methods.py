# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2024 Carl Drougge                       #
# Modifications copyright (c) 2020-2021 Anders Berkeman                    #
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

import os
import sys
import datetime
import pathlib
from glob import glob
from collections import defaultdict, OrderedDict
from importlib import import_module

from accelerator.compat import iteritems, itervalues, first_value
from accelerator.compat import NoneType, monotonic

from accelerator.colourwrapper import colour
from accelerator.error import AcceleratorError
from accelerator.extras import DotDict, _OptionString, OptionEnum, OptionDefault, RequiredOption
from accelerator.runner import new_runners
from accelerator.setupfile import _sorted_set

from accelerator import __version__ as ax_version

class MethodLoadException(AcceleratorError):
	__slots__ = ('module_list',)
	def __init__(self, lst):
		AcceleratorError.__init__(self, 'Failed to load ' + ', '.join(lst))
		self.module_list = lst


# Collect information on methods
class Methods(object):
	def __init__(self, server_config):
		package_list = server_config['method_directories']
		# read all methods
		self.db = {}
		for package, autodiscover in package_list.items():
			package_dir = self._check_package(package)
			db_ = read_methods_conf(package_dir, autodiscover)
			for method, meta in db_.items():
				if method in self.db:
					raise AcceleratorError(
						"Method \"%s\" defined both in \"%s\" and \"%s\"!" %
						(method, package, self.db[method]['package'],)
					)
				self.db[method] = DotDict(package=package, **meta)
		t0 = monotonic()
		per_runner = defaultdict(list)
		for key, val in iteritems(self.db):
			package = val['package']
			per_runner[val['version']].append((package, key))
		self.runners = new_runners(server_config, set(per_runner))
		warnings = []
		failed = []
		self.hash = {}
		self.params = {}
		self.descriptions = {}
		self.typing = {}
		for version, data in iteritems(per_runner):
			runner = self.runners.get(version)
			if not runner:
				msg = '%%s.%%s (unconfigured interpreter %s)' % (version)
				failed.extend(msg % t for t in sorted(data))
				continue
			v = runner.get_ax_version()
			if v != ax_version:
				if runner.python == sys.executable:
					raise AcceleratorError(f"Server is using accelerator {ax_version} but {v} is currently installed, please restart server.")
				else:
					print(f"WARNING: Server is using accelerator {ax_version} but runner {version!r} is using accelerator {v}.")
			w, f, h, p, d = runner.load_methods(package_list, data)
			warnings.extend(w)
			failed.extend(f)
			self.hash.update(h)
			self.params.update(p)
			self.descriptions.update(d)
		for key, params in iteritems(self.params):
			self.typing[key] = options2typing(key, params.options)
			params.defaults = params2defaults(params)
			params.required = options2required(params.options)
		def prt(a, prefix):
			maxlen = (max(len(e) for e in a) + len(prefix))
			line = '=' * maxlen
			print()
			print(line)
			for e in sorted(a):
				msg = prefix + e
				print(msg + ' ' * (maxlen - len(msg)))
			print(line)
			print()
		if warnings:
			prt(warnings, 'WARNING: ')
		if failed:
			print(colour.WHITEBG + colour.RED + colour.BOLD)
			prt(failed, 'FAILED to import ')
			print(colour.RESET)
			raise MethodLoadException(failed)
		print("Updated %d methods on %d runners in %.1f seconds" % (
		      len(self.hash), len(per_runner), monotonic() - t0,
		     ))

	def _check_package(self, package):
		try:
			package_mod = import_module(package)
			if not hasattr(package_mod, "__file__"):
				raise ImportError("no __file__")
		except ImportError:
			raise AcceleratorError(f"Failed to import {package}, maybe missing __init__.py?")
		if not package_mod.__file__:
			raise AcceleratorError(f"{package} has no __file__, maybe missing __init__.py?")
		return os.path.dirname(package_mod.__file__)


	def params2optset(self, params):
		optset = set()
		for optmethod, method_params in iteritems(params):
			for group, d in iteritems(method_params):
				filled_in = dict(self.params[optmethod].defaults[group])
				filled_in.update(d)
				for optname, optval in iteritems(filled_in):
					optset.add(f'{optmethod} {group}-{optname} {_reprify(optval)}')
		return optset

def _reprify(o):
	if isinstance(o, OptionDefault):
		o = o.default
	if isinstance(o, (bytes, str, int, float, bool, NoneType)):
		return repr(o)
	if isinstance(o, set):
		return f"[{', '.join(map(_reprify, _sorted_set(o)))}]"
	if isinstance(o, (list, tuple)):
		return f"[{', '.join(map(_reprify, o))}]"
	if isinstance(o, dict):
		assert isinstance(o, OrderedDict)
		return '{%s}' % (', '.join('%s: %s' % (_reprify(k), _reprify(v),) for k, v in iteritems(o)),)
	if isinstance(o, (datetime.datetime, datetime.date, datetime.time, datetime.timedelta, pathlib.Path, pathlib.PurePath)):
		return str(o)
	raise AcceleratorError(f'Unhandled {type(o)} in dependency resolution')



def params2defaults(params):
	d = DotDict()
	for key in ('datasets', 'jobs',):
		r = {}
		for v in params[key]:
			if isinstance(v, list):
				r[v[0]] = []
			else:
				r[v] = None
		d[key] = r
	def fixup(item):
		if isinstance(item, dict):
			items = iteritems(item)
			if not isinstance(item, OrderedDict):
				items = sorted(items)
			d = OrderedDict((k, fixup(v)) for k, v in items)
			if len(d) == 1 and first_value(d) is None and first_value(item) is not None:
				d.clear()
			return d
		if isinstance(item, (list, tuple, set,)):
			l = [fixup(v) for v in item]
			if l == [None] and list(item) != [None]:
				l = []
			return type(item)(l)
		if isinstance(item, (type, OptionEnum)):
			return None
		assert isinstance(item, (bytes, str, int, float, bool, OptionEnum, NoneType, datetime.datetime, datetime.date, datetime.time, datetime.timedelta, pathlib.PosixPath, pathlib.PurePosixPath,)), type(item)
		return item
	def fixup0(item):
		if isinstance(item, RequiredOption):
			item = item.value
		if isinstance(item, OptionDefault):
			item = item.default
		return fixup(item)
	d.options = {k: fixup0(v) for k, v in iteritems(params.options)}
	return d


def options2required(options):
	res = set()
	def chk(key, value):
		if isinstance(value, (_OptionString, RequiredOption)):
			res.add(key)
		elif isinstance(value, OptionEnum):
			if None not in value._valid:
				res.add(key)
		elif isinstance(value, dict):
			for v in itervalues(value):
				chk(key, v)
		elif isinstance(value, (list, tuple, set,)):
			for v in value:
				chk(key, v)
	for key, value in iteritems(options):
		chk(key, value)
	return res


def options2typing(method, options):
	from accelerator.job import JobWithFile
	res = {}
	def value2spec(value):
		if isinstance(value, list):
			if not value:
				return
			fmt = '[%s]'
			value = value[0]
		else:
			fmt = '%s'
		typ = None
		if value is JobWithFile or isinstance(value, JobWithFile):
			typ = 'JobWithFile'
		elif isinstance(value, set):
			typ = 'set'
		elif value in (datetime.datetime, datetime.date, datetime.time, datetime.timedelta,):
			typ = value.__name__
		elif value in (pathlib.Path, pathlib.PosixPath,) or isinstance(value, pathlib.PosixPath):
			typ = 'Path'
		elif value in (pathlib.PurePath, pathlib.PurePosixPath,) or isinstance(value, pathlib.PurePosixPath):
			typ = 'PurePath'
		elif isinstance(value, (datetime.datetime, datetime.date, datetime.time, datetime.timedelta,)):
			typ = type(value).__name__
		if typ:
			return fmt % (typ,)
	def collect(key, value, path=''):
		path = f"{path}/{key}"
		if isinstance(value, dict):
			for v in itervalues(value):
				collect('*', v, path)
			return
		spec = value2spec(value)
		assert res.get(path, spec) == spec, f'Method {method} has incompatible types in options{path}'
		res[path] = spec
	for k, v in iteritems(options):
		collect(k, v)
	# reverse by key len, so something inside a dict always comes before
	# the dict itself. (We don't currently have any dict-like types, but we
	# might later.)
	return sorted(([k[1:], v] for k, v in iteritems(res) if v), key=lambda i: -len(i[0]))


def read_methods_conf(dirname, autodiscover):
	""" read and parse the methods.conf file """
	db = {}
	if autodiscover:
		methods = glob(os.path.join(dirname, 'a_*.py'))
		for method in methods:
			if method not in db:
				db[os.path.basename(method)[2:-3]] = DotDict(version='DEFAULT')
	filename = os.path.join(dirname, 'methods.conf')
	if autodiscover and not os.path.exists(filename):
		return db
	with open(filename) as fh:
		for lineno, line in enumerate(fh, 1):
			data = line.split('#')[0].split()
			if not data:
				continue
			method = data.pop(0)
			if autodiscover and (method not in db):
				# in auto-discover, anything in methods.conf goes
				continue
			try:
				version = data.pop(0)
			except IndexError:
				version = 'DEFAULT'
			if data:
				raise AcceleratorError(f'Trailing garbage on {filename}:{lineno}: {line}')
			db[method] = DotDict(version=version)
	return db
