# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2021 Anders Berkeman                    #
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

import re
import os
import shlex

from accelerator.compat import url_quote_more

from accelerator.extras import DotDict


_re_var = re.compile(r'(?<!\\)\$\{([^\}=]*)(?:=([^\}]*))?\}')
def interpolate(s):
	"""Replace ${FOO=BAR} with os.environ.get('FOO', 'BAR')
	(just ${FOO} is of course also supported, but not $FOO)"""
	return _re_var.subn(lambda m: os.environ.get(m.group(1), m.group(2)), s)[0]


class HostPortTuple(tuple):
	__slots__ = ()
	def __str__(self):
		return "http://%s:%d" % self

def resolve_listen(listen):
	if listen.startswith('http://'):
		tmp = listen[7:].rstrip('/')
		if '/' not in tmp and ':' in tmp:
			listen = tmp
	if '/' not in listen and ':' in listen:
		hostname, port = listen.rsplit(':', 1)
		listen = HostPortTuple((hostname or 'localhost', int(port),))
		url = 'http://%s:%d' % listen
	else:
		url = None
	return listen, url

def fixup_listen(project_directory, listen):
	listen, url = listen
	if not isinstance(listen, tuple):
		socket = os.path.join(project_directory, listen)
		url = 'unixhttp://' + url_quote_more(os.path.realpath(socket))
	return listen, url


def load_config(filename):
	from accelerator.error import UserError

	multivalued = {'workdirs', 'method packages', 'interpreters'}
	required = {'slices', 'workdirs', 'method packages'}
	known = {'target workdir', 'listen', 'urd', 'board listen', 'result directory', 'input directory', 'project directory'} | required | multivalued
	cfg = {key: [] for key in multivalued}

	def fixpath(fn, realpath=True):
		# convert relative path to absolute wrt location of config file
		p = os.path.join(project_directory, fn)
		if realpath:
			p = os.path.realpath(p)
		else:
			p = os.path.normpath(p)
		return p

	class _E(Exception):
		__slots__ = ()
	def parse_package(val):
		if len(val) == 2:
			if val[1] != 'auto-discover':
				raise _E(f'Either no option or "auto-discover" for package {val[0]!r}.')
			else:
				return (val[0], True)
		return (val[0], False)
	def parse_workdir(val):
		return val[0], fixpath(val[1])
	def parse_interpreter(val):
		return val[0], fixpath(val[1], False)
	def check_interpreter(val):
		if val[0] == 'DEFAULT':
			raise _E("Don't override DEFAULT interpreter")
		if not os.path.isfile(val[1]):
			raise _E(f'{val!r} does not exist')
	def check_workdirs(val):
		name, path = val
		if val in cfg['workdirs']:
			# The exact same thing was repeated.
			# This is allowed, because you want to be able to have something like
			#    ${USER} /workdirs/${USER}
			#    foo     /workdirs/foo
			#    bar     /workdirs/bar
			# in your config, and have it still work for the foo and bar users.
			return
		if name in (v[0] for v in cfg['workdirs']):
			raise _E(f'Workdir {name} redefined')
		if path in (v[1] for v in cfg['workdirs']):
			raise _E(f'Workdir path {path!r} re-used')
		if project_directory == path or project_directory.startswith(path + '/'):
			raise _E(f'project directory ({project_directory!r}) is under workdir {name} ({path!r})')

	def resolve_urd(val):
		orig_val = val
		is_local = (val[0] == 'local')
		if val[0] in ('local', 'remote'):
			if len(val) == 1:
				val = ['.socket.dir/urd']
			else:
				val = [val[1]]
		if len(val) != 1:
			raise _E(f"urd takes 1 or 2 values (expected {' '.join(parsers['urd'][0])}, got {orig_val!r})")
		return is_local, resolve_listen(val[0])

	parsers = {
		'slices': (['count'], int),
		'workdirs': (['name', 'path'], parse_workdir),
		'interpreters': (['name', 'path'], parse_interpreter),
		'listen': (['path or [host]:port'], resolve_listen),
		'urd': (['["local" or "remote"] and/or', '[path or [host]:port]'], resolve_urd), # special cased
		'board listen': (['path or [host]:port'], resolve_listen),
		'input directory': (['path'], fixpath),
		'result directory': (['path'], fixpath),
		'method packages': (['package', '[auto-discover]'], parse_package),
	}
	checkers = dict(
		interpreter=check_interpreter,
		workdirs=check_workdirs,
	)

	with open(filename, 'r', encoding='utf-8') as fh:
		lines = list(enumerate(fh, 1))
	def parse(handle):
		key = None
		for n, line in lines:
			lineno[0] = n
			line_stripped = line.strip()
			if not line_stripped or line_stripped[0] == '#':
				continue
			if line == line.lstrip():
				if ':' not in line:
					raise _E('Expected a ":"')
				key, val = line.split(':', 1)
				if key not in known:
					raise _E(f'Unknown key {key!r}')
			else:
				if not key:
					raise _E('First line indented')
				val = line
			val = shlex.split(interpolate(val), posix=True, comments=True)
			if val:
				handle(key, val)
	def just_project_directory(key, val):
		if key == 'project directory':
			if len(val) != 1:
				raise _E(f"{key} takes a single value path (maybe you meant to quote it?)")
			project_directory[0] = val[0]
	def everything(key, val):
		if key in parsers:
			args, p = parsers[key]
			if key in ('urd', 'method packages'):
				want_count = (1, 2)
				want_count_str = "1 or 2"
			else:
				want_count = [len(args)]
				want_count_str = str(want_count[0])
			if len(val) not in want_count:
				if len(args) == 1:
					raise _E(f"{key} takes a single value {args[0]} (maybe you meant to quote it?)")
				else:
					raise _E(f"{key} takes {want_count_str} values (expected {' '.join(args)}, got {val!r})")
			if len(args) == 1:
				val = val[0]
			val = p(val)
		elif len(val) == 1:
			val = val[0]
		else:
			raise _E(f"{key} takes a single value (maybe you meant to quote it?)")
		if key in checkers:
			checkers[key](val)
		if key in multivalued:
			cfg[key].append(val)
		else:
			if key in cfg:
				raise _E(f"{key!r} doesn't take multiple values")
			cfg[key] = val

	try:
		project_directory = [os.path.dirname(filename)]
		lineno = [None]
		parse(just_project_directory)
		lineno = [None]
		project_directory = os.path.realpath(project_directory[0])
		parse(everything)
		lineno = [None]

		missing = set()
		for req in required:
			if not cfg[req]:
				missing.add(req)
		if missing:
			raise _E(f'Missing required keys {missing!r}')

		# Reformat result a bit so the new format doesn't require code changes all over the place.
		rename = {
			'target workdir': 'target_workdir',
			'method packages': 'method_directories',
			'input directory': 'input_directory',
			'result directory': 'result_directory',
			'project directory': 'project_directory',
			'board listen': 'board_listen',
		}
		res = DotDict({rename.get(k, k): v for k, v in cfg.items()})
		if 'listen' not in res:
			res.listen = '.socket.dir/server', None
		if 'target_workdir' not in res:
			res.target_workdir = res.workdirs[0][0]
		if 'project_directory' not in res:
			res.project_directory = os.path.dirname(filename)
		res.project_directory = os.path.realpath(res.project_directory)
		if 'input_directory' not in res:
			res.input_directory = fixpath('')
		if 'result_directory' not in res:
			res.result_directory = fixpath('')
		res.workdirs = dict(res.workdirs)
		if res.target_workdir not in res.workdirs:
			raise _E(f'target workdir {res.target_workdir!r} not in defined workdirs {set(res.workdirs)!r}')
		res.interpreters = dict(res.interpreters)
		for exe in res.interpreters.values():
			assert os.path.exists(exe), f'Executable {exe!r} does not exist.'
		res.listen, res.url = fixup_listen(res.project_directory, res.listen)
		if res.get('urd'):
			res.urd_local, listen = res.urd
			res.urd_listen, res.urd = fixup_listen(res.project_directory, listen)
		else:
			res.urd_local, res.urd_listen, res.urd = False, None, None
		res.board_listen, _ = fixup_listen(res.project_directory, res.get('board_listen', ('.socket.dir/board', None)))
		res.method_directories = dict(res.method_directories)
	except _E as e:
		if lineno[0] is None:
			prefix = f'Error in {filename}:\n'
		else:
			prefix = f'Error on line {lineno[0]} of {filename}:\n'
		raise UserError(prefix + e.args[0])

	res.config_filename = os.path.realpath(filename)
	return res
