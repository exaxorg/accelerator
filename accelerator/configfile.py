############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019 Anders Berkeman                         #
# Modifications copyright (c) 2019 Carl Drougge                            #
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

import re
import os
import sys
from functools import partial

from accelerator.compat import quote_plus, open

from accelerator.extras import DotDict


_re_var = re.compile(r'\$\{([^\}=]*)(?:=([^\}]*))?\}')
def interpolate(s):
	"""Replace ${FOO=BAR} with os.environ.get('FOO', 'BAR')
	(just ${FOO} is of course also supported, but not $FOO)"""
	return _re_var.subn(lambda m: os.environ.get(m.group(1), m.group(2)), s)[0]


def resolve_socket_url(path):
	if '://' in path:
		return path
	else:
		return 'unixhttp://' + quote_plus(os.path.realpath(path))


def load_config(filename):
	key = None
	multivalued = {'workdirs', 'method packages', 'interpreters'}
	required = {'slices', 'logfile', 'workdirs', 'method packages'}
	known = {'target workdir', 'urd', 'result directory', 'source directory', 'project directory'} | required | multivalued
	cfg = {key: [] for key in multivalued}

	class _E(Exception):
		pass
	def parse_pair(thing, val):
		a = val.split()
		if len(a) != 2 or not a[1].startswith('/'):
			raise _E("Invalid %s specification %r (expected 'name /path')" % (thing, val,))
		return a
	def check_interpreter(val):
		if val[0] == 'DEFAULT':
			raise _E("Don't override DEFAULT interpreter")
		if not os.path.isfile(val[1]):
			raise _E('%r does not exist' % (val,))

	parsers = dict(
		slices=int,
		workdirs=partial(parse_pair, 'workdir'),
		interpreters=partial(parse_pair, 'interpreter'),
		urd=resolve_socket_url,
	)
	checkers = dict(
		interpreter=check_interpreter,
	)

	with open(filename, 'r', encoding='utf-8') as fh:
		try:
			for lineno, line in enumerate(fh, 1):
				line = line.split('#', 1)[0].rstrip()
				if not line.strip():
					continue
				if line == line.strip():
					if ':' not in line:
						raise _E('Expected a ":"')
					key, val = line.split(':', 1)
					if key not in known:
						raise _E('Unknown key %r' % (key,))
				else:
					if not key:
						raise _E('First line indented')
					val = line
				val = interpolate(val).strip()
				if val:
					if key in parsers:
						val = parsers[key](val)
					if key in checkers:
						checkers[key](val)
					if key in multivalued:
						cfg[key].append(val)
					else:
						if key in cfg:
							raise _E("%r doesn't take multiple values" % (key,))
						cfg[key] = val
		except _E as e:
			print('Error on line %d of %s:\n%s' % (lineno, filename, e.args[0],), file=sys.stderr)
			sys.exit(1)

	missing = set()
	for req in required:
		if not cfg[req]:
			missing.add(req)
	if missing:
		print('Error in %s: Missing required keys %r' % (filename, missing,), file=sys.stderr)
		exit(1)

	# Reformat result a bit so the new format doesn't require code changes all over the place.
	rename = {
		'target workdir': 'target_workdir',
		'method packages': 'method_directories',
		'source directory': 'source_directory',
		'result directory': 'result_directory',
		'project directory': 'project_directory',
	}
	res = DotDict({rename.get(k, k): v for k, v in cfg.items()})
	if 'target_workdir' not in res:
		res.target_workdir = res.workdirs[0][0]
	if 'project_directory' not in res:
		res.project_directory = os.path.dirname(filename)
	res.workdirs = dict(res.workdirs)
	res.interpreters = dict(res.interpreters)
	return res