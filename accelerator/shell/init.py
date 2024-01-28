# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2019-2024 Carl Drougge                                     #
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


from __future__ import print_function
from __future__ import division


a_example = r"""description = r'''
This is just an example. It doesn't even try to do anything useful.

You can run it to see that your installation works.
'''

options = dict(
	message=str,
)

def analysis(sliceno):
	return sliceno

def synthesis(analysis_res):
	print("Sum of all sliceno:", sum(analysis_res))
	print("Message:", options.message)
"""


build_script = r"""def main(urd):
	urd.build('example', message='Hello world!')
"""


methods_conf_template = r"""# The {name} package uses auto-discover in accelerator.conf,
# so you don't need to write anything here.
#
# But if you want to override the interpreter for some method, you can.
# For example:
# some_method 2.7
# would run some_method on the 2.7 interpreter.
"""


config_template = r"""# The configuration is a collection of key value pairs.
#
# Values are specified as
# key: value
# or for several values
# key:
# 	value 1
# 	value 2
# 	...
# (any leading whitespace is ok)
#
# Use ${{VAR}} or ${{VAR=DEFAULT}} to use environment variables.
#
# Created by accelerator version {version}

slices: {slices}
workdirs:
	{all_workdirs}

# Target workdir defaults to the first workdir, but you can override it.
# (this is where jobs without a workdir override are built)
target workdir: {first_workdir_name}

method packages:
	{name} auto-discover
	{examples} auto-discover
	accelerator.standard_methods
	accelerator.test_methods

# listen directives can be [host]:port or socket path.
# urd should be prefixed with "local" to run it together with the server
# or "remote" to not run it together with the server.
listen: {listen.server}
board listen: {listen.board}
urd: local {listen.urd}

result directory: ./results
input directory: {input}

# If you want to run methods on different python interpreters you can
# specify names for other interpreters here, and put that name after
# the method in methods.conf.
# You automatically get four names for the interpreter that started
# the server: DEFAULT, {major}, {major}.{minor} and {major}.{minor}.{micro} (adjusted to the actual
# version used). You can override these here, except DEFAULT.
# interpreters:
# 	2.7 /path/to/python2.7
# 	test /path/to/beta/python
"""


def find_free_ports(low, high, count=3, hostname='localhost'):
	import random
	import socket
	ports = list(range(low, high - count))
	random.shuffle(ports)
	res = {}
	def free(port):
		if port not in res:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				s.bind((hostname, port))
				res[port] = True
			except socket.error:
				res[port] = False
			s.close()
		return res[port]
	for port in ports:
		if all(free(port + n) for n in range(count)):
			return port
	raise Exception('Failed to find %d consecutive free TCP ports on %s in range(%d, %d)' % (count, hostname, low, high))


def git(method_dir):
	from subprocess import check_call
	from accelerator.compat import FileNotFoundError
	from sys import stderr
	from os.path import exists
	if exists('.git'):
		print('WARNING: .git already exists, skipping git init', file=stderr)
		return
	try:
		check_call(['git', 'init', '--quiet'])
	except FileNotFoundError:
		print('WARNING: git appears to not be installed, skipping git init', file=stderr)
		return
	with open('.gitignore', 'w') as fh:
		fh.write('/.socket.dir\n')
		fh.write('/urd.db\n')
		fh.write('/workdirs\n')
		fh.write('/results\n')
		fh.write('/venv\n')
		fh.write('/.venv\n')
		fh.write('__pycache__\n')
		fh.write('*.pyc\n')
	check_call(['git', 'add', '--', 'accelerator.conf', '.gitignore', method_dir])


def main(argv, cfg):
	from os import makedirs, listdir, chdir, environ
	from os.path import exists, join, realpath, dirname, split
	import re
	from sys import version_info
	from argparse import RawTextHelpFormatter
	from accelerator.configfile import interpolate
	from accelerator.shell import user_cfg
	from accelerator.shell.parser import ArgumentParser
	from accelerator.compat import shell_quote
	from accelerator.error import UserError
	from accelerator.extras import DotDict
	import accelerator

	parser = ArgumentParser(
		prog=argv.pop(0),
		description=r'''
			creates an accelerator project directory.
			defaults to the current directory.
			creates accelerator.conf, a method dir, a workdir and result dir.
			both the method directory and workdir will be named <NAME>,
			"dev" by default.
		'''.replace('\t', ''),
		formatter_class=RawTextHelpFormatter,
	)

	configurable_args = {
		'--examples',
		'--input',
		'--name',
		'--no-git',
		'--slices',
		'--tcp',
		'--workdir-template',
		# --workdir is also configurable, but handled further down
	}
	bool_fallbacks = set()
	def add_argument(name, default=None, help='', **kw):
		if name in configurable_args:
			cfg_name = name[2:]
			default = user_cfg.get('init', cfg_name, fallback=default)
			if default is not None:
				if kw.get('action') == 'store_true':
					if default.lower() == 'true':
						bool_fallbacks.add(cfg_name.replace('-', '_'))
					elif default.lower() != 'false':
						raise UserError("Configuration init.%s must be either true or false, not %r." % (cfg_name, default,))
					default = None
				elif 'type' in kw:
					default = kw['type'](default)
		if '[DEFAULT]' in help:
			help = help.replace('[DEFAULT]', default)
		parser.add_argument(name, default=default, help=help, **kw)

	add_argument('--slices', default=None, type=int, help='override slice count detection')
	add_argument('--name', default='dev', help='name of method dir and (default) workdir, default "[DEFAULT]"')
	add_argument('--input', default='# /some/path where you want import methods to look.', help='input directory')
	add_argument('--force', action='store_true', negation='dont', help='go ahead even though directory is not empty, or workdir\nexists with incompatible slice count')
	add_argument('--tcp', default=False, metavar='HOST/PORT', nargs='?', help='listen on TCP instead of unix sockets.\nspecify HOST (can be IP) to listen on that host\nspecify PORT to use range(PORT, PORT + 3)\nspecify both as HOST:PORT')
	add_argument('--no-git', action='store_true', negation='yes', help='don\'t create git repository')
	add_argument('--examples', action='store_true', negation='no', help='copy examples to project directory')
	add_argument('--workdir-template', metavar='TEMPLATE', default='./workdirs/[name]', help='where to put workdir, default "[DEFAULT]"')
	add_argument('--workdir', metavar='NAME or TEMPLATE', action='append', help='name of workdir (default --name), can specify several')
	add_argument('directory', default='.', help='project directory to create. default "."', metavar='DIR', nargs='?')
	options = parser.parse_intermixed_args(argv)
	for name in bool_fallbacks:
		if getattr(options, name) is None:
			setattr(options, name, True)

	assert options.name
	assert '/' not in options.name

	if options.tcp is False:
		listen = DotDict(
			board='.socket.dir/board',
			server='.socket.dir/server',
			urd='.socket.dir/urd',
		)
	else:
		hostport = options.tcp or ''
		if hostport.endswith(']'): # ipv6
			host, port = hostport, None
		elif ':' in hostport:
			host, port = hostport.rsplit(':', 1)
		elif hostport.isdigit():
			host, port = '', hostport
		else:
			host, port = hostport, None
		if port:
			port = int(port)
		else:
			port = find_free_ports(0x3000, 0x8000)
		listen = DotDict(
			server='%s:%d' % (host, port,),
			board='%s:%d' % (host, port + 1,),
			urd='%s:%d' % (host, port + 2,),
		)

	if options.slices is None:
		from multiprocessing import cpu_count
		default_slices = cpu_count()
	else:
		default_slices = options.slices

	if not options.input.startswith('#'):
		options.input = shell_quote(realpath(options.input))
	prefix = realpath(options.directory)
	project = split(prefix)[1]

	template_vars = dict(
		name=options.name,
		project=project,
		slices=str(default_slices),
		user=environ.get('USER', 'NO-USER'),
	)
	template_re = re.compile(r'\[(' + r'|'.join(template_vars.keys()) + r')\]')
	def template_var(m):
		name = m.group(1)
		if name == 'slices':
			# If the path depends on slices, we can't change that later.
			options.slices = default_slices
		return template_vars[name]

	if not options.workdir:
		import shlex
		workdir = shlex.split(user_cfg.get('init', 'workdir', fallback=''), posix=True)
		options.workdir = workdir or ['[name]']
	workdirs = []
	for name in options.workdir:
		if ':' in name:
			name, path = name.split(':', 1)
		elif '/' in name:
			path = name.rstrip('/')
			name = path.rsplit('/', 1)[-1]
		else:
			path = options.workdir_template
		# using --name when expanding the name part
		template_vars['name'] = options.name
		name = template_re.sub(template_var, name)
		# and then using the expanded name part when expanding path
		template_vars['name'] = name
		path = template_re.sub(template_var, path)
		workdirs.append((name, path))
	first_workdir_path = interpolate(workdirs[0][1])

	def slices_conf(workdir):
		return join(workdir, '.slices')

	def slice_count(workdir):
		try:
			with open(slices_conf(workdir), 'r') as fh:
				return int(fh.read())
		except IOError:
			return None

	if options.slices is None:
		options.slices = slice_count(first_workdir_path) or default_slices

	if not options.force:
		if exists(options.directory) and set(listdir(options.directory)) - {'venv', '.venv'}:
			raise UserError('Directory %r is not empty.' % (options.directory,))
		def plausible_jobdir(n):
			parts = n.rsplit('-', 1)
			return len(parts) == 2 and parts[0] == options.name and parts[1].isnumeric()
		for _, workdir in workdirs:
			workdir = interpolate(workdir)
			if exists(workdir):
				workdir_slices = slice_count(workdir)
				if workdir_slices not in (None, options.slices):
					raise UserError('Workdir %r has %d slices, refusing to continue with %d slices' % (workdir, workdir_slices, options.slices,))
		if exists(first_workdir_path) and any(map(plausible_jobdir, listdir(first_workdir_path))):
			raise UserError('Workdir %r already has jobs in it.' % (first_workdir_path,))

	if not exists(options.directory):
		makedirs(options.directory)
	chdir(options.directory)
	for dir_to_make in ('.socket.dir', 'urd.db',):
		if not exists(dir_to_make):
			makedirs(dir_to_make, 0o750)
	if not exists('results'):
		makedirs('results')
	for _, path in workdirs:
		path = interpolate(path)
		if not exists(path):
			makedirs(path)
		if options.force or not exists(slices_conf(path)):
			with open(slices_conf(path), 'w') as fh:
				fh.write('%d\n' % (options.slices,))
	method_dir = options.name
	if not exists(method_dir):
		makedirs(method_dir)
	with open(join(method_dir, '__init__.py'), 'w') as fh:
		pass
	with open(join(method_dir, 'methods.conf'), 'w') as fh:
		fh.write(methods_conf_template.format(name=options.name))
	with open(join(method_dir, 'a_example.py'), 'w') as fh:
		fh.write(a_example)
	with open(join(method_dir, 'build.py'), 'w') as fh:
		fh.write(build_script)
	if options.examples:
		from shutil import copytree
		from accelerator import examples
		copytree(dirname(examples.__file__), 'examples')
		examples = 'examples'
	else:
		examples = '# accelerator.examples'
	all_workdirs = ['%s %s' % (shell_quote(name), shell_quote(path),) for name, path in workdirs]
	with open('accelerator.conf', 'w') as fh:
		fh.write(config_template.format(
			name=shell_quote(options.name),
			first_workdir_name=shell_quote(workdirs[0][0]),
			all_workdirs='\n\t'.join(all_workdirs),
			slices=options.slices,
			version=accelerator.__version__,
			examples=examples,
			input=options.input,
			major=version_info.major,
			minor=version_info.minor,
			micro=version_info.micro,
			listen=DotDict({k: shell_quote(v) for k, v in listen.items()}),
		))
	if not options.no_git:
		git(method_dir)
