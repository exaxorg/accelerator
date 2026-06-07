############################################################################
#                                                                          #
# Copyright (c) 2026 Carl Drougge                                          #
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


from .parser import ArgumentParser
from accelerator.compat import shell_quote


def main(argv, cfg):
	parser = ArgumentParser(prog=argv.pop(0), description='''display the configuration''')
	args = parser.parse_intermixed_args(argv)

	def show(key, fmt=shell_quote, *, spacing=False):
		value = fmt(d.pop(key.replace(' ', '_')))
		if not value:
			return
		if spacing:
			print()
		if isinstance(value, list):
			print(f'{key}:')
			for v in value:
				print(f'\t{v}')
		else:
			print(f'{key}: {value}')

	def fmt_kv(d):
		return [f'{shell_quote(k)} {shell_quote(v)}' for k, v in d.items()]

	def fmt_listen(v):
		if isinstance(v, tuple):
			return f'{shell_quote(v[0])}:{v[1]}'
		else:
			return shell_quote(v)

	def fmt_method_dirs(d):
		res = []
		for name, auto_discover in d.items():
			name = shell_quote(name)
			if auto_discover:
				res.append(f'{name} auto-discover')
			else:
				res.append(name)
		return res

	def fmt_setenv(d):
		res = sorted(shell_quote(k) for k, v in d.items() if v is None)
		for k, v in sorted((k, v) for k, v in d.items() if v is not None):
			res.append(f'{shell_quote(k)}={shell_quote(v)}')
		return res

	# Get rid of some that aren't really in the config as such.
	d = dict(cfg)
	d.pop('config_filename')
	d.pop('urd')
	d.pop('url')
	d.pop('user_cwd')

	print(f'# Configuration parsed from {cfg.config_filename}')
	print()

	show('slices', str)
	show('workdirs', fmt_kv)
	show('target workdir')
	print()
	show('method directories', fmt_method_dirs)
	print()

	show('listen', fmt_listen)
	show('board listen', fmt_listen)
	urd_local_remote = 'local' if d.pop('urd_local') else 'remote'
	urd_listen = d.pop('urd_listen')
	if urd_listen:
		print(f'urd: {urd_local_remote} {fmt_listen(urd_listen)}')
	else:
		print('# no urd')
	print()

	show('input directory')
	show('project directory')
	show('result directory')

	show('interpreters', fmt_kv, spacing=True)
	show('setenv', fmt_setenv, spacing=True)

	assert not d, f'Unhandled configuration items {d}'
