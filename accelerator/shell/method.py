# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2020-2024 Carl Drougge                                     #
# Modifications copyright (c) 2021 Anders Berkeman                         #
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

from accelerator.compat import terminal_size
from accelerator.unixhttp import call
from accelerator.shell import printdesc
from accelerator.shell.parser import ArgumentParser
from collections import defaultdict

def main(argv, cfg):
	parser = ArgumentParser(prog=argv.pop(0), description='''gives description and options for method(s), or lists methods with no method specified.''')
	parser.add_argument('method', nargs='*')
	args = parser.parse_intermixed_args(argv)
	methods = call(cfg.url + '/methods')
	columns = terminal_size().columns
	if args.method:
		for name in args.method:
			if name in methods:
				data = methods[name]
				print(f'{data.package}.{name}:')
				if data.description.text:
					for line in data.description.text.split('\n'):
						if line:
							print(' ', line)
						else:
							print()
					print()
				if cfg.get('interpreters'):
					print(f'Runs on <{data.version}> {data.description.interpreter}')
					print()
				for k in ('datasets', 'jobs',):
					if data.description.get(k):
						print(f'{k}:')
						klen = max(len(k) for k in data.description[k])
						template = '  %%-%ds # %%s' % (klen,)
						for k, v in data.description[k].items():
							if v:
								print(template % (k, v[0],))
								for cmt in v[1:]:
									print(template % ('', cmt,))
							else:
								print(' ', k)
				if data.description.get('options'):
					print('options:')
					klen = max(len(k) for k in data.description.options)
					vlens = [len(v[0]) for v in data.description.options.values() if len(v) > 1]
					vlen = max(vlens or [0])
					firstlen = klen + vlen + 5
					template = '  %%-%ds = %%s' % (klen,)
					template_cmt = '%%-%ds  # %%s' % (firstlen,)
					for k, v in data.description.options.items():
						first = template % (k, v[0],)
						if len(v) > 1:
							afterlen = max(len(cmt) for cmt in v[1:]) + firstlen + 4
							if afterlen <= columns:
								print(template_cmt % (first, v[1],))
								for cmt in v[2:]:
									print(template_cmt % ('', cmt,))
							else:
								for cmt in v[1:]:
									print('  #', cmt)
								print(first)
						else:
							print(first)
			else:
				print(f'Method {name!r} not found')
	else:
		by_package = defaultdict(list)
		for name, data in sorted(methods.items()):
			by_package[data.package].append(name)
		by_package.pop('accelerator.test_methods', None)
		for package, names in sorted(by_package.items()):
			print(f'{package}:')
			items = [(name, methods[name].description.text) for name in names]
			printdesc(items, columns, 'method')
