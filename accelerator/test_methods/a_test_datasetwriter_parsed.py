# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2022-2024 Carl Drougge                                     #
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

description = r'''
Test the various :parsed types on DatasetWriter
'''

def synthesis(job):
	inf = float('inf')
	for none_support in (False, True):
		for t, parse_values, values in (
			('int64', ['-1', '0', '999999999999999999'], [-1, 0, 999999999999999999]),
			('int32', ['-1', '0', '999999999'], [-1, 0, 999999999]),
			('float64', ['1', '-2', '3.3'], [1.0, -2.0, 3.3]),
			('float32', ['inf', '0.1', '-inf'], [inf, 0.10000000149011612, -inf]),
			('complex64', ['1', '2j', '(-3.3)', '4-5j'], [1+0j, 0+2j, -3.3+0j, 4-5j]),
			('complex32', ['infj', 'inf-infj', 'inf+0.5j'], [complex(0, inf), complex(inf, -inf), complex(inf, 0.5)]),
			('number', ['-1', '2.2', '99999999999999999999'], [-1, 2.2, 99999999999999999999]),
			('json', ['1', '["2"]', '{"3": 3.3}'], [1, ['2'], {'3': 3.3}]),
		):
			if t == 'json':
				bad_value = object
			else:
				bad_value = 'not valid'
			name = '%s %s none_support' % (t, 'with' if none_support else 'without',)
			dw = job.datasetwriter(name=name, allow_missing_slices=True)
			dw.add('data', 'parsed:' + t, none_support=none_support)
			dw.set_slice(0)
			for v in values:
				dw.write(v)
			dw.set_slice(1)
			for v in parse_values:
				assert isinstance(v, str), 'oops: %r' % (v,)
				dw.write(v)
			try:
				dw.write(bad_value)
				raise Exception("parsed:%s accepted %r as a value" % (t, bad_value,))
			except (ValueError, TypeError):
				pass
			# json will of course accept None even without none_support
			apparent_none_support = (none_support or t == 'json')
			dw.set_slice(2)
			try:
				dw.write(None)
				if not apparent_none_support:
					raise Exception('parsed:%s accepted None without none_support' % (t,))
			except (ValueError, TypeError):
				if apparent_none_support:
					raise Exception('parsed:%s did not accept None despite none_support' % (t,))
			ds = dw.finish()
			for sliceno, desc in enumerate(("normal values", "parseable values",)):
				got = list(ds.iterate(sliceno, 'data'))
				assert got == values, "parsed:%s (%s) %s gave %r, wanted %r" % (t, ds.quoted, desc, got, values,)
			if apparent_none_support:
				got = list(ds.iterate(2, 'data'))
				assert got == [None], "parsed:%s (%s) gave %r, wanted [None]" % (t, ds.quoted, got,)
			dw = job.datasetwriter(name=name + ' with default', allow_missing_slices=True)
			default = None if none_support else 42
			dw.add('data', 'parsed:' + t, none_support=none_support, default=default)
			dw.set_slice(0)
			dw.write('1')
			dw.write(bad_value)
			ds = dw.finish()
			got = list(ds.iterate(0, 'data'))
			assert got == [1, default], "parsed:%s with default=%s (%s) gave %r, wanted [1, %s]" % (t, default, ds.quoted, got, default)
