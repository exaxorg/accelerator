# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2020-2024 Carl Drougge                                     #
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
Verify that all column types come out correctly in csvexport.
'''

from datetime import date, time, datetime

from accelerator import subjobs, status
from accelerator.dsutil import _convfuncs

def synthesis(job):
	dw = job.datasetwriter()
	todo = {
		'ascii',
		'bool',
		'bytes',
		'complex32',
		'complex64',
		'date',
		'datetime',
		'float32',
		'float64',
		'int32',
		'int64',
		'json',
		'number',
		'pickle',
		'time',
		'unicode',
	}
	check = {n for n in _convfuncs if not n.startswith('parsed:')}
	assert todo == check, f'Missing/extra column types: {check - todo!r} {todo - check!r}'
	for name in sorted(todo):
		t = name
		dw.add(name, t, none_support=True)
	write = dw.get_split_write()
	write(
		'a', True, b'hello',
		42, 1e100+0.00000000000000001j,
		date(2020, 6, 23), datetime(2020, 6, 23, 12, 13, 14),
		1.0, float('-inf'), -10, -20,
		{'json': True}, 0xfedcba9876543210beef,
		1+2j, time(12, 13, 14), 'bl\xe5',
	)
	d = {}
	d['recursion'] = d
	write(
		'b', False, b'bye',
		2-3j, -7,
		date(1868,  1,  3), datetime(1868,  1,  3, 13, 14, 5),
		float('inf'), float('nan'), 0, 0,
		[False, None], 42.18,
		d, time(13, 14, 5), 'bl\xe4',
	)
	write(
		None, None, None,
		None, None,
		None, None,
		None, None, None, None,
		None, None,
		None, None, None,
	)
	ds = dw.finish()
	sep = '\x1e'
	for sep, q, none_as, last_line in (
		('\x1e', '', None, (
			'None', 'None', 'None',
			'None', 'None',
			'None', 'None',
			'None', 'None', 'None', 'None',
			'null', 'None',
			'None', 'None', 'None',
		)),
		('\x1e', 'a', '', (
			'', '', '',
			'', '',
			'', '',
			'', '', '', '',
			'', '',
			'', '', '',
		)),
		('\x00', '0', None, (
			'None', 'None', 'None',
			'None', 'None',
			'None', 'None',
			'None', 'None', 'None', 'None',
			'null', 'None',
			'None', 'None', 'None',
		)),
		(':', '"', '"', (
			'"', '"', '"',
			'"', '"',
			'"', '"',
			'"', '"', '"', '"',
			'"', '"',
			'"', '"', '"',
		)),
		(':', '"', {'time': 'never', 'float32': '"0"'}, (
			'None', 'None', 'None',
			'None', 'None',
			'None', 'None',
			'"0"', 'None', 'None', 'None',
			'null', 'None',
			'None', 'never', 'None',
		)),
	):
		with status(f"Checking with sep={sep!r}, q={q!r}, none_as={none_as!r}"):
			exp = subjobs.build('csvexport', filename='test.csv', separator=sep, source=ds, quote_fields=q, none_as=none_as, lazy_quotes=False)
			with exp.open('test.csv', 'r', encoding='utf-8') as fh:
				def expect(*a):
					want = sep.join(q + v.replace(q, q + q) + q for v in a) + '\n'
					got = next(fh)
					assert want == got, f'wanted {want!r}, got {got!r} from {exp} (export of {ds})'
				expect(*sorted(todo))
				expect(
					'a', 'True', 'hello',
					'(42+0j)', '(1e+100+1e-17j)',
					'2020-06-23', '2020-06-23 12:13:14',
					'1.0', '-inf', '-10', '-20',
					'{"json": true}', '1203552815971897489538799',
					'(1+2j)', '12:13:14', 'bl\xe5',
				)
				expect(
					'b', 'False', 'bye',
					'(2-3j)', '(-7+0j)',
					'1868-01-03', '1868-01-03 13:14:05',
					'inf', 'nan', '0', '0',
					'[false, null]', '42.18',
					"{'recursion': {...}}", '13:14:05', 'bl\xe4',
				)
				expect(*last_line)
