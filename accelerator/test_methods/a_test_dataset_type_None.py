# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2023-2024 Carl Drougge                                     #
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

from __future__ import unicode_literals

description = r'''
Test the +None types in dataset_type.

Types two datasets, one with
	['real value', '', ' ']
in each row and one with
	[None, 'real value', '', ' '].

These are both typed with and without +None on the types, with defaults
set to the first value.

Most types reject empty strings (including space), so the results should
mostly be
	([None] +) [value, None, value]
and
	([None] +) [value, value, value]

The resulting columns should have None support if typed as +None or
if the source had None support, but not otherwise.
'''

from accelerator import subjobs
from datetime import date, time, datetime

def nonename(name):
	a = name.split(':')
	a[0] += '+None'
	return ':'.join(a)

def synthesis(job):
	# {type: (string, typed value, overrides for ['', ' '])}
	# Not testing every type, but all implementations.
	types2test = {
		'ascii:encode'     : ('\u00fe', '\\303\\276', [ '',  ' '],),
		'asciistrip'       : (    '  ',           '', [ '',   ''],),
		'bytes'            : ('\u00fe',  b'\xc3\xbe', [b'', b' '],),
		'unicode:utf-8'    : ('\u00fe',     '\u00fe', [ '',  ' '],),
		'complex64'        : ('1-0.2j',     (1-0.2j), None,),
		'float32'          : (    '42',         42.0, None,),
		'float32i'         : (  '4two',          4.0, [0, 0],),
		'float64'          : (   '4.2',         4.20, None,),
		'float64i'         : (  '.4.2',          0.4, [0, 0],),
		'floatbool'        : (     '9',         True, None,),
		'floatbooli'       : (     'y',        False, [False, False],),
		'int32_10'         : (    '18',           18, None,),
		'int64_16i'        : (  'bark',         0xba, [0, 0],),
		'int64_8'          : (  '1000',          512, None,),
		'json'             : ('[1, 2]',       [1, 2], None,),
		'number'           : (   '1.8',          1.8, None,),
		'number:int'       : (     '7',            7, None,),
		'strbool'          : (    'no',        False, [False, True],), # everything unrecognised is True, so ' ' is True
		'datetime:%Y'      : (  '2008', datetime(2008, 1, 1), None,),
		'datetimei:%Y'     : (  '200x', datetime( 200, 1, 1), None,),
		'date:%Y'          : (  '1997', date(1997, 1, 1)    , None,),
		'datei:%Y'         : (  '199x', date( 199, 1, 1)    , None,),
		'time:%H:%M'       : ( '18:27', time(18, 27)        , None,),
		'timei:%H:%M'      : ( '18:2x', time(18,  2)        , None,),
	}
	defaults = {k: v[0] for k, v in types2test.items()}
	for ds_with_None in (False, True):
		dw = job.datasetwriter(
			name='with_None' if ds_with_None else 'without_None',
			allow_missing_slices=True,
		)
		for name in types2test:
			dw.add(name, 'unicode', none_support=ds_with_None)
		dw.set_slice(0)
		if ds_with_None:
			dw.write_dict({k: None for k in types2test})
		dw.write_dict({k: v[0] for k, v in types2test.items()})
		dw.write_dict({k: '' for k in types2test})
		dw.write_dict({k: ' ' for k in types2test})
		ds = dw.finish()
		for type_with_None in (False, True):
			column2type = {k: [str, nonename][type_with_None](k) for k in types2test}
			typed = subjobs.build('dataset_type', source=ds, column2type=column2type, defaults=defaults).dataset()
			for name, (_, value, values4empty) in types2test.items():
				assert typed.columns[name].none_support == ds_with_None or type_with_None
				if values4empty:
					want = [value] + values4empty
				else:
					want = [value, value, value]
				if type_with_None:
					want[1] = None
				if ds_with_None:
					want.insert(0, None)
				got = list(typed.iterate(0, name))
				assert want == got, 'Column %r in %s has %r, should have %r' % (name, typed.quoted, got, want,)
