############################################################################
#                                                                          #
# Copyright (c) 2025 Carl Drougge                                          #
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
Test the hash sequence splitter.
'''

from accelerator.splitters import HashSplitter
from datetime import datetime, date, time
import struct


def prepare(job):
	# All the supported column types with some values.
	typ2values = {
		'int32': list(range(1000)),
		'int64': list(range(1000, 2000)),
		'number': list(range(100)) + [x / 37 for x in range(1000)],
		'float32': [x / 3 for x in range(1000)],
		'float64': [x / 3 for x in range(1000)],
		'complex32': [complex(x / 7, x / 3) for x in range(1000)],
		'complex64': [complex(x / 7, x / 3) for x in range(1000)],
		'bool': [True, False, 1, 0],
		'datetime': [datetime(2025, 11, 6, 0, 27, x // 17, x) for x in range(1000)],
		'date': [date(2025, m, d) for m in range(1, 13) for d in range(1, 29)],
		'time': [time(0, 27, x // 17, x) for x in range(1000)],
		'ascii': [chr(x) for x in range(128)] + ['', 'abcd'],
		'bytes': [chr(x).encode('iso-8859-1') for x in range(256)] + [b'', b'abcd'],
		'unicode': [chr(x) for x in range(1000)] + ['', 'a\u0308', 'åäö'],
	}
	# Test all the parsed types too.
	for typ in ('complex32', 'complex64', 'float32', 'float64', 'int32', 'int64', 'number'):
		typ2values['parsed:' + typ] = [str(v) for v in typ2values[typ]]
	typ2values['parsed:int32'].extend(['01', '001', '0001', '00009999'])
	for typ, values in typ2values.items():
		# All types also get a None.
		values.append(None)
		dw = job.datasetwriter(name=typ, hashlabel='value')
		dw.add('value', typ, none_support=True)
		w = dw.get_split_write()
		for v in values:
			w(v)
		dw.finish()
	plain = {
		typ: HashSplitter(values, typ)
		for typ, values in typ2values.items()
	}
	tupled = {
		typ: HashSplitter([(v, 42) for v in values], typ)
		for typ, values in typ2values.items()
	}
	own_hashfunc = HashSplitter(range(1000), lambda x: x // 16)
	return plain, tupled, typ2values, own_hashfunc


# The 32bit float types quantize in the dataset.
def float32ify(v):
	v = float(v)
	return struct.unpack('=f', struct.pack('=f', v))[0]

def complex32ify(v):
	v = complex(v)
	return complex(float32ify(v.real), float32ify(v.imag))


def analysis(sliceno, slices, job, prepare_res):
	plain, tupled, _, own_hashfunc = prepare_res
	fixups = {
		'complex32': complex32ify,
		'float32': float32ify,
		'parsed:complex32': complex32ify,
		'parsed:complex64': complex,
		'parsed:float32': float32ify,
		'parsed:float64': float,
		'parsed:int32': int,
		'parsed:int64': int,
		'parsed:number': float,
	}
	for typ, values in plain.items():
		assert [(v, 42) for v in values] == tupled[typ]
		if typ in fixups:
			f = fixups[typ]
			values = [None if v is None else f(v) for v in values]
		assert values == list(job.dataset(typ).iterate(sliceno, 'value'))
	assert all((v // 16) % slices == sliceno for v in own_hashfunc)
	return own_hashfunc, list(own_hashfunc)

def synthesis(prepare_res, analysis_res):
	plain, tupled, typ2values, own_hashfunc = prepare_res
	for typ, values in plain.items():
		assert [(v, 42) for v in values] == tupled[typ]
		assert len(values) == len(typ2values[typ])
		assert values == typ2values[typ]
	assert own_hashfunc == list(range(1000))
	# This doesn't get a dataset, so the test in analysis doesn't verify
	# that no data was lost.
	split_own_hashfunc, split_own_hashfunc_plainlist = analysis_res.merge_auto()
	assert split_own_hashfunc == split_own_hashfunc_plainlist
	assert sorted(split_own_hashfunc) == list(range(1000))
