# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2024 Carl Drougge                                          #
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
from __future__ import unicode_literals

import gzip
import math
import os
import struct
import sys

from accelerator._dsutil import siphash24
from accelerator.dsutil import typed_writer
from accelerator.subjobs import build

description = r'''
Test NaN values in the various float supporting types, making sure they
hash as expected, including for unusual NaN representations.
'''

def synthesis(job, slices):
	normal_NaN = float('NaN')
	unusual_NaN_0 = struct.unpack('=f', b'\x7f\xc0\xc0\x7f')[0]
	unusual_NaN_1 = struct.unpack('=f', b'\x7f\xc1\xc1\x7f')[0]
	assert math.isnan(normal_NaN)
	assert math.isnan(unusual_NaN_0)
	assert math.isnan(unusual_NaN_1)
	assert struct.pack('=f', unusual_NaN_0) == b'\x7f\xc0\xc0\x7f'
	assert struct.pack('=f', unusual_NaN_1) == b'\x7f\xc1\xc1\x7f'

	f32_normal_NaN = struct.pack('=f', normal_NaN)
	f32_unusual_NaN_0 = struct.pack('=f', unusual_NaN_0)
	f32_unusual_NaN_1 = struct.pack('=f', unusual_NaN_1)
	f64_normal_NaN = struct.pack('=d', normal_NaN)
	f64_unusual_NaN_0 = struct.pack('=d', unusual_NaN_0)
	f64_unusual_NaN_1 = struct.pack('=d', unusual_NaN_1)

	if sys.byteorder == 'big':
		f32_std_nan = b'\x7f\xc0\x00\x00'
		f64_std_nan = b'\x7f\xf8\x00\x00\x00\x00\x00\x00'
	else:
		f32_std_nan = b'\x00\x00\xc0\x7f'
		f64_std_nan = b'\x00\x00\x00\x00\x00\x00\xf8\x7f'

	# This is always what should be hashed, regardless of platform NaN representation.
	want_hash_float = siphash24(f64_std_nan)
	want_hash_complex = siphash24(f64_std_nan + f64_std_nan)

	def test_encoding_and_hash(typ, nan, *want):
		writer = typed_writer(typ)
		assert writer.hash(nan) == want_hash_float
		with writer('tmp', compression='gzip', none_support=True) as w:
			w.write(nan)
		with gzip.open('tmp', 'rb') as fh:
			got_bytes = fh.read()
		os.remove('tmp')
		assert got_bytes in want, 'bad NaN representation in %s: got %r, wanted something in %r' % (typ, got_bytes, want,)

	# Test that they either encode as themselves or as one of the normal NaNs,
	# and all hash the same as the standard float64 NaN.
	test_encoding_and_hash('float32', normal_NaN, f32_std_nan, f32_normal_NaN)
	test_encoding_and_hash('float32', unusual_NaN_0, f32_std_nan, f32_normal_NaN, f32_unusual_NaN_0)
	test_encoding_and_hash('float32', unusual_NaN_1, f32_std_nan, f32_normal_NaN, f32_unusual_NaN_1)
	test_encoding_and_hash('float64', normal_NaN, f64_std_nan, f64_normal_NaN)
	test_encoding_and_hash('float64', unusual_NaN_0, f64_std_nan, f64_normal_NaN, f64_unusual_NaN_0)
	test_encoding_and_hash('float64', unusual_NaN_1, f64_std_nan, f64_normal_NaN, f64_unusual_NaN_1)
	test_encoding_and_hash('number', normal_NaN, b'\x01' + f64_std_nan, b'\x01' + f64_normal_NaN)
	test_encoding_and_hash('number', unusual_NaN_0, b'\x01' + f64_std_nan, b'\x01' + f64_normal_NaN, b'\x01' + f64_unusual_NaN_0)
	test_encoding_and_hash('number', unusual_NaN_1, b'\x01' + f64_std_nan, b'\x01' + f64_normal_NaN, b'\x01' + f64_unusual_NaN_1)

	def mk_dws(typ):
		kw = {
			'name': typ + ' NaNs',
			'columns': {'ix': 'ascii', 'nan': typ},
		}
		dw_unhashed = job.datasetwriter(**kw)
		kw['name'] += ' hashed'
		kw['hashlabel'] = 'nan'
		dw_hashed = job.datasetwriter(**kw)
		return dw_unhashed, dw_unhashed.get_split_write(), dw_hashed, dw_hashed.get_split_write()

	for typ, values in (
		('complex32', [complex(normal_NaN, normal_NaN), complex(unusual_NaN_0, unusual_NaN_1)]),
		('complex64', [complex(normal_NaN, normal_NaN), complex(unusual_NaN_1, unusual_NaN_0)]),
		('float32', [normal_NaN, unusual_NaN_0, unusual_NaN_1]),
		('float64', [normal_NaN, unusual_NaN_0, unusual_NaN_1]),
		('number', [normal_NaN, unusual_NaN_0, unusual_NaN_1]),
	):
		dw_u, w_u, dw_h, w_h = mk_dws(typ)
		h = typed_writer(typ).hash
		want_h = (want_hash_complex if typ.startswith('complex') else want_hash_float)
		for ix, v in enumerate(values):
			assert want_h == h(v), 'value index %d did not hash correctly for type %s' % (ix, typ,)
			w_u(str(ix), v)
			w_h(str(ix), v)
		ds_h = dw_h.finish()
		assert set(ds_h.lines) == {0, len(values)}, 'Not all NaNs ended up in the same slice in %s' % (ds_h.quoted,)
		expect_lines = ds_h.lines
		ds_u = dw_u.finish()
		ds = build('dataset_hashpart', source=ds_u, hashlabel='nan').dataset()
		assert set(ds.lines) == {0, len(values)}, 'Not all NaNs ended up in the same slice in %s (dataset_hashpart from %s)' % (ds.quoted, ds_u.quoted,)
		assert expect_lines == ds.lines, 'dataset_hashpart (%s) disagrees with datasetwriter (%s) about NaN slicing' % (ds.quoted, ds_h.quoted,)
		ds = build('dataset_type', source=ds_u, hashlabel='nan', column2type={'ix': 'number'}).dataset()
		assert set(ds.lines) == {0, len(values)}, 'Not all NaNs ended up in the same slice in %s (dataset_type from %s)' % (ds.quoted, ds_u.quoted,)
		assert expect_lines == ds.lines, 'dataset_type (%s) disagrees with datasetwriter (%s) about NaN slicing' % (ds.quoted, ds_h.quoted,)
		rehash_lines = [len(list(ds_u.iterate(sliceno, rehash=True, hashlabel='nan'))) for sliceno in range(slices)]
		assert expect_lines == rehash_lines, 'ds.iterate(rehash=True) of %s disagrees with datasetwriter hashing (%s) about NaN slicing' % (ds_u.quoted, ds_h.quoted,)
