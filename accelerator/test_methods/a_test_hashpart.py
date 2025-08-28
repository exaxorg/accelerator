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

description = r'''
Verify the dataset_hashpart method with various options.
'''

from datetime import date

from accelerator import subjobs
from accelerator.dataset import DatasetWriter, Dataset
from accelerator.dsutil import typed_writer

data = [
	{"a column": "text", "also a column": b"octets", "number": 10, "date": date(1978, 1, 1)},
	{"a column": "a", "also a column": b"0", "number": -900, "date": date(2009, 1, 1)},
	{"a column": "b", "also a column": b"1", "number": -901, "date": date(2009, 1, 2)},
	{"a column": "c", "also a column": b"2", "number": -902, "date": date(2009, 1, 3)},
	{"a column": "d", "also a column": b"3", "number": -903, "date": date(2009, 1, 4)},
	{"a column": "e", "also a column": b"4", "number": -904, "date": date(2009, 1, 5)},
	{"a column": "f", "also a column": b"5", "number": -905, "date": date(2009, 1, 6)},
	{"a column": "g", "also a column": b"6", "number": -906, "date": date(2009, 1, 7)},
	{"a column": "h", "also a column": b"7", "number": -907, "date": date(2009, 1, 8)},
	{"a column": "i", "also a column": b"8", "number": -908, "date": date(2009, 1, 9)},
	{"a column": "j", "also a column": b"9", "number": -909, "date": date(2009, 2, 1)},
	{"a column": "k", "also a column": b"Z", "number": -999, "date": date(2009, 3, 1)},
	{"a column": "l", "also a column": b"Y", "number": -989, "date": date(2009, 4, 1)},
	{"a column": "m", "also a column": b"X", "number": -979, "date": date(2009, 5, 1)},
	{"a column": "n", "also a column": b"W", "number": -969, "date": date(2009, 6, 1)},
	{"a column": "o", "also a column": b"V", "number": -959, "date": date(2009, 7, 1)},
	{"a column": "p", "also a column": b"A", "number": -949, "date": date(2009, 8, 1)},
	{"a column": "q", "also a column": b"B", "number": -939, "date": date(2009, 9, 1)},
	{"a column": "r", "also a column": b"C", "number": -929, "date": None},
	{"a column": "s", "also a column": b"D", "number": None, "date": date(1970, 1, 1)},
	{"a column": "B", "also a column": None, "number": -242, "date": date(1970, 2, 3)},
	{"a column": None, "also a column": b"F", "number": -123, "date": date(1970, 4, 1)},
]
bonus_data = [
	{"a column": "foo", "also a column": b"bar", "number": 42, "date": date(2019, 4, 10)},
]
columns = {
	"a column": ("ascii", True),
	"also a column": ("bytes", True),
	"number": ("int32", True),
	"date": ("date", True),
}

def write(data, columns=columns, **kw):
	dw = DatasetWriter(columns=columns, **kw)
	w = dw.get_split_write_dict()
	for values in data:
		w(values)
	return dw.finish()

def verify(slices, data, source, previous=None, **options):
	jid = subjobs.build(
		"dataset_hashpart",
		datasets=dict(source=source, previous=previous),
		options=options,
	)
	hl = options["hashlabel"]
	h = typed_writer(columns[hl][0]).hash
	ds = Dataset(jid)
	good = {row[hl]: row for row in data}
	names = list(source.columns)
	for slice in range(slices):
		for row in ds.iterate_chain(slice, names):
			row = dict(zip(names, row))
			assert h(row[hl]) % slices == slice, f"row {row!r} is incorrectly in slice {slice} in {ds}"
			want = good[row[hl]]
			assert row == want, f'{ds} (rehashed from {source}) did not contain the right data for "{hl}".\nWanted\n{want!r}\ngot\n{row!r}'
	want_lines = len(data)
	got_lines = ds.chain().lines()
	assert got_lines == want_lines, f'{ds} (rehashed from {source}) had {got_lines} lines, should have had {want_lines}'
	return ds

def verify_empty(source, previous=None, **options):
	jid = subjobs.build(
		"dataset_hashpart",
		datasets=dict(source=source, previous=previous),
		options=options,
	)
	ds = Dataset(jid)
	chain = ds.chain_within_job()
	assert list(chain.iterate(None)) == [], f"source={source} previous={previous} did not produce empty dataset in {ds}"
	assert chain[0].previous == previous, f"Empty {ds} should have had previous={previous}, but had {chain[0].previous}"

def synthesis(params):
	ds = write(data)
	for colname in data[0]:
		verify(params.slices, data, ds, hashlabel=colname)
	# ok, all the hashing stuff works out, let's test the chaining options.
	bonus_ds = write(bonus_data, name="bonus", previous=ds)
	# no chaining options - full chain
	verify(params.slices, data + bonus_data, bonus_ds, hashlabel="date")
	# just the bonus ds
	verify(params.slices, bonus_data, bonus_ds, hashlabel="date", length=1)
	# built as a chain
	verify(params.slices, data + bonus_data, bonus_ds, hashlabel="date", chain_slices=True)
	# normal chaining
	a = verify(params.slices, data, ds, hashlabel="date")
	b = verify(params.slices, data + bonus_data, bonus_ds, hashlabel="date", previous=a)
	assert b.chain() == [a, b], f"chain of {b} is not [{a}, {b}] as expected"
	# chain_slices sparseness
	empty = write([], name="empty")
	verify_empty(empty, hashlabel="date")
	verify_empty(empty, hashlabel="date", chain_slices=True)
	ds = verify(params.slices, [], empty, hashlabel="date", chain_slices=True)
	# two populated slices with the same data, should end up in two datasets.
	dw = DatasetWriter(columns=columns, name="0 and 2", allow_missing_slices=True)
	dw.set_slice(0)
	dw.write_dict(data[0])
	dw.set_slice(2)
	dw.write_dict(data[0])
	ds = verify(params.slices, [data[0], data[0]], dw.finish(), hashlabel="date", chain_slices=True)
	got_slices = len(ds.chain())
	assert got_slices == params.slices, f"{ds} (built with chain_slices=True) has {got_slices} datasets in chain, expected {params.slices}."

	# test varying types and available columns over the chain (including the hashlabel type)
	v1 = write([{'a': '101', 'b':  201 }], columns={'a': 'ascii',  'b': 'int32'}, name='varying1')
	v2 = write([{'a':  102 , 'c': '202'}], columns={'a': 'number', 'c': 'ascii'}, name='varying2', previous=v1)
	v3 = write([{'a':  103             }], columns={'a': 'int32'               }, name='varying3', previous=v2)
	hashed_varying = subjobs.build("dataset_hashpart", source=v3, hashlabel="a").dataset().chain()
	assert len(hashed_varying) == 3
	for unhashed, hashed in zip([v1, v2, v3], hashed_varying):
		assert {n: dc.type for n, dc in unhashed.columns.items()} == {n: dc.type for n, dc in hashed.columns.items()}
		assert list(unhashed.iterate(0)) == list(hashed.iterate(None))
		assert hashed.hashlabel == 'a'
		hash_t = unhashed.columns['a'].type
		hash_v = next(unhashed.iterate(0, 'a'))
		want_slice = typed_writer(hash_t).hash(hash_v) % params.slices
		assert hashed.lines[want_slice] == 1
