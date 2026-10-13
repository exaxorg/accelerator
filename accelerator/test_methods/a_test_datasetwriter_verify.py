# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2019-2026 Carl Drougge                                     #
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
Verify that each slice contains the expected data after test_datasetwriter.
'''

from datetime import date
from sys import version_info

from accelerator.dataset import Dataset
from accelerator.dsutil import typed_writer
from . import test_data

depend_extra=(test_data,)

datasets = ('source',)

def chk_cols(ds, want):
	have = set(ds.columns)
	for colname, spec in sorted(want.items()):
		assert colname in have, f"{ds.quoted} has no column named {colname}"
		have.remove(colname)
		if not isinstance(spec, tuple):
			spec = (spec, False, None)
		dc = ds.columns[colname]
		ds_spec = (dc.type, dc.none_support, dc.caption)
		assert spec == ds_spec, f"{ds.quoted} has wrong column {colname}\nwant {spec}\ngot  {ds_spec}"

def analysis(sliceno, params):
	want_cols = {"a": "number", "b": ("ascii", False, "the caption")}
	chk_cols(datasets.source, want_cols)
	assert list(datasets.source.iterate(sliceno, "a")) == [sliceno, 42]
	assert list(datasets.source.iterate(sliceno, "b")) == ["a", str(sliceno)]
	named = Dataset(datasets.source, "named")
	chk_cols(named, {"c": "bool", "d": "date"})
	assert list(named.iterate(sliceno, "c")) == [True, False]
	assert list(named.iterate(sliceno, "d")) == [date(1536, 12, min(sliceno + 1, 31)), date(2236, 5, min(sliceno + 1, 31))]
	if sliceno < test_data.value_cnt:
		passed = Dataset(datasets.source, "passed")
		chk_cols(passed, test_data.columns)
		good = tuple(v[sliceno] for _, v in sorted(test_data.data.items()))
		assert list(passed.iterate(sliceno)) == [good]
		if version_info > (3, 6, 0):
			want_fold = (sliceno == 1)
			assert next(passed.iterate(sliceno, "datetime")).fold == want_fold
			assert next(passed.iterate(sliceno, "time")).fold == want_fold
	synthesis_split = Dataset(datasets.source, "synthesis_split")
	chk_cols(synthesis_split, {"a": "int32", "b": "unicode"})
	values = zip((1, 2, 3,), "abc")
	hash = typed_writer("int32").hash
	good = [v for v in values if hash(v[0]) % params.slices == sliceno]
	assert list(synthesis_split.iterate(sliceno)) == good
	synthesis_manual = Dataset(datasets.source, "synthesis_manual")
	chk_cols(synthesis_manual, {"sliceno": "int32"})
	assert list(synthesis_manual.iterate(sliceno, "sliceno")) == [sliceno]
	nonetest = Dataset(datasets.source, "nonetest")
	chk_cols(nonetest, test_data.columns)
	good = (None,) * len(test_data.data)
	assert list(nonetest.iterate(sliceno)) == [good]
