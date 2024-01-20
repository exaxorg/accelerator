# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2021-2024 Carl Drougge                                     #
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

description = r'''
Test dataset_fanout with varying types, hashlabel and chain truncation.
'''

from accelerator import subjobs
from accelerator.compat import unicode

from itertools import cycle

def synthesis(job):
	def mk(name, types, lines, hashlabel=None, previous=None):
		columns = {chr(ix): typ for ix, typ in enumerate(types, 65)}
		dw = job.datasetwriter(name=name, columns=columns, hashlabel=hashlabel, previous=previous)
		w = dw.get_split_write_list()
		for line in lines:
			w(line)
		return dw.finish()

	def chk(job, colnames, types, ds2lines, previous={}, hashlabel=None):
		have_ds = set(ds.name for ds in job.datasets)
		want_ds = set(ds2lines)
		assert have_ds == want_ds, 'Job %r should have had datasets %r but had %r' % (job, want_ds, have_ds,)
		colnames = sorted(colnames)
		for ds, lines in ds2lines.items():
			ds = job.dataset(ds)
			assert ds.hashlabel == hashlabel, 'Dataset %s should have had hashlabel %s but had %s' % (ds.quoted, hashlabel, ds.hashlabel,)
			assert ds.previous == previous.get(ds.name), 'Dataset %s should have had previous %s but had %s' % (ds.quoted, previous.get(ds.name), ds.previous,)
			ds_colnames = sorted(ds.columns)
			assert ds_colnames == colnames, 'Dataset %s should have had columns %r but had %r' % (ds.quoted, colnames, ds_colnames,)
			ds_types = tuple(col.type for _, col in sorted(ds.columns.items()))
			assert ds_types == types, 'Dataset %s should have had columns with types %r but had %r' % (ds.quoted, types, ds_types,)
			have_lines = sorted(ds.iterate(None))
			want_lines = sorted(lines)
			assert have_lines == want_lines, 'Dataset %s should have contained %r but contained %r' % (ds.quoted, want_lines, have_lines,)

	# just a simple splitting
	a = mk('a', ('unicode', 'ascii', 'int64'), [('a', 'a', 1), ('b', 'b', 2), ('a', 'c', 3)], hashlabel='A')
	j_a_A = subjobs.build('dataset_fanout', source=a, column='A')
	chk(j_a_A, 'BC', ('ascii', 'int64'), {'a': [('a', 1), ('c', 3)], 'b': [('b', 2)]})
	j_a_B = subjobs.build('dataset_fanout', source=a, column='B')
	chk(j_a_B, 'AC', ('unicode', 'int64'), {'a': [('a', 1)], 'b': [('b', 2)], 'c': [('a', 3)]}, hashlabel='A')

	# non-text columns should work too
	j_a_C = subjobs.build('dataset_fanout', source=a, column='C')
	chk(j_a_C, 'AB', ('unicode', 'ascii'), {'1': [('a', 'a')], '2': [('b', 'b')], '3': [('a', 'c')]}, hashlabel='A')

	b = mk('b', ('ascii', 'unicode', 'int32', 'int32'), [('a', 'aa', 11, 111), ('b', 'bb', 12, 112), ('a', 'cc', 13, 113), ('d', 'dd', 14, 114)], previous=a)
	# with previous
	j_b_A = subjobs.build('dataset_fanout', source=b, column='A', previous=j_a_A)
	chk(
		j_b_A,
		'BCD',
		('unicode', 'int32', 'int32'),
		{'a': [('aa', 11, 111), ('cc', 13, 113)], 'b': [('bb', 12, 112)], 'd': [('dd', 14, 114)]},
		previous={'a': j_a_A.dataset('a'), 'b': j_a_A.dataset('b')},
	)

	# without previous, but only getting the data from b because of length=1
	j_b_A_len1 = subjobs.build('dataset_fanout', source=b, column='A', length=1)
	chk(
		j_b_A_len1,
		'BCD',
		('unicode', 'int32', 'int32'),
		{'a': [('aa', 11, 111), ('cc', 13, 113)], 'b': [('bb', 12, 112)], 'd': [('dd', 14, 114)]},
	)

	# with "wrong" previous, inheriting some empty datasets.
	j_b_A_C = subjobs.build('dataset_fanout', source=b, column='A', previous=j_a_C)
	chk(
		j_b_A_C,
		'BCD',
		('unicode', 'int32', 'int32'),
		{'a': [('aa', 11, 111), ('cc', 13, 113)], 'b': [('bb', 12, 112)], 'd': [('dd', 14, 114)], '1': [], '2': [], '3': []},
		previous={'1': j_a_C.dataset('1'), '2': j_a_C.dataset('2'), '3': j_a_C.dataset('3')},
	)

	# without previous, getting data from both a and b and the "widest" type for the columns.
	# (discards the D column since it doesn't exist in a.)
	j_b_A_None = subjobs.build('dataset_fanout', source=b, column='A')
	chk(
		j_b_A_None,
		'BC',
		('unicode', 'int64'),
		{'a': [('a', 1), ('aa', 11), ('c', 3), ('cc', 13)], 'b': [('b', 2), ('bb', 12)], 'd': [('dd', 14)]},
	)

	# test more type combinations, and switching hashlabel (to an included column)
	tt_a = mk(
		'tt_a',
		('ascii', 'int32', 'float32', 'number', 'complex32', 'number'),
		[('a', 1, 2.5, 3, 1+2j, 3.14)],
		hashlabel='B',
	)
	tt_b = mk(
		'tt_b',
		('ascii', 'int64', 'float64', 'int32', 'complex64', 'float64'),
		[('a', 11, 12.5, 13, 11+2j, 13.14)],
		hashlabel='B',
		previous=tt_a,
	)
	tt_c = mk(
		'tt_c',
		('ascii', 'int32', 'int64', 'float64', 'complex32', 'float32'),
		[('a', 111, 112, 113.5, 111+2j, 314.0), ('b', 0, 0, 0, 0, 0)],
		hashlabel='C',
		previous=tt_b,
	)

	# first two, some type changes
	j_tt_b = subjobs.build('dataset_fanout', source=tt_b, column='A')
	chk(
		j_tt_b,
		'BCDEF',
		('int64', 'float64', 'number', 'complex64', 'number'),
		{'a': [(1, 2.5, 3, 1+2j, 3.14), (11, 12.5, 13, 11+2j, 13.14)]},
		hashlabel='B',
	)

	# all three in one, more types become number
	j_tt_c = subjobs.build('dataset_fanout', source=tt_c, column='A')
	chk(
		j_tt_c,
		'BCDEF',
		('int64', 'number', 'number', 'complex64', 'number'),
		{'a': [(1, 2.5, 3, 1+2j, 3.14), (11, 12.5, 13, 11+2j, 13.14), (111, 112, 113.5, 111+2j, 314.0)], 'b': [(0, 0, 0, 0, 0)]},
		hashlabel=None,
	)

	# just two (checking that earlier types are not considered)
	j_tt_c_len2 = subjobs.build('dataset_fanout', source=tt_c, column='A', length=2)
	chk(
		j_tt_c_len2,
		'BCDEF',
		('int64', 'number', 'number', 'complex64', 'float64'),
		{'a': [(11, 12.5, 13, 11+2j, 13.14), (111, 112, 113.5, 111+2j, 314.0)], 'b': [(0, 0, 0, 0, 0)]},
		hashlabel=None,
	)

	# using previous to only get one source dataset, again checking that earlier
	# types are not considered and that only a gets a previous (and b doesn't)
	j_tt_c_b = subjobs.build('dataset_fanout', source=tt_c, column='A', previous=j_tt_b)
	chk(
		j_tt_c_b,
		'BCDEF',
		('int32', 'int64', 'float64', 'complex32', 'float32'),
		{'a': [(111, 112, 113.5, 111+2j, 314.0)], 'b': [(0, 0, 0, 0, 0)]},
		hashlabel='C',
		previous={'a': j_tt_b.dataset('a')},
	)

	# it generally works, let's make an exhaustive test of compatible types
	# (to check that the values actually are compatible)
	previous = None
	all_types = []
	want_data = []
	for ix, types in enumerate(zip(
		cycle(['ascii']), # this is the split column
		['int32', 'int64', 'float32', 'float64', 'number'],
		cycle(['complex64', 'complex32']),
		cycle(['float64', 'float32']),
		cycle(['int64', 'int32']),
		cycle(['unicode', 'ascii']),
	)):
		data = [('data',) + (ix + 1000,) * 4 + (unicode(ix),)]
		want_data.append(data[0][1:])
		all_types.append(
			mk('all types %d' % (ix,), types, data, previous=previous)
		)
		previous = all_types[-1]

	j_all = subjobs.build('dataset_fanout', source=all_types[-1], column='A')
	chk(
		j_all,
		'BCDEF',
		('number', 'complex64', 'float64', 'int64', 'unicode'),
		{'data': want_data},
	)

	# the B column doesn't have number any more here, but should still become number.
	j_all_except_number = subjobs.build('dataset_fanout', source=all_types[-2], column='A')
	chk(
		j_all_except_number,
		'BCDEF',
		('number', 'complex64', 'float64', 'int64', 'unicode'),
		{'data': want_data[:-1]},
	)
