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

description = r'''
Test the dataset_concat method.
'''

from datetime import date, time, datetime
from itertools import chain

from accelerator import subjobs, JobError
from accelerator.compat import PY2
from accelerator.dsutil import _type2iter

def synthesis(job):
	# Test using all types
	types = {
		'a': 'ascii',
		'b': 'bool',
		'c': 'bytes',
		'd': 'complex32',
		'e': 'complex64',
		'f': 'date',
		'g': 'datetime',
		'h': 'float32',
		'i': 'float64',
		'j': 'int32',
		'k': 'int64',
		'l': 'json',
		'm': 'number',
		'n': 'pickle',
		'o': 'time',
		'p': 'unicode',
	}
	missing = set(_type2iter) - set(types.values())
	assert not missing, missing
	if PY2:
		del types['n'] # no pickle type on python2

	def data(ix):
		d = {
			'a': '%d' % (ix,),
			'b': bool(ix % 2),
			'c': b'%d' % (ix,),
			'd': complex(0, ix),
			'e': complex(0, -ix),
			'f': date(2000 + ix // 12, ix % 12 + 1, 1),
			'g': datetime.fromtimestamp(ix),
			'h': 1.0 / (ix + 1),
			'i': 2.0 / (ix + 1),
			'j': ix,
			'k': -ix,
			'l': {'json': ix},
			'm': -1.0 / (ix + 1),
			'n': {ix},
			'o': time(0, ix // 60 % 60, ix % 60),
			'p': u'%d' % (ix,),
			'extra': 0,
		}
		return {k: v for k, v in d.items() if k in types}

	# default to hashlabel='b' so that only two slices get data
	def mk_ds(name, num, hashlabel='b', empty=False, **kw):
		dw = job.datasetwriter(name=name, columns=types, hashlabel=hashlabel, **kw)
		write = dw.get_split_write()
		if not empty:
			for ix in range(100 * num, 100 * num + 100):
				write(**data(ix))
		return dw.finish()

	previous = None
	for num in range(8):
		previous = mk_ds(str(num), num, previous=previous)
	# we want one empty to make sure nothing breaks from that
	empty = mk_ds('empty', 0, empty=True, previous=previous)
	assert len(list(empty.iterate(None))) == 0
	last = mk_ds('8', 8, previous=empty)
	numbered = last.chain()
	assert len(numbered) == 10

	# Verify contents, chaining and types (including none_support)
	def chk(source, previous, want_in_chain, want, do_sort=True, none_support=()):
		ds = subjobs.build('dataset_concat', source=source, previous=previous).dataset()
		assert ds.chain() == want_in_chain + [ds]
		want = list(chain(*[w.iterate(None) for w in want]))
		got = list(ds.iterate(None))
		if do_sort:
			got.sort()
			want.sort()
		assert want == got, source
		want_types = {k: (v, k in none_support) for k, v in types.items()}
		got_types = {k: (v.type, v.none_support) for k, v in ds.columns.items()}
		assert want_types == got_types, source
		return ds

	a = chk(numbered[3], None, [], numbered[:4])
	b = chk(numbered[6], a, [a], numbered[4:7])
	# c uses just one dataset, so order should not change from source
	c = chk(numbered[7], b, [a, b], [numbered[7]], do_sort=False)
	# make sure an empty dataset alone is fine
	chk(empty, c, [a, b, c], [empty])
	# and that an empty dataset as part of the chain is fine
	chk(last, b, [a, b], numbered[7:])
	# empty last in chain, equivalent to c above
	chk(empty, b, [a, b], [numbered[7]], do_sort=False)
	# empty first in chain
	chk(last, c, [a, b, c], numbered[8:], do_sort=False)

	# Test some things that should fail
	def want_fail(why, **kw):
		try:
			subjobs.build('dataset_concat', **kw)
			raise Exception('dataset_concat(%r) should have failed: %s' % (kw, why,))
		except JobError:
			pass

	bad_hl = mk_ds('bad_hl', 8, hashlabel='a', previous=numbered[7])
	want_fail('differing hashlabels', source=bad_hl)

	types['extra'] = 'int64'
	extra_col = mk_ds('extra_col', 8, previous=numbered[7])
	want_fail('extra column', source=extra_col)
	del types['extra']

	del types['k']
	missing_col = mk_ds('missing_col', 8, previous=numbered[7])
	want_fail('column k missing', source=missing_col)

	types['k'] = 'number' # should be int64
	bad_type = mk_ds('bad_type', 8, previous=numbered[7])
	want_fail('different types on column k', source=bad_type)

	# Test that having a none_support somewhere is inherited
	types['k'] = ('int64', True)
	with_none = mk_ds('with_none', 8, previous=numbered[7])
	types['k'] = 'int64'
	without_none = mk_ds('without_none', 9, previous=with_none)
	chk(without_none, c, [a, b, c], [with_none, without_none], none_support="k")
