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
Test the range argument to the dataset iteration functions
and chain.with_column().
'''

from datetime import date

date_a = date(2022, 1, 1)
date_b = date(2022, 1, 2)
date_c = date(2022, 1, 3)
date_d = date(2022, 1, 4)

def test(iterfun):
	# verify it contains the expected data
	assert list(iterfun(0, 'a')) == [1, 2, 21, 22, 301, 302]
	assert list(iterfun(0, 'b')) == [1001, 1002, 1021, 1022, 1301, 1302]
	# with the single used column
	assert list(iterfun(0, 'a', range={'a': (2, 22)})) == [2, 21]
	# with the single used column, as a list
	assert list(iterfun(0, ['a'], range={'a': (2, 22)})) == [(2,), (21,)]
	# with the first of several columns
	assert list(iterfun(0, ['a', 'b'], range={'a': (2, 22)})) == [(2, 1002), (21, 1021)]
	# with another of several columns
	assert list(iterfun(0, ['a', 'b'], range={'b': (1002, 1022)})) == [(2, 1002), (21, 1021)]
	# with a column that isn't returned
	assert list(iterfun(0, 'a', range={'b': (1002, 1022)})) == [2, 21]
	# ... as a list
	assert list(iterfun(0, ['a'], range={'b': (1002, 1022)})) == [(2,), (21,)]
	# sloppy range
	assert list(iterfun(0, 'a', range={'a': (2, 22)}, sloppy_range=True)) == [1, 2, 21, 22] # not really guaranteed, but the current implementation does
	# no lower limit
	assert list(iterfun(0, 'a', range={'a': (None, 5)})) == [1, 2]
	# no upper limit
	assert list(iterfun(0, 'a', range={'a': (22, None)})) == [22, 301, 302]
	# no limit at all, despite setting range
	assert list(iterfun(0, 'a', range={'a': (None, None)})) == [1, 2, 21, 22, 301, 302]

def synthesis(job):
	a = job.datasetwriter(name='a', columns={'a': 'int32', 'b': 'int32'}, allow_missing_slices=True)
	b = job.datasetwriter(name='b', columns={'a': 'int32', 'b': 'int64', 'c': 'date'}, allow_missing_slices=True, previous=a)
	c = job.datasetwriter(name='c', columns={'a': 'int64', 'b': 'number', 'c': ('date', True)}, allow_missing_slices=True, previous=b)
	a.set_slice(0)
	a.write(1, 1001)
	a.write(2, 1002)
	a = a.finish()
	b.set_slice(0)
	b.write(21, 1021, date_a)
	b.write(22, 1022, date_b)
	b = b.finish()
	c.set_slice(0)
	c.write(301, 1301, date_c)
	c.write(302, 1302, date_d)
	c = c.finish()

	test(c.iterate_chain)
	# test range on plain ds.iterate
	assert list(c.iterate(0, 'a', range={'a': (None, 302)})) == [301]
	# return several columns but filter on a non-returned one
	assert list(c.iterate(0, ['a', 'c'], range={'b': (None, 1302)})) == [(301, date_c)]
	# filter on a datetime type (a special case in the range check code)
	assert list(c.iterate(0, 'a', range={'c': (None, date_d)})) == [301]
	# really should be the same as c.iterate_chain
	chain = c.chain()
	test(chain.iterate)

	# test the chain range filter
	assert list(chain.range('a', 22, 301).iterate(0, 'a')) == [21, 22]
	assert list(chain.range('a', 22, None).iterate(0, 'a')) == [21, 22, 301, 302]
	assert list(chain.range('a', None, 22).iterate(0, 'a')) == [1, 2, 21, 22]
	# test the chain column filter
	assert list(chain.with_column('c').iterate(0, 'a')) == [21, 22, 301, 302]
	assert list(chain.with_column('b', 'int64').iterate(0, 'a')) == [21, 22]
	assert list(chain.with_column('b', ('int32', 'int64')).iterate(0, 'a')) == [1, 2, 21, 22]
	assert list(chain.with_column('c', none_support=False).iterate(0, 'a')) == [21, 22]
	assert list(chain.with_column('c', none_support=True).iterate(0, 'a')) == [301, 302]
