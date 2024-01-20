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
Test the dataset_rename_columns method, and ds.link_to_here with rename
and column_filter.
'''

from accelerator import subjobs, DatasetUsageError

def synthesis(job):
	columns = dict(
		a='int32',
		b='int64',
		c='number',
	)
	type2value = dict(
		int32=1,
		int64=2,
		number=3,
	)
	dw = job.datasetwriter(name='a', hashlabel='a', columns=columns)
	dw.get_split_write()(1, 2, 3)
	a = dw.finish()
	dw = job.datasetwriter(name='b', hashlabel='b', columns=columns, previous=a)
	dw.get_split_write()(1, 2, 3)
	b = dw.finish()
	names = ('link%d' % (ix,) for ix in range(1000)) # more than enough
	def chk(ds, want_hashlabel, want_previous, want_coltypes, rename):
		got_job = subjobs.build('dataset_rename_columns', rename=rename, source=ds)
		chk_inner(got_job.dataset(), want_hashlabel, want_previous, want_coltypes)
		got_ds = ds.link_to_here(name=next(names), rename=rename)
		chk_inner(got_ds, want_hashlabel, want_previous, want_coltypes)
	def chk_inner(got_ds, want_hashlabel, want_previous, want_coltypes):
		assert got_ds.hashlabel == want_hashlabel
		assert got_ds.previous == want_previous
		got_cols = set(got_ds.columns)
		want_cols = set(want_coltypes)
		extra = got_cols - want_cols
		assert not extra, 'got extra columns %r' % (extra,)
		missing = want_cols - got_cols
		assert not missing, 'missing columns %r' % (missing,)
		for colname, want_type in want_coltypes.items():
			assert got_ds.columns[colname].type == want_type
			assert list(got_ds.iterate(None, colname)) == [type2value[want_type]]
	# just a simple rename
	chk(a, 'a', None, dict(a='int32', b='int64', d='number'), dict(c='d'))
	# rename the hashlabel
	chk(a, 'n', None, dict(n='int32', b='int64', c='number'), dict(a='n'))
	# rename the hashlabel to original name
	chk(a, 'a', None, dict(a='int32', b='int64', c='number'), dict(a='a'))
	# exchange two columns, one is the hashlabel
	chk(a, 'b', None, dict(a='int64', b='int32', c='number'), dict(a='b', b='a'))
	# rename over a column
	chk(b, 'b', a, dict(b='int64', c='int32'), dict(a='c'))
	# rename over hashlabel
	chk(b, None, a, dict(b='int32', c='number'), dict(a='b'))
	# discard hashlabel
	chk(b, None, a, dict(a='int32', c='number'), dict(b=None))
	# discard hashlabel, but also rename another column over it
	chk(b, None, a, dict(b='int32', c='number'), dict(b=None, a='b'))
	# discard a column, but also rename hashlabel to that name
	chk(b, 'a', a, dict(a='int64', c='number'), dict(a=None, b='a'))

	# try a few with column_filter too
	# rename hashlabel, only keep that
	got_ds = a.link_to_here(name=next(names), rename=dict(a='b'), column_filter='b')
	chk_inner(got_ds, 'b', None, dict(b='int32'))
	# discard hashlabel
	got_ds = a.link_to_here(name=next(names), column_filter='c')
	chk_inner(got_ds, None, None, dict(c='number'))
	# rename hashlabel but then discard it
	got_ds = a.link_to_here(name=next(names), rename=dict(a='b'), column_filter='c')
	chk_inner(got_ds, None, None, dict(c='number'))
	# rename over hashlabel, keep everthing but specify the column_filter
	got_ds = a.link_to_here(name=next(names), rename=dict(b='a'), column_filter='ac')
	chk_inner(got_ds, None, None, dict(a='int64', c='number'))

	# and try a few that should not be allowed
	def failme(ds, msg, **kw):
		try:
			ds.link_to_here('failme', **kw)
			raise Exception(msg)
		except DatasetUsageError:
			pass
	failme(a, 'renamed hashlabel, got to keep original name', rename=dict(a='b'), column_filter='ab')
	failme(a, 'renamed non-existant column', rename=dict(d='e'))
	failme(a, 'renamed two columns to the same name', rename=dict(a='c', b='c'))
	failme(a, 'got to keep non-existant column', column_filter='abcd')
