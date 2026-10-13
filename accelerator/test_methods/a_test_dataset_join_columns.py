############################################################################
#                                                                          #
# Copyright (c) 2026 Carl Drougge                                          #
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
Test the dataset_join_columns method, with various types and column counts,
in chains with varying types, with and without None.
'''

from accelerator import subjobs
from datetime import datetime


def synthesis(job):
	def mk(name, types, values, **kw):
		dw = job.datasetwriter(name=name, allow_missing_slices=True, **kw)
		for ix, typ in enumerate(types, 1):
			dw.add(str(ix), typ)
		dw.set_slice(0)
		for v in values:
			dw.write(*v)
		return dw.finish()

	def chk(want, **kw):
		chain = subjobs.build('dataset_join_columns', **kw).dataset().chain_within_job()
		assert len(chain) == len(want), chain
		if 'previous' in kw:
			assert chain[0].previous == kw['previous']
		for ds, want in zip(chain, want):
			want = list(want)
			got = list(ds.iterate(0, kw.get('colname', 'joined column')))
			assert got == want, f"Bad data in {ds.quoted}\n  wanted {want}\n  got    {got}"
		return chain

	unicode_vl = [
		['a', 'b', 'c'],
		['x', 'y', 'z'],
		['foo', 'bar', 'baz'],
	]
	bytes_vl = [
		[None if v is None else v.encode('utf-8') for v in values]
		for values in unicode_vl
	]
	datetime_vl = [
		datetime(1978, 1, 1, 2, 0, 0),
		datetime(2001, 2, 3, 4, 5, 6, 7),
		datetime(2025, 11, 17, 1, 19, 9, 283153),
	]
	date_time_vl = [[dt.date(), dt.time()] for dt in datetime_vl]
	time_date_vl = [[dt.time(), dt.date()] for dt in datetime_vl]

	for with_none in [False, True]:
		if with_none:
			unicode_vl[1][1] = None
			bytes_vl[1][1] = None
			date_time_vl[1][1] = None
			time_date_vl[1][1] = None
			datetime_vl[1] = None
			suffix = ' none'
			# Second column has none_support
			typ3 = lambda t: [t, (t, True), t]
			none1maybe = lambda *values: [[v[0], None] + v[2:] for v in values]
		else:
			suffix = ''
			typ3 = lambda t: [t, t, t]
			none1maybe = lambda *values: list(values)

		ds_bytes = mk('bytes' + suffix, typ3('bytes'), bytes_vl)
		ds_ascii = mk('ascii' + suffix, typ3('ascii'), unicode_vl, previous=ds_bytes)
		ds_unicode = mk('unicode' + suffix, typ3('unicode'), unicode_vl, previous=ds_ascii)
		ds_date_time = mk('date time' + suffix, ['date', ('time', with_none)], date_time_vl, previous=ds_unicode)
		ds_time_date = mk('time date' + suffix, ['time', ('date', with_none)], time_date_vl, previous=ds_date_time)

		# Join two columns with a separator (which is ignored for datetimes)
		joined_strs = ['b a', 'y x', 'bar foo']
		joined_bytes = [v.encode('utf-8') for v in joined_strs]
		want = none1maybe(joined_bytes, joined_strs, joined_strs, datetime_vl, datetime_vl)
		chain = chk(want, source=ds_time_date, columns=['2', '1'], separator=' ', colname='x')
		assert all(set(ds.columns) == {'1', '2', '3', 'x'} for ds in chain[:3]), chain
		assert all(set(ds.columns) == {'1', '2',      'x'} for ds in chain[3:]), chain
		assert all(ds.columns['x'].none_support == with_none for ds in chain), chain

		# Join four columns, two of which are the same, discarding the source columns
		joined_strs = ['cabc', 'zxyz', 'bazfoobarbaz']
		joined_bytes = [v.encode('utf-8') for v in joined_strs]
		want = none1maybe(joined_bytes, joined_strs, joined_strs)
		chain = chk(want, source=ds_unicode, columns=['3', '1', '2', '3'], discard_source_columns=True)
		assert all(set(ds.columns) == {'joined column'} for ds in chain), chain
		assert all(ds.columns['joined column'].none_support == with_none for ds in chain), chain

		# Mostly to check that previous= works correctly.
		chain = chk([datetime_vl, datetime_vl], source=ds_time_date, columns=['1', '2'], previous=chain[-1])
		assert all(set(ds.columns) == {'1', '2', 'joined column'} for ds in chain), chain
		assert all(ds.columns['joined column'].none_support == with_none for ds in chain), chain

	# ascii+ascii => ascii, ascii+unicode => unicode
	ds_mixed = mk('mixed', ['date', 'ascii', 'ascii', 'unicode'], [[datetime.now().date(), 'a', 'b', 'ä']])
	ds, = chk([['a&b']], source=ds_mixed, columns=['2', '3'], separator='&', discard_source_columns=True)
	assert set(ds.columns) == {'1', '4', 'joined column'}, ds
	assert ds.columns['joined column'].type == 'ascii', ds
	assert ds.columns['joined column'].none_support == False, ds
	ds, = chk([['b,ä']], source=ds_mixed, columns=['3', '4'], separator=',')
	assert ds.columns['joined column'].type == 'unicode', ds
	assert ds.columns['joined column'].none_support == False, ds

	# Only part of the chain needs none_support, check that the joined column retains this
	ds_mixed2 = mk('mixed2', ['time', 'ascii', ('ascii', True)], [[datetime.now().time(), 'c', None]], previous=ds_mixed)
	chain = chk([['a+b'], [None]], source=ds_mixed2, columns=['2', '3'], separator='+')
	assert all(ds.columns['joined column'].type == 'ascii' for ds in chain), chain
	assert [ds.columns['joined column'].none_support for ds in chain] == [False, True], chain

	# Can't join date or time with ascii
	from accelerator.error import JobError
	for source in [ds_mixed, ds_mixed2]:
		try:
			chk([['nope']], source=source, columns=['1', '2'])
		except JobError as e:
			assert any("Unjoinable column types" in msg for msg in e.status.values())
