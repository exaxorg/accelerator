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
Test DatasetWriter.add(..., default=...)
'''

from datetime import datetime, timedelta

def synthesis(job):
	now = datetime.now()
	later = now + timedelta(days=1, hours=1)
	for   t         , good_value    , default_value, bad1        , bad2 in (
		('int64'    , 1             , 2            , float('inf'), None),
		('int32'    , 1             , 2            , float('inf'), None),
		('float64'  , 0.1           , 0.2          , 'bad'       , None),
		('float32'  , 1             , 2            , 'bad'       , None),
		('number'   , 1             , 2.1          , 'bad'       , None),
		('complex64', 0.1+1j        , 2-0.2j       , 'bad'       , None),
		('complex32', 1+1j          , 2-2j         , 'bad'       , None),
		('bool'     , False         , True         , 2           , None),
		('datetime' , now           , later        , 95          , None),
		('date'     , now.date()    , later.date() , ()          , None),
		('time'     , now.time()    , later.time() , ''          , None),
		('json'     , {'foo': 'bar'}, [1, 2, 3]    , datetime    , now),
		('ascii'    , 'foo'         , 'bar'        , u'\xe4'     , b'\xe4'),
		('bytes'    , b'foo'        , b'bar'       , u'baz'      , None),
		('unicode'  , u'\xe4'       , u'\xe5'      , b'baz'      , None),
	):
		for bad_default in (bad1, bad2):
			dw = job.datasetwriter(name='failme')
			try:
				dw.add('data', t, default=bad_default)
				dw.get_split_write()
				raise Exception('%s accepted %r as default value' % (t, bad_default,))
			except (TypeError, ValueError, OverflowError):
				pass
			dw.discard()
		dw = job.datasetwriter(name=t, allow_missing_slices=True)
		dw.add('data', t, default=default_value)
		dw.set_slice(0)
		dw.write(good_value)
		dw.write(bad1)
		dw.write(bad2)
		ds = dw.finish()
		want = [good_value, default_value, default_value]
		got = list(ds.iterate(0, 'data'))
		assert got == want, '%s failed, wanted %r but got %r' % (ds.quoted, want, got,)

		dw = job.datasetwriter(name=t + ' default=None', allow_missing_slices=True)
		dw.add('data', t, default=None, none_support=True)
		dw.set_slice(0)
		dw.write(good_value)
		dw.write(bad1)
		dw.write(bad2)
		ds = dw.finish()
		want = [good_value, None, None]
		got = list(ds.iterate(0, 'data'))
		assert got == want, '%s failed, wanted %r but got %r' % (ds.quoted, want, got,)

		# make sure default=None hashes correctly
		if t != 'json':
			dw = job.datasetwriter(name=t + ' default=None hashed', hashlabel='data')
			dw.add('data', t, default=None, none_support=True)
			w = dw.get_split_write()
			w(bad1)
			w(None)
			w(bad1) # bad2 might be None, so don't use that.
			ds = dw.finish()
			want = [None, None, None]
			got = list(ds.iterate(0, 'data'))
			assert got == want, '%s slice 0 failed, wanted %r but got %r' % (ds.quoted, want, got,)
