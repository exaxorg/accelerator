############################################################################
#                                                                          #
# Copyright (c) 2022 Carl Drougge                                          #
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
	for t, good_value, default_value in (
		('int64', 1, 2),
		('int32', 1, 2),
		('bits64', 1, 2),
		('bits32', 1, 2),
		('float64', 0.1, 0.2),
		('float32', 1, 2),
		('number', 1, 2.1),
		('complex64', 0.1+1j, 2-0.2j),
		('complex32', 1+1j, 2-2j),
		('bool', False, True),
		('datetime', now, later),
		('date', now.date(), later.date()),
		('time', now.time(), later.time()),
	):
		dw = job.datasetwriter(name=t, allow_missing_slices=True)
		dw.add('data', t, default=default_value)
		dw.set_slice(0)
		dw.write(good_value)
		dw.write(())
		ds = dw.finish()
		want = [good_value, default_value]
		got = list(ds.iterate(0, 'data'))
		assert got == want, '%s failed, wanted %r but got %r' % (ds.quoted, want, got,)
