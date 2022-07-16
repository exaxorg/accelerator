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

from accelerator import subjobs

def synthesis(job):
	dw = job.datasetwriter(name='a')
	dw.add('a', 'ascii')
	dw.add('b', 'bits32')
	dw.get_split_write()('a', 18)
	a = dw.finish()
	dw = job.datasetwriter(name='b', previous=a)
	dw.add('a', 'ascii')
	dw.add('b', 'bits32')
	dw.add('c', 'bits64')
	w = dw.get_split_write()
	w('b', 0xffffffff, 42)
	w('c', 0, 0xffffffffffffffff)
	b = dw.finish()
	# and one to make sure not having any bits types is ok
	dw = job.datasetwriter(name='c', previous=b)
	dw.add('a', 'ascii')
	dw.get_split_write()('c')
	c = dw.finish()
	aa = subjobs.build('dataset_unbits', source=a).dataset()
	bb = subjobs.build('dataset_unbits', source=b, previous=aa).dataset()
	cc = subjobs.build('dataset_unbits', source=c, previous=bb).dataset()
	assert [col.type for _, col in sorted(aa.columns.items())] == ['ascii', 'int64']
	assert [col.type for _, col in sorted(bb.columns.items())] == ['ascii', 'int64', 'number']
	assert [col.type for _, col in sorted(cc.columns.items())] == ['ascii']
	assert aa.previous == None
	assert bb.previous == aa
	assert cc.previous == bb
	assert list(aa.iterate(None)) == [('a', 18)]
	assert list(bb.iterate(None)) == [('b', 0xffffffff, 42), ('c', 0, 0xffffffffffffffff)]
	assert list(bb.iterate_chain(None, 'b')) == [18, 0xffffffff, 0]
	assert list(cc.iterate(None)) == [('c',)]
