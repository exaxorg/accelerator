############################################################################
#                                                                          #
# Copyright (c) 2021 Carl Drougge                                          #
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

description = """
Tests that [] in datasets and jobs works, giving a DatasetList and a JobList.

Also tests JobList[method], DatasetList.lines, .iterate and .range.
"""


datasets = (['dslist'],)
jobs = (['joblist'],)

def names(dslist):
	return [ds.name for ds in dslist]

def synthesis(slices):
	assert len(jobs.joblist) == 2
	assert len(jobs.joblist['test_datasetwriter_copy'].datasets) == 7

	assert len(datasets.dslist) == 6
	assert datasets.dslist.lines() == 6 * 3
	assert names(datasets.dslist) == ['a0', 'a1', 'b0', 'b1', 'c0', 'c1']
	assert set(datasets.dslist.iterate(None, '1')) == {2, 4, 6, 8, 10, 12, 13, 15, 17, 19, 21, 23}
	assert names(datasets.dslist.range('1', 12, 13)) == ['a1', 'c1']
	assert names(datasets.dslist.range('1', 12, 14)) == ['a1', 'b0', 'c1']
	assert names(datasets.dslist.range('1', start=23)) == ['b1']
	assert names(datasets.dslist.range('1', stop=8)) == ['a0', 'c0']
