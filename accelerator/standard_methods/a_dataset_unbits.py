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
Convert bits32 columns to int64 and bits64 columns to number.

This exists because the bits types will go away in the next release.
'''

datasets = ('source', 'previous',)

def prepare(job):
	columns = {}
	for name, col in datasets.source.columns.items():
		if col.type.startswith('bits'):
			columns[name] = ('int64' if col.type == 'bits32' else 'number')
	if not columns:
		datasets.source.link_to_here(override_previous=datasets.previous)
		job.finish_early()
	dw = job.datasetwriter(columns=columns, previous=datasets.previous, parent=datasets.source)
	return dw, sorted(columns)

def analysis(sliceno, prepare_res):
	dw, columns = prepare_res
	write = dw.write
	for line in datasets.source.iterate(sliceno, columns):
		write(*line)
