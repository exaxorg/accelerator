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
Concatenate a dataset chain into a single dataset.

Reads datasets.source until datasets.previous: 'source', so you can chain
these jobs easily.

This is a performance optimisation, when you are willing to pay some time
now to avoid the cost of switching datasets later.
'''

datasets = ('source', 'previous',)

def prepare(job):
	chain = datasets.source.chain(stop_ds={datasets.previous: 'source'})
	columns = {name: col.type for name, col in datasets.source.columns.items()}
	hashlabel = datasets.source.hashlabel
	for ds in chain:
		if columns != {name: col.type for name, col in ds.columns.items()}:
			raise Exception('Dataset %s does not have the same columns as %s' % (ds.quoted, datasets.source.quoted,))
		if hashlabel != ds.hashlabel:
			raise Exception('Dataset %s has hashlabel %r, expected %r' % (ds.quoted, ds.hashlabel, hashlabel,))
	dw = job.datasetwriter(hashlabel=hashlabel, previous=datasets.previous, copy_mode=True)
	for name, t in sorted(columns.items()):
		dw.add(name, t, none_support=chain.none_support(name))
	return dw, chain

def analysis(sliceno, job, prepare_res):
	dw, chain = prepare_res
	write = dw.write
	for items in chain.iterate(sliceno, copy_mode=True):
		write(*items)
