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

description = r'''
Create one dataset per value in a column in a dataset (chain).

Reads source back to previous.source.

All datasets in previous get a dataset here, even if empty.
'''

from collections import defaultdict
import itertools
import re

from accelerator.compat import unicode, izip
from accelerator import OptionString, NoSuchDatasetError
from accelerator import subjobs, status


options = {
	'column': OptionString('split based on this column'),
	'length': -1, # Go back at most this many datasets. You almost always want -1 (which goes until previous.source)
}

datasets = ('source',)

jobs = ('previous',)


def prepare(job):
	chain = datasets.source.chain(stop_ds={jobs.previous: 'source'}, length=options.length)
	columns = defaultdict(set)
	none_support = defaultdict(bool)
	hashlabel = set()
	seen_all = set(chain[0].columns)
	for ds in chain:
		if options.column not in ds.columns:
			raise Exception('%r does not have column %r' % (ds, options.column,))
		hashlabel.add(ds.hashlabel)
		seen_all &= set(ds.columns)
		for name, col in ds.columns.items():
			columns[name].add(col.type)
			none_support[name] |= col.none_support
	seen_all.discard(options.column)
	if not seen_all:
		raise Exception('Chain has no common columns (except %r)' % (options.column,))
	columns = {k: columns[k] for k in seen_all}
	if len(hashlabel) == 1:
		hashlabel = hashlabel.pop()
		if hashlabel == options.column:
			hashlabel = None
	else:
		hashlabel = None
	for name, types in columns.items():
		# this relies on internal knowledge that copy_mode is compatible for these types.
		if 'unicode' in types:
			types.discard('ascii')
		if 'number' in types:
			types.discard('int32')
			types.discard('int64')
			types.discard('float32')
			types.discard('float64')
		if 'complex64' in types:
			types.discard('complex32')
		if 'float64' in types:
			types.discard('float32')
		if 'int64' in types:
			types.discard('int32')
		if len(types) > 1 and not (types - {'int32', 'int64', 'float32', 'float64'}):
			types = {'number'}
		if len(types) > 1:
			raise Exception("Column %r has incompatible types: %r" % (name, types,))
		columns[name] = (types.pop(), none_support[name],)

	collect = subjobs.build(
		'dataset_fanout_collect',
		source=datasets.source,
		previous=jobs.previous,
		column=options.column,
		length=options.length,
	)
	values = collect.load()
	if jobs.previous:
		previous = {ds.name: ds for ds in jobs.previous.datasets}
		values.update(previous)
	else:
		previous = {}

	with status('Creating %d datasets' % (len(values),)):
		writers = {
			name: job.datasetwriter(
				name=name,
				columns=columns,
				hashlabel=hashlabel,
				previous=previous.get(name),
				copy_mode=True,
			)
			for name in values
		}
	return writers, sorted(columns), chain


def analysis(sliceno, prepare_res):
	writers, columns, chain = prepare_res
	key_it = chain.iterate(sliceno, options.column)
	# we can't just use chain.iterate because of protections against changing types with copy_mode
	values_it = itertools.chain.from_iterable(ds.iterate(sliceno, columns, copy_mode=True, status_reporting=False) for ds in chain)
	for key, values in izip(key_it, values_it):
		writers[unicode(key)].write(*values)
