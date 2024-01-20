# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2024 Carl Drougge                       #
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

from __future__ import division
from __future__ import absolute_import

description = r'''
Rewrite a dataset (or chain to previous) with new hashlabel.
'''

from shutil import copyfileobj
from os.path import exists

from accelerator import OptionString

options = {
	'hashlabel'                 : OptionString,
	'caption'                   : '"%(caption)s" hashed on %(hashlabel)s',
	'length'                    : -1, # Go back at most this many datasets. You almost always want -1 (which goes until previous.source)
	'chain_slices'              : False, # one dataset per slice (avoids rewriting at the end)
}

datasets = ('source', 'previous',)

def prepare(job, slices):
	previous = datasets.previous
	res = []
	chain = datasets.source.chain(stop_ds={datasets.previous: 'source'}, length=options.length)
	if not chain:
		raise Exception("previous had the same source - this job makes no sense")
	for ix, ds in enumerate(chain):
		res.append(prepare_one(ix, ds, previous, job, chain, slices))
		previous = res[-1][0][-1] # dws[-1]
	return chain, res

def prepare_one(ix, source, previous, job, chain, slices):
	caption = options.caption % dict(caption=source.caption, hashlabel=options.hashlabel)
	filename = source.filename
	dws = []
	for sliceno in range(slices):
		if sliceno == slices - 1 and options.chain_slices and ix == len(chain) - 1:
			name = "default"
		else:
			name = '%d.%d' % (ix, sliceno,)
		dw = job.datasetwriter(
			caption="%s (slice %d)" % (caption, sliceno),
			hashlabel=options.hashlabel,
			filename=filename,
			previous=previous,
			name=name,
			for_single_slice=sliceno,
			copy_mode=True,
		)
		previous = dw
		dws.append(dw)
	names = []
	cols = {}
	for n, c in source.columns.items():
		# names has to be in the same order as the add calls
		# so the iterator returns the same order the writer expects.
		names.append(n)
		cols[n] = (c.type, chain.none_support(n))
		for dw in dws:
			if dw:
				dw.add(n, c.type, none_support=cols[n][1])
	return dws, names, caption, filename, cols

def analysis(sliceno, prepare_res):
	chain, p_res = prepare_res
	for ds, p in zip(chain, p_res):
		analysis_one(sliceno, ds, p)

def analysis_one(sliceno, source, prepare_res):
	dws, names = prepare_res[:2]
	write = dws[sliceno].get_split_write_list()
	for values in source.iterate(sliceno, names, copy_mode=True):
		write(values)

def synthesis(prepare_res, job, slices):
	if not options.chain_slices:
		chain, p_res = prepare_res
		previous = datasets.previous
		for ix, p in enumerate(p_res):
			previous = synthesis_one(ix, chain, p, job, slices, previous)

def synthesis_one(ix, chain, prepare_res, job, slices, previous):
	# If we don't want a chain we abuse our knowledge of dataset internals
	# to avoid recompressing. Don't do this stuff yourself.
	dws, names, caption, filename, cols = prepare_res
	merged_dw = job.datasetwriter(
		name='default' if ix == len(chain) - 1 else str(ix),
		caption=caption,
		hashlabel=options.hashlabel,
		filename=filename,
		previous=previous,
		meta_only=True,
		columns=cols,
	)
	merged_dw.set_compressions(dws[0]._compressions)
	for sliceno in range(slices):
		merged_dw.set_lines(sliceno, sum(dw._lens[sliceno] for dw in dws))
		for dwno, dw in enumerate(dws):
			merged_dw.set_minmax((sliceno, dwno), dw._minmax[sliceno])
		for n in names:
			fn = merged_dw.column_filename(n, sliceno=sliceno)
			with open(fn, "wb") as out_fh:
				for dw in dws:
					fn = dw.column_filename(n, sliceno=sliceno)
					if exists(fn):
						with open(fn, "rb") as in_fh:
							copyfileobj(in_fh, out_fh)
	for dw in dws:
		dw.discard()
	return merged_dw
