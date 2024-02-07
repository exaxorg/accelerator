# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2024 Carl Drougge                                          #
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
from __future__ import print_function
from __future__ import unicode_literals

from accelerator.error import NoSuchWhateverError
from accelerator.shell.parser import ArgumentParser
from accelerator.shell.parser import name2ds

from collections import Counter
import sys


# multiprocessing.Pool insists on pickling the function even when using
# forking (as we do). Thus we need this top level indirection function.
_indirected_func = None
def _indirection(sliceno):
	return _indirected_func(sliceno)

def main(argv, cfg):
	parser = ArgumentParser(prog=argv.pop(0), description='''show a histogram of column(s) from a dataset.''')
	parser.add_argument('-m', '--max-count', metavar='NUM',     help="show at most this many values", type=int)
	parser.add_argument('dataset', help='can be specified in the same ways as for "ax ds"')
	parser.add_argument('column', nargs='+', help='you can specify multiple columns')
	args = parser.parse_intermixed_args(argv)

	try:
		ds = name2ds(cfg, args.dataset)
	except NoSuchWhateverError as e:
		print(e, file=sys.stderr)
		return 1

	ok = True
	for col in args.column:
		if col not in ds.columns:
			print("Dataset %s does not have column %s." % (ds.quoted, col,), file=sys.stderr)
			ok = False
	if not ok:
		return 1

	columns = args.column[0] if len(args.column) == 1 else args.column
	useful_slices = [ix for ix, count in enumerate(ds.lines) if count > 0]
	if not useful_slices:
		return

	def one_slice(sliceno):
		return Counter(ds.iterate(sliceno, columns))

	if len(useful_slices) == 1:
		hist = one_slice(useful_slices[0])
	else:
		global _indirected_func
		_indirected_func = one_slice
		from multiprocessing import Pool
		pool = Pool(len(useful_slices))
		try:
			hist = None
			for part in pool.imap_unordered(_indirection, useful_slices, 1):
				if hist:
					hist.update(part)
				else:
					hist = part
		finally:
			pool.close()

	hist = hist.most_common(args.max_count)
	for k, v in hist:
		print('%s\t%d' % (k, v,))
