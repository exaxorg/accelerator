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

from accelerator.colourwrapper import colour
from accelerator.compat import fmt_num, num_types, int_types
from accelerator.error import NoSuchWhateverError
from accelerator.extras import quote
from accelerator.shell.parser import ArgumentParser
from accelerator.shell.parser import name2ds
from accelerator import g

from collections import Counter
import math
import sys


# multiprocessing.Pool insists on pickling the function even when using
# forking (as we do). Thus we need this top level indirection function.
_indirected_func = None
def _indirection(sliceno):
	return _indirected_func(sliceno)


NaN = float('NaN')
def merge_nans(hist):
	# NaN != NaN, so they are not collected in a single key.
	# Unless they are the same object, like our global above.
	# dsutil also uses only a single (different) NaN object.
	# (But that does not survive pickling.)
	nan_count = 0
	for k, v in list(hist.items()):
		if isinstance(k, float) and math.isnan(k):
			nan_count += v
			del hist[k]
	if nan_count:
		hist[NaN] = nan_count
		# Now all NaNs are under a single key.
	return hist


def format_aligned(hist):
	def fmt_k(k):
		if isinstance(k, num_types):
			return fmt_num(k)
		else:
			return str(k).expandtabs()
	empty = [v == 0 for k, v in hist]
	hist = [(fmt_k(k), fmt_num(v)) for k, v in hist]
	klen = max(len(k) for k, v in hist)
	vlen = max(len(v) for k, v in hist)
	total_len = klen + vlen + 2
	hist = [(k, ' ' * (total_len - len(k) - len(v)), v) for k, v in hist]
	hist = [
		(colour(k, 'hist/empty') if e else k, s, colour(v, 'hist/empty') if e else v)
		for e, (k, s, v) in zip(empty, hist)
	]
	return hist, colour('%s', 'hist/range') + '%s' + colour('%s', 'hist/count')

def format_bars(hist):
	from accelerator.compat import terminal_size
	columns = terminal_size()[0]
	a_hist, a_fmt = format_aligned(hist)
	max_len = columns - len(''.join(a_hist[0])) - 3
	if max_len < 2:
		return a_hist, a_fmt
	if sys.stdout.encoding.lower().startswith('utf-8'):
		characters = ('', '\u258f', '\u258e', '\u258d', '\u258c', '\u258b', '\u258a', '\u2589', '\u2588')
	else:
		characters = ('', '', '', '', '', '#', '#', '#', '#')
	values = [v for k, v in hist]
	max_v = max(values)
	if max_v > max_len:
		def mkbar(v):
			size = v * max_len / max_v
			bonus = int(round((size - int(size)) * 8))
			return characters[-1] * int(size) + characters[bonus]
	else:
		mkbar = characters[-1].__mul__
	hist = [(a_fmt % a, mkbar(v)) for a, v in zip(a_hist, values)]
	return hist, '%s  ' + colour('%s', 'hist/bar')

def _escape(hist, char):
	def q(s):
		if char in s or s.startswith('"'):
			return '"' + s.replace('"', '""') + '"'
		else:
			return s
	return [(q(str(k)), v) for k, v in hist]

def format_csv(hist):
	return _escape(hist, ','), '%s,%d'

def format_ssv(hist):
	return _escape(hist, ';'), '%s;%d'

def format_tsv(hist):
	return _escape(hist, '\t'), '%s\t%d'

formatters = {
	'aligned': format_aligned,
	'bars'   : format_bars,
	'csv'    : format_csv,
	'ssv'    : format_ssv,
	'tsv'    : format_tsv,
}


def main(argv, cfg):
	parser = ArgumentParser(prog=argv.pop(0), description='''show a histogram of column(s) from a dataset.''')
	parser.add_argument('-c', '--chain',     action='store_true', negation='dont', help="follow dataset chain", )
	parser.add_argument(      '--chain-length', '--cl',         metavar='LENGTH',  help="follow chain at most this many datasets", type=int)
	parser.add_argument(      '--stop-ds',   metavar='DATASET', help="follow chain at most to this dataset")
	parser.add_argument('-f', '--format', choices=sorted(formatters), help="output format, " + ' / '.join(sorted(formatters)), metavar='FORMAT', )
	parser.add_argument('-t', '--toplist',   action='store_true', negation='not',  help="don't bin values, show most common")
	parser.add_argument('-m', '--max-count', metavar='NUM',     help="show at most this many values or bins (default ~ 20)", type=int)
	parser.add_argument('-s', '--slice',     action='append',   help="this slice only, can be specified multiple times", type=int)
	parser.add_argument('dataset', help='can be specified in the same ways as for "ax ds"')
	parser.add_argument('column', nargs='*', help='you can specify multiple columns')
	args = parser.parse_intermixed_args(argv)

	if args.max_count and args.max_count < 0:
		args.max_count = 0

	try:
		ds = name2ds(cfg, args.dataset)
	except NoSuchWhateverError as e:
		print(e, file=sys.stderr)
		return 1

	chain = ds.chain(args.chain_length if args.chain else 1, stop_ds=args.stop_ds)
	if not chain:
		return

	if not args.column:
		# Can we guess?
		columns_in_all = set(chain[0].columns)
		for ds in chain:
			columns_in_all.intersection_update(ds.columns)
		if len(columns_in_all) == 1:
			args.column = list(columns_in_all)
		else:
			print('Specify at least one column, choose from:', file=sys.stderr)
			for colname in sorted(columns_in_all):
				print('    ' + quote(colname), file=sys.stderr)
			return 1

	ok = True
	for ds in chain:
		for col in args.column:
			if col not in ds.columns:
				print("Dataset %s does not have column %s." % (ds.quoted, col,), file=sys.stderr)
				ok = False
	if not ok:
		return 1

	varying_bucket_size = False

	columns = args.column[0] if len(args.column) == 1 else args.column
	useful_slices = [sliceno for sliceno in range(g.slices) if chain.lines(sliceno) > 0]
	if args.slice:
		useful_slices = list(set(useful_slices) & set(args.slice))
	if not useful_slices:
		return

	def mp_run(part_func, collect_func):
		if len(useful_slices) == 1:
			return collect_func(None, part_func(useful_slices[0]))
		from multiprocessing import Pool
		global _indirected_func
		_indirected_func = part_func
		pool = Pool(len(useful_slices))
		try:
			collected = None
			for part in pool.imap_unordered(_indirection, useful_slices, 1):
				collected = collect_func(collected, part)
			return collected
		finally:
			pool.close()

	def count_items(sliceno):
		return Counter(chain.iterate(sliceno, columns))
	hist_func = count_items # default, probably overridden if not args.toplist

	if len(args.column) > 1 or chain.min(columns) is None:
		args.toplist = True # Can't do anything else.

	if not args.toplist:
		low, high = chain.min(columns), chain.max(columns)
		if high == low: # just one value, so let's not try to bin it
			args.toplist = True

	if not args.toplist:
		inf = float('inf')
		if low in (inf, -inf) or high in (inf, -inf):
			print("Can't bin to infinity.", file=sys.stderr)
			return 1
		if args.max_count != 0: # otherwise all values are their own bin
			non_ints = chain.filter(lambda ds: not ds.columns[columns].type.startswith('int'))
			is_ints = not non_ints
			if non_ints and all(ds.columns[columns].type == 'number' for ds in non_ints):
				class Failure(Exception):
					pass
				def check_int(sliceno):
					return all(isinstance(v, int_types) for v in non_ints.iterate(sliceno, columns))
				def collect_intness(dummy, good):
					if not good:
						raise Failure() # abort the other slices, for speed
				try:
					mp_run(check_int, collect_intness)
					is_ints = True
				except Failure:
					is_ints = False
			if args.max_count is None:
				range_len = high - low + is_ints
				cands = (20, 21, 19, 22, 18, 23, 17, 24, 16, 25, 15, 14, 13,)
				if is_ints:
					# try to find something that gives equal sized buckets.
					args.max_count = 20 # fallback
					for cand in cands:
						if range_len / cand == range_len // cand:
							args.max_count = cand
							break
				else:
					# find the shortest string representation of the step,
					# as that is likely to look better in the labels.
					best = (1e1000, 20)
					for cand in cands:
						score = len(str(range_len / cand))
						if score < best[0]:
							best = (score, cand)
					args.max_count = best[1]
			if is_ints and high - low < args.max_count:
				args.max_count = high - low + 1
			step = (high - low) / args.max_count
			if is_ints and (high - low + 1) / args.max_count != math.ceil(step):
				varying_bucket_size = True
			def bin_items(sliceno):
				return merge_nans(Counter((v - low) // step for v in chain.iterate(sliceno, columns)))
			hist_func = bin_items
			if is_ints:
				def name(ix):
					a = int(math.ceil(step * ix + low))
					b = step * (ix + 1) + low
					if b == math.floor(b) and ix < args.max_count - 1:
						b = int(b) - 1
					else:
						b = int(b)
					if a == b:
						return fmt_num(a)
					else:
						return '%s - %s' % (fmt_num(a), fmt_num(b))
			else:
				def name(ix):
					a = step * ix + low
					b = step * (ix + 1) + low
					return '%s - %s' % (fmt_num(a), fmt_num(b))
			bin_names = [name(ix) for ix in range(args.max_count)]

	def collect_hist(hist, part):
		if not hist:
			return part
		hist.update(part)
		return hist

	hist = merge_nans(mp_run(hist_func, collect_hist))

	if args.format:
		formatter = formatters[args.format]
	elif sys.stdout.isatty():
		formatter = format_bars
	else:
		formatter = format_tsv

	if args.toplist:
		total_found = len(hist)
		hist = hist.most_common(args.max_count or None)
		hist, fmt = formatter(hist)
	else:
		total_found = 0 # so we don't print about it later
		if args.max_count:
			if NaN in hist:
				print(colour('WARNING: Ignored %d NaN values.' % (hist[NaN],), 'hist/warning'), file=sys.stderr)
			hist[args.max_count - 1] += hist[args.max_count] # top value should not be in a separate bin
			hist, fmt = formatter([(name, hist[ix]) for ix, name in enumerate(bin_names)])
		else:
			hist, fmt = formatter([(k, hist[k]) for k in sorted(hist)])

	for item in hist:
		print(fmt % item)

	if varying_bucket_size:
		print(colour('WARNING: varying bucket size.', 'hist/warning'), file=sys.stderr)

	if total_found > len(hist):
		print(colour(fmt_num(total_found) + ' total found', 'hist/warning'), file=sys.stderr)
