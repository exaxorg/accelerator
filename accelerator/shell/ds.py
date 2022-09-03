############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2022 Carl Drougge                       #
# Modifications copyright (c) 2019-2021 Anders Berkeman                    #
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

from __future__ import division, print_function

import sys
import locale
from datetime import datetime, time, date
from math import ceil, floor, log10, isinf, isnan

from accelerator.compat import terminal_size
from .parser import name2ds, name2job, ArgumentParser
from accelerator.colourwrapper import colour
from accelerator.error import NoSuchWhateverError
from accelerator.extras import quote

MINMAXWIDTH = 13 # minimum number of characters reserved for min/max values
COLUMNS, LINES = terminal_size()


def colwidth(rows):
	# find max string len per column
	return [max(len(s) for s in col) for col in zip(*rows)]

def printcolwise(data, template, printfunc, minrows=8, indent=4):
	if not data:
		return
	cols = (COLUMNS - indent) // (len(template.format(*printfunc(data[0]))) + 2)
	n = int(ceil(len(data) / cols))
	n = max(n, minrows)
	for r in range(n):
		v = data[r::n]
		if v:
			print(' ' * indent + '  '.join(template.format(*printfunc(x)) for x in v))

def original_location(ds, col):
	from accelerator.dataset import Dataset
	from accelerator.job import Job
	parts = col.location.split('/')
	job = Job(parts[0])
	if job.version >= 4: # with /DS/, .[pm] and encoding of problem characters
		import re
		dsname = re.sub(r'\\x[0-9a-f]{2}', lambda m: chr(int(m.group()[2:], 16)), parts[2][:-2]).replace('\\\\', '\\')
	else:
		dsname = parts[1]
	src_ds = Dataset(job, dsname)
	for colname, cand_col in src_ds.columns.items():
		if cand_col.location == col.location and cand_col.offsets == col.offsets:
			return (src_ds == ds), src_ds, colname

def format_location(loc):
	if not loc:
		return '???' # should never happen
	is_local, ds, colname = loc
	if is_local:
		return 'local'
	return '%s in %s' % (quote(colname), ds.quoted,)

def typed_from(ds, loc):
	if not loc:
		return
	_, ds, colname = loc
	if ds.job.method != 'dataset_type':
		return
	src_ds = ds.job.params.datasets.source
	colname = unrename_column(ds.job, src_ds, colname)
	res = 'typed from ' + format_location((False, src_ds, colname))
	orig_loc = original_location(src_ds, src_ds.columns[colname])
	if orig_loc and not orig_loc[0]:
		return '%s, originally %s' % (res, format_location(orig_loc))
	else:
		return res

def unrename_column(type_job, ds, colname):
	rename = type_job.params.options.rename
	rev_rename = {v: k for k, v in rename.items() if k in ds.columns}
	return rev_rename.get(colname, colname)


def main(argv, cfg):
	usage = "%(prog)s [options] ds [ds [...]]"
	parser = ArgumentParser(prog=argv.pop(0), usage=usage)
	parser.add_argument('-c', '--chain',            action='store_true', negation='no',   help='list all datasets in a chain')
	parser.add_argument('-C', '--non-empty-chain',  action='store_true', negation='no',   help='list all non-empty datasets in a chain')
	parser.add_argument('-l', '--list',             action='store_true', negation='dont', help='list all datasets in a job with number of rows')
	parser.add_argument('-L', '--chainedlist',      action='store_true', negation='no',   help='list all datasets in a job with number of chained rows')
	parser.add_argument('-m', '--suppress-minmax',  action='store_true', negation='dont', help='do not print min/max column values')
	parser.add_argument('-n', '--suppress-columns', action='store_true', negation='dont', help='do not print columns')
	parser.add_argument('-q', '--suppress-errors',  action='store_true', negation='dont', help='silently ignores bad input datasets/jobids')
	parser.add_argument('-s', '--slices',           action='store_true', negation='no',   help='list relative number of lines per slice in sorted order')
	parser.add_argument('-S', '--chainedslices',    action='store_true', negation='no',   help='same as -s but for full chain')
	parser.add_argument('-w', '--location',         action='store_true', negation='no',   help='show where (ds/filename) each column is stored')
	parser.add_argument("dataset", nargs='+', help='the job part of the dataset name can be specified in the same ways as for "ax job". you can use ds~ or ds~N to follow the chain N steps backwards, or ^ to follow .parent. this requires specifying the ds-name, so wd-1~ will not do this, but wd-1/default~ will.')
	args = parser.parse_intermixed_args(argv)
	args.chain = args.chain or args.non_empty_chain

	def finish(badinput):
		if badinput and not args.suppress_errors:
			print('Error, failed to resolve datasets:', file=sys.stderr)
			for n, e in badinput:
				print('    %r: %s' % (n, e,), file=sys.stderr)
			exit(1)
		exit()

	badinput = []

	if args.list or args.chainedlist:
		for n in args.dataset:
			try:
				try:
					dsvec = name2ds(cfg, n).job.datasets
				except NoSuchWhateverError:
					dsvec = name2job(cfg, n).datasets
			except Exception as e:
				badinput.append((n, e))
				dsvec = None
			if dsvec:
				print('%s' % (dsvec[0].job,))
				v = []
				for ds in dsvec:
					if args.chainedlist:
						lines = sum(sum(x.lines) for x in ds.chain())
					else:
						lines = sum(ds.lines)
					v.append((ds.quoted, '{:n}'.format(lines)))
				len_n, len_l = colwidth(v)
				template = "{0:%d}  ({1:>%d})" % (len_n, len_l)
				for name, numlines in sorted(v):
					print('    ' + template.format(name, numlines))
		finish(badinput)

	for n in args.dataset:
		try:
			ds = name2ds(cfg, n)
		except NoSuchWhateverError as e:
			badinput.append((n, e))
			continue

		print(ds.quoted)
		if ds.parent:
			if isinstance(ds.parent, tuple):
				print("    Parents:")
				max_n = max(len(x.quoted) for x in ds.parent)
				template = "{1:%d}" % (max_n,)
				data = tuple((None, x.quoted) for ix, x in enumerate(ds.parent))
				data = sorted(data, key = lambda x: x[1])
				printcolwise(data, template, lambda x: x, minrows=8, indent=8)
			else:
				print("    Parent:", ds.parent.quoted)
		print("    Method:", quote(ds.job.method))
		if ds.filename:
			print("    Filename:", quote(ds.filename))
		if ds.previous:
			print("    Previous:", ds.previous.quoted)
		if ds.hashlabel is not None:
			print("    Hashlabel:", quote(ds.hashlabel))

		def prettyminmax(minval, maxval):
			if args.suppress_minmax:
				return ''
			s = '[%%%ds, %%%ds]' % (MINMAXWIDTH, MINMAXWIDTH)
			if minval is None:
				return ''
			elif isinstance(minval, float):
				def intdigits(x):
					if isinf(x) or isnan(x):
						return 3
					return min(MINMAXWIDTH - 2, floor(log10(abs(x)) + 1)) if x else (MINMAXWIDTH - 2)//2
				ints = max(intdigits(minval), intdigits(maxval))
				if ints > 0:
					format = "%% %d.%df" % (ints, MINMAXWIDTH - ints - 2)
				elif ints < -4:
					format = "%% .%de" % (MINMAXWIDTH - 7,)
				else:
					format = "%% .%df" % (MINMAXWIDTH - 3,)
				def format_or_int(v):
					try:
						i = int(v)
						if v == i:
							return i
					except (OverflowError, ValueError):
						pass
					return locale.format_string(format, v)
				return s % (format_or_int(minval), format_or_int(maxval))
			elif isinstance(minval, int):
				return s % (minval, maxval)
			elif isinstance(minval, (date, time, datetime)):
				return s % (minval, maxval)
			else:
				return s % (minval, maxval)

		if not args.suppress_columns:
			print("    Columns:")
			name2typ = {n: c.type + '+None' if c.none_support else c.type for n, c in ds.columns.items()}
			len_n, len_t = colwidth((quote(n), name2typ[n]) for n, c in ds.columns.items())
			if args.location:
				locations = {n: original_location(ds, c) for n, c in ds.columns.items()}
				len_l = max(len(format_location(locations[n])) for n in ds.columns)
				len_c = max(len(c.compression) for c in ds.columns.values())
				template = '        {2} {0:%d}  {1:%d}  {4:%d}  {5:%d}  {3}' % (len_n, len_t, len_l, len_c,)
			else:
				template = '        {2} {0:%d}  {1:%d}  {3}' % (len_n, len_t,)
				locations = {}
			chain = False
			if args.chainedslices or args.chain:
				chain = ds.chain()
			for n, c in sorted(ds.columns.items()):
				if chain:
					minval, maxval = chain.min(n), chain.max(n)
				else:
					minval, maxval = c.min, c.max
				hashdot = colour("*", "ds/highlight") if n == ds.hashlabel else " "
				print(template.format(quote(n), name2typ[n], hashdot, prettyminmax(minval, maxval), format_location(locations.get(n)), c.compression).rstrip())
				if args.location:
					try:
						tf = typed_from(ds, locations[n])
						if tf:
							print(' ' * (13 + len_n + len_t), tf)
					except Exception:
						# source job might be deleted
						pass
			print("    {0:n} columns".format(len(ds.columns)))
		print("    {0:n} lines".format(sum(ds.lines)))

		if ds.previous or args.chain:
			chain = ds.chain()
			full_name = 'Full chain' if args.non_empty_chain else 'Chain'
			in_job = len(ds.chain_within_job())
			if in_job > 1:
				if in_job == len(chain):
					in_job = ' (all within job)'
				else:
					in_job = ' ({0:n} within job)'.format(in_job)
			else:
				in_job = ''
			print("    {0} length {1:n}{2}, from {3} to {4}".format(full_name, len(chain), in_job, chain[0], chain[-1]))
			if args.non_empty_chain:
				chain = [ds for ds in chain if sum(ds.lines)]
				print("    Filtered chain length {0:n}".format(len(chain)))
			if args.chain:
				data = tuple((ix, "%s/%s" % (x.job, x.name), "{:n}".format(sum(x.lines))) for ix, x in enumerate(chain))
				max_n, max_l = colwidth(x[1:] for x in data)
				template = "{0:3}: {1:%d} ({2:>%d})" % (max_n, max_l)
				printcolwise(data, template, lambda x: (x[0], x[1], x[2]), minrows=8, indent=8)

		if args.slices or args.chainedslices:
			if args.chainedslices and ds.previous:
				data = ((ix, '{:n}'.format(sum(x)), sum(x)) for ix, x in enumerate(zip(*(x.lines for x in ds.chain()))))
				print('    Balance, lines per slice, full chain:')
			else:
				data = ((ix, '{:n}'.format(x), x) for ix, x in enumerate(ds.lines))
				if ds.previous:
					print('    Balance, lines per slice, tip dataset:')
				else:
					print('    Balance, lines per slice:')
			data = sorted(data, key=lambda x: -x[2])
			s = sum(x[2] for x in data)
			len_n = max(len(x[1]) for x in data)
			template = "{0:3}: {1!s}%% ({2:>%d})" % (len_n,)
			printcolwise(data, template, lambda x: (x[0], locale.format_string("%6.2f", (100 * x[2] / (s or 1e20))), x[1]), minrows=8, indent=8)
			print("    Max to average ratio: " + locale.format_string("%2.3f", (max(x[2] for x in data) / ((s or 1e20) / len(data)),) ))

		if ds.previous:
			print("    {0:n} total lines in chain".format(sum(sum(ds.lines) for ds in chain)))

	finish(badinput)
