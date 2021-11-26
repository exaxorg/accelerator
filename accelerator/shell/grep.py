############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2021 Carl Drougge                       #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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

# grep in a dataset(chain)

from __future__ import division, print_function

import sys
import re
from multiprocessing import Process, JoinableQueue
from itertools import chain, repeat
from collections import deque, OrderedDict
from argparse import RawTextHelpFormatter, Action
import errno
from os import write
import json
import datetime

from accelerator.compat import ArgumentParser
from accelerator.compat import unicode, izip, PY2
from accelerator.colourwrapper import colour
from .parser import name2ds
from accelerator import g


def main(argv, cfg):
	# -C overrides -A and -B (which in turn override -C)
	class ContextAction(Action):
		def __call__(self, parser, namespace, values, option_string=None):
			namespace.before_context = namespace.after_context = values

	parser = ArgumentParser(
		usage="%(prog)s [options] pattern ds [ds [...]] [column [column [...]]",
		prog=argv.pop(0),
		formatter_class=RawTextHelpFormatter,
	)
	parser.add_argument('-c', '--chain',        action='store_true', help="follow dataset chains", )
	parser.add_argument(      '--colour', '--color', nargs='?', const='always', choices=['auto', 'never', 'always'], type=str.lower, help="colour matched text. can be auto, never or always", metavar='WHEN', )
	parser.add_argument('-i', '--ignore-case',  action='store_true', help="case insensitive pattern", )
	parser.add_argument('-H', '--headers',      action='store_true', help="print column names before output (and on each change)", )
	parser.add_argument('-O', '--ordered',      action='store_true', help="output in order (one slice at a time)", )
	parser.add_argument('-M', '--allow-missing-columns', action='store_true', help="datasets are allowed to not have (some) columns", )
	parser.add_argument('-g', '--grep',         action='append',     help="grep this column only, can be specified multiple times", metavar='COLUMN')
	parser.add_argument('-s', '--slice',        action='append',     help="grep this slice only, can be specified multiple times",  type=int)
	parser.add_argument('-D', '--show-dataset', action='store_true', help="show dataset on matching lines", )
	parser.add_argument('-S', '--show-sliceno', action='store_true', help="show sliceno on matching lines", )
	parser.add_argument('-L', '--show-lineno',  action='store_true', help="show lineno (per slice) on matching lines", )
	supported_formats = ('csv', 'raw', 'json',)
	parser.add_argument('-f', '--format', default='csv', choices=supported_formats, help="output format, csv (default) / " + ' / '.join(supported_formats[1:]), metavar='FORMAT', )
	parser.add_argument('-t', '--separator', help="field separator, default tab / tab-like spaces", )
	parser.add_argument('-B', '--before-context', type=int, default=0, metavar='NUM', help="print NUM lines of leading context", )
	parser.add_argument('-A', '--after-context',  type=int, default=0, metavar='NUM', help="print NUM lines of trailing context", )
	parser.add_argument('-C', '--context',        type=int, default=0, metavar='NUM', action=ContextAction,
		help="print NUM lines of context\n" +
		     "context is only taken from the same slice of the same\n" +
		     "dataset, and may intermix with output from other\n" +
		     "slices. Use -O to avoid that, or -S -L to see it.",
	)
	parser.add_argument('pattern')
	parser.add_argument('dataset', help='can be specified in the same ways as for "ax ds"')
	parser.add_argument('columns', nargs='*', default=[])
	args = parser.parse_intermixed_args(argv)

	if args.before_context < 0 or args.after_context < 0:
		print('Context must be >= 0', file=sys.stderr)
		return 1

	pat_s = re.compile(args.pattern, re.IGNORECASE if args.ignore_case else 0)
	datasets = [name2ds(cfg, args.dataset)]
	columns = []

	for ds_or_col in args.columns:
		if columns:
			columns.append(ds_or_col)
		else:
			try:
				datasets.append(name2ds(cfg, ds_or_col))
			except Exception:
				columns.append(ds_or_col)

	if not datasets:
		parser.print_help(file=sys.stderr)
		return 1

	grep_columns = set(args.grep or ())
	if grep_columns == set(columns):
		grep_columns = set()

	if args.slice:
		want_slices = []
		for s in args.slice:
			assert 0 <= s < g.slices, "Slice %d not available" % (s,)
			if s not in want_slices:
				want_slices.append(s)
	else:
		want_slices = list(range(g.slices))

	if args.chain:
		datasets = list(chain.from_iterable(ds.chain() for ds in datasets))

	def columns_for_ds(ds, columns=columns):
		if columns:
			return [n for n in columns if n in ds.columns]
		else:
			return sorted(ds.columns)

	if columns or grep_columns:
		if args.allow_missing_columns:
			keep_datasets = []
			for ds in datasets:
				if not columns_for_ds(ds):
					continue
				if grep_columns and not columns_for_ds(ds, grep_columns):
					continue
				keep_datasets.append(ds)
			if not keep_datasets:
				return 0
			datasets = keep_datasets
		else:
			bad = False
			need_cols = set(columns)
			if grep_columns:
				need_cols.update(grep_columns)
			for ds in datasets:
				missing = need_cols - set(ds.columns)
				if missing:
					print('ERROR: %s does not have columns %r' % (ds, missing,), file=sys.stderr)
					bad = True
			if bad:
				return 1

	# never and always override env settings, auto (default) sets from env/tty
	if args.colour == 'never':
		colour.disable()
		highlight_matches = False
	elif args.colour == 'always':
		colour.enable()
		highlight_matches = True
	else:
		highlight_matches = colour.enabled

	# Don't highlight everything when just trying to cat
	if args.pattern == '':
		highlight_matches = False

	separator = args.separator
	if separator is None and not sys.stdout.isatty():
		separator = '\t'

	if separator is None:
		# special case where we try to be like a tab, but with spaces.
		# this is useful because terminals typically don't style tabs.
		def separate(items, lens):
			things = []
			for item, item_len in zip(items, lens):
				things.append(item)
				spaces = 8 - (item_len % 8)
				things.append(colour(' ' * spaces, 'grep/separator'))
			return ''.join(things[:-1])
		separator = '\t'
	else:
		separator_coloured = colour(separator, 'grep/separator')
		def separate(items, lens):
			return separator_coloured.join(items)

	def json_default(obj):
		if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
			return str(obj)
		elif isinstance(obj, complex):
			return [obj.real, obj.imag]
		else:
			return repr(obj)

	if args.format == 'csv':
		def escape_item(item):
			if item and (separator in item or item[0] in '\'"' or item[-1] in '\'"'):
				return '"' + item.replace('\n', '\\n').replace('"', '""') + '"'
			else:
				return item.replace('\n', '\\n')
		errors = 'surrogatepass'
	else:
		escape_item = None
		errors = 'replace' if PY2 else 'surrogateescape'

	def grep(ds, sliceno):
		chk = pat_s.search
		def mk_iter(col):
			if ds.columns[col].type == 'ascii':
				it = ds._column_iterator(sliceno, col, _type='unicode')
			else:
				it = ds._column_iterator(sliceno, col)
			if ds.columns[col].type == 'bytes':
				errors = 'replace' if PY2 else 'surrogateescape'
				if ds.columns[col].none_support:
					it = (None if v is None else v.decode('utf-8', errors) for v in it)
				else:
					it = (v.decode('utf-8', errors) for v in it)
			return it
		def colour_item(item):
			pos = 0
			parts = []
			for m in pat_s.finditer(item):
				a, b = m.span()
				parts.extend((item[pos:a], colour(item[a:b], 'grep/highlight')))
				pos = b
			parts.append(item[pos:])
			return ''.join(parts)
		if args.format == 'json':
			prefix = {}
			dumps = json.JSONEncoder(ensure_ascii=False, default=json_default).encode
			if args.show_dataset:
				prefix['dataset'] = ds
			if args.show_sliceno:
				prefix['sliceno'] = sliceno
			def show(lineno, items):
				d = dict(zip(used_columns, items))
				if args.show_lineno:
					prefix['lineno'] = lineno
				if prefix:
					prefix['data'] = d
					d = prefix
				return dumps(d).encode('utf-8', 'surrogatepass')
		else:
			prefix = []
			if args.show_dataset:
				prefix.append(ds)
			if args.show_sliceno:
				prefix.append(str(sliceno))
			prefix = tuple(prefix)
			def show(lineno, items):
				data = list(prefix)
				if args.show_lineno:
					data.append(unicode(lineno))
				show_items = list(map(unicode, items))
				lens = (len(item) for item in data + show_items)
				if highlight_matches:
					show_items = list(map(colour_item, show_items))
				if escape_item:
					lens_unesc = (len(item) for item in data + show_items)
					show_items = list(map(escape_item, show_items))
					lens_esc = (len(item) for item in data + show_items)
					lens = (l + esc - unesc for l, unesc, esc in zip(lens, lens_unesc, lens_esc))
				data.extend(show_items)
				return separate(data, lens).encode('utf-8', errors)
		used_columns = columns_for_ds(ds)
		used_grep_columns = grep_columns and columns_for_ds(ds, grep_columns)
		if grep_columns and set(used_grep_columns) != set(used_columns):
			grep_iter = izip(*(mk_iter(col) for col in used_grep_columns))
		else:
			grep_iter = repeat(None)
		lines_iter = izip(*(mk_iter(col) for col in used_columns))
		if args.before_context:
			before = deque((), args.before_context)
		else:
			before = None
		to_show = 0
		for lineno, (grep_items, items) in enumerate(izip(grep_iter, lines_iter)):
			if any(chk(unicode(item)) for item in grep_items or items):
				while before:
					write(1, show(*before.popleft()) + b'\n')
				to_show = 1 + args.after_context
			if to_show:
				# This will be atomic if the line is not too long
				# (at least up to PIPE_BUF bytes, should be at least 512).
				write(1, show(lineno, items) + b'\n')
				to_show -= 1
			elif before is not None:
				before.append((lineno, items))

	def one_slice(sliceno, q, wait_for):
		try:
			if q:
				q.get()
			for ds in datasets:
				if ds in wait_for:
					q.task_done()
					q.get()
				grep(ds, sliceno)
		except IOError as e:
			if e.errno == errno.EPIPE:
				return
			else:
				raise
		finally:
			# Make sure we are joinable
			try:
				q.task_done()
			except Exception:
				pass

	headers_prefix = []
	if args.show_dataset:
		headers_prefix.append('[DATASET]')
	if args.show_sliceno:
		headers_prefix.append('[SLICE]')
	if args.show_lineno:
		headers_prefix.append('[LINE]')

	# {ds: headers} for each ds where headers change (not including the first).
	# this is every ds where sync between slices has to happen when not --ordered.
	headers = OrderedDict()
	if args.headers:
		current_headers = None
		for ds in datasets:
			candidate_headers = columns_for_ds(ds)
			if candidate_headers != current_headers:
				headers[ds] = current_headers = candidate_headers
		def gen_headers(headers):
			if args.format != 'json':
				show_items = headers_prefix + headers
				if escape_item:
					show_items = list(map(escape_item, show_items))
				coloured = (colour(item, 'grep/header') for item in show_items)
				txt = separate(coloured, map(len, show_items))
				return txt.encode('utf-8', 'surrogatepass') + b'\n'
			else:
				return b''
		# remove the starting ds, so no header changes means no special handling.
		current_headers = headers.pop(datasets[0])
		write(1, gen_headers(current_headers))
		headers_iter = iter(map(gen_headers, headers.values()))

	queues = []
	children = []
	if not args.ordered:
		q = None
		wait_for = set(headers)
		for sliceno in want_slices[1:]:
			if wait_for:
				q = JoinableQueue()
				q.put(None)
				queues.append(q)
			p = Process(
				target=one_slice,
				args=(sliceno, q, wait_for),
				name='slice-%d' % (sliceno,),
			)
			p.daemon = True
			p.start()
			children.append(p)
		want_slices = want_slices[:1]

	for ds in datasets:
		if ds in headers:
			for q in queues:
				q.join()
			write(1, next(headers_iter))
			for q in queues:
				q.put(None)
		for sliceno in want_slices:
			grep(ds, sliceno)
	for c in children:
		c.join()
