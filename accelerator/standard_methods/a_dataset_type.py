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
from __future__ import print_function

from resource import getpagesize
from os import unlink
from os.path import exists
from mmap import mmap
from shutil import copyfileobj
from struct import Struct

from accelerator.compat import unicode, itervalues, PY2

from accelerator.extras import OptionEnum, DotDict, quote
from accelerator.dsutil import typed_writer, typed_reader
from accelerator.error import NoSuchDatasetError
from . import dataset_type

depend_extra = (dataset_type,)

description = r'''
Convert one or more columns in a dataset chain from bytes/ascii/unicode to
any type. Produces one dataset per source dataset if chain_slices=False,
one per slice per source dataset if chain_slices=True (which is slightly
faster to write, slightly slower to read).

Also rehashes if you type the hashlabel, or specify a new hashlabel.
You can also set hashlabel="" to not rehash (and get hashlabel=None).

Without filter_bad the method fails when a value fails to convert and
doesn't have a default. With filter_bad the value is filtered out
together with all other values on the same line.

By default new datasets inherit untouched columns from their parent, but
with filter_bad or when rehashing new independant datasets are produced.
You can set discard_untyped to not copy untouched columns to the new datasets
(and force creating new datasets).

With filter_bad any discarded lines (but only the columns actually typed)
will be saved in separate datasets with ".bad" appended to the name, or
just "bad" for the final one (i.e. not "default.bad").
'''

# Build the +None version of a type name, e.g. number:int => number+None:int
def type_plus_None(typename):
	if ':' in typename:
		a, b = typename.split(':')
		return a + '+None:' + b
	else:
		return typename + '+None'

TYPENAME = OptionEnum(
	list(dataset_type.convfuncs) +
	[type_plus_None(typename) for typename in dataset_type.convfuncs]
)

options = {
	'column2type'               : {'COLNAME': TYPENAME},
	# Anything your system would accept in $TZ. Parses datetimes (but not
	# dates or times) as being in this timezone (you get UTC).
	# No error checking can be done on this (tzset(3) can not return failure).
	# On most 32bit systems this will break dates outside about 1970 - 2037.
	# Setting this will mask most bad dates (mktime(3) "fixes" them).
	# (But "UTC" is special cased and doesn't break anything.)
	# If you set this datetimes will have tzinfo=UTC.
	'timezone'                  : str,
	'hashlabel'                 : str, # leave as None to inherit hashlabel, set to '' to not have a hashlabel
	'defaults'                  : {}, # {'COLNAME': value}, unspecified -> method fails on unconvertible unless filter_bad
	'rename'                    : {}, # {'OLDNAME': 'NEWNAME'} doesn't shadow OLDNAME. (Other COLNAMEs use NEWNAME.)
	                                  # Use {'OLDNAME': None} to discard OLDNAME.
	'caption'                   : 'typed dataset',
	'discard_untyped'           : bool, # Make unconverted columns inaccessible ("new" dataset)
	'filter_bad'                : False, # Discard lines where any column fails typing, saving them in a dataset named "bad"
	'numeric_comma'             : False, # floats as "3,14"
	'length'                    : -1, # Go back at most this many datasets. You almost always want -1 (which goes until previous.source)
	'chain_slices'              : False, # one dataset per slice if rehashing (avoids rewriting at the end)
	'compression'               : 6,     # gzip level
}

datasets = ('source', 'previous',)


byteslike_types = ('bytes', 'ascii', 'unicode',)

cstuff = dataset_type.init()

def prepare(job, slices):
	assert 1 <= options.compression <= 9
	assert datasets.source, 'No source specified'
	if options.timezone:
		cstuff.backend.init(options.timezone.encode('utf-8'))
	else:
		cstuff.backend.init(cstuff.NULL)
	d = datasets.source
	chain = d.chain(stop_ds={datasets.previous: 'source'}, length=options.length)
	res = []
	previous = None
	for ix, ds in enumerate(chain):
		previous = prepare_one(ix, ds, chain, job, slices, previous)
		res.append(previous)
	return res

def prepare_one(ix, source, chain, job, slices, previous_res):
	if ix == len(chain) - 1:
		ds_name = 'default'
	else:
		ds_name = str(ix)
	filename = source.filename
	source_name = source.quoted
	columns = {}
	column2type = dict(options.column2type)
	dup_rename = {}
	just_rename = {}
	rev_rename = {}
	for k, v in options.rename.items():
		if k in source.columns:
			if v in rev_rename:
				raise Exception('Both column %r and column %r rename to %r (in %s)' % (rev_rename[v], k, v, source_name))
			if v is not None:
				rev_rename[v] = k
			if v in column2type:
				dup_rename[k] = v
			else:
				just_rename[k] = v
	renamed_over = set(list(dup_rename.values()) + list(just_rename.values()))
	for k in renamed_over.intersection(dup_rename):
		# renamed over don't need to be duplicated
		just_rename[k] = dup_rename.pop(k)
	if dup_rename:
		dup_ds = source.link_to_here(name='dup.%d' % (ix,), rename=dup_rename, column_filter=dup_rename.values())
		if source.hashlabel in dup_rename:
			just_rename[source.hashlabel] = None
	if just_rename:
		source = source.link_to_here(name='rename.%d' % (ix,), rename=just_rename)
	if dup_rename:
		source = source.merge(dup_ds, name='merge.%d' % (ix,))
	none_support = set()
	for colname, coltype in column2type.items():
		if colname not in source.columns:
			raise Exception("Dataset %s doesn't have a column named %r (has %r)" % (source_name, colname, set(source.columns),))
		dc = source.columns[colname]
		if dc.type not in byteslike_types:
			raise Exception("Dataset %s column %r is type %s, must be one of %r" % (source_name, colname, dc.type, byteslike_types,))
		coltype = coltype.split(':', 1)[0]
		if coltype.endswith('+None'):
			coltype = coltype[:-5]
			none_support.add(colname)
		columns[colname] = dataset_type.typerename.get(coltype, coltype)
		if options.defaults.get(colname, False) is None or dc.none_support:
			none_support.add(colname)
	if options.hashlabel is None:
		hashlabel_override = False
		hashlabel = source.hashlabel
		rehashing = (hashlabel in columns)
	else:
		hashlabel_override = True
		hashlabel = options.hashlabel or None
		rehashing = bool(hashlabel)
	if (options.filter_bad or rehashing) and not options.discard_untyped:
		untyped_columns = set(source.columns)
		untyped_columns -= set(columns) # anything renamed over is irrelevant
		for colname in sorted(untyped_columns):
			dc = source.columns[colname]
			columns[colname] = dc.type
			column2type[colname] = dataset_type.copy_types[dc.type]
			if dc.none_support:
				none_support.add(colname)
	if options.filter_bad or rehashing or options.discard_untyped:
		parent = None
	else:
		parent = source
	if hashlabel and hashlabel not in columns:
		if options.hashlabel:
			raise Exception("Can't rehash %s on discarded column %r." % (source_name, hashlabel,))
		hashlabel = None # it gets inherited from the parent if we're keeping it.
		hashlabel_override = False
	columns = {
		colname: (typ, colname in none_support)
		for colname, typ in columns.items()
	}
	dws = []
	if previous_res:
		# dw or last in dws[] for previous source
		final_previous = previous_res[0] or previous_res[2][-1]
	else:
		final_previous = datasets.previous
	if rehashing:
		previous = final_previous
		for sliceno in range(slices):
			if options.chain_slices and sliceno == slices - 1:
				# This ds will remain and be the tip of the chain for this source
				name = ds_name
			else:
				# This ds is either an earlier part of the chain or will be merged into ds_name in synthesis
				name = '%s.%d' % (ds_name, sliceno,)
			dw = job.datasetwriter(
				columns=columns,
				caption='%s (from %s slice %d)' % (options.caption, source_name, sliceno,),
				hashlabel=hashlabel,
				filename=filename,
				previous=previous,
				meta_only=True,
				name=name,
				for_single_slice=sliceno,
			)
			previous = dw
			dws.append(dw)
	if rehashing and options.chain_slices:
		dw = None
	else:
		dw = job.datasetwriter(
			name=ds_name,
			columns=columns,
			caption='%s (from %s)' % (options.caption, source_name,),
			hashlabel=hashlabel,
			hashlabel_override=hashlabel_override,
			filename=filename,
			parent=parent,
			previous=final_previous,
			meta_only=True,
		)
	if options.filter_bad:
		if previous_res:
			previous = previous_res[1]
		else:
			try:
				previous = datasets.previous.job.dataset('bad')
			except NoSuchDatasetError:
				previous = None
		def best_bad_type(colname):
			dc = source.columns[colname]
			assert dc.type in byteslike_types
			return (dc.type, dc.none_support)
		bad_columns = {name: best_bad_type(name) for name in options.column2type}
		dw_bad = job.datasetwriter(
			name='bad' if ds_name == 'default' else ds_name + '.bad',
			columns=bad_columns,
			previous=previous,
			meta_only=True,
		)
	else:
		dw_bad = None
	return dw, dw_bad, dws, source, source_name, column2type, sorted(columns)


def map_init(vars, name, z='badmap_size'):
	if not vars.badmap_size:
		pagesize = getpagesize()
		line_count = vars.source.lines[vars.sliceno]
		vars.badmap_size = (line_count // 8 // pagesize + 1) * pagesize
		vars.slicemap_size = (line_count * 2 // pagesize + 1) * pagesize
	fh = open(name, 'w+b')
	vars.map_fhs.append(fh)
	fh.truncate(vars[z])
	return fh.fileno()


def analysis(sliceno, slices, prepare_res):
	if options.numeric_comma:
		try_locales = [
			'da_DK', 'nb_NO', 'nn_NO', 'sv_SE', 'fi_FI',
			'en_ZA', 'es_ES', 'es_MX', 'fr_FR', 'ru_RU',
			'de_DE', 'nl_NL', 'it_IT',
		]
		for localename in try_locales:
			localename = localename.encode('ascii')
			if not cstuff.backend.numeric_comma(localename):
				break
			if not cstuff.backend.numeric_comma(localename + b'.UTF-8'):
				break
		else:
			raise Exception("Failed to enable numeric_comma, please install at least one of the following locales: " + " ".join(try_locales))
		dataset_type.numeric_comma = True
	res = [analysis_one(sliceno, slices, p) for p in prepare_res]
	for fn in ('slicemap', 'badmap',):
		fn = '%s%d' % (fn, sliceno,)
		if exists(fn):
			unlink(fn)
	return res

def analysis_one(sliceno, slices, prepare_res):
	dw, dw_bad, dws, source, source_name, column2type, _ = prepare_res
	if source.lines[sliceno] == 0:
		dummy = [0] * slices
		return {}, dummy, {}, {}, dummy
	if dws:
		dw = dws[sliceno]
		rehashing = True
	else:
		rehashing = False
	vars = DotDict(
		sliceno=sliceno,
		slices=slices,
		known_line_count=0,
		badmap_size=0,
		badmap_fd=-1,
		slicemap_size=0,
		slicemap_fd=-1,
		map_fhs=[],
		res_bad_count={},
		res_default_count={},
		res_minmax={},
		first_lap=True,
		rehashing=rehashing,
		hash_lines=None,
		dw=dw,
		dw_bad=dw_bad,
		save_bad=False,
		source=source,
		source_name=source_name,
		column2type=column2type,
	)
	if options.filter_bad:
		vars.badmap_fd = map_init(vars, 'badmap%d' % (sliceno,))
		bad_count, default_count, minmax = analysis_lap(vars)
		if sum(sum(c) for c in itervalues(bad_count)):
			vars.first_lap = False
			vars.res_bad_count = {}
			final_bad_count, default_count, minmax = analysis_lap(vars)
			final_bad_count = [max(c) for c in zip(*final_bad_count.values())]
		else:
			final_bad_count = [0] * slices
	else:
		bad_count, default_count, minmax = analysis_lap(vars)
		final_bad_count = [0] * slices
	for fh in vars.map_fhs:
		fh.close()
	return bad_count, final_bad_count, default_count, minmax, vars.hash_lines


# In python3 indexing into bytes gives integers (b'a'[0] == 97),
# this gives the same behaviour on python2. (For use with mmap.)
class IntegerBytesWrapper(object):
	def __init__(self, inner):
		self.inner = inner
	def close(self):
		self.inner.close()
	def __getitem__(self, key):
		return ord(self.inner[key])
	def __setitem__(self, key, value):
		self.inner[key] = chr(value)

# But even in python3 we can only get int8 support for free,
# and slicemap needs int16.
class Int16BytesWrapper(object):
	_s = Struct('=H')
	def __init__(self, inner):
		self.inner = inner
	def close(self):
		self.inner.close()
	def __getitem__(self, key):
		return self._s.unpack_from(self.inner, key * 2)[0]
	def __setitem__(self, key, value):
		self._s.pack_into(self.inner, key * 2, value)
	def __iter__(self):
		if PY2:
			def it():
				for o in range(len(self.inner) // 2):
					yield self[o]
		else:
			def it():
				for v, in self._s.iter_unpack(self.inner):
					yield v
		return it()


def analysis_lap(vars):
	if vars.rehashing:
		if vars.first_lap:
			out_fn = 'hashtmp.%d' % (vars.sliceno,)
			colname = vars.dw.hashlabel
			coltype = vars.column2type[colname]
			vars.rehashing = False
			real_coltype = one_column(vars, colname, coltype, [out_fn], True)
			vars.rehashing = True
			assert vars.res_bad_count[colname] == [0] # implicitly has a default
			vars.slicemap_fd = map_init(vars, 'slicemap%d' % (vars.sliceno,), 'slicemap_size')
			slicemap = mmap(vars.slicemap_fd, vars.slicemap_size)
			vars.map_fhs.append(slicemap)
			slicemap = Int16BytesWrapper(slicemap)
			hash = typed_writer(real_coltype).hash
			slices = vars.slices
			vars.hash_lines = hash_lines = [0] * slices
			for ix, value in enumerate(typed_reader(real_coltype)(out_fn)):
				dest_slice = hash(value) % slices
				slicemap[ix] = dest_slice
				hash_lines[dest_slice] += 1
			unlink(out_fn)
	for colname, coltype in vars.column2type.items():
		if vars.rehashing:
			out_fns = [vars.dw.column_filename(colname, sliceno=s) for s in range(vars.slices)]
		else:
			out_fns = [vars.dw.column_filename(colname)]
		if options.filter_bad and not vars.first_lap and colname in options.column2type:
			out_fns.append(vars.dw_bad.column_filename(colname, sliceno=vars.sliceno))
			vars.save_bad = True
		else:
			vars.save_bad = False
		one_column(vars, colname, coltype, out_fns)
	return vars.res_bad_count, vars.res_default_count, vars.res_minmax


def one_column(vars, colname, coltype, out_fns, for_hasher=False):
	if for_hasher:
		record_bad = skip_bad = False
	elif vars.first_lap:
		record_bad = options.filter_bad
		skip_bad = False
	else:
		record_bad = 0
		skip_bad = options.filter_bad
	minmax_fn = 'minmax%d' % (vars.sliceno,)

	if coltype.split(':')[0].endswith('+None'):
		coltype = ''.join(coltype.split('+None', 1))
		empty_types_as_None = True
	else:
		empty_types_as_None = False

	fmt = fmt_b = None
	is_null_converter = False
	if coltype in dataset_type.convfuncs:
		shorttype = coltype
		_, cfunc, pyfunc = dataset_type.convfuncs[coltype]
	elif coltype.startswith('null_'):
		shorttype = coltype
		pyfunc = False
		cfunc = True
		is_null_converter = True
	else:
		shorttype, fmt = coltype.split(':', 1)
		_, cfunc, pyfunc = dataset_type.convfuncs[shorttype + ':*']
	if cfunc:
		cfunc = shorttype.replace(':', '_')
	if pyfunc:
		tmp = pyfunc(coltype)
		if callable(tmp):
			pyfunc = tmp
			cfunc = None
		else:
			pyfunc = None
			cfunc, fmt, fmt_b = tmp
	if coltype == 'number':
		cfunc = 'number'
	elif coltype == 'number:int':
		coltype = 'number'
		cfunc = 'number'
		fmt = "int"
	assert cfunc or pyfunc, coltype + " didn't have cfunc or pyfunc"
	coltype = shorttype
	in_fns = []
	in_msgnames = []
	offsets = []
	max_counts = []
	d = vars.source
	assert colname in d.columns, '%s not in %s' % (colname, d.quoted,)
	if not is_null_converter:
		assert d.columns[colname].type in byteslike_types, '%s has bad type in %s' % (colname, d.quoted,)
	in_fns.append(d.column_filename(colname, vars.sliceno))
	in_msgnames.append('%s column %s slice %d' % (d.quoted, quote(colname), vars.sliceno,))
	if d.columns[colname].offsets:
		offsets.append(d.columns[colname].offsets[vars.sliceno])
		max_counts.append(d.lines[vars.sliceno])
	else:
		offsets.append(0)
		max_counts.append(-1)
	if cfunc:
		default_value = options.defaults.get(colname, cstuff.NULL)
		if for_hasher and default_value is cstuff.NULL:
			default_value = None
		default_len = 0
		if default_value is None:
			default_value = cstuff.NULL
			default_value_is_None = True
		else:
			default_value_is_None = False
			if default_value != cstuff.NULL:
				if isinstance(default_value, unicode):
					default_value = default_value.encode("utf-8")
				default_len = len(default_value)
		c = getattr(cstuff.backend, 'convert_column_' + cfunc)
		if vars.rehashing:
			c_slices = vars.slices
		else:
			c_slices = 1
		bad_count = cstuff.mk_uint64(c_slices)
		default_count = cstuff.mk_uint64(c_slices)
		gzip_mode = "wb%d" % (options.compression,)
		if in_fns:
			assert len(out_fns) == c_slices + vars.save_bad
			res = c(*cstuff.bytesargs(in_fns, in_msgnames, len(in_fns), out_fns, gzip_mode, minmax_fn, default_value, default_len, default_value_is_None, empty_types_as_None, fmt, fmt_b, record_bad, skip_bad, vars.badmap_fd, vars.badmap_size, vars.save_bad, c_slices, vars.slicemap_fd, vars.slicemap_size, bad_count, default_count, offsets, max_counts))
			assert not res, 'Failed to convert ' + colname
		vars.res_bad_count[colname] = list(bad_count)
		vars.res_default_count[colname] = sum(default_count)
		coltype = coltype.split(':', 1)[0]
		if is_null_converter:
			dc = d.columns[colname]
			real_coltype = dc.type
			# Some lines may have been filtered out, so these minmax values
			# could be wrong. There's no easy/cheap way to fix that though,
			# and they will never be wrong in the bad direction.
			vars.res_minmax[colname] = [dc.min, dc.max]
		else:
			real_coltype = dataset_type.typerename.get(coltype, coltype)
			if exists(minmax_fn):
				with typed_reader(real_coltype)(minmax_fn) as it:
					vars.res_minmax[colname] = list(it)
				unlink(minmax_fn)
	else:
		# python func
		if for_hasher:
			raise Exception("Can't hash %s on column of type %s." % (vars.source_name, coltype,))
		nodefault = object()
		if colname in options.defaults:
			default_value = options.defaults[colname]
			if default_value is not None:
				default_value = pyfunc(default_value)
		else:
			default_value = nodefault
		if options.filter_bad:
			badmap = mmap(vars.badmap_fd, vars.badmap_size)
			vars.map_fhs.append(badmap)
			if PY2:
				badmap = IntegerBytesWrapper(badmap)
		if vars.rehashing:
			slicemap = mmap(vars.slicemap_fd, vars.slicemap_size)
			vars.map_fhs.append(slicemap)
			slicemap = Int16BytesWrapper(slicemap)
			bad_count = [0] * vars.slices
		else:
			bad_count = [0]
			chosen_slice = 0
		default_count = 0
		dont_minmax_types = {'bytes', 'ascii', 'unicode', 'json', 'complex32', 'complex64'}
		real_coltype = dataset_type.typerename.get(coltype, coltype)
		do_minmax = real_coltype not in dont_minmax_types
		if vars.save_bad:
			bad_fh = typed_writer('bytes')(out_fns.pop(), none_support=True)
		fhs = [typed_writer(real_coltype)(fn, none_support=True) for fn in out_fns]
		if vars.save_bad:
			fhs.append(bad_fh)
		write = fhs[0].write
		col_min = col_max = None
		it = d._column_iterator(vars.sliceno, colname, _type='bytes')
		for ix, v in enumerate(it):
			if vars.rehashing:
				chosen_slice = slicemap[ix]
				write = fhs[chosen_slice].write
			if skip_bad:
				if badmap[ix // 8] & (1 << (ix % 8)):
					bad_count[chosen_slice] += 1
					if vars.save_bad:
						bad_fh.write(v)
					continue
			try:
				if v is None or (empty_types_as_None and v == b''):
					v = None
				else:
					v = pyfunc(v.decode('utf-8'))
			except ValueError:
				if default_value is not nodefault:
					v = default_value
					default_count += 1
				elif record_bad:
					bad_count[chosen_slice] += 1
					bv = badmap[ix // 8]
					badmap[ix // 8] = bv | (1 << (ix % 8))
					continue
				else:
					raise Exception("Invalid value %r with no default in %r in %s" % (v, colname, vars.source_name,))
			if do_minmax and v is not None:
				if col_min is None:
					col_min = col_max = v
				if v < col_min: col_min = v
				if v > col_max: col_max = v
			write(v)
		for fh in fhs:
			fh.close()
		if vars.rehashing:
			slicemap.close()
		if options.filter_bad:
			badmap.close()
		vars.res_bad_count[colname] = bad_count
		vars.res_default_count[colname] = default_count
		vars.res_minmax[colname] = [col_min, col_max]
	return real_coltype

def synthesis(slices, analysis_res, prepare_res):
	# each slice returns [ds0data, ds1data, ...], but we want one list per ds
	analysis_res = zip(*analysis_res)
	for p, a in zip(prepare_res, analysis_res):
		synthesis_one(slices, p, a)

def synthesis_one(slices, prepare_res, analysis_res):
	dw, dw_bad, dws, source, source_name, column2type, columns = prepare_res
	analysis_res = list(analysis_res)
	header_printed = [False]
	def print(msg=''):
		from accelerator.compat import builtins
		if not header_printed[0]:
			if dw:
				ds_name = dw.quoted_ds_name
			else:
				ds_name = quote(dws[0].ds_name[:-1] + '<sliceno>')
			header = '%s -> %s' % (source_name, ds_name)
			builtins.print('%s\n%s' % (header, '=' * len(header)))
			header_printed[0] = True
		builtins.print(msg)
	lines = source.lines
	if options.filter_bad:
		bad_line_count_per_slice = [sum(data[1]) for data in analysis_res]
		lines = [num - b for num, b in zip(lines, bad_line_count_per_slice)]
		bad_line_count_total = sum(bad_line_count_per_slice)
		if bad_line_count_total:
			print()
			print('Bad line count   Column')
			for colname in columns:
				cnt = sum(sum(data[0].get(colname, ())) for data in analysis_res)
				if cnt:
					print('%14d   %s' % (cnt, colname,))
		for s, cnt in enumerate(bad_line_count_per_slice):
			dw_bad.set_lines(s, cnt)
		dw_bad.set_compressions('gzip')
	if options.defaults and sum(sum(data[2].values()) for data in analysis_res):
		print()
		print('Defaulted values')
		for colname in sorted(options.defaults):
			defaulted = [data[2].get(colname, 0) for data in analysis_res]
			if sum(defaulted):
				print('    %s:' % (colname,))
				print('        Slice   Defaulted line count')
				slicecnt = 0
				for sliceno, cnt in enumerate(defaulted):
					if cnt:
						print('        %5d   %d' % (sliceno, cnt,))
						slicecnt += 1
				if slicecnt > 1:
					print('        total   %d' % (sum(defaulted),))
	if dws: # rehashing
		if dw: # not as a chain
			final_bad_count = [data[1] for data in analysis_res]
			hash_lines = [data[4] for data in analysis_res]
			for colname in dw.columns:
				for sliceno in range(slices):
					out_fn = dw.column_filename(colname, sliceno=sliceno)
					with open(out_fn, 'wb') as out_fh:
						for s in range(slices):
							if hash_lines[s][sliceno] - final_bad_count[s][sliceno]:
								src_fn = dws[s].column_filename(colname, sliceno=sliceno)
								with open(src_fn, 'rb') as in_fh:
									copyfileobj(in_fh, out_fh)
			for sliced_dw in dws:
				if sliced_dw:
					sliced_dw.discard()
			for sliceno, counts in enumerate(zip(*[data[4] for data in analysis_res])):
				bad_counts = (data[1][sliceno] for data in analysis_res)
				dw.set_lines(sliceno, sum(counts) - sum(bad_counts))
			for sliceno, data in enumerate(analysis_res):
				dw.set_minmax(sliceno, data[3])
		else:
			for sliceno, data in enumerate(analysis_res):
				if dws[sliceno]:
					dws[sliceno].set_minmax(-1, data[3])
					for s, count in enumerate(data[4]):
						dws[sliceno].set_lines(s, count - data[1][s])
					dws[sliceno].set_compressions('gzip')
	else:
		for sliceno, count in enumerate(lines):
			dw.set_lines(sliceno, count)
		for sliceno, data in enumerate(analysis_res):
			dw.set_minmax(sliceno, data[3])
	if dw:
		dw.set_compressions('gzip')
	if header_printed[0]:
		print()
