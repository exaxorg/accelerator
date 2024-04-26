# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2019-2023 Carl Drougge                                     #
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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

description = r'''
Verify various corner cases in dataset_type.
'''

from collections import defaultdict
from datetime import date, time, datetime, timedelta
from math import isnan
import json
import random
import struct
import sys

from accelerator import subjobs
from accelerator.dispatch import JobError
from accelerator.dataset import Dataset, DatasetWriter
from accelerator.dsutil import typed_writer
from accelerator.compat import PY3, UTC
from accelerator.standard_methods import dataset_type
from accelerator import g

options = {'numeric_comma': True}

depend_extra = (dataset_type,)

all_typenames = set(dataset_type.convfuncs)
used_typenames = set()

def used_type(typ):
	if ':' in typ and typ not in all_typenames:
		typ = typ.split(':', 1)[0] + ':*'
	used_typenames.add(typ)

no_default = object()
def verify(name, types, bytes_data, want, default=no_default, want_fail=False, all_source_types=False, exact_types=False, **kw):
	todo = [('bytes', bytes_data,)]
	if all_source_types:
		uni_data = [v.decode('ascii') for v in bytes_data]
		todo += [('ascii', uni_data,), ('unicode', uni_data,)]
	for coltype, data in todo:
		dsname = '%s %s' % (name, coltype,)
		_verify(dsname, types, data, coltype, want, default, want_fail, exact_types, kw)

def _verify(name, types, data, coltype, want, default, want_fail, exact_types, kw):
	if callable(want):
		assert not exact_types, "Don't provide a callable want with exact_types=True"
		check = want
	else:
		def check(got, fromstr, filtered=False):
			want1 = want if isinstance(want, list) else want[typ]
			if filtered:
				want1 = want1[::2]
			if exact_types:
				got = [(v, type(v).__name__) for v in got]
				want1 = [(v, type(v).__name__) for v in want1]
			assert got == want1, 'Expected %r, got %r from %s.' % (want1, got, fromstr,)
	dw = DatasetWriter(name=name, columns={'data': coltype, 'extra': 'bytes'})
	dw.set_slice(0)
	for ix, v in enumerate(data):
		dw.write(v, b'1' if ix % 2 == 0 else b'skip')
	for sliceno in range(1, g.slices):
		dw.set_slice(sliceno)
	bytes_ds = dw.finish()
	for typ in types:
		opts = dict(column2type=dict(data=typ))
		opts.update(kw)
		if default is not no_default:
			opts['defaults'] = {'data': default}
		try:
			jid = subjobs.build('dataset_type', datasets=dict(source=bytes_ds), options=opts)
		except JobError:
			if want_fail:
				continue
			raise Exception('Typing %r as %s failed.' % (bytes_ds, typ,))
		assert not want_fail, "Typing %r as %s should have failed, but didn't (%s)." % (bytes_ds, typ, jid)
		typed_ds = Dataset(jid)
		got = list(typed_ds.iterate(0, 'data'))
		check(got, '%s (typed as %s from %r)' % (typed_ds, typ, bytes_ds,))
		if opts.get('filter_bad'):
			bad_ds = Dataset(jid, 'bad')
			got_bad = list(bad_ds.iterate(0, 'data'))
			assert got_bad == [b'nah'], "%s should have had a single b'nah', but had %r" % (bad_ds, got_bad,)
		if 'filter_bad' not in opts and not callable(want):
			opts['filter_bad'] = True
			opts['column2type']['extra'] = 'int32_10'
			jid = subjobs.build('dataset_type', datasets=dict(source=bytes_ds), options=opts)
			typed_ds = Dataset(jid)
			got = list(typed_ds.iterate(0, 'data'))
			check(got, '%s (typed as %s from %r with every other line skipped from filter_bad)' % (typed_ds, typ, bytes_ds,), True)
			want_bad = [t for t in bytes_ds.iterate(0) if t[1] == b'skip']
			bad_ds = Dataset(jid, 'bad')
			got_bad = list(bad_ds.iterate(0))
			assert got_bad == want_bad, "Expected %r, got %r from %s" % (want_bad, got_bad, bad_ds,)
		used_type(typ)

def test_numbers():
	verify('floats', ['complex32', 'complex64', 'float32', 'float64', 'number'], [b'1.50', b'-inf', b'5e-1'], [1.5, float('-inf'), 0.5], all_source_types=True)
	if options.numeric_comma:
		verify('numeric_comma', ['complex32', 'complex64', 'float32', 'float64', 'number'], [b'1,5', b'1.0', b'9'], [1.5, 42.0, 9.0], '42', numeric_comma=True)
	verify('float32 rounds', ['complex32', 'float32'], [b'1.2'], [1.2000000476837158])
	verify('filter_bad', ['int32_10', 'int64_10', 'complex32', 'complex64', 'float32', 'float64', 'number'], [b'4', b'nah', b'1', b'0'], [4, 1, 0], filter_bad=True)

	all_source_types = True
	for base, values in (
		(10, (b'27', b'027', b' \r27 '),),
		( 8, (b'33', b'033', b'\t000033'),),
		(16, (b'1b', b'0x1b', b'\r001b',),),
		( 0, (b'27', b'\r033', b'0x1b',),),
	):
		types = ['%s_%d' % (typ, base,) for typ in ('int32', 'int64',)]
		verify('base %d' % (base,), types, values, [27, 27, 27], all_source_types=all_source_types)
		types = [typ + 'i' for typ in types]
		if base == 10:
			types += ['float32i', 'float64i']
		values = [v + b'garbage' for v in values]
		verify('base %d i' % (base,), types, values, [27, 27, 27], all_source_types=all_source_types)
		all_source_types = False
	# python2 has both int and long, let's not check exact types there.
	verify('inty numbers', ['number', 'number:int'], [b'42', b'42.0', b'42.0000000', b'43.', b'.0'], [42, 42, 42, 43, 0], exact_types=PY3)
	if options.numeric_comma:
		verify('inty numbers numeric_comma', ['number', 'number:int'], [b'42', b'42,0', b'42,0000000', b'43,', b',0'], [42, 42, 42, 43, 0], numeric_comma=True, exact_types=PY3)

	# Python 2 accepts 42L as an integer, python 3 doesn't. The number
	# type falls back to python parsing, verify this works properly.
	verify('integer with L', ['number'], [b'42L'], [42], want_fail=PY3)

	# tests both that values outside the range are rejected
	# and that None works as a default value.
	for typ, values, default in (
		('int32_10', (b'2147483648', b'-2147483648', b'1O',), '123',),
		('int32_16', (b'80000000', b'-80000000', b'1O',), None,),
		('int64_10', (b'36893488147419103231', b'9223372036854775808', b'-9223372036854775808', b'1O',), None,),
	):
		if default is None:
			want = [None] * len(values)
		else:
			want = [int(default)] * len(values)
		verify('nearly good numbers ' + typ, [typ], values, want, default, exact_types=True)

	verify('not a number', ['number'], [b'forty two'], [42], want_fail=True)
	verify('just a dot', ['number', 'float32'], [b'.'], [0], want_fail=True)

	verify('strbool', ['strbool'], [b'', b'0', b'FALSE', b'f', b'FaLSe', b'no', b'off', b'NIL', b'NULL', b'y', b'jao', b'well, sure', b' ', b'true'], [False] * 9 + [True] * 5, exact_types=True)
	verify('floatbool false', ['floatbool'], [b'0', b'-0', b'1', b'1004', b'0.00001', b'inf', b'-1', b' 0 ', b'0.00'], [False, False, True, True, True, True, True, False, False], exact_types=True)
	verify('floatbool i', ['floatbooli'], [b'1 yes', b'0 no', b'0.00 also no', b'inf yes', b' 0.01y', b''], [True, False, False, True, True, False], exact_types=True)
	def check_special(got, fromstr):
		msg = 'Expected [inf, -inf, nan, nan, nan, nan, inf], got %r from %s.' % (got, fromstr,)
		for ix, v in ((0, float('inf')), (1, float('-inf')), (-1, float('inf'))):
			assert got[ix] == v, msg
		for ix in range(2, 6):
			v = got[ix]
			if isinstance(v, complex):
				v = v.real
			assert isnan(v), msg
	verify('special floats', ['complex32', 'complex64', 'float32', 'float64', 'number'], [b'+Inf', b'-inF', b'nan', b'NaN', b'NAN', b'Nan', b'INF'], check_special)
	verify('complex', ['complex32', 'complex64'], [b'0.25-5j', b'-0.5j', b'infj'], [0.25-5j, -0.5j, complex(0, float('inf'))], exact_types=True)
	if options.numeric_comma:
		verify('complex numeric_comma', ['complex32', 'complex64'], [b'-0,5+1,5j', b'-0,5+1.5j', b',5j'], [-0.5+1.5j, 42j, 0.5j], '42j', numeric_comma=True, exact_types=True)

def test_bytes():
	want = [b'foo', b'\\bar', b'bl\xe4', b'\x00\t \x08', b'a\xa0b\x255\x00']
	for typs, data in (
		(['bytes', 'bytesstrip'], want,),
		(['bytesstrip'], [b'\t\t\t\rfoo\n \x0c', b'\\bar', b'bl\xe4   \x0b', b' \x00\t \x08', b'a\xa0b\x255\x00\n'],),
	):
		verify(typs[0], typs, data, want)

def test_ascii():
	for prefix, data in (
		('ascii', [b'foo', b'\\bar', b'bl\xe4', b'\x00\t \x08'],),
		('asciistrip', [b'\t\t\t\rfoo\n \x0c', b'\\bar', b'bl\xe4   \x0b', b' \x00\t \x08\t'],),
	):
		for encode, want in (
			(None,      ['foo', '\\bar',    'bl\\344', '\x00\t \x08'],),
			('replace', ['foo', '\\bar',    'bl\\344', '\x00\t \x08'],),
			('encode',  ['foo', '\\134bar', 'bl\\344', '\x00\t \x08'],),
			('strict',  ['foo', '\\bar',    None,      '\x00\t \x08'],),
		):
			if encode:
				typ = prefix + ':' + encode
			else:
				typ = prefix
			verify(typ, [typ], data, want, default=None)
	verify(
		'ascii all source types', ['ascii', 'asciistrip'],
		[b'\t\t\t\rfoo\r \x0c', b'\\bar', b' \x00\t \x08\t'],
		{
			'ascii': ['\t\t\t\rfoo\r \x0c', '\\bar', ' \x00\t \x08\t'],
			'asciistrip': ['foo', '\\bar', '\x00\t \x08'],
		},
		all_source_types=True,
	)

def test_unicode():
	data = [b'foo bar\n', b'\tbl\xe5', b'\tbl\xe5a \xe4  ', b'\tbl\xc3\xa5a ', b'\r+AOU-h']
	want = {
		'unicode:utf-8': ['foo bar\n', '既定 ', '既定 ', '\tblåa ', '\r+AOU-h'],
		'unicode:utf-8/strict': ['foo bar\n', '既定 ', '既定 ', '\tblåa ', '\r+AOU-h'],
		'unicode:utf-8/replace': ['foo bar\n', '\tbl\ufffd', '\tbl\ufffda \ufffd  ', '\tblåa ', '\r+AOU-h'],
		'unicode:utf-8/ignore': ['foo bar\n', '\tbl', '\tbla   ', '\tblåa ', '\r+AOU-h'],
		'unicode:iso-8859-1': ['foo bar\n', '\tblå', '\tblåa ä  ', '\tbl\xc3\xa5a ', '\r+AOU-h'],
		'unicode:ascii/replace': ['foo bar\n', '\tbl\ufffd', '\tbl\ufffda \ufffd  ', '\tbl\ufffd\ufffda ', '\r+AOU-h'],
		'unicode:ascii/ignore': ['foo bar\n', '\tbl', '\tbla   ', '\tbla ', '\r+AOU-h'],
		'unicode:utf-7/ignore': ['foo bar\n', '\tbl', '\tbla   ', '\tbla ', '\råh'],
		'unicode:utf-7/replace': ['foo bar\n', '\tbl\ufffd', '\tbl\ufffda \ufffd  ', '\tbl\ufffd\ufffda ', '\råh'],
		'unicodestrip:utf-8': ['foo bar', '既定', '既定', 'blåa', '+AOU-h'],
		'unicodestrip:utf-8/strict': ['foo bar', '既定', '既定', 'blåa', '+AOU-h'],
		'unicodestrip:utf-8/replace': ['foo bar', 'bl\ufffd', 'bl\ufffda \ufffd', 'blåa', '+AOU-h'],
		# strip happens before ignore, so strip+ignore will have one space from '\tbl\xe5a \xe4  '
		'unicodestrip:utf-8/ignore': ['foo bar', 'bl', 'bla ', 'blåa', '+AOU-h'],
		'unicodestrip:iso-8859-1': ['foo bar', 'blå', 'blåa ä', 'bl\xc3\xa5a', '+AOU-h'],
		'unicodestrip:ascii/replace': ['foo bar', 'bl\ufffd', 'bl\ufffda \ufffd', 'bl\ufffd\ufffda', '+AOU-h'],
		'unicodestrip:ascii/ignore': ['foo bar', 'bl', 'bla ', 'bla', '+AOU-h'],
		'unicodestrip:utf-7/ignore': ['foo bar', 'bl', 'bla ', 'bla', 'åh'],
		'unicodestrip:utf-7/replace': ['foo bar', 'bl\ufffd', 'bl\ufffda \ufffd', 'bl\ufffd\ufffda', 'åh'],
	}
	verify('unicode', list(want), data, want, default='既定 ');
	# 既定 is not ascii, so we need a separate default for ascii-like types.
	want = {
		'unicode:ascii/strict': ['foo bar\n', 'standard', 'standard', 'standard', '\r+AOU-h'],
		'unicodestrip:ascii/strict': ['foo bar', 'standard', 'standard', 'standard', '+AOU-h'],
		'unicodestrip:utf-7/strict': ['foo bar', 'standard', 'standard', 'standard', 'åh'],
	}
	verify('unicode with ascii default', list(want), data, want, default='standard');
	verify('utf7 all', ['unicode:utf-7/replace'], [b'a+b', b'a+-b', b'+ALA-'], ['a\ufffd', 'a+b', '°'], all_source_types=True);

def test_datetimes():
	# These use the python datetime classes to verify valid dates,
	# so on Python < 3.6 they will accept various invalid dates.
	# So we only try nearly-good dates on Python 3.6+.

	# We start with some normal verify() calls which test basic function,
	# normal defaults and timezones.
	name = 'date YYYYMMDD'
	typ = 'date:%Y%m%d'
	data = [b'20190521', b'19700101', b'1970-01-01', b'1980', b'nah']
	want = [date(2019, 5, 21), date(1970, 1, 1), date(1945, 6, 20), date(1945, 6, 20), date(1945, 6, 20)]
	default = '19450620'
	verify(name, [typ], data, want, default, all_source_types=True)
	# A datei:-one with extra numbers at end (later i-versions will not add numbers)
	idata = [v + b'1868' for v in data]
	default += '42'
	verify(name + ' i', [typ.replace(':', 'i:', 1)], idata, want, default)

	# Timezone tests. I hope all systems accept the :Region/City syntax.
	verify('tz a', ['datetime:%Y-%m-%d %H:%M'], [b'2020-09-30 11:44', b'x'], [datetime(2020, 9, 30, 11, 44), datetime(2022, 11, 26, 23, 15)], default='2022-11-26 23:15')
	verify('tz b', ['datetime:%Y-%m-%d %H:%M'], [b'2020-09-30 11:44', b'x'], [datetime(2020, 9, 30, 11, 44, tzinfo=UTC), datetime(2022, 11, 26, 23, 15, tzinfo=UTC)], default='2022-11-26 23:15', timezone='UTC')
	verify('tz c', ['datetime:%Y-%m-%d %H:%M'], [b'2020-09-30 13:44', b'2020-02-22 12:44', b'x'], [datetime(2020, 9, 30, 11, 44, tzinfo=UTC), datetime(2020, 2, 22, 11, 44, tzinfo=UTC), datetime(2022, 11, 26, 23, 15, tzinfo=UTC)], default='2022-11-27 00:15', timezone=':Europe/Stockholm')

	# And now, for efficiency, all the remaining tests will be collected in
	# just three datasets to speed things up.
	# Each gets typed three times, as datetime, date and time.
	# They are also first tested with dsutil.strptime[_i](), both to test
	# those functions and to give better errors.

	from accelerator.dsutil import strptime, strptime_i

	# Test that the functions work at all, including default=
	assert strptime('2022-11-23', '%Y-%m-%d') == datetime(2022, 11, 23)
	assert strptime('abc', '%Y-%m-%d', default='nah') == 'nah'
	assert strptime_i('2022-11-23xyz', '%Y-%m-%d') == (datetime(2022, 11, 23), b'xyz')
	assert strptime_i('abc', '%Y-%m-%d', default='nah') == ('nah', b'abc')
	# If some matching succeeded the remaining string should reflect that even on failure.
	assert strptime_i('2022-xyz', '%Y-%m-%d', default='nah') == ('nah', b'xyz')

	tests = defaultdict(list)

	def good(want, *a):
		# datetime is a subclasses of date, so can't use isinstance
		if type(want) == date:
			want = datetime.combine(want, time(0, 0, 0))
		elif type(want) == time:
			want = datetime.combine(date(1970, 1, 1), want)
		for value in a:
			tests[pattern].append((value, want))
			got = strptime(value, pattern)
			assert got == want, "Parsing %r as %r\n    expected %s\n    got      %s" % (value, pattern, want, got,)
			value += 'x'
			got, remaining = strptime_i(value, pattern)
			assert got == want, "Parsing %r as %r\n    expected %s\n    got      %s" % (value, pattern, want, got,)
			assert remaining == b'x', "Parsing %r as %r left %r unparsed, expected %r" % (value, pattern, remaining, b'x',)

	def bad(*a):
		for value in a:
			tests[pattern].append((value, None))
			try:
				got = strptime(value, pattern)
				raise Exception("Parsing %r as %r gave %s, should have failed" % (value, pattern, got,))
			except ValueError:
				pass

	pattern = '%Y%m%d'
	#                         YYYYmmdd    YYYYmmdd    YYYYmmd    Ymmdd
	good(date(2019,  5, 21), '20190521', '2019 521')
	good(date(1970,  1,  1), '19700101', '1970 1 1', '1970 11')
	good(date(1945,  6, 20), '19450620')
	good(date(   1,  1,  1), '00010101', '   1 1 1', '  01 11', '1 101')
	good(date(9999, 12, 31), '99991231')
	#    YYYYmmd!    YYYYm!        YYYY!         YYYY!   !      Ymmdd!
	bad('19700100', '1970 01 01', '1970-01-01', '1980', 'nah', '1 1010')

	pattern = ' %Y %m %d '
	#                         YYYYmmdd
	good(date(2019,  5, 21), '20190521')
	#                            YYYY mm      dd    YYYY  m d
	good(date(1970,  1,  1), '   1970 01\n\n\n01', '1970\t1 1    ')
	#                         YYYY m dd    Y m d    Y    m      d
	good(date(1945,  6, 20), '1945 6 20')
	good(date(   1,  2,  3), '  01 2  3', '1 2 3', '1\t\r2\n\v\f3 \r ')
	#    YY m!     YYYY!
	bad('70 0 0', '1981')

	pattern = '%Yblah%m%d'
	#                         YYYYblahmmdd
	good(date(2019,  5, 21), '2019blah0521')
	good(date(1970,  1,  1), '1970blah0101')
	good(date(1945,  6, 20), '1945blah0620')
	good(date(  72, 11,  9), '  72blah11 9')
	#    YYYYblah!         YYYYblah!   !
	bad('1970blah-01-01', '1980blah', 'nah')

	pattern = '%Y%m%d%H%M%S'
	good(datetime(2013,  4,  9,  1,  2,  3),
		#YYYYmmddHHMMSS    YYYYmmddHHMMSS    YYYYmmddHHMMDD    YYYYmmddHHMMSS
		'2013 4 9 1 2 3', '2013 40901 203', '201304 901 2 3', '20130409010203',
	)
	#    YYYYmmd!           !                  YYYYmmd!           YYYYmmddHH!!
	bad('2013 4 09 1 2 3', ' 2013 4 9 1 2 3', '201304009010203', '20130409016003')

	pattern = '%H%M%y%m%d'
	#                                     HHMMyymmdd    HHMMyymmdd    HMMyymmd
	good(datetime(2019,  5, 21, 18, 52), '1852190521')
	good(datetime(1970,  1,  1,  0,  0), '0000700101')
	good(datetime(1978,  1,  1,  0,  0), '0000780101')
	good(datetime(2000,  3,  4,  5,  6), '0506000304', ' 5 6 0 3 4', '5 6 0 34')
	#    !        HHMMy!
	bad('today', '56034')

	pattern = '%Y%m%d %H%M%S.%f'
	#                                                 YYYYmmdd HHMMSS.fff
	good(datetime(2019,  5, 21, 18, 52,  6, 123000), '20190521 185206.123')
	#                                                 YYYYmmddHHMMSS.ffffff
	good(datetime(1970,  2,  3,  4,  5,  6,      7), '19700203040506.000007')
	#                                                 YYYYmmdd HHMMSS.f
	good(datetime(1978,  1,  1,  0,  0,  0,      0), '19780101 000000.0')
	#    YYYYMMDD  HHMMSS.!         !
	bad('19700203  040506.-00007', 'today')

	pattern = '%H:%M'
	#                   HH:MM    HH:MM    H:MM
	good(time( 3, 14), '03:14', ' 3:14', '3:14')
	good(time(18, 52), '18:52')
	good(time( 0, 42), '00:42')
	#    H!
	bad('25:10')

	pattern = '%H%M%%f%S' # this is HHMM followed by literal '%f' and then SS
	#                       HHMM%fSS    HHMM%fS    HMM%fSS
	good(time( 3, 14,  0), '0314%f00', ' 314%f0')
	good(time(18, 52,  9), '1852%f09', '1852%f9')
	good(time( 0, 42, 18), '0042%f18')
	good(time( 1,  2,  3), '0102%f03', ' 102%f3', '1 2%f 3')
	#    HHMM%fS!    HHMM%f!   HMM!        HMM%fS!
	bad('1938%f60', '1852%f', '1 02%f 3', '1 2%f ')

	# Test microseconds (%f)
	pattern = '%H%M %f.%S'
	#                               HHMMf.SS
	good(time( 3, 14,  0, 900000), '03149.00')
	#                               HHMMfff.SS
	good(time(18, 52,  9, 456000), '1852456.09')
	#                               HHMM   ffffff.SS
	good(time(19, 38, 44, 123456), '1938   123456.44')
	#                               HHMMffffff.SS
	good(time(18, 52,  9, 456000), '1852456   .09')
	#                               HHMMff.SS
	good(time( 0, 42, 18,      0), '004200.18')
	#    HHMMffffff.S!    HHMMffffff!
	bad('1938123456.60', '1852456    .09')

	# Just month and day (implicitly 1970)
	pattern = '%m%d'
	#                         mmdd    mmd    mmdd    mmd
	good(date(1970,  1,  2), '0102', '012', ' 102', ' 12')
	good(date(1970, 10, 20), '1020')
	good(date(1970,  6, 20), '0620')
	#           m!   mm!   mmd!   mmd!
	bad('nah', '1', '12', '12 ', '12  ')

	# Test that two digit years choose the right century
	pattern = '%y'
	#                         yy    y
	good(date(1969,  1,  1), '69')
	good(date(1970,  1,  1), '70')
	good(date(2000,  1,  1), '00', '0')
	good(date(2008,  1,  1), '08', '8')
	good(date(2019,  1,  1), '19')
	good(date(2068,  1,  1), '68')
	#    yy!     yy!    !
	bad('2000', '100', '-1')

	pattern = '%f%d'
	#                                                ffffffdd    ffd    ffdd
	good(datetime(1970, 1, 30, microsecond=300   ), '00030030')
	good(datetime(1970, 1,  6                    ), '00000006', '0 6', '0 06')
	good(datetime(1970, 1,  3, microsecond=30    ), '00003003')
	good(datetime(1970, 1, 11, microsecond=999999), '99999911')
	#    ffffffd!
	bad('99999999')

	pattern = '%f.%d'
	good(datetime(1970,  1, 30, microsecond=300000), '30.30')
	good(datetime(1970,  1,  6                    ), '0.06')
	good(datetime(1970,  1,  3, microsecond=300   ), '00030.03')
	good(datetime(1970,  1, 11, microsecond=999999), '999999.11')
	#    ffffff.d!
	bad('999999.99')

	pattern = '%f%d'
	#                                                 fffdd
	good(datetime(1970,  1, 30, microsecond=300000), '30 30')
	#                                                 ffdd    fffffdd    ffffffdd
	good(datetime(1970,  1,  6                    ), '0 06', '0    06', '0000   6')
	#                                                 ffffffdd    ffffffdd    fffffd
	good(datetime(1970,  1,  3, microsecond=300   ), '00030003', '00030 03', '0003 3')
	good(datetime(1970,  1, 11, microsecond=999999), '99999911')
	#    ffffffd!    ffffffdd!    ffffffd!
	bad('99999999', '999999 11', '0000    6')

	pattern = '%f%%f%%%d' # this is %f, literal '%f%', %d
	good(datetime(1970,  1, 30, microsecond=300000), '30%f%30')
	good(datetime(1970,  1,  6                    ), '0%f%06', '0000 %f%6')
	good(datetime(1970,  1,  3, microsecond=300   ), '00030%f%03')
	good(datetime(1970,  1, 11, microsecond=999999), '999999%f%11')
	#    ffffff%f%d!    !  (%f doesn't accept leading spaces)
	bad('999999%f%99', ' 30%f%30')

	# Avoid testing "far off" dates with functions that might use time_t
	# if long (the smallest type time_t is likely to be) is small.
	time_t_is_probably_big_enough = (len(struct.pack("@l", 0)) > 4)

	# "Unix" timestamps, seconds since 1970-01-01 00:00:00.
	# Uses gmtime_r, so limited by the platform time_t.
	pattern = '%s.%f'
	good(datetime(1970,  1,  1,  0,  0, 30, 300000), '30.30')
	good(datetime(2019,  5, 24,  1, 54, 13, 847211), '1558662853.847211')
	good(datetime(1970,  1,  1,  0,  0,  0, 100000), '0.1')
	if time_t_is_probably_big_enough:
		good(datetime(2286, 11, 20, 17, 46, 40, 234567), '10000000000.234567')
		# Since %f is separate this is actually later than -10000000000, which is unfortunate
		good(datetime(1653,  2, 10,  6, 13, 20, 111000), '-10000000000.111')
	#    !   !      !   !     !           !          !  (%s accepts no spaces)
	bad('', '.', '0.', '.1', ' 30.30', '30 .30', '30. 30')

	# "Java" timestamps, milliseconds since 1970-01-01 00:00:00.
	# Uses gmtime_r, so limited by the platform time_t.
	pattern = '%J'
	good(datetime(1970,  1,  1,  0,  0,  0,      0), '0')
	good(datetime(2019,  5, 24,  1, 54, 13, 847000), '1558662853847')
	good(datetime(1970,  1,  1,  0,  0,  0,   1000), '1')
	good(datetime(1969, 12, 31, 23, 59, 57, 995000), '-2005')
	if time_t_is_probably_big_enough:
		good(datetime(2286, 11, 20, 17, 46, 40, 234000), '10000000000234')
		# With %J the ms part is in the same number, and thus negative numbers are correctly handled.
		good(datetime(1653,  2, 10,  6, 13, 19, 889000), '-10000000000111')
	#    !    !      !    !  (%J accepts no spaces)
	bad('', '0x0', '0 ', ' 0')

	pattern = 'blah %Jbluh'
	good(datetime(1970,  1,  1,  0,  0,  0,      0), 'blah0bluh')
	good(datetime(1970,  1,  1,  0,  0, 30,      0), 'blah   30000bluh')
	good(datetime(1970,  1,  1,  0,  0,  0,   1000), 'blah1bluh')
	good(datetime(1969, 12, 31, 23, 59, 57, 995000), 'blah-2005bluh')
	bad('bla0bluh', 'blah0blu')

	# Excel dates. First in the default Lotus 1-2-3 format used by MS Excel.
	pattern = '%e'
	epoch = datetime(1899, 12, 31)
	if time_t_is_probably_big_enough:
		good(datetime(1900,  1,  1            ), '1')
		good(datetime(1900,  1,  2            ), '2')
		good(datetime(1900,  1,  2, 18        ), '2.75')
		good(datetime(1900,  2, 27            ), '58')
		good(datetime(1900,  2, 28            ), '59')
		good(datetime(1900,  3,  1            ), '60') # actually invalid (non-existant 1900-02-29)
		good(datetime(1900,  3,  1            ), '61') # same as 60
		good(datetime(1900,  3,  2            ), '62')
	assert datetime(1970, 1, 1) - epoch == timedelta(days=25568) # one off because of incorrect leap year 1900
	good(datetime(1970,  1,  1            ), '25569')
	if time_t_is_probably_big_enough:
		assert datetime(1800, 1, 2) - epoch == timedelta(days=-36522)
		good(datetime(1800,  1,  2            ), '-36522')
		good(datetime(1800,  1,  1, 18        ), '-36522.25')
		good(datetime(1800,  1,  1, 18        ), '4294930773.75') # gnumeric can get confused and produce this kind of thing
		good(datetime(1899, 12, 30            ), '-1')
		good(datetime(1899, 12, 30,  6        ), '-0.75')
		good(datetime(1899, 12, 31            ), '0')
		good(datetime(1899, 12, 31, 18        ), '0.75')
		assert datetime(9999, 12, 31) - epoch == timedelta(days=2958464) # one off because of incorrect leap year 1900
		good(datetime(9999, 12, 31            ), '2958465')
		good(datetime(9999, 12, 31, 23, 59, 58), '2958465.99998')
		assert datetime(1, 1, 1) - epoch == timedelta(days=-693594)
		good(datetime(   1,  1,  1            ), '-693594')
		good(datetime(   1,  1,  1,  0,  0,  1), '-693593.99999')
		good(datetime(1900,  1,  1, 23, 59, 59), '1.99999')
		good(datetime(1900,  1,  1, 23, 59, 58), '1.99998')
		good(datetime(1900,  1,  1, 23, 59, 57), '1.99997')
		good(datetime(1900,  1,  1, 23, 59, 57), '1.99996') # also 57!
		good(datetime(1900,  1,  1, 23, 59, 56), '1.99995')
	bad('-36522.-25')
	bad('2958466', '2958466.00001') # > positive max
	bad('-693595', '-693594.00001') # < negative max

	# Since 1 is the default flag for e, this is the same as %e
	pattern = '%1e'
	if time_t_is_probably_big_enough:
		good(datetime(1900,  1,  1            ), '1')
		good(datetime(1900,  2, 27            ), '58')
		good(datetime(1900,  2, 28            ), '59')
		good(datetime(1900,  3,  1            ), '60') # actually invalid (non-existant 1900-02-29)
		good(datetime(1900,  3,  1            ), '61') # same as 60
		good(datetime(1900,  3,  2            ), '62')
		good(datetime(1899, 12, 30            ), '-1')
	good(datetime(1970,  1,  1            ), '25569')
	# That's enough re-testing.

	# 0 is "Libre Office" dates, where 1900 is not a leap year.
	# (Dates before 1900-03-01 are offset compared to Lotus 1-2-3.)
	pattern = '%0e'
	epoch = datetime(1899, 12, 30)
	if time_t_is_probably_big_enough:
		good(datetime(1899, 12,  31           ), '1')
		good(datetime(1900,  1,  1            ), '2')
		good(datetime(1900,  1,  1, 18        ), '2.75')
		good(datetime(1900,  2, 26            ), '58')
		good(datetime(1900,  2, 27            ), '59')
		good(datetime(1900,  2, 28            ), '60')
		good(datetime(1900,  3,  1            ), '61')
		good(datetime(1900,  3,  2            ), '62')
	assert datetime(1970, 1, 1) - epoch == timedelta(days=25569)
	good(datetime(1970,  1,  1            ), '25569')
	if time_t_is_probably_big_enough:
		assert datetime(1800, 1, 1) - epoch == timedelta(days=-36522)
		good(datetime(1800,  1,  1            ), '-36522')
		good(datetime(1799, 12, 31, 18        ), '-36522.25')
		good(datetime(1799, 12, 31, 18        ), '4294930773.75') # gnumeric can get confused and produce this kind of thing
		good(datetime(1899, 12, 29            ), '-1')
		good(datetime(1899, 12, 29,  6        ), '-0.75')
		good(datetime(1899, 12, 30            ), '0')
		good(datetime(1899, 12, 30, 18        ), '0.75')
		assert datetime(9999, 12, 31) - epoch == timedelta(days=2958465)
		good(datetime(9999, 12, 31            ), '2958465')
		good(datetime(9999, 12, 31, 23, 59, 58), '2958465.99998')
		assert datetime(1, 1, 1) - epoch == timedelta(days=-693593)
		good(datetime(   1,  1,  1            ), '-693593')
		good(datetime(   1,  1,  1,  0,  0,  1), '-693592.99999')
		good(datetime(1899, 12 , 31, 23, 59, 59), '1.99999')
		good(datetime(1899, 12 , 31, 23, 59, 58), '1.99998')
		good(datetime(1899, 12 , 31, 23, 59, 57), '1.99997')
		good(datetime(1899, 12 , 31, 23, 59, 57), '1.99996') # also 57!
		good(datetime(1899, 12 , 31, 23, 59, 56), '1.99995')
	bad('-36522.-25')
	bad('2958466', '2958466.00001') # > positive max
	bad('-693594', '-693593.00001') # < negative max

	# 2 is "Mac Office" dates, epoch is 1904-01-01 and leap years are correct.
	pattern = '%2e'
	epoch = datetime(1904, 1, 1)
	good(datetime(1904,  1,  2            ), '1')
	good(datetime(1904,  1,  3            ), '2')
	good(datetime(1904,  1,  3, 18        ), '2.75')
	good(datetime(1904,  2, 28            ), '58')
	good(datetime(1904,  2, 29            ), '59')
	good(datetime(1904,  3,  1            ), '60')
	good(datetime(1904,  3,  2            ), '61')
	good(datetime(1904,  3,  3            ), '62')
	assert datetime(1974, 1, 2) - epoch == timedelta(days=25569)
	good(datetime(1974,  1,  2            ), '25569')
	if time_t_is_probably_big_enough:
		assert datetime(1804, 1, 3) - epoch == timedelta(days=-36522)
		good(datetime(1804,  1,  3            ), '-36522')
		good(datetime(1804,  1,  2, 18        ), '-36522.25')
		good(datetime(1804,  1,  2, 18        ), '4294930773.75') # gnumeric can get confused and produce this kind of thing
	good(datetime(1903, 12, 31            ), '-1')
	good(datetime(1903, 12, 31,  6        ), '-0.75')
	good(datetime(1904,  1,  1            ), '0')
	good(datetime(1904,  1,  1, 18        ), '0.75')
	if time_t_is_probably_big_enough:
		assert datetime(9999, 12, 31) - epoch == timedelta(days=2957003)
		good(datetime(9999, 12, 31            ), '2957003')
		good(datetime(9999, 12, 31, 23, 59, 58), '2957003.99998')
		assert datetime(1, 1, 1) - epoch == timedelta(days=-695055)
		good(datetime(   1,  1,  1            ), '-695055')
		good(datetime(   1,  1,  1,  0,  0,  1), '-695054.99999')
	good(datetime(1904,  1,  2, 23, 59, 59), '1.99999')
	good(datetime(1904,  1,  2, 23, 59, 58), '1.99998')
	good(datetime(1904,  1,  2, 23, 59, 57), '1.99997')
	good(datetime(1904,  1,  2, 23, 59, 57), '1.99996') # also 57!
	good(datetime(1904,  1,  2, 23, 59, 56), '1.99995')
	bad('-36522.-25')
	bad('2957004', '2957004.00001') # > positive max
	bad('-695056', '-695056.00001') # < negative max

	# Test wildcard characters.
	# '% ' whitespace (exactly one, plain space is any number)
	# '%.' any character except whitespace
	# '%*' any character including whitespace
	# '%#' any digit
	# '%@' any non-digit character, excluding whitespace
	# '%^' any non-digit character, including whitespace
	pattern = '%Y% %m%.%d%*%H%#%M%@%S%^'
	#                                         Y m.d*HH#M@S^    Y  mm.d*HH#M@S^
	good(datetime(   1,  2,  3,  4,  5,  6), '1 2x3x 495x6x', '1\t02x3x 495x6x')
	good(datetime(   1,  2,  3,  4,  5,  6), '1 2.3  495-6 ', '1\n02.3  495-6 ')
	#                                          ? ?    ? ? ?    Y m!
	bad(                                     '1.2.3  495-6 ', '1  02.3  495-6 ')
	bad(                                     '1 2 3  495-6 ')
	bad(                                     '1 2.3  4.5-6 ')
	bad(                                     '1 2.3  495 6 ')
	#                                         Y m.d*HH#M@SS!
	bad(                                     '1 2.3  495-111')
	#                                         Y m.d*H!
	bad(                                     '1 2.3 495-6 ')
	#                                         YYYY mm.dd*HH#M@S^
	good(datetime(   1,  2,  3,  4,  5,  6), '0001  22 33 445/6/')

	# Test separators with counts.
	# Only test with %@, as the repeat logic is shared.
	pattern = '%y%3@%m'
	good(date(2003, 6, 1), '3abc6', '3...6', '3+-_6', '3/@\\6')
	# Test with a range
	pattern = '%y%2,5@%m'
	good(date(2003, 6, 1), '3abcde6', '3..6', '3...6', '3....6')
	bad('3abcdef6', '3.6')
	# Test with a range that includes 0
	pattern = '%y%0,2@%m'
	good(date(2003, 6, 1), '03ab6', '03a6', '036')
	bad('36', '3abc6')

	# Let's generate an exhaustive search (within ASCII) for all separators
	# (and no \0, strptime doesn't handle it.)
	def negate_ascii(chars):
		return {c for c in map(chr, range(1, 128)) if c not in chars}
	for sep, allowed in (
		('% ', '\t\n\v\f\r '),
		('%.', negate_ascii('\t\n\v\f\r ')),
		('%*', negate_ascii('')),
		('%#', '0123456789'),
		('%@', negate_ascii('0123456789\t\n\v\f\r ')),
		('%^', negate_ascii('0123456789')),
	):
		pattern = '%Y' + sep + '%m'
		for c in allowed:
			good(date(3456, 12, 1), '3456' + c + '12')
			bad('3456' + c + c + '12') # don't match several
		for c in negate_ascii(allowed):
			bad('3456' + c + '12')

	# Test optional tails. %/ makes the rest of the pattern optional,
	# but the whole string still has to match.
	pattern = '%d%/%m%y'
	#                             ddmmyy    ddmm    ddm    dd    d
	good(datetime(1970,  1,  6), '060170', '0601', '061', '06', '6')
	good(datetime(1970,  2,  3), '030270', '0302', '032')
	#    ddmm!    d!
	bad('0601x', '60170')

	# Java timestamp with optional ms-fraction
	# (which overrides the fractional part of the full timestamp when used)
	pattern = '%J%/.%3f'
	#                                                JJJJJ    JJJJJ.    JJJJJ.FFF    JJJJJ.FFF
	good(datetime(1970,  1,  1,  0,  0, 30       ), '30000', '30000.', '30000.000', '30303.0  ')
	good(datetime(1970,  1,  1,  0,  0, 30, 50000), '30050', '30050.', '30000.050', '30303.05 ')
	#    JJJJJ!    JJJJJ.FFF!    !
	bad('30050 ', '30000.05  ', ' 30050')

	# Test %?, making the next field(s) optional.
	# The way this works is that as long as it (all) matches it is used,
	# there is no backtracking if fields after the optional part don't match.

	# Test single optional values
	pattern = '%Y%?%b%d'
	#                             YYYYbbbd    YYYYbbbbbbbdd    YYYYd
	good(datetime(2023,  1,  2), '2023jan2', '2023january02', '20232')
	good(datetime(2023,  1,  2), '2023jAN2', '2023JanuarY02', '202302')
	#    YYYY!   YYYYbbb!   YYYYbb! (and "jab2" isn't ok for d)
	bad('2023', '2023jan', '2023jab2')

	# Test optional values with a count (%H only applies if %b worked)
	pattern = '%Y%2?%b%H%d'
	#                                 YYYYbbbHHd    YYYYbbbbbbbHHdd
	good(datetime(2023,  1,  3, 16), '2023jan163', '2023january1603')
	#                                 YYYYdd
	good(datetime(2023,  1, 16,  0), '202316')
	#    YYYYdd!    YYYYbbbbbbbHH!
	bad('2023163', '2023january03')

	# %? doesn't have to match %-things, here it's also matching the ":"
	pattern = '%Y-%m %3?%H:%M %d'
	#                                     YYYY-mm HH:MM dd    YYYY-mm HH:MMd
	good(datetime(2018, 10,  1, 12,  1), '2018-10 12:01 01', '2018-10 12: 11')
	#                                     YYYY-mm dd    YYYY-mm HH:MM dd
	good(datetime(2018, 10, 11,  0,  0), '2018-10 11', '2018-10 00:00 11')
	#    YYYY-mm HH:MM!   YYYY-MM HH:MM!   YYYY-MM HH:!! (fails '  ' as MM, then fails with trailing ':  1' after the dd)
	bad('2018-10 00:00', '2018-10 00: 1', '2018-10 05:  1')

	# Make sure the spaces weren't important (except for readability)
	pattern = '%Y-%m%3?%H:%M%d'
	#                                     YYYY-mmHH:MMdd    YYYY-mmHH:MMd
	good(datetime(2018, 10,  1, 12,  1), '2018-1012:0101', '2018-1012: 11')
	#                                     YYYY-mmdd    YYYY-mmHH:MMdd
	good(datetime(2018, 10, 11,  0,  0), '2018-1011', '2018-1000:0011')
	#    YYYY-mmHH:MM!   YYYY-MMHH:MM!   YYYY-MMHH:!! (fails '  ' as MM, then fails with trailing ':  1' after the dd)
	bad('2018-1000:00', '2018-1000: 1', '2018-1005:  1')

	# Test optional values with a range (%H only applies if %b worked, and %M only if %H)
	pattern = '%d%2,3?%b%H%M%Y'
	#                                     ddbbbHHMMYYYY    dbbbHHMMYYYY
	good(datetime(2023,  1,  3, 16,  4), '03jan16042023', '3jan16 42023')
	#                                     ddbbbHHYYYY  (98 is not a possible M value)
	good(datetime(9876,  1,  3, 16    ), '03jan169876')
	#                                     ddYYYY
	good(datetime(1604,  1,  3        ), '031604')
	#    ddYYYY!       ddbbbH!  (99 is not a possible H value, so it fails there, and then on 'j' for Y)
	bad('0316042023', '03jan9999')

	# Recursive optional bits
	# This is hard to read, maybe "(a(b%Y)c)(d(efg)h)(%Y)-%m (ABC[D%H%M]) %d" helps?
	# A %? inside a %? counts as a single token, regardless of count.
	pattern = '%3?a%2?b%Yc%3?d%3?efgh%?%Y-%m %3,6?ABCD%H%M %d'
	#                                  abYYYYcdefghYYYY-mm ABCDHHMM d
	good(datetime(2022, 11, 7, 1, 2), 'ab1998cdefgh2022-11 ABCD0102 7',)
	#                                  abYYYYc-mm d    abYYYYc-mm ABCd
	good(datetime(2023, 12, 8, 0, 0), 'ab2023c-12 8', 'ab2023c-12 ABC8')
	#    abYYYYcdefghYYYY-mm !            abYYYYcdefghYYYY-mm AB!
	bad('ab1998cdefgh2022-11 BCD0102 7', 'ab1998cdefgh2022-11 AB 7')

	# Test the else function %:
	# Accept YYYYmmdd if that fits, otherwise an Excel date
	pattern = '%3?%Y%m%d%:%e'
	#                       eeeee    YYYYmmdd
	good(date(1997, 5, 5), '35555', '19970505')
	bad('')

	# A plausable date thing, accepting both name and number for the month.
	# Try it in both orders.
	for pattern in ('%d-%?%b%:%m-%y', '%d-%?%m%:%b-%y'):
		#                         dd-mm-yy,   dd-m-yy,   dd-bbb-yy,   dd-bbbbb-yy
		good(date(1997,  3, 31), '31-03-97', '31-3-97', '31-mar-97', '31-march-97')
		#    dd-bbb!        dd-bbb!       dd-bbb-yy!      dd-mm!         dd-m!
		bad('31-mar03-97', '31-mar3-97', '31-mar-03-97', '31-03mar-97', '31-3mar-97', '31--97')

	# Test %: recursively too.
	# This is either the same as above or the same in opposite order.
	# I.e. it's one of those terrible things that guesses which end the
	# year is in depending on if the first number if valid as a day or not.
	#             11233334444566   11233334444566
	pattern = '%6?%d-%?%b%:%m-%y%6:%y-%?%b%:%m-%d'
	good(date(1985, 12, 24), '24-dec-85', '24-12-85', '85-12-24', '85-dec-24')
	#        both years,  repeating everything in various orders
	bad('', '85-dec-85', '24-12-8585-12-24', '24-12-8524-12-85', '85-12-2485-12-24', '85-12-2424-12-85')

	# Combining %/ with %? and %:

	# If there is a month name %/ starts applying, thus making the X optional,
	# but the X still has to match to make the %3? happy!
	# So in practice the %/ only applies to the -%d, and of course only
	# if the %b matches.
	pattern = '%y-%3?%b%/X%:%m-%d'
	#                         yy-mm-dd    yy-bbbX-dd    yy-bbbX-    yy-bbbX
	good(date(1978, 11,  1), '78-11-01', '78-novX-01', '78-novX-', '78-novX')
	good(date(2008,  4, 14), '08-04-14', '08-aprX-14')
	#    yy-mm!   yy-bbb!      yy-bbb!    yy-bbb!
	bad('78-11', '78-nov-01', '78-nov-', '78-nov')

	# Same thing, but put the %/ in the else.
	# Does not match exactly the same things as above, because %: only
	# cares about the count for skipping when the initial %? matched. In
	# other words, the X does not have to match to make the %/ apply.
	pattern = '%y-%?%m%3:%b%/X-%d'
	#                         yy-mm-dd    yy-bbbX-dd    yy-bbbX-    yy-bbbX    yy-bbb
	good(date(1978, 11,  1), '78-11-01', '78-novX-01', '78-novX-', '78-novX', '78-nov')
	good(date(2008,  4, 14), '08-04-14', '08-aprX-14')
	#    yy-mm!   yy-bbb!      yy-bbb!
	bad('78-11', '78-nov-01', '78-nov-')

	# Use %:%3? instead of %3: to almost match the same as in the first case,
	# but not quite as that then makes both sides optional.
	pattern = '%y-%?%m%:%3?%b%/X-%d'
	#                         yy-mm-dd    yy-bbbX-dd    yy-bbbX-    yy-bbbX
	good(date(1978, 11,  1), '78-11-01', '78-novX-01', '78-novX-', '78-novX')
	good(date(2008,  4, 14), '08-04-14', '08-aprX-14')
	#                         yy--dd
	good(date(1978,  1, 23), '78--23')
	#    yy-mm!   yy-bbb!      yy-bbb!    yy-bbb!
	bad('78-11', '78-nov-01', '78-nov-', '78-nov')

	# Test %I and %p.
	pattern = '%I%p'
	good(time( 0,  0), '12am')
	good(time( 1,  0), '01Am', '1aM')
	good(time(12,  0), '12pm')
	good(time(13,  0), '01Pm', '1pM')
	bad('0pm', '00pm', '13pm', '12', '13')

	# In "wrong" order.
	pattern = '%p%I'
	good(time( 0,  0), 'AM12')
	good(time( 1,  0), 'AM01', 'AM1')
	good(time(12,  0), 'PM12')
	good(time(13,  0), 'PM01', 'PM1')
	bad('PM0', 'PM00', 'PM13', '12', 'PM')

	# Test that %p works with %H. (Has to come after of course.)
	pattern = '%H%p'
	good(time( 2,  0), '14AM', '02AM', '2AM')
	good(time(14,  0), '14PM', '02PM', '2PM')
	bad('14')

	# Test that %p does not affect %H when %p comes first
	pattern = '%p%H'
	good(time( 2,  0), 'AM02', 'PM02')
	good(time(14,  0), 'AM14', 'PM14')

	# Test %C
	pattern = '%C%y'
	good(date(   1, 1, 1), '0001')
	good(date(1850, 1, 1), '1850')
	good(date(2070, 1, 1), '2070')
	good(date(9999, 1, 1), '9999')

	# In "wrong" order
	pattern = '%y%C'
	good(date(   1, 1, 1), '0100')
	good(date(1850, 1, 1), '5018')
	good(date(2070, 1, 1), '7020')
	good(date(9999, 1, 1), '9999')

	# Test %C to override century from %Y
	pattern = '%Y%C'
	good(date(   1, 1, 1), '200100')
	good(date(1850, 1, 1), '195018')
	good(date(2070, 1, 1), '187020')
	good(date(9999, 1, 1), '199999')

	# Test %Y to override %C
	pattern = '%C%Y'
	good(date(   1, 1, 1), '200001')
	good(date(1850, 1, 1), '191850')
	good(date(2070, 1, 1), '182070')
	good(date(9999, 1, 1), '199999')

	# Test keeping century from %Y with %y
	pattern = '%Y%y'
	good(date(   1, 1, 1), '008801')
	good(date(1850, 1, 1), '186850')
	good(date(2070, 1, 1), '202370')
	good(date(9999, 1, 1), '992299')

	# A complete and totally idiotic format.
	pattern = '%y%I%M%C%d%p%b%S'
	#                                         yyIIMMCCDppbbbS    yyIIMMCCddppbbbbbbbSS
	good(datetime(1956, 10,  5, 23, 34,  8), '561134195pmoct8', '56113419 5Pmoctober08')
	#    yy!!MMCCDppbbbS    yyIIMMCCddppbbbbbb!     yyIIMMCCdppbbbS!
	bad('562134195pmoct8', '56113419 5PMoctobe08', '561134195pmoct8 ')

	# Save all of these in three datasets with one column per pattern.
	#     One with only good values
	#     One with only good values, with trailing garbage (typed with *i:)
	#     One with both good and bad values (typed with defaults)
	# Columns are padded with None-values so they are all the same length.
	# The order of each column is random, just in case there is some
	# inter-value state.
	# Each dataset is typed as datetime, date and time.
	def one(name, type_suffix, value_suffix, with_defaults):
		dw = DatasetWriter(name=name, allow_missing_slices=True)
		colnames = []
		to_write = []
		want = []
		max_len = max(len(values) for _, values in tests.items())
		for pattern, values in tests.items():
			dw.add(pattern, 'ascii', none_support=True)
			colnames.append(pattern)
			if not with_defaults: # remove bad
				values = [v for v in values if v[1] is not None]
			values = values + [(None, None)] * (max_len - len(values))
			random.shuffle(values)
			to_write.append([None if w is None else w + value_suffix for w, _ in values])
			want.append([w for _, w in values])
		dw.set_slice(0)
		for line in zip(*to_write):
			dw.write_dict(dict(zip(colnames, line)))
		source = dw.finish()

		def check(type_as, fix):
			column2type = {col: type_as + type_suffix + ':' + col for col in colnames}
			kw = dict(source=source, column2type=column2type)
			if with_defaults:
				kw['defaults'] = {col: None for col in colnames}
			ds = subjobs.build('dataset_type', **kw).dataset()
			for col, wrote, good in zip(colnames, to_write, want):
				got = list(ds.iterate(0, col))
				assert len(got) == len(wrote) == len(good) == max_len, col
				for got, wrote, good in zip(got, wrote, good):
					if good is not None:
						good = fix(good)
					assert got == good, 'Typing %r as %r gave %r, expected %r' % (wrote, column2type[col], got, good,)

		check('datetime', lambda dt: dt)
		check('date', lambda dt: dt.date())
		check('time', lambda dt: dt.time())

	one('good datetimes', '', '', False)
	one('good datetimes with trailing garbage', 'i', 'whee', False)
	one('good and bad datetimes', '', '', True)

	if sys.version_info >= (3, 6):
		# These can't be part of the dataset_type calls above, as they will only
		# fail when typed as dates, not as times. Therefore, they are after the
		# one() calls, so only the strptime part of the testing is done.
		pattern = '%Y-%m-%d'
		good(date(1992, 2, 29), '1992-02-29')
		bad('2019-02-29', '1970-02-31', '1980-06-31')
		# And then use verify() to test the same values in dataset_type.
		for type_as, func in (
			('date', date),
			('datetime', datetime),
		):
			verify('nearly good %s YYYY-MM-DD' % (type_as,), ['%s:%s' % (type_as, pattern,)], [b'2019-02-29', b'1970-02-31', b'1980-06-31', b'1992-02-29'], [None, None, None, func(1992, 2, 29)], None, False)


def test_filter_bad_across_types():
	columns={
		'bytes': 'bytes',
		'float64': 'bytes',
		'int32_10': 'ascii',
		'json': 'unicode',
		'number:int': 'unicode',
		'unicode:utf-8': 'bytes',
	}
	# all_good, *values
	# Make sure all those types (except bytes) can filter other lines,
	# and be filtered by other lines. And that several filtering values
	# is not a problem (line 11).
	data = [
		[True,  b'first',    b'1.1', '1',  '"a"',   '001', b'ett',],
		[True,  b'second',   b'2.2', '2',  '"b"',   '02',  b'tv\xc3\xa5',],
		[True,  b'third',    b'3.3', '3',  '["c"]', '3.0', b'tre',],
		[False, b'fourth',   b'4.4', '4',  '"d"',   '4.4', b'fyra',],       # number:int bad
		[False, b'fifth',    b'5.5', '-',  '"e"',   '5',   b'fem',],        # int32_10 bad
		[False, b'sixth',    b'6.b', '6',  '"f"',   '6',   b'sex',],        # float64 bad
		[False, b'seventh',  b'7.7', '7',  '{"g"}', '7',   b'sju',],        # json bad
		[False, b'eigth',    b'8.8', '8',  '"h"',   '8',   b'\xa5\xc3tta',],# unicode:utf-8 bad
		[True,  b'ninth',    b'9.9', '9',  '"i"',   '9',   b'nio',],
		[True,  b'tenth',    b'10',  '10', '"j"',   '10',  b'tio',],
		[False, b'eleventh', b'11a', '1-', '"k",',  '1,',  b'elva',],       # float64, int32_10 and number:int bad
		[True,  b'twelfth',  b'12',  '12', '"l"',   '12',  b'tolv',],
	]
	want_bad = [tuple(l[1:]) for l in data if not l[0]]
	dw = DatasetWriter(name="filter bad across types", columns=columns, allow_missing_slices=True)
	cols_to_check = ['int32_10', 'bytes', 'json', 'unicode:utf-8']
	if PY3:
		# z so it sorts last.
		dw.add('zpickle', 'pickle')
		cols_to_check.append('zpickle')
		for ix in range(len(data)):
			data[ix].append({ix})
	dw.set_slice(0)
	want = []
	def add_want(ix):
		v = data[ix]
		want.append((int(v[3]), v[1], json.loads(v[4]), v[6].decode('utf-8'),))
		if PY3:
			want[-1] = want[-1] + (v[7],)
	for ix, v in enumerate(data):
		if v[0]:
			add_want(ix)
		dw.write(*v[1:])
	source_ds = dw.finish()
	# Once with just filter_bad, once with some defaults too.
	defaults = {}
	for _ in range(2):
		jid = subjobs.build(
			'dataset_type',
			datasets=dict(source=source_ds),
			options=dict(column2type={t: t for t in columns}, filter_bad=True, defaults=defaults),
		)
		typed_ds = Dataset(jid)
		got = list(typed_ds.iterate(0, cols_to_check))
		assert got == want, "Expected %r, got %r from %s (from %r%s)" % (want, got, typed_ds, source_ds, ' with defaults' if defaults else '')
		bad_ds = Dataset(jid, 'bad')
		got_bad = list(bad_ds.iterate(0, sorted(columns)))
		assert got_bad == want_bad, "Expected %r, got %r from %s (from %r%s)" % (want_bad, got_bad, bad_ds, source_ds, ' with defaults' if defaults else '')
		# make more lines "ok" for the second lap
		if not defaults:
			want_bad.pop(0) # number:int
			want_bad.pop(1) # float64
			want_bad.pop(1) # json
		defaults = {'number:int': '0', 'float64': '0', 'json': '"replacement"'}
		add_want(3)
		add_want(5)
		data[6][4] = '"replacement"'
		add_want(6)
		want.sort() # adding them out of order, int32_10 sorts correctly.

def test_rename():
	def mk(name, colnames="abc", **kw):
		dw = DatasetWriter(name='rename_' + name, **kw)
		assert len(colnames) == 3
		for name, typ in zip(colnames, ('ascii', 'bytes', 'unicode')):
			dw.add(name, typ, none_support=(typ == 'unicode'))
		dw.get_split_write()('0', b'1', '2')
		return dw.finish()
	plain = mk('plain')
	with_hashlabel = mk('with_hashlabel', hashlabel='a')

	# Rename all, with some name hidden. Untyped a remains visible.
	jid = subjobs.build(
		'dataset_type',
		column2type=dict(b='int32_10', c='int64_10', d='number'),
		rename=dict(a='b', b='c', c='d'),
		source=plain,
	)
	typed_ds = jid.dataset()
	coltypes = sorted((name, col.type, col.none_support) for name, col in typed_ds.columns.items())
	assert coltypes == [('a', 'ascii', False), ('b', 'int32', False), ('c', 'int64', False), ('d', 'number', True)], coltypes
	assert list(typed_ds.iterate(None)) == [('0', 0, 1, 2)]

	# Rename hashlabel a => b, c untouched except for rehashing
	jid = subjobs.build(
		'dataset_type',
		column2type=dict(b='int32_10'),
		rename=dict(a='b'),
		source=with_hashlabel,
	)
	typed_ds = jid.dataset()
	coltypes = sorted((name, col.type, col.none_support) for name, col in typed_ds.columns.items())
	assert coltypes == [('b', 'int32', False), ('c', 'unicode', True)], coltypes
	assert list(typed_ds.iterate(None)) == [(0, '2')]
	assert typed_ds.hashlabel == 'b'

	# Discard hashlabel
	jid = subjobs.build(
		'dataset_type',
		column2type=dict(b='int32_10'),
		rename=dict(a=None),
		source=with_hashlabel,
	)
	typed_ds = jid.dataset()
	coltypes = sorted((name, col.type, col.none_support) for name, col in typed_ds.columns.items())
	assert coltypes == [('b', 'int32', False), ('c', 'unicode', True)], coltypes
	assert list(typed_ds.iterate(None)) == [(1, '2')]
	assert typed_ds.hashlabel == None

	# test renaming several columns to the same name
	# (but in different parts of the chain)
	plain2 = mk('plain2', colnames="def", previous=plain)
	plain3 = mk('plain3', colnames="abi", previous=plain2)
	# once without rehashing, and once with
	hash_slice = typed_writer('number').hash(1) % g.slices
	for hashlabel, want_slice in ((None, 0), ('one', hash_slice)):
		jid = subjobs.build(
			'dataset_type',
			column2type=dict(zero='number', one='number', two='number'),
			hashlabel=hashlabel,
			rename=dict(a='zero', d='zero', b='one', e='one', c='two', f='two', i='two'),
			source=plain3,
		)
		typed_dss = jid.dataset().chain()
		assert len(typed_dss) == 3
		for org_cols, ds in zip(["abc", "def", "abi"], typed_dss):
			assert ds.hashlabel == hashlabel
			assert set(ds.columns) == set(org_cols) | {'zero', 'one', 'two'}
			assert list(ds.iterate(want_slice)) == [('0', b'1', '2', 1, 2, 0)]
			assert sum(ds.lines) == 1

def test_filter_bad_with_rename_and_chain():
	dw = DatasetWriter(name="filter bad with rename", allow_missing_slices=True)
	dw.add('a', 'ascii')
	dw.add('b', 'bytes')
	dw.add('c', 'unicode')
	dw.set_slice(0)
	dw.write('0', b'1', '2')
	dw.write('9', B'A', 'B')
	dw.write('C', B'D', 'E')
	source_ds = dw.finish()
	jid = subjobs.build(
		'dataset_type',
		column2type=dict(b='int32_10', c='int64_16', d='int32_16'),
		filter_bad=True,
		rename=dict(a='b', b='c', c='d'),
		source=source_ds,
	)
	typed_ds = jid.dataset()
	coltypes = sorted((name, col.type) for name, col in typed_ds.columns.items())
	assert coltypes == [('a', 'ascii'), ('b', 'int32'), ('c', 'int64'), ('d', 'int32')], coltypes
	assert list(typed_ds.iterate(0)) == [('0', 0, 1, 2), ('9', 9, 10, 11)]
	bad_ds = jid.dataset('bad')
	coltypes = sorted((name, col.type) for name, col in bad_ds.columns.items())
	assert coltypes == [('b', 'ascii'), ('c', 'bytes'), ('d', 'unicode')], coltypes
	assert list(bad_ds.iterate(0)) == [('C', b'D', 'E')]

	dw = DatasetWriter(name="filter bad with rename chain", allow_missing_slices=True, previous=source_ds)
	dw.add('a', 'ascii')
	dw.add('b', 'ascii')
	dw.add('c', 'ascii')
	dw.set_slice(0)
	dw.write('3', '4', '5')
	dw.write('6', '7', 'eight')
	source_ds = dw.finish()
	jid = subjobs.build(
		'dataset_type',
		column2type=dict(a='number', b='int32_10', c='int64_10'),
		defaults=dict(a='8'),
		filter_bad=True,
		rename=dict(a='b', b='c', c='a'),
		source=source_ds,
	)
	typed_ds = jid.dataset()
	assert len(typed_ds.chain()) == 2
	coltypes = sorted((name, col.type) for name, col in typed_ds.columns.items())
	assert coltypes == [('a', 'number'), ('b', 'int32'), ('c', 'int64')], coltypes
	assert list(typed_ds.iterate_chain(0)) == [(2, 0, 1), (5, 3, 4), (8, 6, 7)]
	bad_ds = jid.dataset('bad')
	assert len(bad_ds.chain()) == 2
	coltypes = sorted((name, col.type) for name, col in bad_ds.columns.items())
	assert coltypes == [('a', 'ascii'), ('b', 'ascii'), ('c', 'ascii')], coltypes
	coltypes = sorted((name, col.type) for name, col in bad_ds.previous.columns.items())
	assert coltypes == [('a', 'unicode'), ('b', 'ascii'), ('c', 'bytes')], coltypes
	assert list(bad_ds.iterate_chain(0)) == [('B', '9', b'A'), ('E', 'C', b'D')]

def test_None():
	types = {
		'bool': 'floatbool',
		'complex32': 'complex32', 'complex64': 'complex64',
		'float32': 'float32', 'float64': 'float64',
		'int32': 'int32_10', 'int64': 'int64_10',
		'number': 'number',
		'date': 'date:%Y-%m-%d',
		'datetime': 'datetime:%Y-%m-%dT%H:%M:%S',
		'time': 'time:%H:%M:%S',
		'ascii': 'ascii:strict', 'unicode': 'unicode:utf-8',
	}
	values = {
		'bool': b'-1',
		'date': b'2022-07-08',
		'datetime': b'2022-07-08T22:23:24',
		'time': b'22:23:24',
		'ascii': b'text',
		'unicode': b'\xe5\xad\x97',
	}
	values = {k: values.get(k, b'%d' % (ix,)) for ix, k in enumerate(sorted(types))}
	want_values = {
		'bool': True,
		'date': date(2022, 7, 8),
		'datetime': datetime(2022, 7, 8, 22, 23, 24),
		'time': time(22, 23, 24),
		'ascii': 'text',
		'unicode': '字',
	}
	want_values = {k: want_values.get(k, ix) for ix, k in enumerate(sorted(types))}
	def test(name, dw_kw, type_kw, write_lines, want_none_count):
		dw = DatasetWriter(name=name, allow_missing_slices=True, **dw_kw)
		for typ in types:
			dw.add(typ, 'bytes', none_support=True)
		dw.set_slice(0)
		for d in write_lines:
			dw.write(**d)
		source_ds = dw.finish()
		ds = subjobs.build(
			'dataset_type',
			source=source_ds,
			column2type=types,
			**type_kw
		).dataset()
		for colname, value in want_values.items():
			want = [value] + [None] * want_none_count
			assert list(ds.iterate_chain(0, colname)) == want, colname
		return source_ds
	# One line with good values, one with Nones
	prev = test('None values', {}, {}, [values, dict.fromkeys(values, None)], 1)
	# One line with bad values (plus the previous ds)
	test('bad None values', {'previous': prev}, {'defaults': dict.fromkeys(values, None)}, [dict.fromkeys(values, b'\xff')], 2)

def test_column_discarding():
	dw = DatasetWriter(name='column discarding')
	dw.add('a', 'bytes')
	dw.add('b', 'bytes')
	dw.add('c', 'bytes')
	w = dw.get_split_write()
	w(b'a', b'b', b'c')
	source = dw.finish()

	# Discard b because it's not typed
	ac_implicit = subjobs.build(
		'dataset_type',
		source=source,
		column2type=dict(a='ascii', c='ascii'),
		discard_untyped=True,
	).dataset()
	assert sorted(ac_implicit.columns) == ['a', 'c'], '%s: %r' % (ac_implicit, sorted(ac_implicit.columns),)
	assert list(ac_implicit.iterate(None)) == [('a', 'c',)], ac_implicit

	# Discard b explicitly
	ac_explicit = subjobs.build(
		'dataset_type',
		source=source,
		column2type=dict(a='ascii', c='ascii'),
		rename=dict(b=None),
	).dataset()
	assert sorted(ac_explicit.columns) == ['a', 'c'], '%s: %r' % (ac_explicit, sorted(ac_explicit.columns),)
	assert list(ac_explicit.iterate(None)) == [('a', 'c',)], ac_explicit

	# Discard c by overwriting it with b. Keep untyped b.
	ac_bASc = subjobs.build(
		'dataset_type',
		source=source,
		column2type=dict(a='ascii', c='ascii'),
		rename=dict(b='c'),
	).dataset()
	assert sorted(ac_bASc.columns) == ['a', 'b', 'c'], '%s: %r' % (ac_bASc, sorted(ac_bASc.columns),)
	assert list(ac_bASc.iterate(None)) == [('a', b'b', 'b',)], ac_bASc

	# Discard c by overwriting it with b. Also type b as a different type.
	abc_bASc = subjobs.build(
		'dataset_type',
		source=source,
		column2type=dict(a='ascii', b='strbool', c='ascii'),
		rename=dict(b='c'),
	).dataset()
	assert sorted(abc_bASc.columns) == ['a', 'b', 'c'], '%s: %r' % (abc_bASc, sorted(abc_bASc.columns),)
	assert list(abc_bASc.iterate(None)) == [('a', True, 'b',)], abc_bASc

def test_rehash_with_empty_slices():
	dw = DatasetWriter(name='rehash with empty slices', hashlabel='a')
	dw.add('a', 'ascii')
	dw.add('b', 'ascii')
	w = dw.get_split_write()
	w('a', '42')
	w('42', 'b')
	source = dw.finish()
	hashfunc = typed_writer('int32').hash
	def verify_hashing(caption, want_values, **kw):
		ds = subjobs.build(
			'dataset_type',
			source=source,
			column2type=dict(a='int32_10'),
			caption=caption,
			**kw
		).dataset()
		got_values = set()
		for sliceno in range(g.slices):
			for got in ds.iterate(sliceno):
				assert hashfunc(got[0]) % g.slices == sliceno
				assert got not in got_values
				got_values.add(got)
		assert want_values == got_values
	verify_hashing('with discard', {(42, 'b',)}, filter_bad=True)
	# using defaults uses some different code paths
	verify_hashing('with default=0 (probably two slices)', {(0, '42',), (42, 'b',)}, defaults=dict(a='0'))
	verify_hashing('with default=42 (one slice)', {(42, '42',), (42, 'b',)}, defaults=dict(a='42'))

def test_varying_hashlabel():
	columns = {'a': 'ascii', 'b': 'ascii', 'c': 'ascii'}
	def mk(name, hashlabel, previous):
		dw = DatasetWriter(name=name, hashlabel=hashlabel, previous=previous, columns=columns)
		w = dw.get_split_write()
		w('1', '2', '3')
		return dw.finish()
	ds1 = mk('hlchain0', None, None)
	ds2 = mk('hlchain1', 'a', ds1)
	ds3 = mk('hlchain2', 'b', ds2)
	ds4 = mk('hlchain3', 'c', ds3)
	typed_dss = subjobs.build(
		'dataset_type',
		source=ds4,
		column2type=dict(a='int32_10', b='int32_10', c='int32_10'),
	).dataset().chain()
	assert list(typed_dss.iterate(None)) == 4 * [(1, 2, 3)]
	assert [ds.hashlabel for ds in typed_dss] == [None, 'a', 'b', 'c']
	h = typed_writer('int32').hash
	assert typed_dss[0].lines[0] == 1
	for ix in (1, 2, 3):
		assert typed_dss[ix].lines[h(ix) % g.slices] == 1


def synthesis():
	test_bytes()
	test_ascii()
	test_unicode()
	test_numbers()
	test_datetimes()
	test_None()
	test_column_discarding()
	test_rehash_with_empty_slices()
	test_rename()
	test_varying_hashlabel()

	verify('json', ['json'],
		[b'null', b'[42, {"a": "b"}]', b'\r  {  "foo":\r"bar" \r   }\t ', b'nope'],
		[None, [42, {'a': 'b'}], {'foo': 'bar'}, ['nah']],
		default='["nah"]', all_source_types=True,
	)

	test_filter_bad_across_types()
	test_filter_bad_with_rename_and_chain()

	for t in (all_typenames - used_typenames):
		print(t)
