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

# This is a separate file from a_dataset_type so setup.py can import
# it and make the _dataset_type module at install time.

from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from collections import namedtuple
from functools import partial
import sys
import struct
import codecs
import json

from accelerator.compat import NoneType, iteritems

from . import c_backend_support

__all__ = ('convfuncs', 'typerename', 'typesizes', 'minmaxfuncs',)

def _resolve_unicode(coltype, strip=False):
	_, fmt = coltype.split(':', 1)
	if '/' in fmt:
		codec, errors = fmt.split('/')
	else:
		codec, errors = fmt, 'strict'
	assert errors in ('strict', 'replace', 'ignore',)
	b''.decode(codec) # trigger error on unknown
	canonical = codecs.lookup(codec).name
	if canonical == codecs.lookup('utf-8').name:
		selected = 'unicode_utf8'
	elif canonical == codecs.lookup('iso-8859-1').name:
		selected = 'unicode_latin1'
	elif canonical == codecs.lookup('ascii').name:
		if errors == 'strict':
			selected = 'ascii_strict'
		else:
			selected = 'unicode_ascii'
	else:
		selected = 'unicode'
	if strip:
		if '_' in selected:
			selected = selected.replace('_', 'strip_', 1)
		else:
			selected += 'strip'
	return selected, codec, errors

_c_conv_bytes_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (dt_isspace(*line)) {
		line++;
		len--;
	}
	while (len && dt_isspace(line[len - 1])) len--;
#endif
	const uint8_t *ptr = (uint8_t *)line;
'''

_c_conv_ascii_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (dt_isspace(*line)) {
		line++;
		len--;
	}
	while (len && dt_isspace(line[len - 1])) len--;
#endif
	const uint8_t *ptr = (uint8_t *)line;
	char *free_ptr = 0;
	int32_t enc_cnt = 0;
	for (uint32_t i = 0; i < (uint32_t)len; i++) {
		enc_cnt += (%%(enctest)s);
	}
	if (enc_cnt) {
		int64_t elen = (int64_t)len + ((int64_t)enc_cnt * 3);
		err1(elen > 0x7fffffff);
		char *buf = PyMem_Malloc(elen);
		err1(!buf);
		free_ptr = buf;
		int32_t bi = 0;
		for (uint32_t i = 0; i < (uint32_t)len; i++) {
%(conv)s
		}
		ptr = (uint8_t *)buf;
		len = elen;
	}
'''
_c_conv_ascii_encode_template = r'''
			if (%(enctest)s) {
				buf[bi++] = '\\';
				buf[bi++] = '0' + (ptr[i] >> 6);
				buf[bi++] = '0' + ((ptr[i] >> 3) & 7);
				buf[bi++] = '0' + (ptr[i] & 7);
			} else {
				buf[bi++] = ptr[i];
			}
'''
_c_conv_ascii_cleanup = r'''
		if (free_ptr) PyMem_Free(free_ptr);
'''

_c_conv_ascii_strict_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (dt_isspace(*line)) {
		line++;
		len--;
	}
	while (len && dt_isspace(line[len - 1])) len--;
#endif
	const uint8_t *ptr = (uint8_t *)line;
	for (uint32_t i = 0; i < (uint32_t)len; i++) {
		if (ptr[i] > 127) {
			ptr = 0;
			break;
		}
	}
'''

_c_null_blob_template = r'''
	int32_t len = g.linelen;
	const uint8_t *ptr = (uint8_t *)line;
'''

_c_conv_unicode_setup = r'''
	PyObject *decoder = PyCodec_Decoder(fmt);
	if (!decoder) {
		PyErr_Format(PyExc_ValueError, "No decoder for '%s'.", fmt);
		goto err;
	}
	PyObject *dec_errors = PyUnicode_FromString(fmt_b);
	err1(!dec_errors);
	PyObject *tst_bytes = PyBytes_FromStringAndSize("a", 1);
	if (tst_bytes) {
		PyObject *tst_res = PyObject_CallFunctionObjArgs(decoder, tst_bytes, dec_errors, 0);
		Py_DECREF(tst_bytes);
		if (tst_res) {
			if (PyTuple_Check(tst_res)) {
				if (!PyUnicode_Check(PyTuple_GetItem(tst_res, 0))) {
					PyErr_Format(PyExc_ValueError, "Decoder for '%s' does not produce unicode.", fmt);
				}
			}
			Py_DECREF(tst_res);
		}
	}
	err1(PyErr_Occurred());
'''
_c_conv_unicode_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (dt_isspace(*line)) {
		line++;
		len--;
	}
	while (len && dt_isspace(line[len - 1])) len--;
#endif
	const uint8_t *ptr = 0;
	PyObject *tmp_bytes = PyBytes_FromStringAndSize(line, len);
	err1(!tmp_bytes);
	PyObject *tmp_res = PyObject_CallFunctionObjArgs(decoder, tmp_bytes, dec_errors, 0);
	Py_DECREF(tmp_bytes);
	if (tmp_res) {
#if PY_MAJOR_VERSION < 3
		PyObject *tmp_utf8bytes = PyUnicode_AsUTF8String(PyTuple_GET_ITEM(tmp_res, 0));
		err1(!tmp_utf8bytes);
		Py_DECREF(tmp_res);
		tmp_res = tmp_utf8bytes;
		ptr = (const uint8_t *)PyBytes_AS_STRING(tmp_utf8bytes);
		Py_ssize_t newlen = PyBytes_GET_SIZE(tmp_utf8bytes);
#else
		PyObject *tmp_uni = PyTuple_GET_ITEM(tmp_res, 0);
		Py_ssize_t newlen;
		ptr = (const uint8_t *)PyUnicode_AsUTF8AndSize(tmp_uni, &newlen);
#endif
		if (newlen > 0x7fffffff) {
			ptr = 0;
		} else {
			len = newlen;
		}
	} else {
		PyErr_Clear();
	}
'''
_c_conv_unicode_cleanup = r'''
		Py_XDECREF(tmp_res);
'''
_c_conv_unicode_specific_template = r'''
	int32_t len = g.linelen;
#if %(strip)d
	while (dt_isspace(*line)) {
		line++;
		len--;
	}
	while (len && dt_isspace(line[len - 1])) len--;
#endif
	const uint8_t *ptr = 0;
	PyObject *tmp_res = %(func)s(line, len, fmt_b);
	if (tmp_res) {
#if PY_MAJOR_VERSION < 3
		PyObject *tmp_utf8bytes = PyUnicode_AsUTF8String(tmp_res);
		err1(!tmp_utf8bytes);
		Py_DECREF(tmp_res);
		tmp_res = tmp_utf8bytes;
		ptr = (const uint8_t *)PyBytes_AS_STRING(tmp_utf8bytes);
		Py_ssize_t newlen = PyBytes_GET_SIZE(tmp_utf8bytes);
#else
		Py_ssize_t newlen;
		ptr = (const uint8_t *)PyUnicode_AsUTF8AndSize(tmp_res, &newlen);
#endif
		if (newlen > 0x7fffffff) {
			ptr = 0;
		} else {
			len = newlen;
		}
	} else {
		PyErr_Clear();
	}
'''

_c_conv_date_template = r'''
	struct tm tm;
	int32_t f;
	const char *pres = line;
	int ret = mystrptime(&pres, fmt, &tm, &f);
	if (!ret && ((!%(whole)d) || !*pres)) {
		uint32_t *p = (uint32_t *)ptr;
		%(conv)s
	} else {
		ptr = 0;
	}
'''
_c_conv_datetime = r'''
		if (use_tz == 1) {
			tm.tm_isdst = -1;
			time_t t = mktime(&tm);
			gmtime_r(&t, &tm);
		}
		const uint32_t year = tm.tm_year + 1900;
		const uint32_t mon  = tm.tm_mon + 1;
		const uint32_t mday = tm.tm_mday;
		const uint32_t hour = tm.tm_hour;
		const uint32_t min  = tm.tm_min;
		const uint32_t sec  = tm.tm_sec;
		// Our definition of a valid date is whatever Python will accept.
		// On Python 2 that's unfortunately pretty much anything.
		PyObject *o = PyDateTime_FromDateAndTime(year, mon, mday, hour, min, sec, f);
		if (o) {
			Py_DECREF(o);
			p[0] = year << 14 | mon << 10 | mday << 5 | hour;
			p[1] = min << 26 | sec << 20 | f;
			if (use_tz) p[0] |= 0x20000000; // mark as UTC
		} else {
			PyErr_Clear();
			ptr = 0;
		}
'''
_c_conv_date = r'''
		(void) f; // not used for dates, but we don't want the compiler complaining.
		const uint32_t year = tm.tm_year + 1900;
		const uint32_t mon  = tm.tm_mon + 1;
		const uint32_t mday = tm.tm_mday;
		// Our definition of a valid date is whatever Python will accept.
		// On Python 2 that's unfortunately pretty much anything.
		PyObject *o = PyDate_FromDate(year, mon, mday);
		if (o) {
			Py_DECREF(o);
			p[0] = year << 9 | mon << 5 | mday;
		} else {
			PyErr_Clear();
			ptr = 0;
		}
'''
_c_conv_time = r'''
		const uint32_t hour = tm.tm_hour;
		const uint32_t min  = tm.tm_min;
		const uint32_t sec  = tm.tm_sec;
		p[0] = 32277536 | hour; // 1970 if read as datetime
		p[1] = min << 26 | sec << 20 | f;
'''

_c_conv_float_template = r'''
		(void) fmt;
		char *endptr;
		%(type)s value = %(func)s(line, &endptr);
#if %(whole)d
		if (line == endptr || *dt_skipspace(endptr)) { // not a valid float
			ptr = 0;
		} else {
#else
		if (1) {
#endif
			// inf and truncated to zero are ok here.
			%(type)s *p = (%(type)s *)ptr;
			*p = value;
		}
'''

_c_conv_int_template = r'''
		(void) fmt;
		errno = 0;
		const char *startptr = line;
		char *endptr;
#if %(unsigned)d
		while (dt_isspace(*startptr)) startptr++;
		if (*startptr == '-') {
			ptr = 0;
		} else {
#endif
		%(rtype)s value = %(func)s(startptr, &endptr, %(base)d);
#if %(whole)d
		if (line == endptr || *dt_skipspace(endptr)) { // not a valid int
			ptr = 0;
		} else
#endif
		if (errno == ERANGE) { // out of range
			ptr = 0;
		} else {
			%(type)s *p = (%(type)s *)ptr;
			*p = value;
			if (value != *p || ((%(nonemarker)s) && value == %(nonemarker)s)) {
				// Over/underflow (values that don't fit are not ok)
				ptr = 0;
			}
		}
#if %(unsigned)d
		}
#endif
'''

_c_conv_strbool = r'''
		(void) fmt;
		if (!strcasecmp(line, "false")
		    || !strcasecmp(line, "0")
		    || !strcasecmp(line, "f")
		    || !strcasecmp(line, "no")
		    || !strcasecmp(line, "off")
		    || !strcasecmp(line, "nil")
		    || !strcasecmp(line, "null")
		    || !*line
		) {
			*ptr = 0;
		} else {
			*ptr = 1;
		}
'''

_c_conv_floatbool_template = r'''
		(void) fmt;
		char *endptr;
		double value = strtod(line, &endptr);
#if %(whole)d
		if (line == endptr || *dt_skipspace(endptr)) { // not a valid float
			ptr = 0;
		} else {
#else
		if (1) {
#endif
			// inf and truncated to zero are ok here.
			*ptr = !!value;
		}
'''


MinMaxTuple = namedtuple('MinMaxTuple', 'typename setup code extra')
def _c_minmax_simple(typename, min_const, max_const, check_none):
	d = dict(type=typename, min_const=min_const, max_const=max_const, check_none=check_none)
	setup = r'''
		do {
			*col_min = %(max_const)s;
			*col_max = %(min_const)s;
		} while (0)
	''' % d
	code = r'''
		do {
			const %(type)s cand_value = *(const %(type)s *)ptr;
			if (%(check_none)s) { // Some of these need to ignore None-values
				/*NAN-handling 1*/
				minmax_any = 1;
				if (cand_value < *col_min) *col_min = cand_value;
				if (cand_value > *col_max) *col_max = cand_value;
				/*NAN-handling 2*/
			}
		} while (0)
	''' % d
	return MinMaxTuple(typename, setup, code, '',)

def _c_minmax_float(typename, check_none):
	_, setup, code, _ = _c_minmax_simple(typename, '-INFINITY', 'INFINITY', check_none)
	code = code.replace('/*NAN-handling 1*/', r'''
			if (isnan(cand_value)) {
				if (!minmax_any) minmax_any = -1;
			} else {
	''').replace('/*NAN-handling 2*/', '}')
	extra = r'''
		if (minmax_any == -1) {
			const %s nan = strtod("nan", 0);
			err1(!isnan(nan));
			col_min[0] = col_max[0] = nan;
		}
	''' % (typename,)
	return MinMaxTuple(typename, setup, code, extra)

_c_minmax_datetime = MinMaxTuple(
	'uint32_t',
	r'''
		do {
			col_min[0] = 163836919; col_min[1] = 4021288960; // 9999-12-31 23:59:59
			col_max[0] = 17440;     col_max[1] = 0;          // 0001-01-01 00:00:00
		} while (0)
	''',
	r'''
		do {
			const uint32_t * const cand_p = (const uint32_t *)ptr;
			if (cand_p[0]) { // Ignore None-values
				minmax_any = 1;
				if (cand_p[0] < col_min[0] || (cand_p[0] == col_min[0] && cand_p[1] < col_min[1])) {
					col_min[0] = cand_p[0];
					col_min[1] = cand_p[1];
				}
				if (cand_p[0] > col_max[0] || (cand_p[0] == col_max[0] && cand_p[1] > col_max[1])) {
					col_max[0] = cand_p[0];
					col_max[1] = cand_p[1];
				}
			}
		} while (0)
	''',
	'',
)

minmaxfuncs = {
	'float64'  : _c_minmax_float('double', 'memcmp(ptr, noneval_float64, 8)'),
	'float32'  : _c_minmax_float('float' , 'memcmp(ptr, noneval_float32, 4)'),
	'int64'    : _c_minmax_simple('int64_t' , '-INT64_MAX', 'INT64_MAX' , 'cand_value != INT64_MIN'),
	'int32'    : _c_minmax_simple('int32_t' , '-INT32_MAX', 'INT32_MAX' , 'cand_value != INT32_MIN'),
	'bool'     : _c_minmax_simple('uint8_t' , '0'         , '1'         , 'cand_value != 255'),
	'datetime' : _c_minmax_datetime,
	'date'     : _c_minmax_simple('uint32_t', '545'       , '5119903'   , 'cand_value'),
	'time'     : _c_minmax_datetime,
}

if len(struct.pack("@L", 0)) == 8:
	strtol_f = 'strtol'
	strtoul_f = 'strtoul'
	long_t = 'long'
	ulong_t = 'unsigned long'
	longobj_f = 'PyLong_FromLong'
elif len(struct.pack("@q", 0)) == 8:
	strtol_f = 'strtoll'
	strtoul_f = 'strtoull'
	long_t = 'long long'
	ulong_t = 'unsigned long long'
	longobj_f = 'PyLong_FromLongLong'
else:
	raise Exception("Unable to find a suitable 64 bit integer type")

if sys.byteorder == 'little':
	noneval_data = r'''
	// These are signaling NaNs with extra DEADness in the significand
	static const unsigned char noneval_float64[8] = {0xde, 0xad, 0xde, 0xad, 0xde, 0xad, 0xf0, 0xff};
	static const unsigned char noneval_float32[4] = {0xde, 0xad, 0x80, 0xff};
	'''
elif sys.byteorder == 'big':
	noneval_data = r'''
	// These are signaling NaNs with extra DEADness in the significand
	static const unsigned char noneval_float64[8] = {0xff, 0xf0, 0xde, 0xad, 0xde, 0xad, 0xde, 0xad};
	static const unsigned char noneval_float32[4] = {0xff, 0x80, 0xde, 0xad};
	'''
else:
	raise Exception('Unknown byteorder ' + sys.byteorder)

noneval_data += r'''
// The smallest value is one less than -biggest, so that seems like a good signal value.
static const int64_t noneval_int64 = INT64_MIN;
static const int32_t noneval_int32 = INT32_MIN;

static const uint64_t noneval_datetime = 0;
static const uint64_t noneval_time = 0;
static const uint32_t noneval_date = 0;

static const uint8_t noneval_bool = 255;
'''

numeric_comma = False

def _conv_json(_):
	return json.JSONDecoder().decode

def _conv_complex(t):
	if numeric_comma:
		def conv_complex(v):
			return complex(v.replace('.', 'dot').replace(',', '.'))
		return conv_complex
	else:
		return complex

ConvTuple = namedtuple('ConvTuple', 'size conv_code_str pyfunc')
# Size is bytes per value, or 0 for variable size.
# If pyfunc is specified it is called with the type string
# and can return either (type, fmt, fmt_b) or a callable for
# doing the conversion. type needs not be the same type that
# was passed in, but the passed type determines the actual
# type in the dataset.
# If conv_code_str and size is set, the destination type must exist in minmaxfuncs.
convfuncs = {
	'complex64'    : ConvTuple(16, None, _conv_complex),
	'complex32'    : ConvTuple(8, None, _conv_complex),
	# no *i-types for complex since we just reuse the python complex constructor.
	'float64'      : ConvTuple(8, _c_conv_float_template % dict(type='double', func='strtod', whole=1), None),
	'float32'      : ConvTuple(4, _c_conv_float_template % dict(type='float', func='strtof', whole=1) , None),
	'float64i'     : ConvTuple(8, _c_conv_float_template % dict(type='double', func='strtod', whole=0), None),
	'float32i'     : ConvTuple(4, _c_conv_float_template % dict(type='float', func='strtof', whole=0) , None),
	'int64_0'      : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=1, base=0 , unsigned=0), None),
	'int32_0'      : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=0 , unsigned=0), None),
	'int64_8'      : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=1, base=8 , unsigned=0), None),
	'int32_8'      : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=8 , unsigned=0), None),
	'int64_10'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=1, base=10, unsigned=0), None),
	'int32_10'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=10, unsigned=0), None),
	'int64_16'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=1, base=16, unsigned=0), None),
	'int32_16'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=1, base=16, unsigned=0), None),
	'int64_0i'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=0, base=0 , unsigned=0), None),
	'int32_0i'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=0 , unsigned=0), None),
	'int64_8i'     : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=0, base=8 , unsigned=0), None),
	'int32_8i'     : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=8 , unsigned=0), None),
	'int64_10i'    : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=0, base=10, unsigned=0), None),
	'int32_10i'    : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=10, unsigned=0), None),
	'int64_16i'    : ConvTuple(8, _c_conv_int_template % dict(type='int64_t' , rtype=long_t         , func=strtol_f , nonemarker='INT64_MIN', whole=0, base=16, unsigned=0), None),
	'int32_16i'    : ConvTuple(4, _c_conv_int_template % dict(type='int32_t' , rtype='long'         , func='strtol' , nonemarker='INT32_MIN', whole=0, base=16, unsigned=0), None),
	'strbool'      : ConvTuple(1, _c_conv_strbool, None),
	'floatbool'    : ConvTuple(1, _c_conv_floatbool_template % dict(whole=1)                   , None),
	'floatbooli'   : ConvTuple(1, _c_conv_floatbool_template % dict(whole=0)                   , None),
	'datetime:*'   : ConvTuple(8, _c_conv_date_template % dict(whole=1, conv=_c_conv_datetime,), None),
	'date:*'       : ConvTuple(4, _c_conv_date_template % dict(whole=1, conv=_c_conv_date,    ), None),
	'time:*'       : ConvTuple(8, _c_conv_date_template % dict(whole=1, conv=_c_conv_time,    ), None),
	'datetimei:*'  : ConvTuple(8, _c_conv_date_template % dict(whole=0, conv=_c_conv_datetime,), None),
	'datei:*'      : ConvTuple(4, _c_conv_date_template % dict(whole=0, conv=_c_conv_date,    ), None),
	'timei:*'      : ConvTuple(8, _c_conv_date_template % dict(whole=0, conv=_c_conv_time,    ), None),
	'bytes'        : ConvTuple(0, _c_conv_bytes_template % dict(strip=0), None),
	'bytesstrip'   : ConvTuple(0, _c_conv_bytes_template % dict(strip=1), None),
	# unicode[strip]:encoding or unicode[strip]:encoding/errorhandling
	# errorhandling can be one of strict (fail, default), replace (with \ufffd) or ignore (remove char)
	'unicode:*'    : ConvTuple(0, [_c_conv_unicode_setup, _c_conv_unicode_template % dict(strip=0), _c_conv_unicode_cleanup], _resolve_unicode),
	'unicodestrip:*':ConvTuple(0, [_c_conv_unicode_setup, _c_conv_unicode_template % dict(strip=1), _c_conv_unicode_cleanup], partial(_resolve_unicode, strip=True)),
	# ascii[strip]:errorhandling, can be replace (default, replace >127 with \ooo),
	# encode (same as replace, plus \ becomes \134) or strict (>127 is an error).
	'ascii'             : ConvTuple(0, None, lambda _: ('ascii_replace', None, None),),
	'asciistrip'        : ConvTuple(0, None, lambda _: ('asciistrip_replace', None, None),),
	'ascii:replace'     : ConvTuple(0, ['', _c_conv_ascii_template % dict(strip=0, conv=_c_conv_ascii_encode_template) % dict(enctest="ptr[i] > 127"), _c_conv_ascii_cleanup], None),
	'asciistrip:replace': ConvTuple(0, ['', _c_conv_ascii_template % dict(strip=1, conv=_c_conv_ascii_encode_template) % dict(enctest="ptr[i] > 127"), _c_conv_ascii_cleanup], None),
	'ascii:encode'      : ConvTuple(0, ['', _c_conv_ascii_template % dict(strip=0, conv=_c_conv_ascii_encode_template) % dict(enctest="ptr[i] > 127 || ptr[i] == '\\\\'"), _c_conv_ascii_cleanup], None),
	'asciistrip:encode' : ConvTuple(0, ['', _c_conv_ascii_template % dict(strip=1, conv=_c_conv_ascii_encode_template) % dict(enctest="ptr[i] > 127 || ptr[i] == '\\\\'"), _c_conv_ascii_cleanup], None),
	'ascii:strict'      : ConvTuple(0, _c_conv_ascii_strict_template % dict(strip=0), None),
	'asciistrip:strict' : ConvTuple(0, _c_conv_ascii_strict_template % dict(strip=1), None),
	# The number type is handled specially, so no code here.
	'number'       : ConvTuple(0, None, None), # integer when possible (up to +-2**1007-1), float otherwise.
	'number:int'   : ConvTuple(0, None, None), # Never float, but accepts int.0 (or int.00 and so on)
	'json'         : ConvTuple(0, None, _conv_json),
}

# These are not made available as valid values in column2type, but they
# can be selected based on the :fmt specified in those values.
# null_* is used when just copying a column with filtering.
hidden_convfuncs = {
	'unicode_utf8'       : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=0, func='PyUnicode_DecodeUTF8'), _c_conv_unicode_cleanup], None),
	'unicodestrip_utf8'  : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=1, func='PyUnicode_DecodeUTF8'), _c_conv_unicode_cleanup], None),
	'unicode_latin1'     : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=0, func='PyUnicode_DecodeLatin1'), _c_conv_unicode_cleanup], None),
	'unicodestrip_latin1': ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=1, func='PyUnicode_DecodeLatin1'), _c_conv_unicode_cleanup], None),
	'unicode_ascii'      : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=0, func='PyUnicode_DecodeASCII'), _c_conv_unicode_cleanup], None),
	'unicodestrip_ascii' : ConvTuple(0, ['', _c_conv_unicode_specific_template % dict(strip=1, func='PyUnicode_DecodeASCII'), _c_conv_unicode_cleanup], None),
	'null_blob'          : ConvTuple(0, _c_null_blob_template, None),
	'null_1'             : 1,
	'null_4'             : 4,
	'null_8'             : 8,
	'null_16'            : 16,
	'null_number'        : 0,
}

# The actual type produced, when it is not the same as the key in convfuncs
# Note that this is based on the user-specified type, not whatever the
# resolving function returns.
typerename = {
	'strbool'      : 'bool',
	'floatbool'    : 'bool',
	'floatbooli'   : 'bool',
	'int64_0'      : 'int64',
	'int32_0'      : 'int32',
	'int64_8'      : 'int64',
	'int32_8'      : 'int32',
	'int64_10'     : 'int64',
	'int32_10'     : 'int32',
	'int64_16'     : 'int64',
	'int32_16'     : 'int32',
	'int64_0i'     : 'int64',
	'int32_0i'     : 'int32',
	'int64_8i'     : 'int64',
	'int32_8i'     : 'int32',
	'int64_10i'    : 'int64',
	'int32_10i'    : 'int32',
	'int64_16i'    : 'int64',
	'int32_16i'    : 'int32',
	'float64i'     : 'float64',
	'float32i'     : 'float32',
	'datetimei'    : 'datetime',
	'datei'        : 'date',
	'timei'        : 'time',
	'bytesstrip'   : 'bytes',
	'asciistrip'   : 'ascii',
	'unicodestrip' : 'unicode',
	'number:int'   : 'number',
}

# Byte size of each (real) type
typesizes = {typerename.get(key.split(':')[0], key.split(':')[0]): convfuncs[key].size for key in convfuncs}

# Verify that all types have working (well, findable) writers
# and something approaching the right type of data.
def _test():
	from accelerator.dsutil import typed_writer, _convfuncs
	for key, data in iteritems(convfuncs):
		key = key.split(":")[0]
		typed_writer(typerename.get(key, key))
		assert data.size in (0, 1, 4, 8, 16), (key, data)
		if isinstance(data.conv_code_str, list):
			for v in data.conv_code_str:
				assert isinstance(v, (str, NoneType)), (key, data)
		else:
			assert isinstance(data.conv_code_str, (str, NoneType)), (key, data)
		if data.conv_code_str and data.size:
			assert typerename.get(key, key) in minmaxfuncs
		assert data.pyfunc is None or callable(data.pyfunc), (key, data)
	for key, mm in iteritems(minmaxfuncs):
		for v in mm:
			assert isinstance(v, str), key
	known = set(v for v in _convfuncs if ':' not in v)
	copy_missing = known - set(copy_types)
	copy_extra = set(copy_types) - known
	assert not copy_missing, 'copy_types missing %r' % (copy_missing,)
	assert not copy_extra, 'copy_types contains unexpected %r' % (copy_extra,)


convert_template = r'''
%(proto)s
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	g g;
	gzFile outfhs[slices + save_bad];
	memset(outfhs, 0, sizeof(outfhs));
	const char *line;
	int res = 1;
	// these are two long because datetime needs that, everything else uses only the first element
	%(typename)s buf[2];
	%(typename)s defbuf[2];
	%(typename)s col_min[2];
	%(typename)s col_max[2];
	int minmax_any = 0;
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	G_INIT(1);
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		if (badmap == MAP_FAILED) badmap = 0;
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		if (slicemap == MAP_FAILED) slicemap = 0;
		err1(!slicemap);
	}
	if (default_value) {
		err1(default_value_is_None);
		char *ptr = (char *)defbuf;
		line = default_value;
		g.linelen = default_len;
		%(convert)s;
		err1(!ptr);
	}
	if (default_value_is_None) {
		memcpy(defbuf, &%(noneval_name)s, sizeof(%(noneval_name)s));
		default_value = ""; // Used as a bool later
	}
	%(minmax_setup)s;
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	for (; i < max_count && (line = read_line(&g)); i++) {
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			MAYBE_SAVE_BAD_BLOB;
			continue;
		}
		char *ptr = (char *)buf;
		if (line == NoneMarker || (empty_types_as_None && g.linelen == 0)) {
			memcpy(ptr, &%(noneval_name)s, %(datalen)s);
		} else {
			%(convert)s;
		}
		if (!ptr) {
			if (record_bad && !default_value) {
				badmap[i / 8] |= 1 << (i %% 8);
				bad_count[chosen_slice] += 1;
				continue;
			}
			if (!default_value) {
				PyErr_Format(PyExc_ValueError, "Failed to convert \"%%s\" from %%s line %%lld", line, g.msgname, (long long)i - first_line + 1);
				goto err;
			}
			ptr = (char *)defbuf;
			default_count[chosen_slice] += 1;
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		%(minmax_code)s;
		err1(gzwrite(outfhs[chosen_slice], ptr, %(datalen)s) != %(datalen)s);
	}
	current_file++;
	if (current_file < in_count) {
		G_INIT(0);
		goto more_infiles;
	}
	if (minmax_any) {
		%(minmax_extra)s
		gzFile minmaxfh = gzopen(minmax_fn, gzip_mode);
		err1(!minmaxfh);
		res = g.error;
		if (gzwrite(minmaxfh, col_min, %(datalen)s) != %(datalen)s) res = 1;
		if (gzwrite(minmaxfh, col_max, %(datalen)s) != %(datalen)s) res = 1;
		if (gzclose(minmaxfh)) res = 1;
	} else {
		res = g.error;
	}
err:
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices + save_bad; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	return res;
}
'''

convert_number_template = r'''
// Up to +-(2**1007 - 1). Don't increase this.
#define GZNUMBER_MAX_BYTES 127

static inline int convert_number_do(const char *inptr, char * const outptr_, const int allow_float, double *r_d, PyObject **r_o)
{
	unsigned char *outptr = (unsigned char *)outptr_;
	// First remove whitespace at the start
	while (dt_isspace(*inptr)) inptr++;
	// Then check length and what symbols we have
	int inlen = 0;
	int hasdot = 0, hasexp = 0, hasletter = 0;
	while (1) {
		const char c = inptr[inlen];
		if (!c) break;
		if (c == decimal_separator) {
			if (hasdot || hasexp) return 0;
			hasdot = 1;
		}
		if (c == 'e' || c == 'E') {
			if (hasexp) return 0;
			hasexp = 1;
		}
		if (c == 'x' || c == 'X' || c == 'p' || c == 'P') {
			// Avoid accepting strange float formats that only some C libs accept.
			// (Things like "0x1.5p+5", which as I'm sure you can see is 42.)
			return 0;
		}
		if (c == 'n' || c == 'N') {
			// Could be 'nan' or 'inf', both of which are ok floats.
			hasletter = 1;
		}
		inlen++;
	}
	// Now remove whitespace at end
	while (inlen && dt_isspace(inptr[inlen - 1])) inlen--;
	if (!inlen) return 0; // empty (except whitespace) => error
	// Then remove ending zeroes if there is a decimal dot and no exponent
	if (hasdot && !hasexp) {
		while (inlen && inptr[inlen - 1] == '0') inlen--;
		// And remove the dot if it's the last character.
		if (inlen && inptr[inlen - 1] == decimal_separator) {
			// Woo, it was an int in disguise!
			inlen--;
			hasdot = 0;
		}
	}
	if (!inlen) { // Some form of ".000"
		if (inptr[1] != '0') return 0; // just a decimal separator is not a valid number
		*outptr = 0x85; // This is the most compact encoding of 0
		*r_d = 0;
		return 1;
	}
	if (hasdot || hasexp || hasletter) { // Float
		if (!allow_float) return 0;
		char *end;
		errno = 0;
		const double value = strtod(inptr, &end);
		if (errno || end < inptr + inlen) {
			return 0;
		} else {
			*outptr = 1;
			memcpy(outptr + 1, &value, 8);
			*r_d = value;
			return 9;
		}
	} else {
		char *end;
		errno = 0;
		const int64_t value = %(strtol_f)s(inptr, &end, 10);
		if (errno || end != inptr + inlen) { // big or invalid
			PyObject *s = PyBytes_FromStringAndSize(inptr, inlen);
			if (!s) exit(1); // All is lost
			PyObject *i = PyNumber_Long(s);
			if (!i) PyErr_Clear();
			Py_DECREF(s);
			if (!i) return 0;
#if PY_VERSION_HEX < 0x030d00a4
			const size_t len_bits = _PyLong_NumBits(i);
			err1(len_bits == (size_t)-1);
			size_t len_bytes = len_bits / 8 + 1;
#else
			Py_ssize_t len_bytes = PyLong_AsNativeBytes(i, NULL, 0, 1);
			err1(len_bytes <= 0);
#endif
			err1(len_bytes >= GZNUMBER_MAX_BYTES);
			if (len_bytes < 8) len_bytes = 8; // Could happen for "42L" or similar.
			*outptr = len_bytes;
#if PY_VERSION_HEX < 0x030d00a4
			err1(_PyLong_AsByteArray((PyLongObject *)i, outptr + 1, len_bytes, 1, 1) < 0);
#else
			err1(PyLong_AsNativeBytes(i, outptr + 1, len_bytes, 1) != len_bytes);
#endif
#if %(big_endian)d
			if (len_bytes == 8) {
				// We ended up not using the big encoding, so the number
				// should be native endian. This can happen for numbers
				// that are not accepted by strtol (for some other reason
				// than size) but which PyNumber_Long does accept.
				for (int ix = 1; ix < 5; ix++) {
					// The first char is the length, swap [1:9].
					unsigned char tmp = outptr[9 - ix];
					outptr[9 - ix] = outptr[ix];
					outptr[ix] = tmp;
				}
			}
#endif
			*r_o = i;
			return len_bytes + 1;
err:
			Py_DECREF(i);
			return 0;
		} else {
			if (value <= 122 && value >= -5) {
				*outptr = 0x80 | (value + 5);
				*r_d = value;
				return 1;
			}
			if (value <= INT16_MAX && value >= INT16_MIN) {
				int16_t value16 = value;
				*outptr = 2;
				memcpy(outptr + 1, &value16, 2);
				*r_d = value;
				return 3;
			}
			if (value <= INT32_MAX && value >= INT32_MIN) {
				int32_t value32 = value;
				*outptr = 4;
				memcpy(outptr + 1, &value32, 4);
				*r_d = value;
				return 5;
			}
			*outptr = 8;
			memcpy(outptr + 1, &value, 8);
			if (value <= ((int64_t)1 << 53) && value >= -((int64_t)1 << 53)) {
				// Fits in a double without precision loss
				*r_d = value;
			} else {
				*r_o = %(longobj_f)s(value);
				if (!*r_o) return 0;
			}
			return 9;
		}
	}
}

%(proto)s
{
	g g;
	gzFile outfhs[slices + save_bad];
	memset(outfhs, 0, sizeof(outfhs));
	const char *line;
	int  res = 1;
	char buf[GZNUMBER_MAX_BYTES];
	char defbuf[GZNUMBER_MAX_BYTES];
	char buf_col_min[GZNUMBER_MAX_BYTES];
	char buf_col_max[GZNUMBER_MAX_BYTES];
	double def_d = 0;
	PyObject *def_o = 0;
	int  deflen = 0;
	int  minlen = 0;
	int  maxlen = 0;
	int  saw_nan = 0;
	PyObject *o_col_min = 0;
	PyObject *o_col_max = 0;
	double d_col_min = 0;
	double d_col_max = 0;
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	const int allow_float = !fmt;
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	G_INIT(1);
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		if (badmap == MAP_FAILED) badmap = 0;
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		if (slicemap == MAP_FAILED) slicemap = 0;
		err1(!slicemap);
	}
	if (default_value) {
		err1(default_value_is_None);
		deflen = convert_number_do(default_value, defbuf, allow_float, &def_d, &def_o);
		err1(!deflen);
	}
	if (default_value_is_None) {
		defbuf[0] = 0;
		deflen = 1;
	}
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	for (; i < max_count && (line = read_line(&g)); i++) {
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			MAYBE_SAVE_BAD_BLOB;
			continue;
		}
		char *ptr = buf;
		double d_v = 0;
		PyObject *o_v = 0;
		int do_minmax = 1;
		int len;
		if (line == NoneMarker || (empty_types_as_None && g.linelen == 0)) {
			ptr = "\0";
			len = 1;
			do_minmax = 0;
		} else {
			len = convert_number_do(line, ptr, allow_float, &d_v, &o_v);
		}
		if (!len) {
			if (record_bad && !deflen) {
				badmap[i / 8] |= 1 << (i %% 8);
				bad_count[chosen_slice] += 1;
				continue;
			}
			if (!deflen) {
				PyErr_Format(PyExc_ValueError, "Failed to convert \"%%s\" from %%s line %%lld", line, g.msgname, (long long)i - first_line + 1);
				goto err;
			}
			ptr = defbuf;
			len = deflen;
			if (def_o) {
				Py_INCREF(def_o);
				o_v = def_o;
			} else {
				d_v = def_d;
			}
			default_count[chosen_slice] += 1;
			do_minmax = !default_value_is_None;
		}
		// minmax tracking, not done for None/NaN-values
		if (do_minmax && isnan(d_v)) {
			saw_nan = 1;
			do_minmax = 0;
		}
		if (do_minmax) {
			if (!o_v && o_col_min) {
				o_v = PyFloat_FromDouble(d_v);
				err1(!o_v);
			}

			if (minlen) {
				if (o_v) {
					if (!o_col_min) {
						o_col_min = PyFloat_FromDouble(d_col_min);
						o_col_max = PyFloat_FromDouble(d_col_max);
					}
					if (PyObject_RichCompareBool(o_v, o_col_min, Py_LT)) {
						memcpy(buf_col_min, ptr, len);
						minlen = len;
						Py_INCREF(o_v);
						Py_DECREF(o_col_min);
						o_col_min = o_v;
					}
					if (PyObject_RichCompareBool(o_v, o_col_max, Py_GT)) {
						memcpy(buf_col_max, ptr, len);
						maxlen = len;
						Py_INCREF(o_v);
						Py_DECREF(o_col_max);
						o_col_max = o_v;
					}
					Py_DECREF(o_v);
				} else {
					if (d_v < d_col_min) {
						memcpy(buf_col_min, ptr, len);
						minlen = len;
						d_col_min = d_v;
					}
					if (d_v > d_col_max) {
						memcpy(buf_col_max, ptr, len);
						maxlen = len;
						d_col_max = d_v;
					}
				}
			} else {
				memcpy(buf_col_min, ptr, len);
				memcpy(buf_col_max, ptr, len);
				minlen = maxlen = len;
				d_col_min = d_col_max = d_v;
				if (o_v) {
					o_col_min = o_col_max = o_v;
					Py_INCREF(o_v);
				}
			}
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		err1(gzwrite(outfhs[chosen_slice], ptr, len) != len);
	}
	current_file++;
	if (current_file < in_count) {
		G_INIT(0);
		goto more_infiles;
	}
	if (!minlen && saw_nan) {
		const double nan = strtod("nan", 0);
		err1(!isnan(nan));
		buf_col_min[0] = buf_col_max[0] = 1;
		memcpy(buf_col_min + 1, &nan, 8);
		memcpy(buf_col_max + 1, &nan, 8);
		minlen = maxlen = 9;
	}
	if (minlen) {
		gzFile minmaxfh = gzopen(minmax_fn, gzip_mode);
		err1(!minmaxfh);
		res = g.error;
		if (gzwrite(minmaxfh, buf_col_min, minlen) != minlen) res = 1;
		if (gzwrite(minmaxfh, buf_col_max, maxlen) != maxlen) res = 1;
		if (gzclose(minmaxfh)) res = 1;
	} else {
		res = g.error;
	}
err:
	Py_XDECREF(o_col_min);
	Py_XDECREF(o_col_max);
	Py_XDECREF(def_o);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices + save_bad; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
	return res;
}
'''

proto_template = 'static int convert_column_%s(const char **in_fns, const char **in_msgnames, int in_count, const char **out_fns, const char *gzip_mode, const char *minmax_fn, const char *default_value, uint32_t default_len, int default_value_is_None, int empty_types_as_None, const char *fmt, const char *fmt_b, int record_bad, int skip_bad, int badmap_fd, size_t badmap_size, int save_bad, int slices, int slicemap_fd, size_t slicemap_size, uint64_t *bad_count, uint64_t *default_count, off_t *offsets, int64_t *max_counts)'

protos = []
funcs = [noneval_data]

proto = proto_template % ('number',)
code = convert_number_template % dict(proto=proto, strtol_f=strtol_f, longobj_f=longobj_f, big_endian=(sys.byteorder == 'big'))
protos.append(proto + ';')
funcs.append(code)

convert_blob_template = r'''
%(proto)s
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	g g;
	gzFile outfhs[slices + save_bad];
	memset(outfhs, 0, sizeof(outfhs));
	const char *line;
	int res = 1;
	uint8_t *defbuf = 0;
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	G_INIT(1);
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		if (badmap == MAP_FAILED) badmap = 0;
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		if (slicemap == MAP_FAILED) slicemap = 0;
		err1(!slicemap);
	}
%(setup)s
	if (default_value) {
		err1(default_value_is_None);
		line = default_value;
		g.linelen = default_len;
%(convert)s
		err1(!ptr);
		defbuf = malloc((uint32_t)len + 5);
		err1(!defbuf);
		if (len < 255) {
			defbuf[0] = len;
			memcpy(defbuf + 1, ptr, len);
			default_len = len + 1;
		} else {
			defbuf[0] = 255;
			memcpy(defbuf + 1, &len, 4);
			memcpy(defbuf + 5, ptr, len);
			default_len = (uint32_t)len + 5;
		}
		default_value = (const char *)defbuf;
%(cleanup)s
	}
	if (default_value_is_None) {
		default_value = "\xff\0\0\0\0";
		default_len = 5;
	}
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	for (; i < max_count && (line = read_line(&g)); i++) {
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			MAYBE_SAVE_BAD_BLOB;
			continue;
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		if (line == NoneMarker || (empty_types_as_None && g.linelen == 0)) {
			err1(gzwrite(outfhs[chosen_slice], "\xff\0\0\0\0", 5) != 5);
			continue;
		}
%(convert)s
		if (ptr) {
			if (len > 254) {
				uint8_t lenbuf[5];
				lenbuf[0] = 255;
				memcpy(lenbuf + 1, &len, 4);
				err1(gzwrite(outfhs[chosen_slice], lenbuf, 5) != 5);
			} else {
				uint8_t len8 = len;
				err1(gzwrite(outfhs[chosen_slice], &len8, 1) != 1);
			}
		} else {
			if (record_bad && !default_value) {
				badmap[i / 8] |= 1 << (i %% 8);
				bad_count[chosen_slice] += 1;
%(cleanup)s
				continue;
			}
			if (!default_value) {
				PyErr_Format(PyExc_ValueError, "Failed to convert \"%%s\" from %%s line %%lld", line, g.msgname, (long long)i - first_line + 1);
				goto err;
			}
			ptr = (const uint8_t *)default_value;
			len = default_len;
			default_count[chosen_slice] += 1;
		}
		err1(gzwrite(outfhs[chosen_slice], ptr, len) != len);
%(cleanup)s
	}
	current_file++;
	if (current_file < in_count) {
		G_INIT(0);
		goto more_infiles;
	}
	res = g.error;
err:
	if (defbuf) free(defbuf);
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices + save_bad; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	return res;
}
'''

null_number_template = r'''
%(proto)s
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	g g;
	gzFile outfhs[slices];
	memset(outfhs, 0, sizeof(outfhs));
	int res = 1;
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	G_INIT(1);
	err1(save_bad);
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		if (badmap == MAP_FAILED) badmap = 0;
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		if (slicemap == MAP_FAILED) slicemap = 0;
		err1(!slicemap);
	}
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	unsigned char buf[GZNUMBER_MAX_BYTES];
	for (; i < max_count && !read_fixed(&g, buf, 1); i++) {
		int z = buf[0];
		if (z == 1) z = 8;
		if (z >= 0x80) z = 0;
		if (z) {
			err1(read_fixed(&g, buf + 1, z));
		}
		z++;
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			continue;
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		err1(gzwrite(outfhs[chosen_slice], buf, z) != z);
	}
	current_file++;
	if (current_file < in_count) {
		G_INIT(0);
		goto more_infiles;
	}
	res = g.error;
err:
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	return res;
}
'''

null_template = r'''
%(proto)s
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
#endif
	g g;
	gzFile outfhs[slices];
	memset(outfhs, 0, sizeof(outfhs));
	int res = 1;
	char *badmap = 0;
	uint16_t *slicemap = 0;
	int chosen_slice = 0;
	int current_file = 0;
	G_INIT(1);
	err1(save_bad);
	if (badmap_fd != -1) {
		badmap = mmap(0, badmap_size, PROT_READ | PROT_WRITE, MAP_NOSYNC | MAP_SHARED, badmap_fd, 0);
		if (badmap == MAP_FAILED) badmap = 0;
		err1(!badmap);
	}
	if (slicemap_fd != -1) {
		slicemap = mmap(0, slicemap_size, PROT_READ, MAP_NOSYNC | MAP_SHARED, slicemap_fd, 0);
		if (slicemap == MAP_FAILED) slicemap = 0;
		err1(!slicemap);
	}
	int64_t i = 0;
	int64_t first_line;
	int64_t max_count;
more_infiles:
	first_line = i;
	max_count = max_counts[current_file];
	if (max_count < 0) {
		max_count = INT64_MAX;
	} else {
		max_count += first_line;
	}
	unsigned char buf[%(size)d];
	for (; i < max_count && !read_fixed(&g, buf, %(size)d); i++) {
		if (slicemap) chosen_slice = slicemap[i];
		if (skip_bad && badmap[i / 8] & (1 << (i %% 8))) {
			bad_count[chosen_slice] += 1;
			continue;
		}
		if (!outfhs[chosen_slice]) {
			outfhs[chosen_slice] = gzopen(out_fns[chosen_slice], gzip_mode);
			err1(!outfhs[chosen_slice]);
		}
		err1(gzwrite(outfhs[chosen_slice], buf, %(size)d) != %(size)d);
	}
	current_file++;
	if (current_file < in_count) {
		G_INIT(0);
		goto more_infiles;
	}
	res = g.error;
err:
	if (g_cleanup(&g)) res = 1;
	for (int i = 0; i < slices; i++) {
		if (outfhs[i] && gzclose(outfhs[i])) res = 1;
	}
	if (badmap) munmap(badmap, badmap_size);
	if (slicemap) munmap(slicemap, slicemap_size);
#ifdef CFFI_ATE_MY_GIL
	if (PyErr_Occurred()) {
		PyErr_PrintEx(0);
	}
	PyGILState_Release(gstate);
#endif
	return res;
}
'''

for name, ct in sorted(list(convfuncs.items()) + list(hidden_convfuncs.items())):
	if isinstance(ct, int):
		proto = proto_template % (name,)
		if ct:
			code = null_template % dict(proto=proto, size=ct,)
		else:
			code = null_number_template % dict(proto=proto,)
	elif not ct.conv_code_str:
		continue
	elif ct.size:
		if ':' in name:
			shortname = name.split(':', 1)[0]
		else:
			shortname = name
		proto = proto_template % (shortname,)
		destname = typerename.get(shortname, shortname)
		mm = minmaxfuncs[destname]
		noneval_name = 'noneval_' + destname
		code = convert_template % dict(proto=proto, datalen=ct.size, convert=ct.conv_code_str, minmax_setup=mm.setup, minmax_code=mm.code, minmax_extra=mm.extra, noneval_name=noneval_name, typename=mm.typename)
	else:
		proto = proto_template % (name.replace(':*', '').replace(':', '_'),)
		args = dict(proto=proto, convert=ct.conv_code_str, setup='', cleanup='')
		if isinstance(ct.conv_code_str, list):
			args['setup'], args['convert'], args['cleanup'] = ct.conv_code_str
		code = convert_blob_template % args
	protos.append(proto + ';')
	funcs.append(code)

copy_types = {typerename.get(k.split(':')[0], k.split(':')[0]): 'null_%d' % (v.size,) if v.size else 'null_blob' for k, v in convfuncs.items()}
copy_types['number'] = 'null_number'
copy_types['pickle'] = 'null_blob'


all_c_functions = r'''
#include <zlib.h>
#include <time.h>
#include <stdlib.h>
#include <strings.h>
#include <ctype.h>
#include <errno.h>
#include <sys/mman.h>
#include <math.h>
#include <float.h>
#include <locale.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <bytesobject.h>
#include <datetime.h>

#ifndef MAP_NOSYNC
#  define MAP_NOSYNC 0
#endif

#define err1(v) if (v) goto err
#define err2(v, msg) if (v) { err = msg; goto err; }
#define Z (128 * 1024)

// Py_FileSystemDefaultEncoding is deprecated in Python 3.12.
// For consistency we use NULL (utf-8) on all python3 versions.
#if PY_MAJOR_VERSION < 3
#  define DEFAULT_ENCODING Py_FileSystemDefaultEncoding
#else
#  define DEFAULT_ENCODING NULL
#endif

typedef struct {
	gzFile fh;
	int len;
	int pos;
	int error;
	uint32_t saved_size;
	int32_t linelen;
	const char *filename;
	const char *msgname;
	char *largetmp;
	char buf[Z + 1];
} g;

static const char NoneMarker[1] = {0};
static char decimal_separator = '.';

static inline int dt_isspace(const int c)
{
	return (c == 32 || (c >= 9 && c <= 13));
}
static inline int dt_notspace(int c)
{
	return !dt_isspace(c);
}
static inline int dt_isany(int c)
{
	return 1;
}
static inline int dt_isdigit(int c)
{
	return c >= '0' && c <= '9';
}
static inline int dt_notdigit(int c)
{
	return !dt_isdigit(c);
}
static inline int dt_notspacedigit(int c)
{
	return dt_notspace(c) && dt_notdigit(c);
}
static inline int dt_ispercent(int c)
{
	return c == '%';
}

static inline char *dt_skipspace(char *ptr)
{
	while (dt_isspace(*ptr)) ptr++;
	return ptr;
}

#define G_INIT(first) err1(g_init(&g, in_fns[current_file], in_msgnames[current_file], offsets[current_file], first));

static int g_init(g *g, const char *filename, const char *msgname, off_t offset, const int first)
{
	if (!first) {
		int e = gzclose(g->fh);
		g->fh = 0;
		if (e || g->error) return 1;
	}
	g->fh = 0;
	g->pos = g->len = 0;
	g->error = 0;
	g->filename = filename;
	g->msgname = msgname;
	if (first) g->largetmp = 0;
	int fd = open(filename, O_RDONLY);
	if (fd < 0) return 1;
	if (lseek(fd, offset, 0) != offset) goto errfd;
	g->fh = gzdopen(fd, "rb");
	if (!g->fh) goto errfd;
	return 0;
errfd:
	close(fd);
	return 1;
}

static int g_cleanup(g *g)
{
	if (g->largetmp) free(g->largetmp);
	if (g->fh) return gzclose(g->fh);
	return 0;
}

static int numeric_comma(const char *localename)
{
	decimal_separator = ',';
	if (setlocale(LC_NUMERIC, localename)) {
		return strtod("1,5", 0) != 1.5;
	}
	return 1;
}

static int read_chunk(g *g, int offset)
{
	if (g->error) return 1;
	const int len = gzread(g->fh, g->buf + offset, Z - offset);
	if (len <= 0) {
		(void) gzerror(g->fh, &g->error);
		return 1;
	}
	g->len = offset + len;
	g->buf[g->len] = 0;
	g->pos = 0;
	return 0;
}

static inline const char *read_line(g *g)
{
	if (g->largetmp) {
		free(g->largetmp);
		g->largetmp = 0;
	}
	if (g->pos >= g->len) {
		if (read_chunk(g, 0)) return 0;
	}
	if (!g->pos) {
		uint8_t *uptr = (uint8_t *)g->buf + g->pos;
		g->saved_size = *uptr;
	}
	uint32_t size = g->saved_size;
	g->pos++;
size_again:
	if (size == 255) {
		const int offset = g->len - g->pos;
		if (offset < 4) {
			memmove(g->buf, g->buf + g->pos, offset);
			if (read_chunk(g, offset) || g->len < 4) {
				PyErr_Format(PyExc_IOError, "%s: Format error (%s)", g->msgname, g->filename);
				g->error = 1;
				return 0;
			}
			goto size_again;
		}
		memcpy(&size, g->buf + g->pos, 4);
		g->pos += 4;
		if (size == 0) {
			if (g->len > g->pos) {
				uint8_t *uptr = (uint8_t *)g->buf + g->pos;
				g->saved_size = *uptr;
			}
			g->linelen = 0;
			return NoneMarker;
		} else if (size < 255 || size > 0x7fffffff) {
			PyErr_Format(PyExc_IOError, "%s: Format error (%s)", g->msgname, g->filename);
			g->error = 1;
			return 0;
		}
	}
	unsigned int avail = g->len - g->pos;
	if (size > Z) {
		g->largetmp = malloc(size + 1);
		if (!g->largetmp) {
			perror("malloc");
			g->error = 1;
			return 0;
		}
		memcpy(g->largetmp, g->buf + g->pos, avail);
		const int fill_len = size - avail;
		const int read_len = gzread(g->fh, g->largetmp + avail, fill_len);
		if (read_len != fill_len) {
			PyErr_Format(PyExc_IOError, "%s: Format error (%s)", g->msgname, g->filename);
			g->error = 1;
			return 0;
		}
		g->largetmp[size] = 0;
		g->linelen = size;
		g->pos = g->len;
		return g->largetmp;
	}
	if (avail < size) {
		memmove(g->buf, g->buf + g->pos, avail);
		if (read_chunk(g, avail)) {
			PyErr_Format(PyExc_IOError, "%s: Format error (%s)", g->msgname, g->filename);
			g->error = 1;
			return 0;
		}
		avail = g->len;
		if (avail < size) {
			PyErr_Format(PyExc_IOError, "%s: Format error (%s)", g->msgname, g->filename);
			g->error = 1;
			return 0;
		}
	}
	char *res = g->buf + g->pos;
	g->pos += size;
	if (g->len > g->pos) {
		uint8_t *uptr = (uint8_t *)g->buf + g->pos;
		g->saved_size = *uptr;
	}
	res[size] = 0;
	g->linelen = size;
	return res;
}

static inline int read_fixed(g *g, unsigned char *res, int z)
{
	if (g->pos >= g->len) {
		if (read_chunk(g, 0)) return 1;
	}
	int avail = g->len - g->pos;
	if (avail < z) {
		err1(z <= 1); // This can't happen, but some compilers produce warnings without it.
		memcpy(res, g->buf + g->pos, avail);
		res += avail;
		z -= avail;
		if (read_chunk(g, 0) || g->len < z) {
err:
			PyErr_Format(PyExc_IOError, "%s: Format error (%s)", g->msgname, g->filename);
			g->error = 1;
			return 1;
		}
	}
	memcpy(res, g->buf + g->pos, z);
	g->pos += z;
	return 0;
}

// Here we re-implement the useful parts of strptime plus some extensions.

#define TM_NUMBER(low, high) do {                                   \
		const int maxdigits = strlen(#high);                        \
		if (tm_number(s, low, high, maxdigits, &num)) return 1;     \
	} while (0)

static int tm_number(const char **s, int low, int high, int maxdigits, int *r_num)
{
	// initial spaces are accepted (as initial zeroes)
	while (maxdigits > 1 && dt_isspace(**s)) {
		(*s)++;
		maxdigits--;
	}
	int num = 0;
	int c = **s;
	if (c < '0' || c > '9') return 1;
	int ix = 0;
	while (ix < maxdigits && c >= '0' && c <= '9') {
		num = num * 10 + c - '0';
		c = (*s)[++ix];
	}
	if (num < low || num > high) return 1;
	*s += ix;
	if (r_num) *r_num = num;
	return 0;
}

struct mytm {
	int century;
	int year;
	int pm;
	int optional;
	int bad_pattern;
	int previous_matched;
	int32_t fraction;
};

// for %%b
static const char * const monthnames[] = {
	"JANUARY",
	"FEBRUARY",
	"MARCH",
	"APRIL",
	"MAY",
	"JUNE",
	"JULY",
	"AUGUST",
	"SEPTEMBER",
	"OCTOBER",
	"NOVEMBER",
	"DECEMBER"
};

typedef int (*tm_matcher)(int c);

static int tm_skip(const char **s, int low, int high, tm_matcher match)
{
	int pos = 0;
	if (low == -1) low = 1;
	if (high == -1) high = low;
	while (pos < high && (*s)[pos] && match((*s)[pos])) pos++;
	if (pos < low) return 1;
	*s += pos;
	return 0;
}

static int tm_parse_percent(const char **format, int *r_low, int *r_high)
{
	const char *p = *format + 1;
	if (dt_isdigit(*p)) {
		if (tm_number(&p, 0, 999999999, 9, r_low)) return 1;
		if (*p == ',') {  // it's a range
			p++;
			if (tm_number(&p, *r_low, 999999999, 9, r_high)) return 1;
			if (*r_high < *r_low || *r_high == 0) return 1;
		} else {          // it's a count
			*r_high = -1;
		}
	} else {
		*r_high = *r_low = -1;
	}
	if (!*p) return 1;
	*format = p;
	return 0;
}

static int tm_skip_fmt(const char **format, int count)
{
	while (count--) {
		if (**format == '%') {
			int low, high;
			if (tm_parse_percent(format, &low, &high)) return 1;
			if (**format == '?' || **format == ':' || **format == '-') {
				count += abs(high == -1 ? low : high);
			}
		}
		if (!**format) {
			// not enough format -> error
			return 1;
		}
		(*format)++;
	}
	return 0;
}

static int mystrptime2(const char **s, const char **format, struct tm *tm, struct mytm *mytm, int *r_count, const int max_count);

static int tm_ignore(const char **s, const char **format, struct tm *tm, struct mytm *mytm, const int ignore_count)
{
	if (ignore_count < 1) goto bad;
	if (**format != '-') goto bad;
	(*format)++;
	const struct tm save_tm = *tm;
	const struct mytm save_mytm = *mytm;
	int count = 0;
	const int ret = mystrptime2(s, format, tm, mytm, &count, ignore_count);
	if (mytm->bad_pattern) return 1;
	if (count < ignore_count) {
		if (tm_skip_fmt(format, ignore_count - count)) goto bad;
	}
	*tm = save_tm;
	*mytm = save_mytm;
	(*format)--; // caller advances one before using format
	return ret;
bad:
	mytm->bad_pattern = 1;
	return 1;
}

static int tm_optional(const char **s, const char **format, struct tm *tm, struct mytm *mytm, const int low, const int high)
{
	if (low < 1 || high < 1 || high < low) goto bad;
	const struct tm save_tm = *tm;
	const struct mytm save_mytm = *mytm;
	const char *try_s = *s;
	int count = 0;
	if (**format != '?') goto bad;
	(*format)++;
	(void) mystrptime2(&try_s, format, tm, mytm, &count, high);
	if (mytm->bad_pattern) return 1;
	if (count < high) {
		if (tm_skip_fmt(format, high - count)) goto bad;
	}
	if (count < low) {
		mytm->previous_matched = 0;
		*tm = save_tm;
		*mytm = save_mytm;
	} else {
		mytm->previous_matched = 1;
		*s = try_s;
	}
	(*format)--; // caller advances one before using format
	return 0;
bad:
	mytm->bad_pattern = 1;
	return 1;
}

static int tm_fraction(const char **s, const int digits, struct mytm *mytm)
{
	// This is always considered to be six digits (unless overridden), so
	// "123000", "123" and "123   " are all the same number.
	// (Unlike other numbers spaces are allowed at the end, not start.)
	// Stops early when digits < 6.
	// Discards digits at the end when digits > 6.
	// Pre-fill the buffer with 0 to help with that.
	char buf[7] = "000000\0";
	int pos = 0, space = 0;
	if (**s < '0' || **s > '9') return 1;
	if (digits < 1) return 1;
	while (pos < digits) {
		if (space) {
			if (!dt_isspace(**s)) break;
		} else {
			if (dt_isspace(**s)) {
				space = 1;
			} else {
				if (**s < '0' || **s > '9') break;
				if (pos < 6) buf[pos] = **s;
			}
		}
		pos++;
		(*s)++;
	}
	mytm->fraction = strtol(buf, 0, 10);
	return 0;
}

// "Excel" time, days since an epoch as a float
// Possible flag values:
//     0 LibreOffice dates, epoch is 1899-12-30
//     1 Lotus 1-2-3 dates, epoch is 1899-12-31 and 1900 is a leap year.
//     2 Excel Mac dates, epoch is 1904-01-01.
// The default flag value is 1, because that's what Excel for Windows (and web) uses.
// If all your dates are >= 1900-03-01 flags 0 and 1 are the same
// (i.e. you very rarely have to worry about the flags.).
//
//  value       flags = 0           flags = 1           flags = 2
// -36522       1800-01-01          1800-01-02          1804-01-03
//     -1       1899-12-29          1899-12-30          1903-12-31
//     -0.75    1899-12-29 06:00    1899-12-30 06:00    1903-12-31 06:00
//      0       1899-12-30          1899-12-31          1904-01-01
//      1       1899-12-31          1900-01-01          1904-01-02
//      2       1900-01-01          1900-01-02          1904-01-03
//      2.75    1900-01-01 18:00    1900-03-01 00:00    1904-01-03 18:00
//     59       1900-02-27          1900-02-28          1904-02-29
//     60       1900-02-28          1900-03-01  !!!     1904-03-01
//     61       1900-03-01          1900-03-01  !!!     1904-03-02
//  25569       1970-01-01          1970-01-01          1974-01-02

static int tm_excel_time(const char **s, int flags, struct tm *tm, struct mytm *mytm)
{
	if (dt_isspace(**s)) return 1; // strtoll accepts leading spaces
	if (flags == -1) flags = 1;
	errno = 0;
	char *end;
	int seconds = 0;
	int negative = (**s == '-'); // can't check days, because "-0" is 0.
	long long days = strtoll(*s, &end, 10);
	if (errno || end == *s) return 1;
	if (days > 0xf0000000LL) {
		// Sometimes Gnumeric adds 1 << 32 to negative dates.
		days -= 0x100000000LL;
		if (days >= 0) return 1;
		negative = 0; // -36522.25 -> 4294930773.75, so -36523 + 0.75 is correct
	}
	if (flags & 2) days += 1462; // base year is 1904 in Mac version of Excel
	if (flags & 1 && days < 61) days++; // 1900 is not actually a leap year
	if (*end == decimal_separator) {
		double frac = strtod(end, &end);
		// errors return 0 without changing end, which is fine.
		if (frac) {
			if (negative) frac = -frac;
			seconds = round(frac * 86400);
			if (seconds > 86399) seconds = 86399; // don't overflow to the next day
			if (seconds < -86399) seconds = -86399; // don't overflow to the previous day
		}
	}
	if (days < -693593 || days > 2958465) return 1; // 0001-01-01 to 9999-12-31
	if (seconds < 0 && days == -693593) return 1; // don't underflow 0001-01-01
	time_t t = (days - 25569) * 86400 + seconds;
	if (t != (days - 25569) * 86400 + seconds) return 1; // time_t not wide enough
	if (!gmtime_r(&t, tm)) return 1;
	mytm->century = tm->tm_year / 100 + 19;
	mytm->year = tm->tm_year % 100;
	mytm->pm = -1;
	*s = end;
	return 0;
}

static int tm_conv(const char **s, const char **format, struct tm *tm, struct mytm *mytm)
{
	char *end;
	int num;
	int low, high;
	if (tm_parse_percent(format, &low, &high)) {
		mytm->bad_pattern = 1;
		return 1;
	}
	switch (**format) {
		case 'Y': // YYYY
			TM_NUMBER(1, 9999);
			mytm->century = num / 100;
			mytm->year = num % 100;
			return 0;
		case 'C': // Century number
			TM_NUMBER(0, 99);
			mytm->century = num;
			return 0;
		case 'y': // YY
			TM_NUMBER(0, 99);
			mytm->year = num;
			return 0;
		case 'm': // month num
			TM_NUMBER(1, 12);
			tm->tm_mon = num - 1;
			return 0;
		case 'b': // month name
			for (int month = 0; month < 12; month++) {
				const char * const name = monthnames[month];
				for (int ix = 0; ; ix++) {
					const char c = (*s)[ix];
					const char n = name[ix];
					if (c == 0 || (c != n && c != (n | 32))) {
						if (n == 0) {
							*s += ix;
						} else if (ix > 2) { // short version matched
							*s += 3;
						} else {
							break;
						}
						tm->tm_mon = month;
						return 0;
					}
				}
			}
			return 1;
		case 'd': // mday
			TM_NUMBER(1, 31);
			tm->tm_mday = num;
			return 0;
		case 'H': // 24h hour
			TM_NUMBER(0, 23);
			tm->tm_hour = num;
			mytm->pm = -1;
			return 0;
		case 'I': // 12h hour
			TM_NUMBER(1, 12);
			tm->tm_hour = num;
			return 0;
		case 'p': // AM/PM
			if (**s == 0 || ((*s)[1] != 'm' && (*s)[1] != 'M')) return 1;
			if (**s == 'a' || **s == 'A') {
				mytm->pm = 0;
			} else if (**s == 'p' || **s == 'P') {
				mytm->pm = 1;
			} else {
				return 1;
			}
			if (tm->tm_hour > 12) tm->tm_hour -= 12;
			*s += 2;
			return 0;
		case 'M': // minute
			TM_NUMBER(0, 59);
			tm->tm_min = num;
			return 0;
		case 'S': // second
			TM_NUMBER(0, 59);
			tm->tm_sec = num;
			return 0;
		case 's': // unix epoch time
		case 'J': // java epoch time
			if (dt_isspace(**s)) return 1; // strtoll accepts leading spaces
			errno = 0;
			long long lt = strtoll(*s, &end, 10);
			if (errno || end == *s) return 1;
			time_t t;
			int32_t frac = mytm->fraction;
			if (**format == 's') {
				t = lt;
				if (t != lt) return 1;
			} else { // must be J
				t = lt / 1000;
				if (lt / 1000 != t) return 1;
				frac = (lt % 1000) * 1000;
				if (frac < 0) {
					frac += 1000000;
					t--;
				}
			}
			if (!gmtime_r(&t, tm)) return 1;
			mytm->fraction = frac;
			mytm->century = tm->tm_year / 100 + 19;
			mytm->year = tm->tm_year % 100;
			mytm->pm = -1;
			*s = end;
			return 0;
		case 'e':
			return tm_excel_time(s, low, tm, mytm);
		case 'f': // microsecond
			return tm_fraction(s, (low == -1) ? 6 : low, mytm);
		case '%': // literal "%"
			return tm_skip(s, low, high, dt_ispercent);
		case ' ':
			return tm_skip(s, low, high, dt_isspace);
		case '.':
			return tm_skip(s, low, high, dt_notspace);
		case '*':
			return tm_skip(s, low, high, dt_isany);
		case '#':
			if (low == -1) {
				return tm_skip(s, 1, 1, dt_isdigit);
			} else {
				int digits;
				if (high == -1) {
					// %N#, up to N digits
					digits = low;
					if (digits < 1 || digits > 9) return 1;
					high = 10;
					while (--low) high *= 10;
					high--;
				} else {
					// %LOW,HIGH#, allow as many digits as the high number takes
					digits = 1;
					int tmp = high;
					while (tmp > 9) {
						tmp /= 10;
						digits++;
					}
				}
				return tm_number(s, low, high, digits, 0);
			}
		case '^':
			return tm_skip(s, low, high, dt_notdigit);
		case '@':
			return tm_skip(s, low, high, dt_notspacedigit);
		case '-': // parse but don't use value from next item(s)
			return tm_ignore(s, format, tm, mytm, abs(low));
		case '/': // anything after this is optional
			mytm->optional = 1;
			return 0;
		case '?': // one (or range of) optional item(s) follow
			if (low == -1) low = 1;
			if (high == -1) high = low;
			return tm_optional(s, format, tm, mytm, low, high);
		case ':': // skipped if the previous ? matched (no range support)
			if (low == -1) low = 1;
			if (high != -1) goto bad;
			if (mytm->previous_matched) {
				(*format)++;
				if (tm_skip_fmt(format, low)) goto bad;
				(*format)--; // caller advances one before using format
			}
			return 0;
	}
bad:
	mytm->bad_pattern = 1;
	return 1;
}

static int mystrptime2(const char **s, const char **format, struct tm *tm, struct mytm *mytm, int *r_count, const int max_count)
{
	while (**format && *r_count < max_count) {
		switch (**format) {
			case '%':
				if (tm_conv(s, format, tm, mytm)) return 1;
				break;
			case ' ':
			case '\t':
			case '\n':
			case '\v':
			case '\f':
			case '\r':
				while (dt_isspace(**s)) (*s)++;
				break;
			default:
				if (**s != **format) return 1;
				(*s)++;
				break;
		}
		if (mytm->bad_pattern) return 1;
		(*format)++;
		(*r_count)++;
	}
	return 0;
}

static int mystrptime(const char **s, const char *format, struct tm *tm, int32_t *r_frac)
{
	struct mytm mytm = {-1, 70, -1};
	memset(tm, 0, sizeof(*tm));
	tm->tm_mday = 1;
	int count = 0;
	const int ret = mystrptime2(s, &format, tm, &mytm, &count, INT_MAX);
	if (mytm.century == -1) mytm.century = (mytm.year < 69 ? 20 : 19);
	tm->tm_year = (mytm.century - 19) * 100 + mytm.year;
	if (mytm.pm != -1) {
		if (tm->tm_hour == 12) tm->tm_hour = 0;
		if (mytm.pm) tm->tm_hour += 12;
	}
	*r_frac = mytm.fraction;
	return (mytm.optional ? mytm.bad_pattern : ret);
}

#undef TM_NUMBER

#define MAYBE_SAVE_BAD_BLOB                                                \
	if (save_bad) {                                                        \
		if (!outfhs[slices]) {                                             \
			outfhs[slices] = gzopen(out_fns[slices], gzip_mode);           \
			err1(!outfhs[slices]);                                         \
		}                                                                  \
		int32_t len = g.linelen;                                           \
		if (line == NoneMarker) {                                          \
			line = "\xff\0\0\0\0";                                         \
			len = 5;                                                       \
		} else {                                                           \
			if (len > 254) {                                               \
				uint8_t lenbuf[5];                                         \
				lenbuf[0] = 255;                                           \
				memcpy(lenbuf + 1, &len, 4);                               \
				err1(gzwrite(outfhs[slices], lenbuf, 5) != 5);             \
			} else {                                                       \
				uint8_t len8 = len;                                        \
				err1(gzwrite(outfhs[slices], &len8, 1) != 1);              \
			}                                                              \
		}                                                                  \
		err1(gzwrite(outfhs[slices], line, len) != len);                   \
	}

static int use_tz = 0;

static void init(const char *tz)
{
#ifdef CFFI_ATE_MY_GIL
	PyGILState_STATE gstate = PyGILState_Ensure();
	PyDateTime_IMPORT;
	PyGILState_Release(gstate);
#else
	PyDateTime_IMPORT;
#endif
	if (tz) {
		if (!strcmp(tz, "UTC")) {
			use_tz = -1; // true, but not 1, so not using mktime(3).
		} else {
			use_tz = 1;
			if (setenv("TZ", tz, 1)) exit(1);
			tzset();
		}
	}
}
''' + ''.join(funcs)


extra_c_functions = r'''
static PyObject *py_init(PyObject *dummy, PyObject *o_tz)
{
	const char *tz;
	if (str_or_0(o_tz, &tz)) return 0;
	init(tz);
	Py_RETURN_NONE;
}

static PyObject *py_numeric_comma(PyObject *dummy, PyObject *o_localename)
{
	const char *localename;
	if (str_or_0(o_localename, &localename)) return 0;
	if (numeric_comma(localename)) Py_RETURN_TRUE;
	Py_RETURN_FALSE;
}

static PyObject *_py_strptime(PyObject *args, PyObject *kwds, const char **r_p)
{
	static int first_time = 1;
	if (first_time) {
		PyDateTime_IMPORT;
		first_time = 0;
	}
	const char *value, *format;
	PyObject *default_obj = 0;
	static char *kwlist[] = {
		"value", "format", "default", 0
	};
	if (!PyArg_ParseTupleAndKeywords(
		args, kwds, "etet|O", kwlist,
		DEFAULT_ENCODING, &value,
		DEFAULT_ENCODING, &format,
		&default_obj
	)) return 0;
	struct tm tm;
	int32_t f;
	const char *p;
	const char **p_p = (r_p ? r_p : &p);
	*p_p = value;
	if (!mystrptime(p_p, format, &tm, &f) && (r_p || !**p_p)) {
		const uint32_t year = tm.tm_year + 1900;
		const uint32_t mon  = tm.tm_mon + 1;
		const uint32_t mday = tm.tm_mday;
		const uint32_t hour = tm.tm_hour;
		const uint32_t min  = tm.tm_min;
		const uint32_t sec  = tm.tm_sec;
		PyObject *dt = PyDateTime_FromDateAndTime(year, mon, mday, hour, min, sec, f);
		if (dt) return dt;
	}
	if (default_obj) {
		PyErr_Clear();
		Py_INCREF(default_obj);
		return default_obj;
	} else {
		PyErr_Format(PyExc_ValueError, "Failed to parse '%s' as '%s'", value, format);
		return 0;
	}
}

static PyObject *py_strptime(PyObject *dummy, PyObject *args, PyObject *kwds)
{
	return _py_strptime(args, kwds, 0);
}

static PyObject *py_strptime_i(PyObject *dummy, PyObject *args, PyObject *kwds)
{
	const char *remaining;
	PyObject *res = _py_strptime(args, kwds, &remaining);
	if (!res) return 0;
#if PY_MAJOR_VERSION < 3
	return Py_BuildValue("(Ns)", res, remaining);
#else
	return Py_BuildValue("(Ny)", res, remaining);
#endif
}
'''


c_module_wrapper_template = r'''
static PyObject *py_%s(PyObject *self, PyObject *args)
{
	int good = 0;
	const char *err = 0;
	PyObject *o_in_fns;
	PyObject *o_in_msgnames;
	int in_count;
	const char **in_fns = 0;
	const char **in_msgnames = 0;
	PyObject *o_out_fns;
	const char **out_fns = 0;
	const char *gzip_mode;
	const char *minmax_fn;
	PyObject *o_default_value;
	const char *default_value;
	int default_len;
	int default_value_is_None;
	int empty_types_as_None;
	PyObject *o_fmt;
	const char *fmt;
	PyObject *o_fmt_b;
	const char *fmt_b;
	int record_bad;
	int skip_bad;
	int badmap_fd;
	PY_LONG_LONG badmap_size;
	int save_bad;
	int slices;
	int slicemap_fd;
	PY_LONG_LONG slicemap_size;
	PyObject *o_bad_count;
	uint64_t *bad_count = 0;
	PyObject *o_default_count;
	uint64_t *default_count = 0;
	PyObject *o_offsets;
	off_t *offsets = 0;
	PyObject *o_max_counts;
	int64_t *max_counts = 0;
	if (!PyArg_ParseTuple(args, "OOiOetetOiiiOOiiiLiiiLOOOO",
		&o_in_fns,
		&o_in_msgnames,
		&in_count,
		&o_out_fns,
		DEFAULT_ENCODING, &gzip_mode,
		DEFAULT_ENCODING, &minmax_fn,
		&o_default_value,
		&default_len,
		&default_value_is_None,
		&empty_types_as_None,
		&o_fmt,
		&o_fmt_b,
		&record_bad,
		&skip_bad,
		&badmap_fd,
		&badmap_size,
		&save_bad,
		&slices,
		&slicemap_fd,
		&slicemap_size,
		&o_bad_count,
		&o_default_count,
		&o_offsets,
		&o_max_counts
	)) {
		return 0;
	}
	if (str_or_0(o_default_value, &default_value)) return 0;
	if (str_or_0(o_fmt, &fmt)) return 0;
	if (str_or_0(o_fmt_b, &fmt_b)) return 0;
#define LISTCHK(name, cnt) \
	err2(!PyList_Check(o_ ## name) || PyList_Size(o_ ## name) != cnt, \
		#name " must be list with " #cnt " elements" \
	);
	LISTCHK(bad_count, slices);
	LISTCHK(default_count, slices);
	LISTCHK(in_fns, in_count);
	LISTCHK(in_msgnames, in_count);
	LISTCHK(offsets, in_count);
	LISTCHK(max_counts, in_count);
	LISTCHK(out_fns, slices + save_bad);
#undef LISTCHK
	in_fns = malloc(in_count * sizeof(*in_fns));
	err1(!in_fns);
	in_msgnames = malloc(in_count * sizeof(*in_msgnames));
	err1(!in_msgnames);
	offsets = malloc(in_count * sizeof(*offsets));
	err1(!offsets);
	max_counts = malloc(in_count * sizeof(*max_counts));
	err1(!max_counts);
	for (int i = 0; i < in_count; i++) {
		in_fns[i] = PyBytes_AS_STRING(PyList_GetItem(o_in_fns, i));
		err1(!in_fns[i]);
		in_msgnames[i] = PyBytes_AS_STRING(PyList_GetItem(o_in_msgnames, i));
		err1(!in_msgnames[i]);
		offsets[i] = PyLong_AsLongLong(PyList_GetItem(o_offsets, i));
		err1(PyErr_Occurred());
		max_counts[i] = PyLong_AsLongLong(PyList_GetItem(o_max_counts, i));
		err1(PyErr_Occurred());
	}

	out_fns = calloc(slices + save_bad, sizeof(*out_fns));
	err1(!out_fns);
	default_count = calloc(slices, 8);
	err1(!default_count);
	bad_count = calloc(slices, 8);
	err1(!bad_count);
	for (int i = 0; i < slices + save_bad; i++) {
		out_fns[i] = PyBytes_AS_STRING(PyList_GetItem(o_out_fns, i));
		err1(!out_fns[i]);
	}

	err1(%s(in_fns, in_msgnames, in_count, out_fns, gzip_mode, minmax_fn, default_value, default_len, default_value_is_None, empty_types_as_None, fmt, fmt_b, record_bad, skip_bad, badmap_fd, badmap_size, save_bad, slices, slicemap_fd, slicemap_size, bad_count, default_count, offsets, max_counts));
	for (int i = 0; i < slices; i++) {
		err1(PyList_SetItem(o_default_count, i, PyLong_FromUnsignedLongLong(default_count[i])));
		err1(PyList_SetItem(o_bad_count, i, PyLong_FromUnsignedLongLong(bad_count[i])));
	}
	good = 1;
err:
	if (bad_count) free(bad_count);
	if (default_count) free(default_count);
	if (out_fns) free(out_fns);
	if (max_counts) free(max_counts);
	if (offsets) free(offsets);
	if (in_msgnames) free(in_msgnames);
	if (in_fns) free(in_fns);
	if (good) Py_RETURN_NONE;
	if (err) {
		PyErr_SetString(PyExc_ValueError, err);
	} else if (!PyErr_Occurred()) {
		PyErr_SetString(PyExc_ValueError, "internal error");
		return 0;
	}
	return 0;
}
'''

extra_method_defs = [
	'{"init", py_init, METH_O, 0}',
	'{"numeric_comma", py_numeric_comma, METH_O, 0}',
	'{"strptime", (PyCFunction)py_strptime, METH_VARARGS | METH_KEYWORDS, "strptime(value, format, default=<no>) -> datetime\\nlike \\"datetime:format\\" in dataset_type."}',
	'{"strptime_i", (PyCFunction)py_strptime_i, METH_VARARGS | METH_KEYWORDS, "strptime_i(value, format, default=<no>) -> (datetime, remaining_bytes)\\nlike \\"datetimei:format\\" in dataset_type."}',
]

c_module_code, c_module_hash = c_backend_support.make_source('dataset_type', all_c_functions, protos, extra_c_functions, extra_method_defs, c_module_wrapper_template)

def init():
	_test()
	extra_protos = [
		'static int numeric_comma(const char *localename);',
		'static void init(const char *tz);',
	]
	return c_backend_support.init('dataset_type', c_module_hash, protos, extra_protos, all_c_functions)
