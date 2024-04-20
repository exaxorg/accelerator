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

from __future__ import print_function
from __future__ import division

from accelerator import _dsutil
from accelerator.compat import str_types, PY3
from accelerator.standard_methods._dataset_type import strptime, strptime_i

_convfuncs = {
	'number'   : _dsutil.WriteNumber,
	'complex64': _dsutil.WriteComplex64,
	'complex32': _dsutil.WriteComplex32,
	'float64'  : _dsutil.WriteFloat64,
	'float32'  : _dsutil.WriteFloat32,
	'int64'    : _dsutil.WriteInt64,
	'int32'    : _dsutil.WriteInt32,
	'bool'     : _dsutil.WriteBool,
	'datetime' : _dsutil.WriteDateTime,
	'date'     : _dsutil.WriteDate,
	'time'     : _dsutil.WriteTime,
	'bytes'    : _dsutil.WriteBytes,
	'ascii'    : _dsutil.WriteAscii,
	'unicode'  : _dsutil.WriteUnicode,
	'parsed:number'   : _dsutil.WriteParsedNumber,
	'parsed:complex64': _dsutil.WriteParsedComplex64,
	'parsed:complex32': _dsutil.WriteParsedComplex32,
	'parsed:float64'  : _dsutil.WriteParsedFloat64,
	'parsed:float32'  : _dsutil.WriteParsedFloat32,
	'parsed:int64'    : _dsutil.WriteParsedInt64,
	'parsed:int32'    : _dsutil.WriteParsedInt32,
}

_type2iter = {
	'number'   : _dsutil.ReadNumber,
	'complex64': _dsutil.ReadComplex64,
	'complex32': _dsutil.ReadComplex32,
	'float64'  : _dsutil.ReadFloat64,
	'float32'  : _dsutil.ReadFloat32,
	'int64'    : _dsutil.ReadInt64,
	'int32'    : _dsutil.ReadInt32,
	'bool'     : _dsutil.ReadBool,
	'datetime' : _dsutil.ReadDateTime,
	'date'     : _dsutil.ReadDate,
	'time'     : _dsutil.ReadTime,
	'bytes'    : _dsutil.ReadBytes,
	'ascii'    : _dsutil.ReadAscii,
	'unicode'  : _dsutil.ReadUnicode,
}

def typed_writer(typename):
	if typename not in _convfuncs:
		raise ValueError("Unknown writer for type %s" % (typename,))
	return _convfuncs[typename]

def typed_reader(typename):
	if typename not in _type2iter:
		raise ValueError("Unknown reader for type %s" % (typename,))
	return _type2iter[typename]

_nodefault = object()

from json import JSONEncoder, JSONDecoder, loads as json_loads
class WriteJson(object):
	__slots__ = ('fh', 'encode')
	min = max = None
	def __init__(self, *a, **kw):
		default = kw.pop('default', _nodefault)
		if PY3:
			self.fh = _dsutil.WriteUnicode(*a, **kw)
			self.encode = JSONEncoder(ensure_ascii=False, separators=(',', ':')).encode
		else:
			self.fh = _dsutil.WriteBytes(*a, **kw)
			self.encode = JSONEncoder(ensure_ascii=True, separators=(',', ':')).encode
		self.encode = self._wrap_encode(self.encode, default)
	def _wrap_encode(self, encode, default):
		if default is _nodefault:
			return encode
		default = encode(default)
		def wrapped_encode(o):
			try:
				return encode(o)
			except (TypeError, ValueError):
				return default
		return wrapped_encode
	def write(self, o):
		self.fh.write(self.encode(o))
	@property
	def count(self):
		return self.fh.count
	@property
	def compression(self):
		return self.fh.compression
	def close(self):
		self.fh.close()
	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		self.close()
_convfuncs['json'] = WriteJson

class WriteParsedJson(WriteJson):
	"""This assumes strings are the object you wanted and parse them as json.
	If they are unparseable you get an error."""
	__slots__ = ()
	def _wrap_encode(self, encode, default):
		if default is not _nodefault:
			if isinstance(default, str_types):
				default = json_loads(default)
			default = encode(default)
		def wrapped_encode(o):
			try:
				if isinstance(o, str_types):
					o = json_loads(o)
				return encode(o)
			except (TypeError, ValueError):
				if default is _nodefault:
					raise
				return default
		return wrapped_encode
_convfuncs['parsed:json'] = WriteParsedJson

class ReadJson(object):
	__slots__ = ('fh', 'decode')
	def __init__(self, *a, **kw):
		if PY3:
			self.fh = _dsutil.ReadUnicode(*a, **kw)
		else:
			self.fh = _dsutil.ReadBytes(*a, **kw)
		self.decode = JSONDecoder().decode
	def __next__(self):
		return self.decode(next(self.fh))
	next = __next__
	@property
	def count(self):
		return self.fh.count
	def close(self):
		self.fh.close()
	def __iter__(self):
		return self
	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		self.close()
_type2iter['json'] = ReadJson

from pickle import dumps as pickle_dumps, loads as pickle_loads
class WritePickle(object):
	__slots__ = ('fh',)
	min = max = None
	def __init__(self, *a, **kw):
		assert PY3, "Pickle columns require python 3, sorry"
		assert 'default' not in kw, "default not supported for Pickle, sorry"
		self.fh = _dsutil.WriteBytes(*a, **kw)
	def write(self, o):
		self.fh.write(pickle_dumps(o, 4))
	@property
	def count(self):
		return self.fh.count
	@property
	def compression(self):
		return self.fh.compression
	def close(self):
		self.fh.close()
	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		self.close()
_convfuncs['pickle'] = WritePickle

class ReadPickle(object):
	__slots__ = ('fh',)
	def __init__(self, *a, **kw):
		assert PY3, "Pickle columns require python 3, sorry"
		self.fh = _dsutil.ReadBytes(*a, **kw)
	def __next__(self):
		return pickle_loads(next(self.fh))
	next = __next__
	@property
	def count(self):
		return self.fh.count
	def close(self):
		self.fh.close()
	def __iter__(self):
		return self
	def __enter__(self):
		return self
	def __exit__(self, type, value, traceback):
		self.close()
_type2iter['pickle'] = ReadPickle


class _SanityError(Exception):
	pass

def _sanity_check_float_hashing():
	# Optimisers like to not actually convert to f32 precision when they can.
	# We need to actually convert for float32 and complex32 hashers to work.
	# _dsutil uses "volatile float" for this, but optimisers love to be broken,
	# so test for that.
	# (All formats hash as doubles, so that numbers that do match hash equal.)

	import struct
	exact       = 1.5               # exactly representable as an f32
	f64_inexact = 1.1               # not exactly representable as an f32
	f32_inexact = 1.100000023841858 # what 1.1f should round to.
	bin_exact = struct.pack('=d', exact)
	bin_f64_inexact = struct.pack('=d', f64_inexact)
	bin_f32_inexact = struct.pack('=d', f32_inexact)

	def check(typ, msg, want, *a):
		for ix, v in enumerate(a):
			if v != want:
				raise _SanityError("%s did not hash %s value correctly. (%d)" % (typ, msg, ix,))

	# Test that the float types (including number) hash floats the same,
	# and that float32 rounds as expected.
	h_f32 = _convfuncs['float32'].hash
	h_f64 = _convfuncs['float64'].hash
	h_num = _convfuncs['number'].hash
	hash_exact = _dsutil.siphash24(bin_exact)
	hash_f64_inexact = _dsutil.siphash24(bin_f64_inexact)
	hash_f32_inexact = _dsutil.siphash24(bin_f32_inexact)
	check('float types', 'exact', hash_exact, h_f32(exact), h_f64(exact), h_num(exact))
	check('float64 types', 'inexact', hash_f64_inexact, h_f64(f64_inexact), h_num(f64_inexact))
	check('float types', 'inexact', hash_f32_inexact, h_f32(f64_inexact), h_f32(f32_inexact), h_f64(f32_inexact), h_num(f32_inexact))

	# Same thing but for the complex types.
	h_c32 = _convfuncs['complex32'].hash
	h_c64 = _convfuncs['complex64'].hash
	e_e = complex(exact, exact)
	e_f64i = complex(exact, f64_inexact)
	e_f32i = complex(exact, f32_inexact)
	f64i_e = complex(f64_inexact, exact)
	f32i_e = complex(f32_inexact, exact)
	hash_e_e = _dsutil.siphash24(bin_exact + bin_exact)
	hash_e_f64i = _dsutil.siphash24(bin_exact + bin_f64_inexact)
	hash_e_f32i = _dsutil.siphash24(bin_exact + bin_f32_inexact)
	hash_f64i_e = _dsutil.siphash24(bin_f64_inexact + bin_exact)
	hash_f32i_e = _dsutil.siphash24(bin_f32_inexact + bin_exact)
	check('complex types', '(exact+exactj)', hash_e_e, h_c32(e_e), h_c64(e_e))
	check('complex64', '(exact+inexactj)', hash_e_f64i, h_c64(e_f64i))
	check('complex types', '(exact+inexactj)', hash_e_f32i, h_c32(e_f64i), h_c32(e_f32i), h_c64(e_f32i))
	check('complex64', '(inexact+exactj)', hash_f64i_e, h_c64(f64i_e))
	check('complex types', '(inexact+exactj)', hash_f32i_e, h_c32(f64i_e), h_c32(f32i_e), h_c64(f32i_e))

	# Test the same things, but with a value that looks like an int when
	# correctly rounded to float32. (The complex values use a 0 imaginary
	# component to trigger "like float" hashing.)
	intlike_v = 8765432.1 # rounds to 8765432.0 as an f32
	int_v     = int(intlike_v)
	bin_int = struct.pack('=Q', int_v)
	hash_int = _dsutil.siphash24(bin_int)
	check('float32/complex32', 'int-like float', hash_int, h_f32(intlike_v), h_c32(complex(intlike_v, 0)))
	check('float64/complex64', 'int-like float', hash_int, h_f64(float(int_v)), h_c64(complex(int_v, 0)))
	check('number', 'int-like float', hash_int, h_num(float(int_v)))
	# Should probably check that the int types get it right too.
	h_i32 = _convfuncs['int32'].hash
	h_i64 = _convfuncs['int64'].hash
	check('int types', 'normal', hash_int, h_i64(int_v), h_i32(int_v), h_num(int_v))

try:
	_sanity_check_float_hashing()
	_fail = None
except _SanityError as e:
	_fail = str(e)
if _fail:
	raise Exception("_dsutil module miscompiled: " + _fail)


from accelerator.compat import UTC
_dsutil._set_utctz(UTC)
del UTC
