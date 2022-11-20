############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2022 Carl Drougge                       #
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
