# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
# Modifications copyright (c) 2019-2025 Carl Drougge                       #
# Modifications copyright (c) 2023-2024 Pablo Correa Gómez                 #
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

from collections import OrderedDict
from accelerator.compat import str_types

class DotDict(OrderedDict):
	"""Like an OrderedDict, but with d.foo as well as d['foo'].
	(Names beginning with _ will have to use d['_foo'] syntax.)
	The normal dict.f (get, items, ...) still return the functions.
	"""

	__slots__ = () # all the .stuff is actually items, not attributes

	# this is a workaround for older versions not inheriting from OrderedDict.
	# without this, unpickling of pickles that were not ordered will fail.
	# it throws the arguments away, as they will be passed to __init__ too
	# anyway. (Or set as items, in the pickle case.)
	def __new__(cls, other=(), **kw):
		obj = OrderedDict.__new__(cls)
		OrderedDict.__init__(obj)
		return obj

	def __getattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		try:
			return self[name]
		except KeyError:
			pass # raise new error outside this except clause
		raise AttributeError(name)

	def __setattr__(self, name, value):
		if name[0] == "_":
			raise AttributeError(name)
		self[name] = value

	def __delattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		del self[name]

class _ListTypePreserver(list):
	"""Base class to inherit from in list subclasses that want their custom type preserved when slicing."""

	__slots__ = ()

	def __getitem__(self, item):
		if isinstance(item, slice):
			return self.__class__(list.__getitem__(self, item))
		else:
			return list.__getitem__(self, item)

	def __add__(self, other):
		if not isinstance(other, list):
			return NotImplemented
		return self.__class__(list.__add__(self, other))

	def __radd__(self, other):
		if not isinstance(other, list):
			return NotImplemented
		return self.__class__(list.__add__(other, self))

	def __repr__(self):
		return '%s(%s)' % (self.__class__.__name__, list.__repr__(self))

class OptionEnumValue(str):

	__slots__ = ('_valid', '_prefixes')

	@staticmethod
	def _mktype(name, valid, prefixes):
		return type('OptionEnumValue' + name, (OptionEnumValue,), {'_valid': valid, '_prefixes': prefixes})

	# be picklable
	def __reduce__(self):
		return _OptionEnumValue_restore, (self.__class__.__name__[15:], str(self), self._valid, self._prefixes)

def _OptionEnumValue_restore(name, value, valid, prefixes):
	return OptionEnumValue._mktype(name, valid, prefixes)(value)

class OptionEnum(object):
	"""A little like Enum in python34, but string-like.
	(For JSONable method option enums.)

	>>> foo = OptionEnum('a b c*')
	>>> foo.a
	'a'
	>>> foo.a == 'a'
	True
	>>> foo.a == foo['a']
	True
	>>> isinstance(foo.a, OptionEnumValue)
	True
	>>> isinstance(foo['a'], OptionEnumValue)
	True
	>>> foo['cde'] == 'cde'
	True
	>>> foo['abc']
	Traceback (most recent call last):
	...
	KeyError: 'abc'

	Pass either foo (for a default of None) or one of the members
	as the value in options{}. You get a string back, which compares
	equal to the member of the same name.

	Set none_ok if you accept None as the value.

	If a value ends in * that matches all endings. You can only access
	these as foo['cde'] (for use in options{}).
	"""

	__slots__ = ('_values', '_valid', '_prefixes', '_sub')

	def __new__(cls, values, none_ok=False):
		if isinstance(values, str_types):
			values = values.replace(',', ' ').split()
		values = list(values)
		valid = set(values)
		prefixes = []
		for v in values:
			if v.endswith('*'):
				prefixes.append(v[:-1])
		if none_ok:
			valid.add(None)
		name = ''.join(v.title() for v in values)
		sub = OptionEnumValue._mktype(name, valid, prefixes)
		d = {}
		for value in values:
			d[value] = sub(value)
		d['_values'] = values
		d['_valid'] = valid
		d['_prefixes'] = prefixes
		d['_sub'] = sub
		return object.__new__(type('OptionEnum' + name, (cls,), d))
	def __getitem__(self, name):
		try:
			return getattr(self, name)
		except AttributeError:
			for cand_prefix in self._prefixes:
				if name.startswith(cand_prefix):
					return self._sub(name)
			raise KeyError(name)
	# be picklable
	def __reduce__(self):
		return OptionEnum, (self._values, None in self._valid)

class _OptionString(str):
	"""Marker value to specify in options{} for requiring a non-empty string.
	You can use plain OptionString, or you can use OptionString('example'),
	without making 'example' the default.
	"""

	__slots__ = ()

	def __call__(self, example):
		return _OptionString(example)
	def __repr__(self):
		if self:
			return 'OptionString(%r)' % (str(self),)
		else:
			return 'OptionString'
OptionString = _OptionString('')

class RequiredOption(object):
	"""Specify that this option is mandatory (that the caller must specify a value).
	None is accepted as a specified value if you pass none_ok=True.
	"""

	__slots__ = ('value', 'none_ok')

	def __init__(self, value, none_ok=False):
		self.value = value
		self.none_ok = none_ok

class OptionDefault(object):
	"""Default selection for complexly typed options.
	foo={'bar': OptionEnum(...)} is a mandatory option.
	foo=OptionDefault({'bar': OptionEnum(...)}) isn't.
	(Default None unless specified.)
	"""

	__slots__ = ('value', 'default')

	def __init__(self, value, *, default=None):
		self.value = value
		self.default = default
