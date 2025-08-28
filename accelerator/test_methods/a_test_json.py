# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2019-2024 Carl Drougge                                     #
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

description = r'''
Verify a few corner cases in the json functions in extras.
'''

from collections import OrderedDict
from itertools import permutations

from accelerator.extras import json_save, json_load, json_encode

def test(name, input, want_obj, want_bytes, **kw):
	json_save(input, name, **kw)
	with open(name, "rb") as fh:
		got_bytes_raw = fh.read()
		assert got_bytes_raw[-1:] == b"\n", name + " didn't even end with a newline"
		got_bytes_raw = got_bytes_raw[:-1]
	as_str = json_encode(input, as_str=True, **kw)
	as_bytes = json_encode(input, as_str=False, **kw)
	assert isinstance(as_str, str) and isinstance(as_bytes, bytes), f"json_encode returns the wrong types: {type(as_str)} {type(as_bytes)}"
	assert as_bytes == got_bytes_raw, "json_save doesn't save the same thing json_encode returns for " + name
	as_str = as_str.encode("utf-8")
	assert as_bytes == as_str, "json_encode doesn't return the same data for as_str=True and False"
	got_obj = json_load(name)
	assert want_obj == got_obj, f"{name} roundtrips wrong (wanted {want_obj!r}, got {got_obj!r})"
	with open(name, "rb") as fh:
		got_bytes_fuzzy = b"".join(line.strip() for line in fh)
	assert want_bytes == got_bytes_fuzzy, f"{name} wrong on disk (but decoded right)"

def synthesis():
	test(
		"simple.json",
		dict(a=1, b="2"),
		dict(a=1, b="2"),
		b'{"a": 1,"b": "2"}',
	)
	test(
		"True False None.json",
		[True, False, None],
		[True, False, None],
		b'[true,false,null]',
	)
	test(
		"list like types.json",
		dict(
			list=[1, 2, 3,],
			set={"foo"},
			tuple=("a", "b", "c",),
		),
		dict(
			list=[1, 2, 3,],
			set=["foo"],
			tuple=["a", "b", "c",],
		),
		b'{"list": [1,2,3],"set": ["foo"],"tuple": ["a","b","c"]}',
	)

	unicode_want = u"bl\xe4"
	test(
		"unicode.json",
		u"bl\xe4",
		unicode_want,
		b'"bl\\u00e4"',
	)

	# Verify that utf-8 encoding also works for reading.
	with open("utf-8.json", "wb") as fh:
		fh.write(b'"bl\xc3\xa4"')
	assert json_load("utf-8.json") == unicode_want

	# This not supposed to work on PY3.
	try:
		test(
			"string encoding.json",
			[u"\xe4", b"\xc3\xa4", [b"\xe4", {b"\xe4": b"\xc3\xa4",},],],
			[b"\xc3\xa4", b"\xc3\xa4", [b"\xc3\xa4", {b"\xc3\xa4": b"\xc3\xa4",},],],
			b'["\\u00e4","\\u00e4",["\\u00e4",{"\\u00e4": "\\u00e4"}]]',
		)
		assert False, "Bytes are not supposed to work in json_encode on PY3"
	except TypeError:
		pass

	# 720 permutations might be a bit much, but at least it's unlikely to
	# miss ordering problems.
	sorted_s = None
	for ix, pairs in enumerate(permutations(zip("abcdef", range(6)))):
		d = OrderedDict()
		s = "{"
		for k, v in pairs:
			d[k] = v
			s += f'"{k}": {v},'
		s = (s[:-1] + "}").encode("ascii")
		if not sorted_s:
			sorted_s = s
			sorted_d = d
		test(f"ordered{ix}.json", d, d, s, sort_keys=False)
		test(f"sorted{ix}.json", d, sorted_d, sorted_s, sort_keys=True)
