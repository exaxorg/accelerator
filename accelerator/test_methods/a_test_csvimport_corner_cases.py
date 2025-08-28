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
Verify various corner cases in csvimport.
'''

from itertools import permutations
from collections import Counter

from accelerator import subjobs
from accelerator.dispatch import JobError
from accelerator.dataset import Dataset
from accelerator.compat import uni

def openx(filename):
	return open(filename, "xb")

def check_array(job, lines, filename, bad_lines=(), **options):
	d = {}
	d_bad = {}
	with openx(filename) as fh:
		for ix, data in enumerate(bad_lines, 1):
			d_bad[ix] = str(ix).encode("ascii") + b"," + data + b"," + data
			fh.write(d_bad[ix] + b"\n")
		for ix, data in enumerate(lines, len(bad_lines) + 1):
			if isinstance(data, tuple):
				data, d[ix] = data
			else:
				d[ix] = data
			ix = str(ix).encode("ascii")
			fh.write(ix + b"," + data + b"," + data + b"\n")
	options.update(
		filename=job.filename(filename),
		allow_bad=bool(bad_lines),
		labels=["ix", "0", "1"],
	)
	verify_ds(options, d, d_bad, {}, filename)

def verify_ds(options, d, d_bad, d_skipped, filename, d_columns=None):
	jid = subjobs.build("csvimport", options=options)
	ds = Dataset(jid)
	if not d_columns:
		d_columns = ["ix", "0", "1"]
	expected_columns = set(d_columns)
	if options.get("lineno_label"):
		expected_columns.add(options["lineno_label"])
		lineno_want = {ix: int(ix) for ix in ds.iterate(None, d_columns[0])}
	assert set(ds.columns) == expected_columns
	# Order varies depending on slice count, so we use a dict {ix: data}
	for ix, a, b in ds.iterate(None, d_columns):
		try:
			ix = int(ix)
		except ValueError:
			# We have a few non-numeric ones
			pass
		assert ix in d, f"Bad index {ix!r} in {filename!r} ({jid})"
		assert a == b == d[ix], f"Wrong data for line {ix!r} in {filename!r} ({jid})"
		del d[ix]
	assert not d, f"Not all lines returned from {filename!r} ({jid}), {set(d.keys())!r} missing"
	if options.get("allow_bad"):
		bad_ds = Dataset(jid, "bad")
		for ix, data in bad_ds.iterate(None, ["lineno", "data"]):
			assert ix in d_bad, f"Bad bad_lineno {ix} in {filename!r} ({jid}/bad) {data!r}"
			assert data == d_bad[ix], f"Wrong saved bad line {ix} in {filename!r} ({jid}/bad).\nWanted {d_bad[ix]!r}.\nGot    {data!r}."
			del d_bad[ix]
		verify_minmax(bad_ds, "lineno")
	assert not d_bad, f"Not all bad lines returned from {filename!r} ({jid}), {set(d_bad.keys())!r} missing"

	if options.get("comment") or options.get("skip_lines"):
		skipped_ds = Dataset(jid, "skipped")
		for ix, data in skipped_ds.iterate(None, ["lineno", "data"]):
			assert ix in d_skipped, f"Bad skipped_lineno {ix} in {filename!r} ({jid}/skipped) {data!r}"
			assert data == d_skipped[ix], f"Wrong saved skipped line {ix} in {filename!r} ({jid}/skipped).\nWanted {d_skipped[ix]!r}.\nGot    {data!r}."
			del d_skipped[ix]
		verify_minmax(skipped_ds, "lineno")
	assert not d_skipped, f"Not all bad lines returned from {filename!r} ({jid}), {set(d_skipped.keys())!r} missing"

	if options.get("lineno_label"):
		lineno_got = dict(ds.iterate(None, [d_columns[0], options.get("lineno_label")]))
		assert lineno_got == lineno_want, f"{lineno_got!r} != {lineno_want!r}"
		verify_minmax(ds, options["lineno_label"])

def verify_minmax(ds, colname):
	data = list(ds.iterate(None, colname))
	minmax_want = (min(data) , max(data),) if data else (None, None,)
	col = ds.columns[colname]
	minmax_got = (col.min, col.max,)
	assert minmax_got == minmax_want, f"{ds}: {minmax_got!r} != {minmax_want!r}"

def require_failure(name, options):
	try:
		subjobs.build("csvimport", options=options)
	except JobError:
		return
	raise Exception(f"File with {name} was imported without error.")

def check_bad_file(job, name, data):
	filename = name + ".txt"
	with openx(filename) as fh:
		fh.write(data)
	options=dict(
		filename=job.filename(filename),
	)
	require_failure(name, options)

def bytechr(i):
	return chr(i).encode("iso-8859-1")

def byteline(start, stop, nl, q):
	s = b''.join(bytechr(i) for i in range(start, stop) if i != nl)
	if q is None:
		return s
	else:
		return s.lstrip(bytechr(q))

# Check that no separator is fine with various newlines, and with various quotes
def check_no_separator(job):
	def write(data):
		fh.write(data + nl_b)
		wrote_c[data] += 1
		if q_b:
			data = q_b + data + q_b
			fh.write(q_b + data.replace(q_b, q_b + q_b) + q_b + nl_b)
			wrote_c[data] += 1
	for nl in (10, 0, 255):
		for q in (None, 0, 34, 13, 10, 228):
			if nl == q:
				continue
			filename = f"no separator.{nl!r}.{q!r}.txt"
			nl_b = bytechr(nl)
			q_b = bytechr(q) if q else b''
			wrote_c = Counter()
			with openx(filename) as fh:
				for splitpoint in range(256):
					write(byteline(0, splitpoint, nl, q))
					write(byteline(splitpoint, 256, nl, q))
			try:
				jid = subjobs.build("csvimport", options=dict(
					filename=job.filename(filename),
					quotes=q_b.decode("iso-8859-1"),
					newline=nl_b.decode("iso-8859-1"),
					separator='',
					labels=["data"],
				))
			except JobError:
				raise Exception(f"Importing {filename!r} failed")
			got_c = Counter(Dataset(jid).iterate(None, "data"))
			assert got_c == wrote_c, f"Importing {filename!r} ({jid}) gave wrong contents"

def check_good_file(job, name, data, d, d_bad={}, d_skipped={}, d_columns=None, **options):
	filename = name + ".txt"
	with openx(filename) as fh:
		fh.write(data)
	options.update(
		filename=job.filename(filename),
	)
	verify_ds(options, d, d_bad, d_skipped, filename, d_columns)

def synthesis(job):
	check_good_file(job, "mixed line endings", b"ix,0,1\r\n1,a,a\n2,b,b\r\n3,c,c", {1: b"a", 2: b"b", 3: b"c"})
	check_good_file(job, "ignored quotes", b"ix,0,1\n1,'a,'a\n2,'b','b'\n3,\"c\",\"c\"\n4,d',d'\n", {1: b"'a", 2: b"'b'", 3: b'"c"', 4: b"d'"})
	check_good_file(job, "ignored quotes and extra fields", b"ix,0,1\n1,\"a,\"a\n2,'b,c',d\n3,d\",d\"\n", {1: b'"a', 3: b'd"'}, allow_bad=True, d_bad={3: b"2,'b,c',d"})
	check_good_file(job, "spaces and quotes", b"ix,0,1\none,a,a\ntwo, b, b\n three,c,c\n4,\"d\"\"\",d\"\n5, 'e',\" 'e'\"\n", {b"one": b"a", b"two": b" b", b" three": b"c", 4: b'd"', 5: b" 'e'"}, quotes=True)
	check_good_file(job, "empty fields", b"ix,0,1\n1,,''\n2,,\n3,'',\n4,\"\",", {1: b"", 2: b"", 3: b"", 4: b""}, quotes=True)
	check_good_file(job, "renamed fields", b"0,1,2\n0,foo,foo", {0: b"foo"}, rename={"0": "ix", "2": "0"})
	check_good_file(job, "discarded field", b"ix,0,no,1\n0,yes,no,yes\n1,a,'foo,bar',a", {0: b"yes", 1: b"a"}, quotes=True, discard={"no"})
	check_good_file(job, "bad quotes", b"""ix,0,1\n1,a,a\n2,"b,"b\n\n3,'c'c','c'c'\n4,"d",'d'\n""", {1: b"a", 4: b"d"}, quotes=True, allow_bad=True, d_bad={3: b'2,"b,"b', 4: b"", 5: b"3,'c'c','c'c'"})
	check_good_file(job, "comments", b"""# blah\nix,0,1\n1,a,a\n2,b,b\n#3,c,c\n4,#d,#d\n""", {1: b"a", 2: b"b", 4: b"#d"}, comment="#", d_skipped={1: b"# blah", 5: b"#3,c,c"})
	check_good_file(job, "not comments", b"""ix,0,1\n1,a,a\n2,b,b\n#3,c,c\n4,#d,#d\n""", {1: b"a", 2: b"b", b"#3": b"c", 4: b"#d"})
	check_good_file(job, "a little of everything", b""";not,1,labels\na,2,1\n;a,3,;a\n";b",4,;b\n'c,5,c'\r\n d,6,' d'\ne,7,e,\n,8,""", {4: b";b", 6: b" d", 8: b""}, allow_bad=True, rename={"a": "0", "2": "ix"}, quotes=True, comment=";", d_bad={5: b"'c,5,c'", 7: b"e,7,e,"}, d_skipped={1: b";not,1,labels", 3: b";a,3,;a"})
	check_good_file(job, "skipped lines", b"""just some text\n\nix,0,1\n1,a,a\n2,b,b""", {1: b"a", 2: b"b"}, skip_lines=2, d_skipped={1: b"just some text", 2: b""})
	check_good_file(job, "skipped and bad lines", b"""not data here\nnor here\nix,0,1\n1,a,a\n2,b\n3,c,c""", {1: b"a", 3: b"c"}, skip_lines=2, allow_bad=True, d_bad={5: b"2,b"}, d_skipped={1: b"not data here", 2: b"nor here"})
	check_good_file(job, "override labels", b"""a,b,c\n0,foo,foo""", {0: b"foo"}, labels=["ix", "0", "1"], label_lines=1)
	check_good_file(job, "only labels", b"""ix,0,1""", {})
	check_good_file(job, "empty file", b"", {}, labels=["ix", "0", "1"])
	check_good_file(job, "empty file with lineno+skip+bad", b"", {}, labels=["ix", "0", "1"], lineno_label="num", comment="#", allow_bad=True)
	check_good_file(job, "lineno with bad lines", b"ix,0,1\n2,a,a\n3,b\nc\n5,d,d\n6,e,e\n7\n8,g,g\n\n", {2: b"a", 5: b"d", 6: b"e", 8: b"g"}, d_bad={3: b"3,b", 4: b"c", 7: b"7", 9: b""}, allow_bad=True, lineno_label="num")
	check_good_file(job, "lineno with skipped lines", b"a\nb\n3,c,c\n4,d,d", {3: b"c", 4: b"d"}, lineno_label="l", labels=["ix", "0", "1"], skip_lines=2, d_skipped={1: b"a", 2: b"b"})
	check_good_file(job, "lineno with comment lines", b"ix,0,1\n2,a,a\n3,b,b\n#4,c,c\n5,d,d", {2: b"a", 3: b"b", 5: b"d"}, lineno_label="another name", comment="#", d_skipped={4: b"#4,c,c"})
	check_good_file(job, "strip labels", b" ix , 0 , 1 \n1,a,a\n2,b ,b ", {1: b"a", 2: b"b "}, strip_labels=True)
	check_good_file(job, "allow extra empty", b"ix,0,1,,,,\n1,a,a\n2,b,b,,\n3,,,", {1: b"a", 2: b"b", 3: b""}, allow_extra_empty=True)
	check_good_file(job, "allow extra empty quoted", b"ix,_0_,1,,,__,\n1,a,a\n_2_,b,b,__,\n3,c,c,__", {1: b"a", 2: b"b", 3: b"c"}, allow_extra_empty=True, quotes='_')
	check_good_file(job, "allow extra empty quoted bad", b"ix,0,1,,,'',\"\"\n1,a,a\n'2',b,b,'',\n3,c,c,\"\"\n4,d,d,'\"\n5,'',\"\",'", {1: b"a", 2: b"b", 3: b"c"}, allow_extra_empty=True, quotes=True, allow_bad=True, d_bad={5: b"4,d,d,'\"", 6: b"5,'',\"\",'"})
	check_good_file(job, "skip empty lines", b"\nix,0,1\n\n\n1,a,a\n", {1: b"a"}, skip_empty_lines=True)
	check_good_file(job, "skip empty lines and comments", b"\r\nix,0,1\n\n\n5,a,a\n#6,b,b\n7,c,c\n#", {5: b"a", 7: b"c"}, skip_empty_lines=True, comment="#", d_skipped={1: b"", 3: b"", 4: b"", 6: b"#6,b,b", 8: b"#"}, lineno_label="line")
	check_good_file(job, "skip empty lines and bad", b"\n\nix,0,1\n4,a,a\n \n6,b,b\n\r\n", {4: b"a", 6: b"b"}, skip_empty_lines=True, comment="#", d_skipped={1: b"", 2: b"", 7: b""}, d_bad={5: b" "}, allow_bad=True, lineno_label="line")
	check_good_file(job, "two label lines", b"a,b,c\nx,y,z\n3,a,a\n4,b,b\n", {3: b"a", 4: b"b"}, label_lines=2, lineno_label="line", d_columns=["a x", "b y", "c z"])
	check_good_file(job, "three label lines with comments between", b"a,b,c\n#2,2,2\nfoo,bar,blutti\n#4,4,4\nd,e,f\n6,a,a\n#7,7,7\n8,b,b\n", {6: b"a", 8: b"b"}, label_lines=3, lineno_label="line", comment="#", d_skipped={2: b"#2,2,2", 4: b"#4,4,4", 7: b"#7,7,7"}, d_columns=["a foo d", "b bar e", "c blutti f"])

	bad_lines = [
		b"bad,bad",
		b",",
		b"bad,",
		b",bad",
		b"',',",
		b"'lo there broken line",
		b"'nope\"",
		b"'bad quotes''",
		b'"bad quote " inside"',
		b'"more ""bad"" quotes """ inside"',
	]
	good_lines = [
		b"\x00",
		(b"'good, good'", b"good, good"),
		(b'"also good, yeah!"', b"also good, yeah!"),
		(b"'single quote''s inside'", b"single quote's inside"),
		(b"'single quote at end: '''", b"single quote at end: '"),
		(b'"""double quotes around"""', b'"double quotes around"'),
		(b'"double quote at end: """', b'double quote at end: "'),
		(b'" I\'m special "', b" I'm special "),
		b"I'm not",
		b" unquoted but with spaces around ",
		(b"','", b","),
		b"\x00\xff",
		b"\xff\x00\x08\x00",
		(b"'lot''s of ''quotes'' around here: '''''''' '", b"lot's of 'quotes' around here: '''' ")
	]
	check_array(job, good_lines, "strange values.txt", bad_lines, quotes=True)
	# The lines will be 2 * length + 3 bytes (plus lf)
	long_lines = [b"a" * length for length in (64 * 1024 - 2, 999, 999, 1999, 3000, 65000, 8 * 1024 * 1024 - 99)]
	check_array(job, long_lines, "long lines.txt")
	check_bad_file(job, "extra field", b"foo,bar\nwith,extra,field\nok,here\n")
	check_bad_file(job, "missing field", b"foo,bar\nmissing\nok,here\n")
	check_bad_file(job, "no valid lines", b"foo\nc,\n")

	# let's also check some really idiotic combinations
	for combo in permutations([0, 10, 13, 255], 3):
		name = "idiotic.%d.%d.%d" % combo
		sep, newline, comment = (uni(chr(x)) for x in combo)
		data = [
			comment,
			sep.join(["ix", "0", "1"]),
			sep.join(["0", "a", "a"]),
			sep.join([comment + "1", "b", "b"]),
			sep.join(["2", "", ""]),
			comment + sep,
			sep.join(["", "", ""]),
			sep.join(["4", ",", ","]),
			comment,
		]
		check_good_file(
			job,
			name,
			data=newline.join(data).encode("iso-8859-1"),
			d={0: b"a", 2: b"", b"": b"", 4: b","},
			d_skipped={k: data[k - 1].encode("iso-8859-1") for k in (1, 4, 6, 9)},
			separator=sep,
			newline=newline,
			comment=comment,
		)

	check_no_separator(job)
