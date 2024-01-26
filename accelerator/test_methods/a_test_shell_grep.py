# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2021-2024 Carl Drougge                                     #
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
Test the "ax grep" shell command. This isn't testing complex regexes,
but rather the various output options and data types.
'''

options = dict(
	command_prefix=['ax', '--config', '/some/path/here'],
)

from subprocess import check_output, Popen, CalledProcessError
import datetime
import os
import errno
import json
from itertools import cycle
from functools import partial

from accelerator.compat import PY2, PY3, izip_longest
from accelerator.dsutil import _convfuncs

def grep_text(args, want, sep='\t', encoding='utf-8', unordered=False, check_output=check_output):
	if not unordered:
		args = ['--ordered'] + args
	cmd = options.command_prefix + ['grep'] + args
	res = check_output(cmd)
	res = res.split(b'\n')[:-1]
	if len(want) != len(res):
		raise Exception('%r gave %d lines, wanted %d.\n%r' % (cmd, len(res), len(want), res,))
	if encoding:
		res = [el.decode(encoding, 'replace') for el in res]
	typ = type(sep)
	want = [sep.join(typ(el) for el in l) for l in want]
	for lineno, (want, got) in enumerate(zip(want, res), 1):
		if want != got:
			raise Exception('%r gave wrong result on line %d:\nWant: %r\nGot:  %r' % (cmd, lineno, want, got,))

# like subprocess.check_output except stdout is a pty
def check_output_pty(cmd):
	a, b = os.openpty()
	Popen(cmd, stdout=a)
	os.close(a)
	res = []
	data = None
	while not res or data:
		try:
			data = os.read(b, 1024)
		except OSError as e:
			# On Linux a pty will return
			# OSError: [Errno 5] Input/output error
			# instead of b'' for EOF. Don't know why.
			# Let's try to be a little restrictive in what we catch.
			if e.errno != errno.EIO:
				raise
			data = b''
		res.append(data)
	os.close(b)
	return b''.join(res)

def grep_json(args, want):
	cmd = options.command_prefix + ['grep', '--ordered', '--format=json'] + args
	res = check_output(cmd)
	res = res.decode('utf-8', 'surrogatepass')
	res = res.split('\n')[:-1]
	if len(want) != len(res):
		raise Exception('%r gave %d lines, wanted %d.\n%r' % (cmd, len(res), len(want), res,))
	for lineno, (want, got) in enumerate(zip(want, res), 1):
		try:
			got = json.loads(got)
		except Exception as e:
			raise Exception('%r made bad json %r on line %d: %s' % (cmd, got, lineno, e,))
		if want != got:
			raise Exception('%r gave wrong result on line %d:\nWant: %r\nGot:  %r' % (cmd, lineno, want, got,))

if PY2:
	def mk_bytes(low, high):
		return b''.join(chr(c) for c in range(low, high))
else:
	def mk_bytes(low, high):
		return bytes(range(low, high))

# looks like 'bar' when matching but 'foo' when printing
# intended to catch if objects are evaluated too many times
class TricksyObject:
	def __init__(self):
		self.counter = 0
	def __str__(self):
		self.counter += 1
		if self.counter == 1:
			return 'bar'
		elif self.counter == 2:
			return 'foo'
		else:
			return 'oops'

def synthesis(job, slices):
	used_types = set()
	def mk_ds(name, types, *lines, **kw):
		dw = job.datasetwriter(name=name, **kw)
		for t in types:
			if isinstance(t, tuple):
				t, name = t
			else:
				name = t
			dw.add(name, t)
			used_types.add(t)
		for sliceno, line in izip_longest(range(slices), lines):
			if sliceno is not None:
				dw.set_slice(sliceno)
			if line:
				dw.write(*line)
		return dw.finish()
	HDR_HI = '\x1b[94;1m'
	HDR_HI_POST = '\x1b[22;39m'
	SEP_HI = '\x1b[36;4m'
	SEP_HI_POST = '\x1b[24;39m'
	COMMA_HI = SEP_HI + ',' + SEP_HI_POST
	TAB_HI = SEP_HI + '\t' + SEP_HI_POST
	os.unsetenv('NO_COLOR')
	os.unsetenv('CLICOLOR')
	os.unsetenv('CLICOLOR_FORCE')
	os.putenv('XDG_CONFIG_HOME', job.path) # make sure we can't be messed up by config
	os.putenv('ACCELERATOR_IGNORE_ETC', 'Y') # make sure we can't be messed up by global config

	def frame(framevalues, *a):
		framevalues = cycle(framevalues)
		return [next(framevalues) + el + next(framevalues) for el in a]

	hdrframe = partial(frame, [HDR_HI, HDR_HI_POST])
	sepframe = partial(frame,  ['', '', SEP_HI, SEP_HI_POST])
	hdrsepframe = partial(frame,  [HDR_HI, HDR_HI_POST, SEP_HI, SEP_HI_POST])

	# start with testing basic output, chaining, column selection and headers.
	a = mk_ds('a', ['int32', 'int64'], [100, 200], [101, 201])
	b = mk_ds('b', ['int32'], [1000], [1001], previous=a)
	c = mk_ds('c', ['float64', 'int32'], [1.42, 3], previous=b)
	grep_text(['', a], [[100, 200], [101, 201]])
	grep_json(['', a], [{'int32': 100, 'int64': 200}, {'int32': 101, 'int64': 201}])
	grep_text(['-S', '', a], [[0, 100, 200], [1, 101, 201]])
	grep_text(['', b + '~'], [[100, 200], [101, 201]]) # verify ds parsing happens
	grep_json(['-S', '-L', '-D', '-c', '', b], [
		{'dataset': a, 'sliceno': 0, 'lineno': 0, 'data': {'int32': 100, 'int64': 200}},
		{'dataset': a, 'sliceno': 1, 'lineno': 0, 'data': {'int32': 101, 'int64': 201}},
		{'dataset': b, 'sliceno': 0, 'lineno': 0, 'data': {'int32': 1000}},
		{'dataset': b, 'sliceno': 1, 'lineno': 0, 'data': {'int32': 1001}},
	])
	grep_text(['-t', '2', '', a], [[100, '"200"'], [101, '"201"']], sep='2') # stupid separator leads to escaping
	grep_text(['-t', '2', '-f', 'raw', '', a], [[100, 200], [101, 201]], sep='2') # but not in the raw format

	# missing columns
	grep_text(['-M', '-c', '', c, 'int32'], [[100], [101], [1000], [1001], [3]]) # not anything missing
	grep_text(['-M', '-c', '-g', 'int64', '', c], [[100, 200], [101, 201]])
	grep_text(['-M', '-c', '-H', '', c, 'int32', 'float64'], [['int32'], [100], [101], [1000], [1001], ['int32', 'float64'], [3, 1.42]])
	grep_text(['-M', '-c', '-H', '', c, 'non-existant'], [])
	grep_text(['-M', '-c', '-g', 'float64', '-g', 'int64', '-H', '', c], [['int32', 'int64'], [100, 200], [101, 201], ['float64', 'int32'], [1.42, 3]])
	grep_text(['-M', '-c', '-H', '', c, 'int64', 'float64'], [['int64'], [200], [201], ['float64'], [1.42]])

	# context
	ctx = job.datasetwriter(name='ctx', columns={'a': 'int32'}, allow_missing_slices=True)
	ctx.set_slice(0)
	for v in range(100):
		if v == 32:
			ctx.set_slice(1)
		ctx.write(v)
	ctx = ctx.finish()
	ctx2 = job.datasetwriter(name='ctx2', columns={'a': 'ascii', 'b': 'ascii'}, allow_missing_slices=True, previous=ctx)
	ctx2.set_slice(1)
	for v in range(26):
		ctx2.write(chr(65 + v), chr(97 + v))
	ctx2 = ctx2.finish()
	# only one before 33 because of slicing, only one after 98 because the ds ends (even though ctx2 continues in slice 1)
	grep_text(['-c', '-A', '3', '-B', '2', '(33|98|Z)', ctx2], [[32], [33], [34], [35], [36], [96], [97], [98], [99], ['X', 'x'], ['Y', 'y'], ['Z', 'z']])

	# --invert-match
	grep_text(['-v', '[a-mO-Y]', ctx2], [[ch.upper(), ch] for ch in 'nz'])
	# ... with --context
	grep_text(['-C', '1', '-v', '[a-mO-Y]', ctx2], [[ch.upper(), ch] for ch in 'mnoyz'])

	# several (partially overlapping) patterns
	grep_text(['-e', '[moyz]', '-e', '[mNO]', ctx2], [[ch.upper(), ch] for ch in 'mnoyz'])
	# ... with --invert-match
	grep_text(['-v', '-e', '[a-ls-x]', '-e', '[p-v]', '-e', 'z', ctx2], [[ch.upper(), ch] for ch in 'mnoy'])
	# ... and --context
	grep_text(['-C', '1', '-v', '-e', '[a-ns-x]', '-e', '[p-v]', '-e', 'y', ctx2], [[ch.upper(), ch] for ch in 'nopyz'])

	# several datasets and columns (testing that unqualified and qualified works the same)
	grep_text(['-M', '01', a, b, 'int32', 'int64'], [[101, 201], [1001]])
	grep_text(['-M', '-d', a, '01', b, 'int32', 'int64'], [[101, 201], [1001]])
	grep_text(['-M', a, b, 'int32', 'int64', '-e', '01'], [[101, 201], [1001]])
	grep_text(['-M', '-d', a, '--column', 'int32', '-n', 'int64', '--regexp', '01', '--dataset', b], [[101, 201], [1001]])
	# + several patterns
	grep_text(['-M', '-e', '000', a, b, 'int32', 'int64', '-e', '01'], [[101, 201], [1000], [1001]])

	# try some colour
	grep_text(['--colour', '-t', ',', '-D', '-S', '-H', '', a], [hdrframe('[DATASET]', '[SLICE]', 'int32', 'int64'), [a, 0, 100, 200], [a, 1, 101, 201]], sep=COMMA_HI)
	os.putenv('CLICOLOR_FORCE', '1')
	grep_text(['-t', ',', '-L', '-S', '-H', '', a], [hdrframe('[SLICE]', '[LINE]', 'int32', 'int64'), [0, 0, 100, 200], [1, 0, 101, 201]], sep=COMMA_HI)
	grep_text(['-t', ',', '-L', '-S', '-H', '--colour=never', '', a], [['[SLICE]', '[LINE]', 'int32', 'int64'], [0, 0, 100, 200], [1, 0, 101, 201]], sep=',')
	grep_text(['-t', ',', '-D', '-H', '', b], [hdrframe('[DATASET]', 'int32'), [b, 1000], [b, 1001]], sep=COMMA_HI)
	grep_text(['-t', ',', '-C', '1', 'F', ctx2], [['E', 'e'], ['\x1b[31mF\x1b[39m', 'f'], ['G', 'g']], sep=COMMA_HI)
	grep_text(['-t', ',', '-D', '-H', '-c', '', b], [hdrframe('[DATASET]', 'int32', 'int64'), [a, 100, 200], [a, 101, 201], hdrframe('[DATASET]', 'int32'), [b, 1000], [b, 1001]], sep=COMMA_HI)
	grep_json(['-s', '0', '', a], [{'int32': 100, 'int64': 200}]) # no colour in json
	grep_text(['-s', '0', '', b, a], [['1000'], ['100', '200']], sep=TAB_HI)
	grep_text(['--color=never', '0', b], [[1000], [1001]])
	os.unsetenv('CLICOLOR_FORCE')
	os.putenv('NO_COLOR', '')
	grep_text(['--colour', 'always', '0', b], [['1\x1b[31m000\x1b[39m'], ['1\x1b[31m00\x1b[39m1']], sep=TAB_HI)
	os.unsetenv('NO_COLOR')

	# test the tab-replacing separator handling
	grep_text(['--color=always', '--tab-length=8', '-H', '-S', '^1', a, c], [
			hdrsepframe('[SLICE]', ' ', 'int32', '   ', 'int64'),
			sepframe('0', '       ', '\x1b[31m1\x1b[39m00', '     ', '200'),
			sepframe('1', '       ', '\x1b[31m1\x1b[39m01', '     ', '201'),
			hdrsepframe('[SLICE]', ' ', 'float64', ' ', 'int32'),
			sepframe('0', '       ', '\x1b[31m1\x1b[39m.42', '    ', '3'),
		], sep='',
	)
	# different length
	grep_text(['--color=always', '--tab-length=3', '-H', '-S', '^1', a, c], [
			hdrsepframe('[SLICE]', '  ', 'int32', ' ', 'int64'),
			sepframe('0', '  ', '\x1b[31m1\x1b[39m00', '   ', '200'),
			sepframe('1', '  ', '\x1b[31m1\x1b[39m01', '   ', '201'),
			hdrsepframe('[SLICE]', '  ', 'float64', '  ', 'int32'),
			sepframe('0', '  ', '\x1b[31m1\x1b[39m.42', '  ', '3'),
		], sep='',
	)
	# with a PTY, to see that this defaults to colour and smart expanded tabs
	grep_text(['-S', '^1', c, 'int32', 'float64'], [
			sepframe('0', '               ', '3', '               ', '\x1b[31m1\x1b[39m.42'),
		], sep='', check_output=check_output_pty,
	)

	if PY3: # no pickle type on PY2
		pickle = mk_ds('pickle', ['pickle'], [TricksyObject()], [''], [{'foo'}])
		grep_text(['', pickle], [['foo'], [''], ["{'foo'}"]])
		grep_text(['.', pickle], [['foo'], ["{'foo'}"]])
		grep_text(['bar', pickle], [['foo']])
		# using -g with the same columns as output is a NOP
		grep_text(['-g', 'pickle', 'bar', pickle], [['foo']])
		# but using it with a different set of columns is not
		pickle2 = mk_ds('pickle2', ['ascii'], ['a'], ['b'], ['c'], parent=pickle)
		grep_text(['-g', 'pickle', 'bar', pickle2], [['a', 'bar']])
		# order doesn't matter for equality, so here we're back to double evaluation.
		grep_text(['-g', 'pickle', '-g', 'ascii', 'bar', pickle2], [['a', 'foo']])
		bytespickle = mk_ds('bytespickle', ['pickle'], [b'\xf0'], [b'\t'])
		# pickles are str()d, not special cased like bytes columns
		grep_text(['-f', 'raw', 'xf0', bytespickle], [["b'\\xf0'"]])
		grep_json(['', bytespickle], [{'pickle': "b'\\xf0'"}, {'pickle': "b'\\t'"}])

	# --only-matching, both the part (default) and columns (with -l) in both csv and json
	grep_text(['-o', '-c', '1', b], [['1', ''], ['11', '1'], ['1'], ['11']])
	grep_text(['-o', '-l', '-c', '1', b], [['100', ''], ['101', '201'], ['1000'], ['1001']])
	grep_json(['-o', '-c', '1', b], [{'int32': '1', 'int64': ''}, {'int32': '11', 'int64': '1'}, {'int32': '1'}, {'int32': '11'}])
	grep_json(['-o', '-l', '-c', '1', b], [{'int32': 100}, {'int32': 101, 'int64': 201}, {'int32': 1000}, {'int32': 1001}])

	# check all possible byte values in all output formats
	allbytes = mk_ds('allbytes', ['ascii', 'bytes'],
		['control chars', mk_bytes(0, 32)],
		['printable', mk_bytes(32, 128)],
		['not ascii', mk_bytes(128, 256)],
	)
	if PY2:
		encoded_not_ascii = raw_not_ascii = '\ufffd'.encode('utf-8') * 128
	else:
		raw_not_ascii = mk_bytes(128, 256)
		encoded_not_ascii = raw_not_ascii.decode('utf-8', 'surrogateescape').encode('utf-8', 'surrogatepass')
	grep_text(
		['--format=raw', '', allbytes],
		[
			[b'control chars', mk_bytes(0, 10)],
			[mk_bytes(11, 32)], # we end up with an extra line because the control chars have a newline
			[b'printable', mk_bytes(32, 128)],
			[b'not ascii', raw_not_ascii],
		],
		encoding=None,
		sep=b'\t',
	)
	grep_text(
		['', allbytes],
		[
			[b'control chars', b'"' + mk_bytes(0, 10) + b'\\n\x0b\x0c\\r' + mk_bytes(14, 32) + b'"'],
			[b'printable', mk_bytes(32, 128)],
			[b'not ascii', encoded_not_ascii],
		],
		encoding=None,
		sep=b'\t',
	)
	grep_json(['', allbytes], [
		{'ascii': 'control chars', 'bytes': mk_bytes(0, 32).decode('utf-8', 'surrogateescape' if PY3 else 'replace')},
		{'ascii': 'printable', 'bytes': mk_bytes(32, 128).decode('utf-8', 'surrogateescape' if PY3 else 'replace')},
		{'ascii': 'not ascii', 'bytes': mk_bytes(128, 256).decode('utf-8', 'surrogateescape' if PY3 else 'replace')},
	])

	# header printing should happen between datasets only when columns change,
	# and must wait for all slices for each switch.
	# to make this predictable without -O, only one slice is used per column set.
	columns = [
		('int32', 'int64',),
		('int64', 'int32',), # not actually a change
		('int32', 'number',),
		('int32',),
		('int32',),
		('int32',),
		('int64',),
	]
	values_every_time = range(10)
	previous = None
	previous_cols = []
	slice = 0
	header_test = []
	for ds_ix, cols in enumerate(columns):
		dw = job.datasetwriter(name='header test %d' % (ds_ix,), previous=previous, allow_missing_slices=True)
		for col in cols:
			dw.add(col, col)
		if sorted(cols) != previous_cols:
			# columns changed, so switch slice to make failure more likely
			previous_cols = sorted(cols)
			slice = (slice + 1) % slices
		dw.set_slice(slice)
		for value in values_every_time:
			args = (value,) * len(cols)
			dw.write(*args)
		previous = dw.finish()
		header_test.append((previous, slice))
	grep_text(
		['-H', '-c', '', previous],
			[['int32', 'int64']] +
			[(v, v,) for v in values_every_time] +
			[(v, v,) for v in values_every_time] +
			[['int32', 'number']] +
			[(v, v,) for v in values_every_time] +
			[['int32']] +
			[(v,) for v in values_every_time] +
			[(v,) for v in values_every_time] +
			[(v,) for v in values_every_time] +
			[['int64']] +
			[(v,) for v in values_every_time],
		unordered=True,
	)
	# test that only a single slice with header changes isn't a problem.
	grep_text(
		['-H', '-D', '-c', '-s', '2', '7', header_test[3][0]], [
			['[DATASET]', 'int32', 'int64'],
			['[DATASET]', 'int32', 'number'],
			[header_test[2][0], '7', '7'], # the other datasets don't have anything in slice 2
			['[DATASET]', 'int32'],
		],
	)
	# test that repeating datasets doesn't repeat header outputs
	grep_text(
		['-H', '-D', '-s', '2', '7', header_test[2][0], header_test[2][0], header_test[3][0], header_test[3][0]], [
			['[DATASET]', 'int32', 'number'],
			[header_test[2][0], '7', '7'],
			[header_test[2][0], '7', '7'],
			['[DATASET]', 'int32'],
		],
	)

	# test --chain-length and --stop-ds
	grep_text(
		['-H', '-D', '-c', '--chain-length=3', '7', header_test[3][0]], [
			['[DATASET]', 'int32', 'int64'],
			[header_test[1][0], '7', '7'],
			['[DATASET]', 'int32', 'number'],
			[header_test[2][0], '7', '7'],
			['[DATASET]', 'int32'],
			[header_test[3][0], '7'],
		],
	)
	# --stop-ds happens first and wins
	grep_text(
		['-H', '-D', '-c', '--chain-length=3', '--stop-ds', header_test[1][0], '7', header_test[3][0]], [
			['[DATASET]', 'int32', 'number'],
			[header_test[2][0], '7', '7'],
			['[DATASET]', 'int32'],
			[header_test[3][0], '7'],
		],
	)
	# ...with several values
	grep_text(
		['-H', '-D', '-c', '--chain-length=3', '--chain-length=2', '7', header_test[3][0], header_test[4][0]], [
			['[DATASET]', 'int32', 'int64'],
			[header_test[1][0], '7', '7'],
			['[DATASET]', 'int32', 'number'],
			[header_test[2][0], '7', '7'],
			['[DATASET]', 'int32'],
			[header_test[3][0], '7'],
			[header_test[3][0], '7'],
			[header_test[4][0], '7'],
		],
	)
	# same thing using --stop-ds for the second ds. (--chain-length=3 applies to both, but only matters for the first)
	grep_text(
		['-H', '-D', '-c', '--chain-length=3', '--stop-ds=', '--stop-ds', header_test[2][0], '7', header_test[3][0], header_test[4][0]], [
			['[DATASET]', 'int32', 'int64'],
			[header_test[1][0], '7', '7'],
			['[DATASET]', 'int32', 'number'],
			[header_test[2][0], '7', '7'],
			['[DATASET]', 'int32'],
			[header_test[3][0], '7'],
			[header_test[3][0], '7'],
			[header_test[4][0], '7'],
		],
	)

	# test --list-matching
	grep_text(['-l', '-c', '', previous], [[ds] for ds, _ in header_test])
	want = [[ds, str(sliceno)] for ds, sliceno in header_test]
	grep_text(['-l', '-c', '-S', '', previous], want)
	grep_text(['-l', '-c', '-S', '-H', '', previous], [['[DATASET]', '[SLICE]']] + want)
	grep_json(['-l', '-c', '', previous], [{'dataset': ds} for ds, _ in header_test])
	grep_json(['-l', '-c', '-S', '', previous], [{'dataset': ds, 'sliceno': sliceno} for ds, sliceno in header_test])
	# test escaping
	unescaped, sliceno = header_test[-1]
	escaped = unescaped.replace('\n', '\\n').replace('\r', '\\r').replace('"', '""')
	grep_text(['-l', '-t', '/', '-S', '', previous], [['"%s"/%d' % (escaped, sliceno)]])

	# more escaping
	escapy = mk_ds('escapy',
		[('ascii', 'spaced name'), ('unicode', 'tabbed\tname')],
		['comma', 'foo,bar'],
		['tab', 'foo\tbar'],
		['newline', 'a brand new\nline'],
		['crlf', 'another brand new\r\nline'],
		['doublequote start', '"foo'],
		['doublequote inside', 'f"oo'],
		['doublequote end', 'foo"'],
		['singlequote start', "'foo"],
		['singlequote inside', "f'oo"],
		['singlequote end', "foo'"],
	)
	grep_text(['-H', '', escapy], [
		['spaced name', '"tabbed\tname"'],
		['comma', 'foo,bar'],
		['tab', '"foo\tbar"'],
		['newline', 'a brand new\\nline'],
		['crlf', 'another brand new\\r\\nline'],
		['doublequote start', '"""foo"'],
		['doublequote inside', 'f"oo'],
		['doublequote end', '"foo"""'],
		['singlequote start', "\"'foo\""],
		['singlequote inside', "f'oo"],
		['singlequote end', "\"foo'\""],
	])
	grep_text(['-H', '-f', 'raw', '', escapy], [
		['spaced name', 'tabbed\tname'],
		['comma', 'foo,bar'],
		['tab', 'foo\tbar'],
		['newline', 'a brand new'],
		['line'], # newline is not escaped
		['crlf', 'another brand new\r'],
		['line'], # newline is not escaped
		['doublequote start', '"foo'],
		['doublequote inside', 'f"oo'],
		['doublequote end', 'foo"'],
		['singlequote start', "'foo"],
		['singlequote inside', "f'oo"],
		['singlequote end', "foo'"],
	])
	grep_text(['-H', '-t', ',', '(bar|newline|end)', escapy], [
		['spaced name', 'tabbed\tname'],
		['comma', '"foo,bar"'],
		['tab', 'foo\tbar'],
		['newline', 'a brand new\\nline'],
		['doublequote end', '"foo"""'],
		['singlequote end', "\"foo'\""],
	], sep=',')
	grep_text(['-H', '-t', ' ', '(tab|inside)', escapy], [
		['"spaced name"', 'tabbed\tname'],
		['tab', 'foo\tbar'],
		['"doublequote inside"', 'f"oo'],
		['"singlequote inside"', "f'oo"],
	], sep=' ')
	grep_json(['', escapy], [
		{'spaced name': 'comma', 'tabbed\tname': 'foo,bar'},
		{'spaced name': 'tab', 'tabbed\tname': 'foo\tbar'},
		{'spaced name': 'newline', 'tabbed\tname': 'a brand new\nline'},
		{'spaced name': 'crlf', 'tabbed\tname': 'another brand new\r\nline'},
		{'spaced name': 'doublequote start', 'tabbed\tname': '"foo'},
		{'spaced name': 'doublequote inside', 'tabbed\tname': 'f"oo'},
		{'spaced name': 'doublequote end', 'tabbed\tname': 'foo"'},
		{'spaced name': 'singlequote start', 'tabbed\tname': "'foo"},
		{'spaced name': 'singlequote inside', 'tabbed\tname': "f'oo"},
		{'spaced name': 'singlequote end', 'tabbed\tname': "foo'"},
	])

	# test -m / --max-count (with the escapy and allbytes datasets)
	# with csv output and headers
	grep_text(['-H', '-m', '5', '', escapy], [
		['spaced name', '"tabbed\tname"'],
		['comma', 'foo,bar'],
		['tab', '"foo\tbar"'],
		['newline', 'a brand new\\nline'],
		['crlf', 'another brand new\\r\\nline'],
		['doublequote start', '"""foo"'],
	])
	# with json output
	grep_json(['--max-count=2', '', escapy], [
		{'spaced name': 'comma', 'tabbed\tname': 'foo,bar'},
		{'spaced name': 'tab', 'tabbed\tname': 'foo\tbar'},
	])
	# and finally with raw output, which gives more lines than matches
	grep_text(['-f', 'raw', '--max-count', '4', '', escapy], [
		['comma', 'foo,bar'],
		['tab', 'foo\tbar'],
		['newline', 'a brand new'],
		['line'], # newline is not escaped, and not counted
		['crlf', 'another brand new\r'],
		['line'], # newline is not escaped, and not counted
	])
	# use the allbytes dataset to test -m with every possible csv/raw byte
	# specify the dataset twice, so we can have all lines and still limit lines.
	grep_text(
		['-f=csv', '-m=4', '', allbytes, allbytes],
		[
			[b'control chars', b'"' + mk_bytes(0, 10) + b'\\n\x0b\x0c\\r' + mk_bytes(14, 32) + b'"'],
			[b'printable', mk_bytes(32, 128)],
			[b'not ascii', encoded_not_ascii],
			[b'control chars', b'"' + mk_bytes(0, 10) + b'\\n\x0b\x0c\\r' + mk_bytes(14, 32) + b'"'],
		],
		encoding=None,
		sep=b'\t',
	)
	grep_text(
		['-f=raw', '-m=4', '', allbytes, allbytes],
		[
			[b'control chars', mk_bytes(0, 10)],
			[mk_bytes(11, 32)], # we end up with an extra line because the control chars have a newline
			[b'printable', mk_bytes(32, 128)],
			[b'not ascii', raw_not_ascii],
			[b'control chars', mk_bytes(0, 10)],
			[mk_bytes(11, 32)], # same newline again
		],
		encoding=None,
		sep=b'\t',
	)

	# test that escaping of ds name works in the right places
	comma_ds = mk_ds(',', ['ascii'], ['foo'])
	commaquote_ds = mk_ds(',"', ['ascii'], ['bar'])
	newline_ds = mk_ds('\n', ['ascii'], ['really?'])
	grep_text(['-t', ',', '-D', '', comma_ds], [['"' + comma_ds.replace('"', '""') + '"', 'foo']], sep=',')
	grep_text(['-t', ',', '-D', '', commaquote_ds], [['"' + commaquote_ds.replace('"', '""') + '"', 'bar']], sep=',')
	grep_text(['-t', ',', '-D', '', newline_ds], [[newline_ds.replace('\n', '\\n'), 'really?']], sep=',')
	# and the same thing but -f raw, so it's not escaped
	grep_text(['-t', ',', '-D', '-f', 'raw', '', comma_ds], [[comma_ds, 'foo']], sep=',')
	grep_text(['-t', ',', '-D', '-f', 'raw', '', commaquote_ds], [[commaquote_ds, 'bar']], sep=',')
	# this ends up looking like two lines
	grep_text(['-t', ',', '-D', '-f', 'raw', '', newline_ds], [[newline_ds.replace('\n', '')], [',really?']], sep=',')

	alltypes = mk_ds('alltypes',
		[
			'ascii',
			'bool',
			'bytes',
			'complex32',
			'complex64',
			'date',
			'datetime',
			'float32',
			'float64',
			'json',
			'number',
			'time',
			'unicode',
		], [
			'foo',
			True,
			b'\xff\x00octets',
			1+2j,
			1.5-0.5j,
			datetime.date(2021, 9, 20),
			datetime.datetime(2021, 9, 20, 1, 2, 3),
			0.125,
			1e42,
			[1, 2, 3, {'FOO': 'BAR'}, None],
			-2,
			datetime.time(4, 5, 6),
			'codepoints\x00\xe4',
		], [
			'',
			False,
			b'',
			0j,
			0j,
			datetime.date(1, 1, 1),
			datetime.datetime(1, 1, 1, 1, 1, 1),
			0.0,
			0.0,
			'json',
			0,
			datetime.time(1, 1, 1),
			'',
		],
	)
	grep_text(['json', alltypes], [['', 'False', '', '0j', '0j', '0001-01-01', '0001-01-01 01:01:01', '0.0', '0.0', 'json', 0, '01:01:01', '']])
	grep_text(['-g', 'json', 'foo', alltypes], [])
	grep_text(['-g', 'bytes', 'tet', alltypes, 'ascii', 'unicode'], [['foo', 'codepoints\x00\xe4']])
	grep_text(['-g', 'bytes', '\\x00', alltypes, 'bool'], [['True']])
	if PY3:
		# python2 doesn't really handle non-utf8 bytes
		grep_text(['-g', 'bytes', '\\udcff', alltypes, 'bool'], [['True']])
	grep_text(['--format=raw', '-g', 'json', '-i', 'foo', alltypes], [[b'foo', b'True', b'\xff\x00octets' if PY3 else b'\xef\xbf\xbd\x00octets', b'(1+2j)', b'(1.5-0.5j)', b'2021-09-20', b'2021-09-20 01:02:03', b'0.125', b'1e+42', b"[1, 2, 3, {'FOO': 'BAR'}, None]" if PY3 else b"[1, 2, 3, {u'FOO': u'BAR'}, None]", b'-2', b'04:05:06', b'codepoints\x00\xc3\xa4']], sep=b'\t', encoding=None)
	grep_json([':05:', alltypes, 'bool', 'time', 'unicode', 'bytes'], [{'bool': True, 'time': '04:05:06', 'unicode': 'codepoints\x00\xe4', 'bytes': '\udcff\x00octets' if PY3 else '\ufffd\x00octets'}])

	columns = [
		'ascii',
		'bytes',
		'complex32',
		'complex64',
		'date',
		'datetime',
		'float32',
		'float64',
		'int32',
		'int64',
		'json',
		'number',
		'time',
		'unicode',
	]
	d = mk_ds('d', columns,
		['42', b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', b'42', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', b'', 42+0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', b'', 0j, 42+0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', b'', 0j, 0j, datetime.date(42, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(42, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 42.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 42.0, 0, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 42, 0, '', 0, datetime.time(1, 1, 1), '',],
		['', b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 42, '', 0, datetime.time(1, 1, 1), '',],
		['', b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '42', 0, datetime.time(1, 1, 1), '',],
		['', b'', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 42, datetime.time(1, 1, 1), '',],
		['a', b'b', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 42), '',],
		['B', b'A', 0j, 0j, datetime.date(1, 1, 1), datetime.datetime(1, 1, 1, 1, 1, 1), 0.0, 0.0, 0, 0, '', 0, datetime.time(1, 1, 1), '42',],
	)
	want = [
		['42', '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', '42', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', '', 42+0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', '', 0j, 42+0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', '', 0j, 0j, '0042-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', '', 0j, 0j, '0001-01-01', '0042-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 42.0, 0.0, 0, 0, '', 0, '01:01:01', ''],
		['', '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 42.0, 0, 0, '', 0, '01:01:01', ''],
		['', '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 42, 0, '', 0, '01:01:01', ''],
		['', '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 42, '', 0, '01:01:01', ''],
		['', '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '42', 0, '01:01:01', ''],
		['', '', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 42, '01:01:01', ''],
		['a', 'b', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:42', ''],
		['B', 'A', 0j, 0j, '0001-01-01', '0001-01-01 01:01:01', 0.0, 0.0, 0, 0, '', 0, '01:01:01', '42'],
	]
	grep_text(['42', d], want)
	def json_fixup(line):
		line = list(line)
		for key in ('complex32', 'complex64',):
			value = line[columns.index(key)]
			line[columns.index(key)] = [value.real, value.imag]
		return line
	want_json = [dict(zip(columns, json_fixup(line))) for line in want]
	grep_json(['42', d], want_json)
	grep_json(['-D', '42', d], [{'dataset': d, 'data': data} for data in want_json])
	grep_text(['-i', 'a', d], [
		['a', 'b', '0j', '0j', '0001-01-01', '0001-01-01 01:01:01', '0.0', '0.0', 0, 0, '', 0, '01:01:42', ''],
		['B', 'A', '0j', '0j', '0001-01-01', '0001-01-01 01:01:01', '0.0', '0.0', 0, 0, '', 0, '01:01:01', '42'],
	])
	grep_text(['-i', 'a', d, 'unicode', 'ascii'], [['', 'a']])
	grep_text(['-i', '-g', 'bytes', 'a', d, 'unicode', 'ascii'], [['42', 'B']])
	grep_json(['-g', 'bytes', '-g', 'ascii', '-D', '-L', '-S', '42', d], [
		{'dataset': d, 'sliceno': 0, 'lineno': 0, 'data': want_json[0]},
		{'dataset': d, 'sliceno': 1, 'lineno': 0, 'data': want_json[1]},
	])
	all_types = {n for n in _convfuncs if not n.startswith('parsed:')}
	if PY2:
		all_types.remove('pickle')
	assert used_types == all_types, 'Missing/extra column types: %r %r' % (all_types - used_types, used_types - all_types,)

	# test the smart tab mode with various lengths

	# with traditional tabs specified differently:
	# t       t       t       t       t       t       t       t       t
	# date    datetime        float32 float64 int32   int64
	# 0042-01-01      0042-01-01 01:01:01     0.0     0.0     0       0
	grep_text(['--color=always', '--tab-length=8/8/1', '-H', '42-01', d, 'date', 'datetime', 'float32', 'float64', 'int32', 'int64'], [
			hdrsepframe('date', '    ', 'datetime', '        ', 'float32', ' ', 'float64', ' ', 'int32', '   ', 'int64'),
			sepframe('00\x1b[31m42-01\x1b[39m-01', '      ', '0001-01-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '       ', '0'),
			sepframe('0001-01-01', '      ', '00\x1b[31m42-01\x1b[39m-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '       ', '0'),
		], sep='',
	)

	# same, but with min_len=2
	# with traditional tabs specified differently:
	# t       t       t       t       t       t       t       t       t
	# date    datetime        float32         float64         int32   int64
	# 0042-01-01      0042-01-01 01:01:01     0.0     0.0     0       0
	grep_text(['--color=always', '--tab-length=8/8/2', '-H', '42-01', d, 'date', 'datetime', 'float32', 'float64', 'int32', 'int64'], [
			hdrsepframe('date', '    ', 'datetime', '        ', 'float32', '         ', 'float64', '         ', 'int32', '   ', 'int64'),
			sepframe('00\x1b[31m42-01\x1b[39m-01', '      ', '0001-01-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '       ', '0'),
			sepframe('0001-01-01', '      ', '00\x1b[31m42-01\x1b[39m-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '       ', '0'),
		], sep='',
	)

	# with the default smart values
	# f               f               f               f               f               f
	# t       t       t       t       t       t       t       t       t       t       t
	# date            datetime        float32         float64         int32           int64
	# 0042-01-01      0042-01-01 01:01:01     0.0     0.0             0               0
	grep_text(['--color=always', '--tab-length=/', '-H', '42-01', d, 'date', 'datetime', 'float32', 'float64', 'int32', 'int64'], [
			hdrsepframe('date', '            ', 'datetime', '        ', 'float32', '         ', 'float64', '         ', 'int32', '           ', 'int64'),
			sepframe('00\x1b[31m42-01\x1b[39m-01', '      ', '0001-01-01 01:01:01', '     ', '0.0', '     ', '0.0', '             ', '0', '               ', '0'),
			sepframe('0001-01-01', '      ', '00\x1b[31m42-01\x1b[39m-01 01:01:01', '     ', '0.0', '     ', '0.0', '             ', '0', '               ', '0'),
		], sep='',
	)
	# same, but explicitly specified
	grep_text(['--color=always', '--tab-length=8/16/2', '-H', '42-01', d, 'date', 'datetime', 'float32', 'float64', 'int32', 'int64'], [
			hdrsepframe('date', '            ', 'datetime', '        ', 'float32', '         ', 'float64', '         ', 'int32', '           ', 'int64'),
			sepframe('00\x1b[31m42-01\x1b[39m-01', '      ', '0001-01-01 01:01:01', '     ', '0.0', '     ', '0.0', '             ', '0', '               ', '0'),
			sepframe('0001-01-01', '      ', '00\x1b[31m42-01\x1b[39m-01 01:01:01', '     ', '0.0', '     ', '0.0', '             ', '0', '               ', '0'),
		], sep='',
	)

	# with field_len=11 which is not a multiple of tab_len=8
	# f          f          f          f          f          f          f
	# t       t       t       t       t       t       t       t       t       t
	# date            datetime        float32 float64 int32   int64           number
	# 0042-01-01      0042-01-01 01:01:01     0.0     0.0     0       0       0
	grep_text(['--color=always', '--tab-length=8/11/1', '-H', '42-01', d, 'date', 'datetime', 'float32', 'float64', 'int32', 'int64', 'number'], [
			hdrsepframe('date', '            ', 'datetime', '        ', 'float32', ' ', 'float64', ' ', 'int32', '   ', 'int64', '           ', 'number'),
			sepframe('00\x1b[31m42-01\x1b[39m-01', '      ', '0001-01-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '       ', '0', '       ', '0'),
			sepframe('0001-01-01', '      ', '00\x1b[31m42-01\x1b[39m-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '       ', '0', '       ', '0'),
		], sep='',
	)

	# with field_len=13 which is not a multiple of tab_len=8
	# f            f            f            f            f            f            f
	# t       t       t       t       t       t       t       t       t       t       t
	# date            datetime        float32 float64         int32           int64   number
	# 0042-01-01      0042-01-01 01:01:01     0.0     0.0     0               0       0
	grep_text(['--color=always', '--tab-length=8/13/1', '-H', '42-01', d, 'date', 'datetime', 'float32', 'float64', 'int32', 'int64', 'number'], [
			hdrsepframe('date', '            ', 'datetime', '        ', 'float32', ' ', 'float64', '         ', 'int32', '           ', 'int64', '   ', 'number'),
			sepframe('00\x1b[31m42-01\x1b[39m-01', '      ', '0001-01-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '               ', '0', '       ', '0'),
			sepframe('0001-01-01', '      ', '00\x1b[31m42-01\x1b[39m-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '               ', '0', '       ', '0'),
		], sep='',
	)

	# exercise the parser a bit more (this is 4/8/3, written stupidly)
	# f       f       f       f       f       f       f       f       f
	# t   t   t   t   t   t   t   t   t   t   t   t   t   t   t   t   t
	# date    datetime    float32     float64     int32   int64   number
	# 0042-01-01      0001-01-01 01:01:01     0.0     0.0     0   0   0
	grep_text(['--color=always', '--tab-length=1/min_LENGTH=3/8', '-T', 'tablen=4', '-H', '42-01', d, 'date', 'datetime', 'float32', 'float64', 'int32', 'int64', 'number'], [
			hdrsepframe('date', '    ', 'datetime', '    ', 'float32', '     ', 'float64', '     ', 'int32', '   ', 'int64', '   ', 'number'),
			sepframe('00\x1b[31m42-01\x1b[39m-01', '      ', '0001-01-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '   ', '0', '   ', '0'),
			sepframe('0001-01-01', '      ', '00\x1b[31m42-01\x1b[39m-01 01:01:01', '     ', '0.0', '     ', '0.0', '     ', '0', '   ', '0', '   ', '0'),
		], sep='',
	)

	# test +n and +g
	# This looks in all columns except float32, but only prints date.
	grep_text(['+g', 'float32', '42.', d, 'date'], [
		['0001-01-01'], # matched complex32
		['0001-01-01'], # matched complex64
		['0042-01-01'], # matched date
		['0001-01-01'], # matched datetime
		# no match in float32, as that is excluded.
		['0001-01-01'], # matched float64
	])
	# Exclude a bunch of columns from printing (and also from matching, as no +g is specified).
	grep_text(['-H', '+n', 'ascii', '+n', 'bytes', '+n', 'complex32', '+n', 'time', '+n', 'unicode', '+n', 'json', '+n', 'number', '+n', 'int32', '+n', 'float32', '42.', d], [
		['complex64', 'date',       'datetime',            'float64', 'int64'],
		['(42+0j)',   '0001-01-01', '0001-01-01 01:01:01', '0.0',     '0'],
		['0j',        '0042-01-01', '0001-01-01 01:01:01', '0.0',     '0'],
		['0j',        '0001-01-01', '0042-01-01 01:01:01', '0.0',     '0'],
		['0j',        '0001-01-01', '0001-01-01 01:01:01', '42.0',    '0'],
	])
	# Exclude a bunch of columns from printing, but only float32 from matching
	grep_text(['-H', '+n', 'ascii', '+n', 'bytes', '+n', 'complex32', '+n', 'time', '+n', 'unicode', '+n', 'json', '+n', 'number', '+n', 'int32', '+g', 'float32', '42.', d], [
		['complex64', 'date',       'datetime',            'float32', 'float64', 'int64'],
		['0j',        '0001-01-01', '0001-01-01 01:01:01', '0.0',     '0.0',     '0'], # complex32 is exluded from printing, but still matches.
		['(42+0j)',   '0001-01-01', '0001-01-01 01:01:01', '0.0',     '0.0',     '0'],
		['0j',        '0042-01-01', '0001-01-01 01:01:01', '0.0',     '0.0',     '0'],
		['0j',        '0001-01-01', '0042-01-01 01:01:01', '0.0',     '0.0',     '0'],
		# no match in float32, as that is excluded.
		['0j',        '0001-01-01', '0001-01-01 01:01:01', '0.0',     '42.0',    '0'],
	])
	# Try excluding something that doesn't exist, and everything in one dataset (b only has int32).
	grep_text(['+g', 'nonexistant', '+g', 'int32', '--chain', '1', c], [
		[101, 201],
		[1.42, 3],
	])

	# test --lined
	def mk_lined_ds(name, *lines, **kw):
		dw = job.datasetwriter(name=name, allow_missing_slices=True, **kw)
		if 'columns' not in kw:
			dw.add('a', 'ascii')
			dw.add('b', 'ascii')
		for sliceno, lines in enumerate(lines):
			dw.set_slice(sliceno)
			for line in lines:
				dw.write(*line)
		return dw.finish()
	lined_a = mk_lined_ds('lined_a', [('aaa', 'aab'), ('abb', 'bbb'), ('AAA', 'BBB')])
	lined_b = mk_lined_ds('lined_b', [('BAAA', 'BAAB')], [('BABB', 'BBBB')], previous=lined_a)

	# "grep -i a" will match all lines, with highlights that only sometimes
	# align with field boundaries.
	# all lines in -c lined_b is three lines in one ds in slice 0, then another
	# in slice 0 from the next ds, and then one in slice 1.
	ODDLINE = '\x1b[30;107m\x1b[K'
	EVENLINE = '\x1b[30;47m\x1b[K'
	TRAILER = '\x1b[K\x1b[m'
	SEP = SEP_HI + '\t\x1b[24;30m'
	grep_text(['--lined', '--colour=always', '-S', '-L', '-c', '-i', 'a', lined_b], [
		[ODDLINE  + '0', '0', '\x1b[31maaa\x1b[30m', '\x1b[31maa\x1b[30mb' + TRAILER],
		[EVENLINE + '0', '1', '\x1b[31ma\x1b[30mbb', 'bbb' + TRAILER],
		[ODDLINE  + '0', '2', '\x1b[31mAAA\x1b[30m', 'BBB' + TRAILER],
		[EVENLINE + '0', '0', 'B\x1b[31mAAA\x1b[30m', 'B\x1b[31mAA\x1b[30mB' + TRAILER],
		[ODDLINE  + '1', '0', 'B\x1b[31mA\x1b[30mBB', 'BBBB' + TRAILER],
	], sep=SEP)
	# all lines in just lined_b is one line each for slice 0 and 1.
	# these two together should catch any issues with switchovers restarting
	# the lining pattern.
	grep_text(['--lined', '--colour=always', '-S', '-L', '-i', 'a', lined_b], [
		[ODDLINE + '0', '0', 'B\x1b[31mAAA\x1b[30m', 'B\x1b[31mAA\x1b[30mB' + TRAILER],
		[EVENLINE  + '1', '0', 'B\x1b[31mA\x1b[30mBB', 'BBBB' + TRAILER],
	], sep=SEP)

	# try with just fg and just bg for the lines, both for the match
	os.mkdir('accelerator')
	with open('accelerator/config', 'w') as fh:
		fh.write('''[colour]
			grep/oddlines = GREEN
			grep/evenlines = GREENBG
			grep/highlight = WHITEBG RED
			grep/header = YELLOWBG
		''')
	# odd lines keep only the non-underline (24) from the end of the separator and add their own green (32)
	SEP_ODD = SEP_HI + '\t\x1b[24;32m'
	# even lines keep the whole reset and don't add any sequence of their own (as the separator doesn't touch the bg)
	SEP_EVEN = TAB_HI
	grep_text(['--lined', '--colour=always', '-S', '-L', '-i', 'a', lined_b], [
		SEP_ODD.join(['\x1b[32m0', '0', 'B\x1b[47;31mAAA\x1b[32;49m', 'B\x1b[47;31mAA\x1b[32;49mB\x1b[m']),
		SEP_EVEN.join(['\x1b[42m\x1b[K1', '0', 'B\x1b[47;31mA\x1b[39;42mBB', 'BBBB' + TRAILER]),
	], sep='')
	# make sure newlines and such are ok, and a colour set in the value
	# (all of this will normally be escaped, but we test with --format=raw)
	lined_silly = mk_lined_ds(
		'lined\nsilly',
		[
			('abc\ndef', 'ghi'),
			('jkl', 'm\x1b[35mno\n\npq\x1b[mr'),
			('foo\nfoo\rbar\nbaz', 'aaa'),
			('foo\nfoo\rbar\nbaz', 'aaa'),
		],
		columns={'\n': 'ascii', '\r': 'ascii'},
	)
	grep_text(['--lined', '--colour=always', '-D', '-H', '--format=raw', '[ij]', lined_silly], [
		# line 1 (the header) starts here
		'\x1b[32m\x1b[43m[DATASET]\x1b[49m' + SEP_ODD + '\x1b[43m',
		'\x1b[49m' + SEP_ODD + '\x1b[43m\r\x1b[49m\x1b[m',
		# line 2 (abc...) starts here
		'\x1b[42m\x1b[K' + job + '/lined\x1b[K',
		'\x1b[Ksilly' + SEP_EVEN + 'abc\x1b[K',
		'\x1b[Kdef' + SEP_EVEN + 'gh\x1b[47;31mi\x1b[39;42m' + TRAILER,
		# line 3 (jkl...) starts here
		'\x1b[32m' + job + '/lined',
		'silly' + SEP_ODD + '\x1b[47;31mj\x1b[32;49mkl' + SEP_ODD + 'm\x1b[35mno',
		'',
		'pq\x1b[;32mr\x1b[m',
	], sep='')
	# and make sure newlines don't get messed up when --lined gets disabled by no colour
	grep_text(['--lined', '--colour=never', '-D', '-H', '--format=raw', '[ij]', lined_silly], [
		# line 1 (the header) starts here
		'[DATASET]\t',
		'\t\r',
		# line 2 (abc...) starts here
		job + '/lined',
		'silly\tabc',
		'def\tghi',
		# line 3 (jkl...) starts here
		job + '/lined',
		'silly\tjkl\tm\x1b[35mno', # the colour in the value stays
		'',
		'pq\x1b[mr',
	], sep='')
	# and finally test that \r is handled correctly on an internal line in
	# a value (i.e. that that line does not end with \e[K, and that \e[K is
	# only used when bg is set.)
	# both these lines contain ('foo\nfoo\rbar\nbaz', 'aaa').
	grep_text(['--lined', '--colour=always', '--format=raw', 'z', lined_silly], [
		# this is the first line, which does not set bg
		'\x1b[32mfoo',
		'foo\rbar',
		'ba\x1b[47;31mz\x1b[32;49m' + SEP_ODD + 'aaa\x1b[m',
		# this is the second line, which does set bg
		'\x1b[42m\x1b[Kfoo\x1b[K',
		'\x1b[Kfoo\rbar', # no \e[K at end because of \r
		'\x1b[Kba\x1b[47;31mz\x1b[39;42m' + SEP_EVEN + 'aaa\x1b[K\x1b[m',
	], sep='')
	# and also try it with --max-count
	grep_text(['--lined', '--max-count=1', '--colour=always', '--format=raw', 'z', lined_silly], [
		# the first (and only) line is three lines long.
		'\x1b[32mfoo',
		'foo\rbar',
		'ba\x1b[47;31mz\x1b[32;49m' + SEP_ODD + 'aaa\x1b[m',
	], sep='')

	# test --numeric
	numbers = mk_ds('numbers',
		[         'ascii',          'bytes', 'number'],
		[         'small',        b'number',  41     ],
		[         'small',        b'number',  42     ],
		[         'small',         b'float',  43.001 ],
		[         'small',         b'float',  44.999 ],
		[         'small',        b'number',  45     ],
		[      'negative',        b'number',  -1     ],
		[    str(10**500),           b'big',  27     ], # big enough to be inf as a float
		[           'big',     str(10**500).encode('ascii'), 27],
		[str(10**500 + 1),        b'bigger',  27     ],
		[        'bigger', str(10**500 + 1).encode('ascii'), 27],
		[           'hex',          b'0x2a',  27     ],
		[         '-0x2a',           b'hex',  27     ],
		[         'space',        b'\n 96 ',  27     ],
		[           '012', b'zero prefixed',  27     ],
		[           '0.2',         b'float',  27     ],
		[        '\n -.7',   b'space float',  27     ],
	)
	# Anything numeric, in any column, i.e. everything.
	grep_text(['-N', '', numbers], [
		[         'small',         'number',  41     ],
		[         'small',         'number',  42     ],
		[         'small',          'float',  43.001 ],
		[         'small',          'float',  44.999 ],
		[         'small',         'number',  45     ],
		[      'negative',         'number',  -1     ],
		[    str(10**500),            'big',  27     ],
		[           'big',     str(10**500),  27     ],
		[str(10**500 + 1),         'bigger',  27     ],
		[        'bigger', str(10**500 + 1),  27     ],
		[           'hex',           '0x2a',  27     ],
		[         '-0x2a',            'hex',  27     ],
		[         'space',        '\\n 96 ',  27     ],
		[           '012',  'zero prefixed',  27     ],
		[           '0.2',          'float',  27     ],
		[       '\\n -.7',    'space float',  27     ],
	])
	# Anything numeric in the ascii column
	grep_text(['-N', '-g', 'ascii', '', numbers], [
		[    str(10**500),            'big',  27     ],
		[str(10**500 + 1),         'bigger',  27     ],
		[         '-0x2a',            'hex',  27     ],
		[           '012',  'zero prefixed',  27     ],
		[           '0.2',          'float',  27     ],
		[       '\\n -.7',    'space float',  27     ],
	])
	# Exact numbers
	grep_text(['-N', '-e=43.001', '-e=12', numbers], [
		[         'small',          'float',  43.001 ],
		[           '012',  'zero prefixed',  27     ],
	])
	# Can also be written with =
	grep_text(['-N', '=42', numbers], [
		[         'small',         'number',  42     ],
		[           'hex',           '0x2a',  27     ],
	])
	# Ranges
	grep_text(['-N', '42:45', numbers], [
		[         'small',         'number',  42     ],
		[         'small',          'float',  43.001 ],
		[         'small',          'float',  44.999 ],
		[           'hex',           '0x2a',  27     ],
	])
	# include end
	grep_text(['-N', '-e=-1:=12', numbers], [
		[      'negative',         'number',  -1     ],
		[           '012',  'zero prefixed',  27     ],
		[           '0.2',          'float',  27     ],
		[       '\\n -.7',    'space float',  27     ],
	])
	# exclude start, include end
	grep_text(['-N', '-e=-1<:=12', numbers], [
		[           '012',  'zero prefixed',  27     ],
		[           '0.2',          'float',  27     ],
		[       '\\n -.7',    'space float',  27     ],
	])
	# exclude start (and end)
	grep_text(['-N', '-e=-1<:12', numbers], [
		[           '0.2',          'float',  27     ],
		[       '\\n -.7',    'space float',  27     ],
	])
	# comparisons
	grep_text(['-N', '>' + str(10**500), numbers], [
		[str(10**500 + 1),         'bigger',  27     ],
		[        'bigger', str(10**500 + 1),  27     ],
	])
	grep_text(['-N', '>=' + str(10**500), numbers], [
		[    str(10**500),            'big',  27     ],
		[           'big',     str(10**500),  27     ],
		[str(10**500 + 1),         'bigger',  27     ],
		[        'bigger', str(10**500 + 1),  27     ],
	])
	grep_text(['-N', '<=12', numbers], [
		[      'negative',         'number',  -1     ],
		[         '-0x2a',            'hex',  27     ],
		[           '012',  'zero prefixed',  27     ],
		[           '0.2',          'float',  27     ],
		[       '\\n -.7',    'space float',  27     ],
	])
	grep_text(['-N', '<12', numbers], [
		[      'negative',         'number',  -1     ],
		[         '-0x2a',            'hex',  27     ],
		[           '0.2',          'float',  27     ],
		[       '\\n -.7',    'space float',  27     ],
	])
	# Try 0.. and hex in the pattern too.
	grep_text(['-N', '0044<:=0x60', numbers], [
		[         'small',          'float',  44.999 ],
		[         'small',         'number',  45     ],
		[         'space',        '\\n 96 ',  27     ],
	])
	# And try it with highlighting too
	with open('accelerator/config', 'w') as fh:
		fh.write('''[colour]
			grep/highlight = RED
			grep/separator =
		''')
	grep_text(['--colour=always', '-N', '0044<:=0x60', numbers], [
		[         'small',          'float',  '\x1b[31m44.999\x1b[39m' ],
		[         'small',         'number',  '\x1b[31m45\x1b[39m'     ],
		[         'space', '\\n \x1b[31m96\x1b[39m ',  27              ],
	])

	# Test --unique

	dw = job.datasetwriter(name='uniq', allow_missing_slices=True)
	dw.add('', 'ascii')
	dw.add('a', 'ascii')
	dw.add('b', 'int32')
	dw.set_slice(0)
	# a lot of lines in slice 0 before the duplicates, to catch races.
	for ix in range(99000):
		dw.write('foo', 'bar', ix)
	for ix in range(99000, 99099):
		dw.write('foo', 'baz', ix)
	dw.set_slice(1)
	for ix in range(99000, 99099):
		dw.write('foo', 'baz', ix)
	# same thing except for case
	for ix in range(99000, 99099):
		dw.write('Foo', 'baz', ix)
	uniq = dw.finish()

	grep_text(['--unique', '--show-sliceno', '99000', uniq], [
		[0, 'foo', 'baz', 99000],
		[1, 'Foo', 'baz', 99000],
	])
	grep_text(['--unique', '--ignore-case', '99000', uniq], [
		['foo', 'baz', 99000],
	])
	grep_text(['--unique=', '--show-sliceno', '000', uniq], [
		[0, 'foo', 'bar', 1000],
		[1, 'Foo', 'baz', 99000],
	])
	grep_text(['--unique=a', '--show-sliceno', '000', uniq], [
		[0, 'foo', 'bar', 1000],
		[0, 'foo', 'baz', 99000],
	])
	grep_text(['--unique=', '--unique=a', '--show-sliceno', '000', uniq], [
		[0, 'foo', 'bar', '1000'],
		[0, 'foo', 'baz', '99000'],
		[1, 'Foo', 'baz', '99000'],
	])
	grep_text(['-u=a', '-u=', '-i', '-S', '000', uniq], [
		[0, 'foo', 'bar', '1000'],
		[0, 'foo', 'baz', '99000'],
	])
	grep_text(['--unique', '--slice=1', '--show-sliceno', '000', uniq], [
		[1, 'foo', 'baz', '99000'],
		[1, 'Foo', 'baz', '99000'],
	])

	dw = job.datasetwriter(name='uniq2', previous=uniq, allow_missing_slices=True)
	dw.add('', 'ascii')
	dw.add('a', 'ascii')
	dw.add('b', 'int32')
	dw.set_slice(2)
	# These match lines in the first dataset.
	dw.write('foo', 'bar', 55000)
	dw.write('foo', 'baz', 99000)
	# These match in the '' column.
	dw.write('foo', 'x', -1000)
	dw.write('Foo', 'x', -1000)
	# This doesn't match anywhere.
	dw.write('no', 'match', -99000)
	uniq2 = dw.finish()

	grep_text(['--unique', '--ignore-case', '--chain', '--show-dataset', '99000', uniq2], [
		[uniq, 'foo', 'baz', '99000'],
		[uniq2, 'no', 'match', '-99000'],
	])
	grep_text(['--unique=', '--chain', '--show-dataset', '000', uniq2], [
		[uniq, 'foo', 'bar', '1000'],
		[uniq, 'Foo', 'baz', '99000'],
		[uniq2, 'no', 'match', '-99000'],
	])
	grep_text(['--unique=', '--unique=a', '--chain', '--show-dataset', '000', uniq2], [
		[uniq, 'foo', 'bar', '1000'],
		[uniq, 'foo', 'baz', '99000'],
		[uniq, 'Foo', 'baz', '99000'],
		[uniq2, 'foo', 'x', '-1000'],
		[uniq2, 'Foo', 'x', '-1000'],
		[uniq2, 'no', 'match', '-99000'],
	])

	dw = job.datasetwriter(name='uniq3', previous=uniq)
	dw.add('a', 'ascii')
	dw.add('b', 'ascii')
	dw.add('c', 'int32')
	w = dw.get_split_write()
	# These match lines in the first dataset, but the column names are different.
	w('foo', 'bar', 55000)
	w('foo', 'baz', 99000)
	# This matches in the a column (first column here, second in the first ds).
	w('baz', 'x', 99000)
	uniq3 = dw.finish()

	grep_text(['--unique', '--show-dataset', '--chain', '99000', uniq3], [
		[uniq, 'foo', 'baz', 99000],
		[uniq, 'Foo', 'baz', 99000],
		[uniq3, 'foo', 'baz', 99000], # repeats because the column names are different
		[uniq3, 'baz', 'x', 99000],
	])
	grep_text(['--unique=a', '--show-dataset', '--chain', '99000', uniq3], [
		[uniq, 'foo', 'baz', 99000],
		[uniq3, 'foo', 'baz', 99000],
		# no baz x line because baz matches the baz from uniq above (both columns are named a)
	])

	# This should fail, because column c doesn't exist in uniq
	cmd = options.command_prefix + ['grep', '--unique=c', '--chain', '000', uniq3]
	try:
		check_output(cmd)
		raise Exception("%r worked, should have complained that %s doesn't have column c" % (cmd, uniq.quoted,))
	except CalledProcessError:
		pass

	grep_text(['--unique=c', '--allow-missing-columns', '--chain', '000', uniq3], [
		# No output from uniq because it has no column c to be unique in
		['foo', 'bar', 55000],
		['foo', 'baz', 99000],
	])

	# This chain is [uniq, uniq3, uniq4], where uniq and uniq4 have the same
	# columns but uniq3 has different columns. This is to test that this change
	# back does not confuse the uniqueness tests.
	# Everything matching is in a different slice.

	dw = job.datasetwriter(name='uniq4', previous=uniq3, allow_missing_slices=True)
	dw.add('', 'ascii')
	dw.add('a', 'ascii')
	dw.add('b', 'int32')
	dw.set_slice(1)
	# Matches a line in uniq3 (slice 2), but column names are different
	dw.write('baz', 'x', 99000)
	# Matches nothing
	dw.write('a', 'b', 59000)
	dw.set_slice(2)
	# Matches a line in uniq (slice 0)
	dw.write('foo', 'bar', 59000)
	uniq4 = dw.finish()

	grep_text(['--unique', '--show-dataset', '--chain', '[59]9000', uniq4], [
		[uniq, 'foo', 'bar', 59000],
		[uniq, 'foo', 'baz', 99000],
		[uniq, 'Foo', 'baz', 99000],
		[uniq3, 'foo', 'baz', 99000], # repeats because the column names are different
		[uniq3, 'baz', 'x', 99000],
		[uniq4, 'baz', 'x', 99000], # repeats because the column names are different
		[uniq4, 'a', 'b', 59000],
	])


	# Test --roundrobin.

	# Test different length slices.
	dw = job.datasetwriter(name='roundrobin', allow_missing_slices=True)
	dw.add('a', 'ascii')
	dw.set_slice(0)
	dw.write('first 0')
	dw.write('second 0')
	dw.write('third 0')
	dw.write('fourth 0')
	dw.write('fifth 0')
	dw.set_slice(1)
	dw.write('first 1')
	dw.write('second 1')
	dw.set_slice(2)
	dw.write('first 2')
	dw.write('second 2')
	dw.write('third 2')
	roundrobin = dw.finish()

	grep_text(['--roundrobin', '', roundrobin], [
		['first 0'],
		['first 1'],
		['first 2'],
		['second 0'],
		['second 1'],
		['second 2'],
		['third 0'],
		['third 2'],
		['fourth 0'],
		['fifth 0'],
	])
	grep_text(['--roundrobin', '--show-sliceno', '--show-lineno', '-s', '0', '-s', '2', '', roundrobin], [
		[0, 0, 'first 0'],
		[2, 0, 'first 2'],
		[0, 1, 'second 0'],
		[2, 1, 'second 2'],
		[0, 2, 'third 0'],
		[2, 2, 'third 2'],
		[0, 3, 'fourth 0'],
		[0, 4, 'fifth 0'],
	])

	# Test several datasets.
	dw = job.datasetwriter(name='roundrobin2', previous=roundrobin)
	dw.add('a', 'int32')
	w = dw.get_split_write()
	for ix in range(10000):
		w(ix)
	roundrobin2 = dw.finish()
	# The middle dataset is longer, to catch ordering problems between processes.
	dw = job.datasetwriter(name='roundrobin3', previous=roundrobin2)
	dw.add('a', 'int32')
	w = dw.get_split_write()
	for ix in (50000, 90000):
		w(ix)
	roundrobin3 = dw.finish()
	grep_text(['--roundrobin', '--show-sliceno', '--show-lineno', '--chain', '', roundrobin3], [
		[0, 0, 'first 0'],
		[1, 0, 'first 1'],
		[2, 0, 'first 2'],
		[0, 1, 'second 0'],
		[1, 1, 'second 1'],
		[2, 1, 'second 2'],
		[0, 2, 'third 0'],
		[2, 2, 'third 2'],
		[0, 3, 'fourth 0'],
		[0, 4, 'fifth 0'],
	] + [[n % slices, n // slices, str(n)] for n in range(10000)] + [[0, 0, 50000], [1, 0, 90000]])
