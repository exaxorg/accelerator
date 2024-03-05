# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2024 Carl Drougge                                          #
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
Test board adding metadata to files and decoding this with "ax sherlock".
'''

options = dict(
	command_prefix=['ax', '--config', '/some/path/here'],
)

from accelerator.compat import url_quote_more, urlopen, Request, HTTPError, unicode

from subprocess import Popen, check_output
import os
import struct
import time


def synthesis(job):
	os.putenv('XDG_CONFIG_HOME', job.path)   # make sure we can't be messed up by config
	os.putenv('ACCELERATOR_IGNORE_ETC', 'Y') # make sure we can't be messed up by global config
	socket_path = os.path.realpath('socket')
	p = Popen(options.command_prefix + ['board-server', socket_path])

	# Allow some time for board-server to start
	for _ in range(32):
		if os.path.exists(socket_path):
			break
		time.sleep(0.1)

	board_url = 'unixhttp://' + url_quote_more(socket_path) + '/'
	job_url = board_url + 'job/' + url_quote_more(job) + '/'

	# This is a cut down version of unixhttp.get() that doesn't do any
	# decoding of the response (as we will fetch binary files).
	def get(filename):
		# The path ends up in the Host-header if we don't override it, and
		# if the path contains characters that aren't in latin-1 that breaks.
		headers = {'Host': 'board'}
		url = job_url + filename
		req = Request(url, headers=headers)
		r = urlopen(req)
		try:
			resp = r.read()
			# It is inconsistent if we get HTTPError or not.
			# It seems we do when using TCP sockets, but not when using unix sockets.
			if r.getcode() >= 400:
				raise HTTPError(url, r.getcode(), resp, {}, None)
			return resp
		finally:
			try:
				r.close()
			except Exception:
				pass

	extra_metadata = br'{"job":"/\n/DoES/NoT.EXIST/NOPE","setup_hash":"nah","host":"cough","method":"none","time":0}'
	def mk_meta(prefix_a, prefix_b=b'', suffix=b'', offset=0):
		data = extra_metadata + suffix
		if isinstance(prefix_a, unicode):
			return struct.pack(prefix_a, len(data) + len(prefix_b) + offset) + prefix_b + data
		elif isinstance(prefix_b, unicode):
			return prefix_a + struct.pack(prefix_b, len(data) + len(prefix_a) + offset) + data
		else:
			return prefix_a + prefix_b + extra_metadata + suffix
	def crc(data):
		from zlib import crc32
		return struct.pack('>I', crc32(data) & 0xffffffff)

	# filename, contents, expected modified head, expected modified tail, has extra block
	# These need to be at least 20 bytes for board to try augmenting them.
	files = [
		('a.png', b'\x89PNG\x0d\x0a\x1a\x0a\x00\x00\x00\x00duMY\xae\x42\x60\x82\x00\x00\x00\x00IEND\xae\x42\x60\x82', 20, 8, False),
		('b.png', b'\x89PNG\x0d\x0a\x1a\x0a' + mk_meta('>I', b'tEXtExAx\0', offset=-4) + crc(b'tEXtExAx\0' + extra_metadata) + b'\x00\x00\x00\x00IEND\xae\x42\x60\x82', 80, 8, True),
		('a.jpg', b'\xff\xd8\xff\xea\x00\x10..............\xff\xff\xff\xd9', b'\xff\xd8\xff\xea\x00\x10..............\xff\xea', b'}\xff\xff\xff\xd9', False),
		('b.jpg', b'\xff\xd8\xff\xea' + mk_meta('>H', b'ExAx\0', offset=2) + b'\xff\xff\xff\xd9', 80, b'}\xff\xff\xff\xd9', True),
		('a.gif', b'GIF87ax................y;', b'GIF89ax................y', b'}\x00;', False),
		# Make sure the chunking decoder works.
		('b.gif', b'GIF89ax!\xff\x0bExAxMeta1.0\x20' + extra_metadata[:32] + struct.pack('<B', len(extra_metadata) - 32) + extra_metadata[32:] + b'\x00data;', 23 + len(extra_metadata) + 2, b'}\x00;', True),
		('a.avi', b'RIFF\x10\x00\x00\x00whatDUMY\x03\x00\x00\x00...\x00', 4, {b'}', b'}\0'}, False),
		('b.avi', b'RIFF' + struct.pack('<I', len(extra_metadata) + 12) + mk_meta(b'whatExAx', '<I', offset=-8), 4, {b'}', b'}\0'}, True),
		('a.mp4', b'\x00\x00\x00\x19ftypisom.............', b'\x00\x00', b'}', False),
		('b.mp4', b'\x00\x00\x00\x0cftypisom' + mk_meta('>I', b'ExAx', offset=4), b'\x00\x00', b'}', True),
		('a.mkv', b'\x1a\x45\xdf\xa3\xec\x40\x10.................', 24, b'}', False),
		('b.mkv', b'\x1a\x45\xdf\xa3\xec\x40' + mk_meta('<B', b'\xeaExAxMeta\x0e\x0a:'), 19 + len(extra_metadata), b'}', True),
		# PDF is stupid with newlines, so test a few versions. Also with and without (valid looking) startxref.
		('a.pdf', b'%PDF-1.5\nstartxref\n9\n%%EOF\n', b'%PDF-1.5\n% ExAx:{', b'}\nstartxref\n9\n%%EOF\n', False),
		('b.pdf', b'%PDF-1.5\r\nnotxref\r9\r%%EOF', b'%PDF-1.5\r\nnotxref\r9\n% ExAx:{', b'}\n%%EOF', False),
		('c.pdf', b'%PDF-1.0\n% ExAx:' + extra_metadata + b'\nnotxref\n9\rstartxref\r\n9\r\n%%EOF', 26 + len(extra_metadata), b'}\nstartxref\r\n9\r\n%%EOF', True),
		('d.pdf', b'%PDF-1.777\r\n% ExAx:' + extra_metadata + b'\nstartxref\r\n9ad\r%%EOF\r\n', 34 + len(extra_metadata), b'}\n%%EOF\r\n', True), # bad startxref - data after
		('e.pdf', b'%PDF-3.9\n' + b'.' * 8000 + b'!\rstartxref\n17\n%%EOF\n', b'%PDF-3.9\n' + b'.' * 8000 + b'!\n% ExAx:{', b'}\nstartxref\n17\n%%EOF\n', False), # >4k
		('a.jxl', b'\xff\x0a...naked JPEG XL...', b'\0\0\0\x0cJXL \r\n\x87\n\0\0\0\x14ftypjxl \0\0\0\0jxl \0\0\0\x1djxlc\xff\x0a...naked JPEG XL...', b'}', False),
		('b.jxl', b'\0\0\0\x0cJXL \r\n\x87\n\0\0\0\x14ftypjxl \0\0\0\0jxl \0\0\0\x0ajxlc\xff\x0a', 42, b'}', False),
		('c.jxl', b'\0\0\0\x0cJXL \r\n\x87\n\0\0\0\x14ftypjxl \0\0\0\0jxl \0\0\0\x0ajxlc\xff\x0a' + mk_meta('>I', b'ExAx', offset=4), 50 + len(extra_metadata), b'}', True),
	]

	for filename, contents, want_head, want_tail, has_extra_block in files:
		if isinstance(want_head, int):
			want_head = contents[:want_head]
		if isinstance(want_tail, int):
			want_tail = contents[-want_tail:]
		with open(filename, 'wb') as fh:
			fh.write(contents)
		modified_contents = get(filename)
		if contents == modified_contents:
			raise Exception('%s was not modified by board' % (filename,))
		filename = 'modified.' + filename
		with open(filename, 'wb') as fh:
			fh.write(modified_contents)
		if not any(modified_contents.startswith(v) for v in (want_head if isinstance(want_head, set) else (want_head,))):
			raise Exception('Expected %s to start with %r, but it did not' % (filename, want_head,))
		if not any(modified_contents.endswith(v) for v in (want_tail if isinstance(want_tail, set) else (want_tail,))):
			raise Exception('Expected %s to end with %r, but it did not' % (filename, want_tail,))
		got = check_output(options.command_prefix + ['sherlock', filename])
		got = got.decode('utf-8').strip()
		if has_extra_block:
			want_jobs = 'NOPE unknown job, does not exist on disk\n' + job
		else:
			want_jobs = job
		if got != want_jobs:
			raise Exception('Expected "ax sherlock %s" to give %r, got %r' % (filename, want_jobs, got,))

	p.terminate()
	p.wait()
