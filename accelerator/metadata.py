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

# Functions for generating, inserting and extracting metadata about the job
# that built files.

from __future__ import print_function

import json
import socket
import struct
import sys
import traceback
import zlib

from accelerator.compat import PY3, FileNotFoundError

if PY3:
	crc32 = zlib.crc32
else:
	def crc32(data):
		return zlib.crc32(data) & 0xffffffff


def b64hash(filename):
	from base64 import b64encode
	from hashlib import sha1
	try:
		with open(filename, 'rb') as fh:
			return b64encode(sha1(fh.read()).digest(), b'_-').rstrip(b'=').decode('ascii')
	except FileNotFoundError:
		return None

def job_metadata(job):
	d = {
		'job': job.path,
		'method': job.method,
		'time': int(job.params.starttime), # who needs sub-second precision?
		'setup_hash': b64hash(job.filename('setup.json')),
		'host': socket.gethostname(),
	}
	res = json.dumps(d)
	if PY3:
		res = res.encode('ascii')
	return res


def generate_png(fh, job, size):
	payload = b'tEXtExAx\0' + job_metadata(job)
	crc = crc32(payload)
	data = (
		struct.pack('>I', len(payload) - 4) +
		payload +
		struct.pack('>I', crc) +
		b'\x00\x00\x00\x00IEND\xae\x42\x60\x82'
	)
	return [
		(0, size - 12),
		(data, len(data)),
	]

def extract_png(fh):
	pass # TODO


def generate_jpeg(fh, job, size):
	# It's probably a JFIF or similar, so skip all initial APP blocks.
	pos = 2
	fh.seek(pos)
	while True:
		data = fh.read(4)
		if len(data) != 4:
			return
		typ, z = struct.unpack('>HH', data)
		if typ < 0xff00 or z < 2:
			return
		if typ < 0xffe0 or typ > 0xffef:
			break
		pos += z + 2
		fh.seek(pos, 0)
	payload = b'ExAx\0' + job_metadata(job)
	data = struct.pack('>HH', 0xffea, len(payload) + 2) + payload
	return [
		(0, pos),
		(data, len(data)),
		(pos, size - pos),
	]

def extract_jpeg(fh):
	pass # TODO


formats = [
	(
		'PNG', generate_png, extract_png,
		b'\x89PNG\x0d\x0a\x1a\x0a',
		b'\x00\x00\x00\x00IEND\xae\x42\x60\x82',
	),
	(
		'JPEG', generate_jpeg, extract_jpeg,
		b'\xff\xd8\xff',
		b'\xff\xd9',
	),
]

def insert_metadata(fh, job, size):
	res = None
	if size > 20 and job:
		fh.seek(0, 0)
		start = fh.read(20)
		fh.seek(-20, 2)
		end = fh.read(20)
		for name, generate, extract, header, trailer in formats:
			if start.startswith(header) and end.endswith(trailer):
				try:
					res = generate(fh, job, size)
				except Exception:
					traceback.print_exc(file=sys.stderr)
				if res:
					break
				print("Failed to generate %s metadata." % (name,), file=sys.stderr)
	return res or [(0, size)]
