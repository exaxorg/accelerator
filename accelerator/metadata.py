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
from accelerator.extras import json_decode

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
	fh.seek(0)
	if fh.read(8) != b'\x89PNG\x0d\x0a\x1a\x0a':
		return
	while True:
		data = fh.read(4)
		if len(data) != 4:
			return
		z, = struct.unpack('>I', data)
		chunk_id = fh.read(4)
		if chunk_id == b'tEXt':
			data = fh.read(z)
			crc, = struct.unpack('>I', fh.read(4))
			if crc == crc32(b'tEXt' + data) and data.startswith(b'ExAx\0'):
				yield data[5:]
		else:
			fh.seek(z + 4, 1)


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
	fh.seek(0)
	data = fh.read(2)
	if data != b'\xff\xd8':
		return
	pos = 2
	# Look at APP blocks until we run out.
	while True:
		data = fh.read(4)
		if len(data) != 4:
			return
		typ, z = struct.unpack('>HH', data)
		if typ < 0xffe0 or typ > 0xffef or z < 2:
			# Not an APP block, so we stop looking.
			return
		pos += z + 2
		if typ == 0xffea:
			data = fh.read(z - 2)
			if len(data) == z - 2 and data.startswith(b'ExAx\0'):
				yield data[5:]
		fh.seek(pos, 0)


def generate_riff(fh, job, size):
	fh.seek(0)
	data = fh.read(4)
	if data != b'RIFF':
		return
	data = fh.read(4)
	if len(data) != 4:
		return
	z, = struct.unpack('<I', data)
	if z + 8 != size:
		# Incomplete file / garbage after.
		return
	payload = job_metadata(job)
	data = b'ExAx' + struct.pack('<I', len(payload)) + payload
	if len(data) % 2 == 1:
		data += b'\0'
	if z + len(data) >= 0xfffffff8:
		# File would become too big.
		return
	return [
		(b'RIFF' + struct.pack('<I', z + len(data)), 8),
		(8, z),
		(data, len(data)),
	]

def extract_riff(fh):
	fh.seek(0)
	data = fh.read(4)
	if data != b'RIFF':
		return
	data = fh.read(4)
	if len(data) != 4:
		return
	z, = struct.unpack('<I', data)
	fh.seek(0, 2)
	if z + 8 != fh.tell():
		# incomplete file / garbage after
		return
	fh.seek(12)
	while True:
		fourcc = fh.read(4)
		data = fh.read(4)
		if len(data) != 4:
			return
		z, = struct.unpack('<I', data)
		if fourcc == b'ExAx':
			data = fh.read(z)
			if len(data) == z:
				yield data
		else:
			fh.seek(z, 1)
		if z % 2 == 1:
			fh.seek(1, 1)


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
	(
		'RIFF based format (AVI, WEBP, ...)', generate_riff, extract_riff,
		b'RIFF',
		b'',
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

def extract_metadata(fh):
	def decode(gen):
		for data in gen:
			try:
				yield json_decode(data.decode('utf-8'))
			except Exception:
				yield None
	fh.seek(0, 0)
	start = fh.read(20)
	if len(start) != 20:
		return
	fh.seek(-20, 2)
	end = fh.read(20)
	for name, generate, extract, header, trailer in formats:
		if start.startswith(header) and end.endswith(trailer):
			res = extract(fh)
			if res:
				res = decode(res)
			return res
