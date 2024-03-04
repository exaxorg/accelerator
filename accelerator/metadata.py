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
import re
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


def b64hash_setup(filename):
	from base64 import b64encode
	from hashlib import sha1
	try:
		with open(filename, 'rb') as fh:
			data = fh.read()
	except FileNotFoundError:
		return None
	# Try to remove the part that differs between finished and unfinished jobs.
	data = data.split(b'\n\n', 1)[-1]
	return b64encode(sha1(data).digest(), b'_-').rstrip(b'=').decode('ascii')

def job_metadata(job):
	d = {
		'job': job.path,
		'method': job.method,
		'time': int(job.params.starttime), # who needs sub-second precision?
		'setup_hash': b64hash_setup(job.filename('setup.json')),
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


def generate_gif(fh, job, size):
	def chunks(data):
		while data:
			part = data[:255]
			data = data[255:]
			yield struct.pack('<B', len(part)) + part
	payload = job_metadata(job)
	# The first chunk of the extension block has to be exactly 11 bytes, so
	# says the specification. 8 bytes Identifier + 3 bytes Authentication Code.
	data = b'!\xff\x0bExAxMeta1.0' + b''.join(chunks(payload)) + b'\x00;'
	return [
		(b'GIF89a', 6), # might have been GIF87a, which does not support application extension blocks.
		(6, size - 7),
		(data, len(data)),
	]

def extract_gif(fh):
	def unchunk(pos):
		while pos + 1 < len(data):
			z, = struct.unpack('<B', data[pos:pos + 1]) # "z = pos[0]" but python2 compatible.
			if z == 0:
				return
			yield data[pos + 1:pos + z + 1]
			pos += z + 1
	fh.seek(0, 2)
	fh.seek(max(0, fh.tell() - 8192), 0)
	data = fh.read()
	signature = b'!\xff\x0bExAxMeta1.0'
	pos = data.find(signature)
	while pos >= 0 and pos + 16 < len(data):
		yield b''.join(unchunk(pos + 14))
		pos = data.find(signature, pos + 17)


def detect_riff(data, size):
	if data.startswith(b'RIFF') and len(data) >= 8:
		return struct.unpack('<I', data[4:8])[0] + 8 == size

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


def generate_isom(fh, job, size, pos=0):
	fh.seek(pos)
	data = fh.read(8)
	if len(data) != 8:
		return
	z, fourcc = struct.unpack('>II', data)
	if fourcc != 0x66747970: # "ftyp"
		return
	pos += z
	while True:
		if pos == size:
			# Found the end of the file without problems, let's put our data here.
			payload = job_metadata(job)
			data = struct.pack('>I', len(payload) + 8) + b'ExAx' + payload
			return [
				(0, size),
				(data, len(data)),
			]
		fh.seek(pos)
		data = fh.read(8)
		if len(data) != 8:
			return
		z, fourcc = struct.unpack('>II', data)
		pos += z

def extract_isom(fh, pos=0):
	fh.seek(pos)
	data = fh.read(8)
	if len(data) != 8:
		return
	z, fourcc = struct.unpack('>II', data)
	if fourcc != 0x66747970: # "ftyp"
		return
	pos += z
	while True:
		fh.seek(pos)
		data = fh.read(8)
		if len(data) != 8:
			return
		z, fourcc = struct.unpack('>II', data)
		if fourcc == 0x45784178: # "ExAx"
			data = fh.read(z - 8)
			if len(data) == z - 8:
				yield data
		pos += z


# Simplify parsing by having enough of a header to just look for that instead of proper EBML parsing.
ebml_signature = b'\xeaExAxMeta\x0e\x0a:'

def generate_ebml(fh, job, size):
	payload = ebml_signature + job_metadata(job)
	if len(payload) > 0x3ffe:
		return
	# This looks like a valid EBML Void element, but as we are just
	# placing it after the file this probably does not matter.
	data = struct.pack('>BH', 0xec, len(payload) | 0x4000) + payload
	return [
		(0, size),
		(data, len(data)),
	]

def extract_ebml(fh):
	fh.seek(0, 2)
	fh.seek(max(0, fh.tell() - 8192), 0)
	data = fh.read()
	pos = data.find(ebml_signature)
	while pos >= 2:
		z = data[pos - 2:pos]
		z, = struct.unpack('>H', z)
		if z & 0xc000 != 0x4000:
			pos = data.find(ebml_signature, pos + 12)
			continue
		z &= 0x3fff
		if z > 0x3ffe or pos + z > len(data):
			return
		pos += len(ebml_signature)
		z -= len(ebml_signature)
		yield data[pos:pos + z]
		pos = data.find(ebml_signature, pos + z)


def generate_pdf(fh, job, size):
	offset = max(0, size - 4096)
	fh.seek(offset, 0)
	filetail = fh.read()
	m = re.search(br'[\r\n]+startxref(?:(?:[ \t\f\v]*%.*)?[\r\n]+)+\d+(?:(?:[ \t\f\v]*%.*)?[\r\n]+)+%%EOF[\r\n]*$', filetail)
	if not m:
		# There should be a startxref just before %%EOF, but if there
		# isn't we'll place our comment directly before the %%EOF marker.
		m = re.search(br'[\r\n]+%%EOF[\r\n]*$', filetail)
	if m:
		pos0 = pos1 = m.start()
		if not pos0: # in case we started in the middle of a newline
			return
		while filetail[pos1] in b'\r\n':
			pos1 += 1
		data = b'\n% ExAx:' + job_metadata(job) + b'\n'
		pos0 += offset
		pos1 += offset
		return [
			(0, pos0),
			(data, len(data)),
			(pos1, size - pos1),
		]

def extract_pdf(fh):
	fh.seek(0, 2)
	fh.seek(max(0, fh.tell() - 8192), 0)
	data = fh.read()
	signature = b'\n% ExAx:{'
	pos = data.find(signature)
	while pos >= 0:
		endpos = data.find(b'\n', pos + len(signature))
		yield data[pos + len(signature) - 1:endpos]
		pos = data.find(signature, pos + 1)


def generate_jxl_naked(fh, job, size):
	fh.seek(0)
	data = fh.read(2)
	if data != b'\xff\x0a':
		return
	# Naked JPEG XL does not allow extra blocks, so we have to box it.
	header = b'\0\0\0\x0cJXL \r\n\x87\n\0\0\0\x14ftypjxl \0\0\0\0jxl ' + \
		struct.pack('>I', size + 8) + b'jxlc'
	payload = job_metadata(job)
	data = struct.pack('>I', len(payload) + 8) + b'ExAx' + payload
	return [
		(header, len(header)),
		(0, size),
		(data, len(data)),
	]

def generate_jxl_boxed(fh, job, size):
	fh.seek(0)
	data = fh.read(12)
	if data != b'\0\0\0\x0cJXL \r\n\x87\n':
		return
	return generate_isom(fh, job, size, 12)

def extract_jxl(fh):
	fh.seek(0)
	data = fh.read(12)
	if data != b'\0\0\0\x0cJXL \r\n\x87\n':
		return
	return extract_isom(fh, 12)


formats = [
	(
		'PNG', None, generate_png, extract_png,
		b'\x89PNG\x0d\x0a\x1a\x0a',
		b'\x00\x00\x00\x00IEND\xae\x42\x60\x82',
	),
	(
		'JPEG', None, generate_jpeg, extract_jpeg,
		b'\xff\xd8\xff',
		b'\xff\xd9',
	),
	(
		'GIF', '.gif', generate_gif, extract_gif,
		re.compile(br'GIF8[79]a'),
		b';',
	),
	(
		'RIFF based format (AVI, WEBP, ...)', None, generate_riff, extract_riff,
		detect_riff,
		b'',
	),
	(
		'ISO media format (MP4, HEIF, ...)', None, generate_isom, extract_isom,
		re.compile(br'\0...ftyp', re.DOTALL),
		b'',
	),
	(
		'EBML based format (MKV, WEBM, ...)', None, generate_ebml, extract_ebml,
		b'\x1a\x45\xdf\xa3',
		b'',
	),
	(
		'PDF', '.pdf', generate_pdf, extract_pdf,
		re.compile(br'%PDF-\d\.\d'),
		re.compile(br'.*[\r\n]%%EOF[\r\n]*$', re.DOTALL),
	),
	(
		'JPEG XL (naked codestream)', '.jxl', generate_jxl_naked, None,
		b'\xff\x0a',
		b'',
	),
	(
		'JPEG XL (boxed)', None, generate_jxl_boxed, extract_jxl,
		re.compile(b'\0\0\0\x0cJXL \r\n\x87\n\0...ftyp', re.DOTALL),
		b'',
	),
]

def matcher(pattern, where):
	if hasattr(pattern, 'match'):
		return lambda v, _: pattern.match(v)
	elif callable(pattern):
		return pattern
	elif where == 'start':
		return lambda v, _: v.startswith(pattern)
	elif where == 'end':
		return lambda v, _: v.endswith(pattern)

formats = [
	(name, ext, generate, extract, matcher(header, 'start'), matcher(trailer, 'end'))
	for name, ext, generate, extract, header, trailer in formats
]

def insert_metadata(filename, fh, job, size):
	res = None
	if size > 20 and job:
		fh.seek(0, 0)
		start = fh.read(20)
		fh.seek(-20, 2)
		end = fh.read(20)
		for name, ext, generate, extract, header, trailer in formats:
			if ext and not filename.lower().endswith(ext):
				continue
			if header(start, size) and trailer(end, size):
				try:
					res = generate(fh, job, size)
				except Exception:
					traceback.print_exc(file=sys.stderr)
				if res:
					break
				print("Failed to generate %s metadata." % (name,), file=sys.stderr)
	return res or [(0, size)]

def extract_metadata(filename, fh):
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
	size = fh.tell() + 20
	end = fh.read(20)
	for name, ext, generate, extract, header, trailer in formats:
		if not extract:
			continue
		if ext and not filename.lower().endswith(ext):
			continue
		if header(start, size) and trailer(end, size):
			res = extract(fh)
			if res:
				res = decode(res)
			return res
