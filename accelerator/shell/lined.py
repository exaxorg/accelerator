# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2022-2024 Carl Drougge                                     #
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

from __future__ import division, print_function

from itertools import cycle
import errno
import os
import sys

from accelerator.colourwrapper import colour
from accelerator.compat import PY2
from accelerator import mp


def split_colour(spec):
	seq, _ = colour.pre_post(spec)
	if seq == '':
		return '', ''
	assert seq.startswith('\x1b[')
	assert seq.endswith('m')
	seq = seq[2:-1]
	assert '\x1b' not in seq
	fg = []
	bg = []
	for part in seq.split(';'):
		code = int(part.split(':', 1)[0])
		if 30 <= code <= 38 or 90 <= code <= 97:
			target = fg
		elif 40 <= code <= 48 or 100 <= code <= 107:
			target = bg
		elif code not in (39, 49):
			print("Sorry, %s can only use colours, not attributes" % (spec,), file=sys.stderr)
			sys.exit(1)
		target.append(part)
	return ';'.join(fg), ';'.join(bg)


# a rather incomplete SGR parser that replaces colour resets by our
# selected colour (if we have one).
def collect_escseq(it, line_fg, line_bg):
	chars = ['\x1b']
	try:
		c = next(it)
		chars.append(c)
		if c == '[':
			while True:
				c = next(it)
				if c == 'm':
					pieces = []
					for piece in ''.join(chars)[2:].split(';'):
						code = int(piece.split(':', 1)[0] or '0', 10)
						if code == 0:
							pieces = ['']
							if line_fg:
								pieces.append(line_fg)
							if line_bg:
								pieces.append(line_bg)
						elif code == 39 and line_fg:
							pieces.append(line_fg)
						elif code == 49 and line_bg:
							pieces.append(line_bg)
						else:
							pieces.append(piece)
					return ('\x1b[', ';'.join(pieces), 'm',)
				chars.append(c)
				if c not in '0123456789;:':
					break
	except (StopIteration, ValueError):
		pass
	return chars


_RC_EPIPE = 124 # process return code used to signal we died with EPIPE

class Liner:
	def __init__(self, process, saved_stdout):
		self.process = process
		self.saved_stdout = saved_stdout

	def close(self):
		os.dup2(self.saved_stdout, 1) # EOF for the liner process (after all children have also exited)
		os.close(self.saved_stdout)
		self.process.join()
		if self.process.exitcode and self.process.exitcode != _RC_EPIPE:
			raise Exception('Liner process exited with %s' % (self.process.exitcode,))


def enable_lines(colour_prefix, lined=True, decode_lines=False, max_count=None, after=0):
	if lined:
		colour._lined = True
		pre_fg0, pre_bg0 = split_colour(colour_prefix + '/oddlines')
		pre_fg1, pre_bg1 = split_colour(colour_prefix + '/evenlines')
		if pre_fg0 == pre_bg0 == pre_fg1 == pre_bg1 == '' and max_count is None:
			return
	else:
		pre_fg0 = pre_bg0 = pre_fg1 = pre_bg1 = ''

	def wrap_EPIPE(*a):
		try:
			return lineme(*a)
		except OSError as e:
			if e.errno == errno.EPIPE:
				exit(_RC_EPIPE)
			raise

	def lineme(lined, max_count, after):
		os.close(liner_w)

		colours = cycle([
			(pre_fg0, pre_bg0),
			(pre_fg1, pre_bg1),
		])

		if PY2:
			in_fh = sys.stdin
			errors = 'replace'
		else:
			in_fh = sys.stdin.buffer.raw
			errors = 'surrogateescape'

		if decode_lines:
			if lined:
				def decode_part(part):
					res = []
					for part in part.split('\\n'):
						part = part.strip('\r')
						if line_bg:
							res.append('\x1b[K')
						res.append(part)
						if line_bg and '\r' not in part:
							res.append('\x1b[K')
						res.append('\n')
					return ''.join(res[:-1]) # final \n is added in the main loop
			else:
				# When not lined the transform should be completely transparent
				def decode_part(part):
					return part.replace('\\n', '\n')

		for line in in_fh:
			line_fg, line_bg = next(colours)
			line = line.strip(b'\r\n').decode('utf-8', errors)
			has_cr = ('\r' in line)
			if max_count is not None:
				if line == '':
					# Empty lines mark the end of output sections, so if we
					# see one when showing the final context we stop.
					if max_count == 0:
						break
					continue
				if line[0] in 'MC': # don't count "I"nfo lines, only "M"atches and "C"ontext
					if line[0] == 'M' and max_count:
						max_count -= 1
					elif max_count == 0 and after > 0:
						after -= 1
				line = line[1:]
			if decode_lines:
				line = '\\'.join(decode_part(part) for part in line.split('\\\\'))
			if lined:
				todo = iter(line)
				data = []
				if line_fg and line_bg:
					data.append('\x1b[%s;%sm' % (line_fg, line_bg,))
				elif line_bg:
					data.append('\x1b[%sm' % (line_bg,))
				elif line_fg:
					data.append('\x1b[%sm' % (line_fg,))
				if line_bg and not decode_lines:
					data.append('\x1b[K') # try to fill the line with bg (if terminal does BCE)
				for c in todo:
					if c == '\x1b':
						data.extend(collect_escseq(todo, line_fg, line_bg))
					else:
						data.append(c)
				if line_bg and not has_cr and not decode_lines:
					# the line might have been long, so if safe and needed try
					# again to fill the line with bg (if terminal does BCE)
					data.append('\x1b[K')
				data.append('\x1b[m\n')
				data = ''.join(data).encode('utf-8', errors)
			else:
				data = line.encode('utf-8', errors) + b'\n'
			while data:
				data = data[os.write(1, data):]
			if max_count is not None and max_count == after == 0:
				break
	liner_r, liner_w = os.pipe()
	liner_process = mp.SimplifiedProcess(
		target=wrap_EPIPE,
		args=(lined, max_count, after,),
		stdin=liner_r,
		name=colour_prefix + '-liner',
	)
	os.close(liner_r)
	saved_stdout = os.dup(1)
	os.dup2(liner_w, 1) # this is stdout for the parent process now
	os.close(liner_w)
	return Liner(liner_process, saved_stdout)
