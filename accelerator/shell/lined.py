############################################################################
#                                                                          #
# Copyright (c) 2022 Carl Drougge                                          #
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
import os
import sys

from accelerator.colourwrapper import colour
from accelerator.compat import PY2
from accelerator import mp


def split_colour(spec):
	seq = colour('/', spec)
	if seq == '/':
		return '', ''
	seq = seq.split('/', 1)[0] # we only want the setting, not the unsetting
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


def enable_lines(colour_prefix, process_setup=lambda: None, decode_lines=False):
	pre_fg0, pre_bg0 = split_colour(colour_prefix + '/oddlines')
	pre_fg1, pre_bg1 = split_colour(colour_prefix + '/evenlines')
	if pre_fg0 == pre_bg0 == pre_fg1 == pre_bg1 == '':
		return

	def lineme():
		os.close(liner_w)
		process_setup()

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
		for line in in_fh:
			line_fg, line_bg = next(colours)
			line = line.rstrip(b'\n').decode('utf-8', errors)
			if decode_lines:
				line = '\\'.join(part.replace('\\n', '\x1b[K\n' if line_bg else '\n') for part in line.split('\\\\'))
			todo = iter(line)
			data = []
			if line_fg and line_bg:
				data.append('\x1b[%s;%sm' % (line_fg, line_bg,))
			elif line_bg:
				data.append('\x1b[%sm' % (line_bg,))
			elif line_fg:
				data.append('\x1b[%sm' % (line_fg,))
			for c in todo:
				if c == '\x1b':
					data.extend(collect_escseq(todo, line_fg, line_bg))
				else:
					data.append(c)
			if line_bg:
				data.append('\x1b[K') # try to fill the line with bg (if terminal does BCE)
			data.append('\x1b[m\n')
			data = ''.join(data).encode('utf-8', errors)
			while data:
				data = data[os.write(1, data):]
	liner_r, liner_w = os.pipe()
	liner_process = mp.SimplifiedProcess(
		target=lineme,
		stdin=liner_r,
		name=colour_prefix + '-liner',
	)
	os.close(liner_r)
	os.dup2(liner_w, 1) # this is stdout for the parent process now
	os.close(liner_w)
	return liner_process
