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

import sys, os
from functools import partial

from accelerator.compat import PY2
from accelerator.error import ColourError

# all gray colours in the 256 colour palette in intensity order
_gray2idx = tuple(ix for _, ix in sorted(
	[(0, 16)] +
	[(gray * 10 + 8, gray + 232) for gray in range(24)] +
	[(rgb * 40 + 55, rgb * 43 + 16) for rgb in (1, 2, 3, 4, 5)]
))

class Colour:
	r"""Constants and functions for colouring output.

	Available as constants named .COLOUR, functions named .colour and
	as direct calls on the object taking (value, *attrs).
	Colours are BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE,
	These can be prefixed with BRIGHT and/or suffixed with BG.
	DEFAULT[BG] restores the default colour.

	BOLD, FAINT, ITALIC, UNDERLINE, BLINK, INVERT, and STRIKE are also
	available. These can be prefixed with NOT to turn them off.

	When using the constants, you should usually end with .RESET.

	>>> colour.RED + 'foo' + colour.DEFAULT == colour.red('foo') == colour('foo', 'red')

	colour(v, 'red', 'bold') and similar produce shorter sequences than other
	ways of combining several attributes.

	You can also use:
		colour(v, '#RGB[bg]') (0 - 5) ("256 colour" mode)
		colour(v, '#GG[bg]') (00 - 1D) (grayscale from "256 colour" mode)
		colour(v, 'XNN[bg]') (00 - FF) (directly specifying a "256 colour" index)
		colour(v, '#RRGGBB[bg]') (00 - FF) (but terminal support is not great)

	Finally you can use names you put in the config file, some of which
	have default values. This also has a fallback system, so 'foo/bar' will
	fall back to 'bar' if 'foo/bar' is not defined. The defaults can be
	found in shell/__init__.py (search for "configuration defaults").

	In the configuration file you can also use <pre >post to just put
	those literal strings around the value. e.g.
	[colour]
		grep/header = <\e[4:3m >\e[24m
	if your terminal supports such extended codes.

	The functions take force=True to return escape sequences even if colour
	is disabled and reset=True to reset all attributes before (and after)
	this sequence. (By default only the changed attributes are reset.)
	"""

	def __init__(self):
		self._names = {}
		self._all = dict(
			BOLD='1',
			FAINT='2',
			ITALIC='3',
			UNDERLINE='4',
			BLINK='5',
			INVERT='7',
			STRIKE='9',
		)
		self._all.update([('NOT' + k, '2' + v) for k, v in self._all.items()])
		self._all['NOTBOLD'] = '22'
		for num, name in enumerate([
			'BLACK', 'RED', 'GREEN', 'YELLOW',
			'BLUE', 'MAGENTA', 'CYAN', 'WHITE',
		]):
			for prefix, base in (('', 30), ('BRIGHT', 90)):
				self._all[prefix + name] = str(base + num)
				self._all[prefix + name + 'BG'] = str(base + 10 + num)
		self._all['DEFAULT'] = '39'
		self._all['DEFAULTBG'] = '49'
		for k in self._all:
			setattr(self, k.lower(), partial(self._single, k))
		self._on = {k: '\x1b[%sm' % (v,) for k, v in self._all.items()}
		self._on['RESET'] = '\x1b[m'
		self._all['RESET'] = ''
		if PY2:
			self._on = {k.encode('ascii'): v.encode('ascii') for k, v in self._on.items()}
			self._off = dict.fromkeys(self._on, b'')
		else:
			self._off = dict.fromkeys(self._on, '')
		self.enable()
		self.__all__ = [k for k in dir(self) if not k.startswith('_')]
		self._lined = False

	def configure_from_environ(self, environ=None, stdout=None):
		# trying to support both https://no-color.org/ and https://bixense.com/clicolors/
		if environ is None:
			environ = os.environ
		if environ.get('CLICOLOR_FORCE', '0') != '0':
			self.enable()
		elif environ.get('NO_COLOR') is not None:
			self.disable()
		elif environ.get('CLICOLOR', '1') != '0' and (stdout or sys.stdout).isatty():
			self.enable()
		else:
			self.disable()

	def enable(self):
		"Turn colours on"
		self.__dict__.update(self._on)
		self.enabled = True

	def disable(self):
		"Turn colours off (make all constants empty)"
		self.__dict__.update(self._off)
		self.enabled = False

	def _single(self, attr, value, reset=False, force=False):
		return self(value, attr, reset=reset, force=force)

	def _expand_names(self, things):
		for thing in things:
			orig_thing = thing
			if self._lined:
				thing = 'lined/' + thing
			if thing not in self._names and '/' in thing:
				# allow up to three elements ("a/b/c"), try both "b/c" and "a/c"
				# (so cmd/blah wins over lined/blah if defined.)
				prefix, thing = thing.split('/', 1)
				if thing not in self._names and '/' in thing:
					middle, thing = thing.split('/', 1)
					a_c = prefix + '/' + thing
					if a_c in self._names:
						thing = a_c
			if thing in self._names:
				for a in self._names[thing]:
					yield orig_thing, a
			else:
				yield orig_thing, thing

	def _literal_split(self, pieces):
		have = []
		for piece in pieces:
			if piece.startswith('<'):
				if have:
					yield '\x1b[' + ';'.join(have) + 'm'
					have = []
				yield piece[1:]
			else:
				have.append(piece)
		if have:
			yield '\x1b[' + ';'.join(have) + 'm'

	# When we drop python 2 we can change this to use normal keywords
	def pre_post(self, *attrs, **kw):
		bad_kw = set(kw) - {'force', 'reset'}
		if bad_kw:
			raise TypeError('Unknown keywords %r' % (bad_kw,))
		if not attrs:
			raise TypeError('specify at least one attr')
		if (self.enabled or kw.get('force')):
			if kw.get('reset'):
				pre = ['0']
			else:
				pre = []
			post = set()
			literal_post = ''
			a_it = self._expand_names(attrs)
			for a_src, a in a_it:
				if not a:
					raise ColourError('%r expanded to nothing' % (a_src,))
				if a.startswith('>'):
					raise ColourError('A >post needs a preceding <pre (expanded %r from %r)' % (a, a_src,))
				if a.startswith('<'):
					try:
						a_post_src, a_post = next(a_it)
					except StopIteration:
						a_post_src = a_post = ''
					if not a_post.startswith('>') or a_src != a_post_src:
						raise ColourError('A <pre needs a following >post (expanded %r from %r)' % (a, a_src,))
					literal_post += a_post[1:]
					pre.append(a)
					continue
				want = a.upper()
				default = self._all['DEFAULTBG' if want.endswith('BG') else 'DEFAULT']
				if want[0] in '#X':
					if want.endswith('BG'):
						prefix = '48'
						want = want[:-2]
					else:
						prefix = '38'
					try:
						if want[0] == '#' and len(want) == 7:
							r, g, b = (str(int(w, 16)) for w in (want[1:3], want[3:5], want[5:7]))
							part = (prefix, '2', r, g, b)
						else:
							if want[0] == '#' and len(want) == 4:
								r, g, b = (int(w, 16) for w in want[1:])
								assert 0 <= r <= 5 and 0 <= g <= 5 and 0 <= b <= 5
								idx = r * 36 + g * 6 + b + 16
							elif want[0] == '#' and len(want) in (2, 3):
								gg = int(want[1:], 16)
								assert 0 <= gg < len(_gray2idx)
								idx = _gray2idx[gg]
							elif want[0] == 'X' and len(want) in (2, 3):
								idx = int(want[1:], 16)
								assert 0 <= idx <= 255
							else:
								raise ValueError()
							part = (prefix, '5', str(idx))
					except (ValueError, AssertionError):
						raise ColourError('Bad colour spec %r (from %r)' % (a, a_src,))
					pre.append(':'.join(part))
					post.add(default)
				else:
					if want not in self._all:
						raise ColourError('Unknown colour/attr %r (from %r)' % (a, a_src,))
					pre.append(self._all[want])
					post.add(self._all.get('NOT' + want, default))
			pre = ''.join(self._literal_split(pre))
			if kw.get('reset'):
				post = '\x1b[m' + literal_post
			elif post:
				post = '\x1b[' + ';'.join(sorted(post)) + 'm' + literal_post
			else:
				post = literal_post
		else:
			pre = post = ''
		return pre, post

	# When we drop python 2 we can change this to use normal keywords
	def __call__(self, value, *attrs, **kw):
		pre, post = self.pre_post(*attrs, **kw)
		if isinstance(value, bytes):
			return b'%s%s%s' % (pre.encode('utf-8'), value, post.encode('utf-8'),)
		return '%s%s%s' % (pre, value, post,)

colour = Colour()
colour.configure_from_environ()
