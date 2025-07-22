# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2024 Carl Drougge                       #
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

# a few things that differ between python2 and python3

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

import sys
try:
	from setproctitle import setproctitle as _setproctitle, getproctitle
	# setproctitle init may be delayed until called (v1.2+) and should happen early
	getproctitle()
	del getproctitle
except ImportError:
	def _setproctitle(title): pass

import builtins
FileNotFoundError = builtins.FileNotFoundError
PermissionError = builtins.PermissionError
import pickle
from urllib.parse import quote as url_quote, quote_plus, unquote_plus, urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
izip = zip
from itertools import zip_longest as izip_longest
imap = map
ifilter = filter
from queue import Queue, Full as QueueFull, Empty as QueueEmpty
import selectors
from time import monotonic
NoneType = type(None)
str_types = (str,)
int_types = (int,)
num_types = (int, float,)
unicode = str
open = builtins.open
long = int
def iterkeys(d):
	return iter(d.keys())
def itervalues(d):
	return iter(d.values())
def iteritems(d):
	return iter(d.items())
def getarglist(func):
	from inspect import getfullargspec
	return getfullargspec(func).args
from shutil import get_terminal_size as terminal_size
from shlex import quote as shell_quote
import datetime
UTC = datetime.timezone.utc
del datetime

def first_value(d):
	return next(itervalues(d) if isinstance(d, dict) else iter(d))

def uni(s):
	if s is None:
		return None
	if isinstance(s, bytes):
		try:
			return s.decode('utf-8')
		except UnicodeDecodeError:
			return s.decode('iso-8859-1')
	return unicode(s)

def url_quote_more(s):
	return quote_plus(s).replace('+', '%20')

if sys.version_info < (3, 6):
	fmt_num = '{:n}'.format
else:
	def fmt_num(num):
		if isinstance(num, float):
			return '{:_.6g}'.format(num)
		else:
			return '{:_}'.format(num)

# This is used in the method launcher to set different titles for each
# phase/slice. You can use it in the method to override that if you want.
def setproctitle(title):
	from accelerator import g
	if hasattr(g, 'params'):
		title = '%s %s (%s)' % (g.job, uni(title), g.params.method,)
	elif hasattr(g, 'job'):
		title = '%s %s' % (g.job, uni(title),)
	else:
		title = uni(title)
	_setproctitle(title)
