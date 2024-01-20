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
Test the alias and colour config for shell commands.
'''

options = dict(
	command_prefix=['ax', '--config', '/some/path/here'],
)

import os
from subprocess import check_output, CalledProcessError

config = '''
[alias]
	path = job -P

[colour]
'''

def ax_cmd(*a):
	cmd = options.command_prefix + list(a)
	res = check_output(cmd)
	res = res.decode('utf-8', 'replace')
	return res.strip()

def synthesis(job):
	os.putenv('XDG_CONFIG_HOME', job.path)
	os.putenv('ACCELERATOR_IGNORE_ETC', 'Y') # make sure we can't be messed up by global config
	os.putenv('CLICOLOR_FORCE', '1')
	os.mkdir('accelerator')
	with open('accelerator/config', 'w') as fh:
		fh.write(config)
	# test an alias
	assert ax_cmd('path', job) == job.path

	def chk(cfgstr, pre, post):
		with open('accelerator/config', 'w') as fh:
			fh.write(config)
			fh.write(cfgstr)
		try:
			got = ax_cmd('job', job).split('\n')[-1]
		except CalledProcessError as e:
			raise Exception("%r could not run: %s" % (cfgstr, e,))
		want = '%sWARNING: Job did not finish%s' % (pre, post,)
		if got != want:
			raise Exception("%r:\nWanted %r\ngot    %r" % (cfgstr, want, got,))

	# test the colour config
	chk('\twarning = CYAN', '\x1b[36m', '\x1b[39m')
	# test that a more specific colour config wins
	chk('\twarning = CYAN\n\tjob/warning = BOLD #030405', '\x1b[1;38:2:3:4:5m', '\x1b[22;39m')
	# test literals in the config
	chk('\twarning = <FOO >BAR', 'FOO', 'BAR')
	# combining literals with named attributes, and escapes
	chk(
		'\twarning = <FOO >BAR\n\tjob/warning = BOLD BLUE <\\x18\\\\\\eFOO\\T >\\?\\\\eBAR\\E UNDERLINE',
		'\x1b[1;34m\x18\\\x1bFOO\t\x1b[4m',
		'\x1b[22;24;39m\\?\\eBAR\x1b',
	)

	# test various other colour formats in config
	for cfgstr, pre, post in (
		('\twarning = #123', '\x1b[38:5:67m', '\x1b[39m'),
		('\twarning = #0 #1cbg', '\x1b[38:5:16;48:5:255m', '\x1b[39;49m'),
		('\twarning = #1d Xffbg', '\x1b[38:5:231;48:5:255m', '\x1b[39;49m'),
		('\twarning = X10 #20ff00bg', '\x1b[38:5:16;48:2:32:255:0m', '\x1b[39;49m'),
	):
		chk(cfgstr, pre, post)
