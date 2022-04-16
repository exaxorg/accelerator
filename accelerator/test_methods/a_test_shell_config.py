############################################################################
#                                                                          #
# Copyright (c) 2021 Carl Drougge                                          #
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
from subprocess import check_output

config = '''
[alias]
	path = job -P

[colour]
	warning = CYAN
'''

def ax_cmd(*a):
	cmd = options.command_prefix + list(a)
	res = check_output(cmd)
	res = res.decode('utf-8', 'replace')
	return res.strip()

def synthesis(job):
	os.putenv('XDG_CONFIG_HOME', job.path)
	os.putenv('CLICOLOR_FORCE', '1')
	os.mkdir('accelerator')
	with open('accelerator/config', 'w') as fh:
		fh.write(config)
	# test an alias
	assert ax_cmd('path', job) == job.path
	# test the colour config
	assert '\x1b[36mWARNING: Job did not finish\x1b[39m' in ax_cmd('job', job)
	# test that a more specific colour config wins
	with open('accelerator/config', 'a') as fh:
		fh.write('\tjob/warning = BOLD #030405')
	assert '\x1b[1;38:2:3:4:5mWARNING: Job did not finish\x1b[22;39m' in ax_cmd('job', job)
	# test literals in the config
	with open('accelerator/config', 'w') as fh:
		fh.write(config.replace('CYAN', '<FOO >BAR'))
	assert 'FOOWARNING: Job did not finishBAR' in ax_cmd('job', job)
	# combining literals with named attributes, and escapes
	with open('accelerator/config', 'a') as fh:
		fh.write('\tjob/warning = BOLD BLUE <\\x18\\\\\\eFOO\T >\\?\\\\eBAR\E UNDERLINE')
	assert '\x1b[1;34m\x18\\\x1bFOO\t\x1b[4mWARNING: Job did not finish\x1b[22;24;39m\\?\\eBAR\x1b' in ax_cmd('job', job)
