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
Test the "ax job" shell command. Primarily tests the job spec parser.
'''

options = dict(
	command_prefix=['ax', '--config', '/some/path/here'],
	want={'spec': 'jobid'},
)

import os
from subprocess import check_output

def ax_job(*a):
	cmd = options.command_prefix + ['job'] + list(a)
	print(cmd)
	res = check_output(cmd)
	res = res.decode('utf-8', 'replace')
	print(res)
	return res.split('\n')

def synthesis(job):
	os.putenv('XDG_CONFIG_HOME', job.path) # make sure we can't be messed up by config
	os.putenv('ACCELERATOR_IGNORE_ETC', 'Y') # make sure we can't be messed up by global config
	os.putenv('CLICOLOR_FORCE', '1')
	res = ax_job(job)
	assert job in res[0], res[0]
	assert '\x1b[31mWARNING: Job did not finish\x1b[39m' in res
	os.unsetenv('CLICOLOR_FORCE')
	os.putenv('NO_COLOR', '')
	res = ax_job(job)
	assert job in res[0], res[0]
	assert 'WARNING: Job did not finish' in res
	for spec, jobid in options.want.items():
		res = ax_job(spec)
		got_jobid = res[0].split('/')[-1]
		assert got_jobid.startswith(jobid), 'Spec %r should have given %r but gave %r' % (spec, jobid, got_jobid,)
