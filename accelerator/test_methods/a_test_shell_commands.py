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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

description = r'''
Test all the ax sub-commands at least a little bit.
'''

options = dict(
	command_prefix=['ax', '--config', '/some/path/here'],
)

import os
from subprocess import check_output

# this is for job --source to have a second file
depend_extra = ('test_shell_commands.txt',)

def ax(cmd):
	cmd = options.command_prefix + cmd
	print(cmd)
	res = check_output(cmd)
	res = res.decode('utf-8', 'replace')
	return res

all_checked = set()

def chk(cmd, want_in_help=[], want_in_call=[], dont_want_in_call=[]):
	all_checked.add(cmd[0])
	for args, want_l, dont_want_l in (
		([cmd[0], '-h'], ['--help'] + want_in_help, []),
		(cmd, want_in_call, dont_want_in_call),
	):
		if not want_l and not dont_want_l:
			continue
		output = ax(args)
		for want in want_l:
			if want not in output:
				print("Expected to find %r in %r output:\n\n%s\n\nbut didn't." % (want, args, output,))
				raise Exception("Failed in command %r" % (cmd[0],))
		for dont_want in dont_want_l:
			if dont_want in output:
				print("Did not expect to find %r in %r output:\n\n%s\n\nbut did." % (dont_want, args, output,))
				raise Exception("Failed in command %r" % (cmd[0],))

def synthesis(job):
	print('look for this later')
	with job.open('filename', 'w') as fh:
		in_written_file = 'stuff\nin\na\nfile\n' # \n is not literal here - not seen in this file
		fh.write(in_written_file)
	dw = job.datasetwriter(columns={'this is a column': 'ascii'})
	dw.get_split_write()('this is a value')
	ds = dw.finish()
	os.putenv('XDG_CONFIG_HOME', job.path) # make sure we can't be messed up by config
	os.putenv('ACCELERATOR_IGNORE_ETC', 'Y') # make sure we can't be messed up by global config
	os.unsetenv('CLICOLOR_FORCE')
	chk(['-h'], want_in_help=['\ncommands:\n', '\naliases:\n'])
	chk(['abort'], want_in_help=['--quiet'])
	chk(['alias', 'cat'], want_in_help=['shows all aliases'], want_in_call=["grep -e ''"])
	chk(['board-server'], want_in_help=['localhost:8520'])
	chk(['ds', '--', ds], want_in_help=['--list'], want_in_call=['Columns:', 'this is a column'])
	chk(['gc'], want_in_help=['--dry-run'])
	chk(['grep', '--', '', ds], want_in_help=['--tab-length'], want_in_call=['this is a value'])
	chk(['hist', '--', ds, 'this is a column'], want_in_help=['show at most this many'], want_in_call=['this is a value\t1'])
	chk(['init', '--no-git', job.filename('projdir')], want_in_help=['--no-git'], want_in_call=[''])
	assert os.path.isdir('projdir/workdirs')
	chk(['intro'], want_in_call=['ax init --examples', 'ax script'])
	chk(['job', '--', job], want_in_help=['--just-output', ':urdlist:[entry]'], want_in_call=['"command_prefix":'])
	chk(['job', '--just-output', '--', job], want_in_call=['look for this later'], dont_want_in_call=['most other things'])
	in_this_file = 'this string is only here, and will be found here'
	in_extra = 'Magic cookie: ' + 'Delicious!' # different formatting here - not seen in this file
	extra_hdr = 'test_shell_commands.txt\n==============' # \n is not literal here - not seen in this file
	chk(['job', '--source', '--', job], want_in_call=[in_this_file, in_extra, extra_hdr]),
	chk(['job', '--source-file=*.txt', '--', job], want_in_call=[in_extra], dont_want_in_call=[extra_hdr, in_this_file]),
	chk(['job', '--file=filename', '--', job], want_in_call=[in_written_file], dont_want_in_call=['===', in_this_file]),
	chk(['method', 'csvimport'], want_in_help=['lists methods'], want_in_call=['filename'])
	chk(['run'], want_in_help=['[script ...]', 'WORKDIR'])
	chk(['script', 'build_tests'], want_in_help=['describes build scripts'], want_in_call=['Needs at least 3 slices to work.'])
	chk(['server'], want_in_help=['--debuggable'])
	chk(['sherlock'], want_in_help=['downloaded through board'])
	chk(['status'], want_in_help=['--short'], want_in_call=['test_shell_commands (', 'synthesis (', 'TAIL OF OUTPUT:'])
	chk(['status', '--short'], want_in_call=['test_shell_commands synthesis ('])
	chk(['urd'], want_in_help=[':urdlist:[entry]'], want_in_call=['/tests.urd'])
	chk(['urd', 'tests.urd/'], want_in_call=['2021-09-27T03:14'])
	chk(['urd', 'tests.urd/2021-09-27T03:14'], want_in_call=['test_shell_data'])
	chk(['urd-server'], want_in_help=['--allow-passwordless'])
	chk(['version'], want_in_call=['Running on '])
	chk(['workdir'], want_in_help=['--full-path'], want_in_call=[job.workdir])
	chk(['workdir', '--', job.workdir], want_in_call=[job, 'test_shell_commands'])

	ax_help = ax(['--help'])
	cmd_list = ax_help.split('\ncommands:\n', 1)[1].split('\naliases:\n', 1)[0]
	cmd_list = {cmd.split()[0] for cmd in cmd_list.strip().split('\n')}
	missed = cmd_list - all_checked
	if missed:
		raise Exception("Didn't check the following commands: " + ' '.join(sorted(missed)))
	not_in_help = all_checked - cmd_list - {'-h', '--help'}
	if not_in_help:
		raise Exception("Missing in help output: " + ' '.join(sorted(not_in_help)))
