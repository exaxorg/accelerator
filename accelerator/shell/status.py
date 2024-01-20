# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2023-2024 Carl Drougge                                     #
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

from accelerator.build import fmttime
from accelerator.error import JobError
from accelerator.job import Job
from accelerator.shell.parser import ArgumentParser
from accelerator.statmsg import print_status_stacks
from accelerator.unixhttp import call

from datetime import datetime
import sys

def main(argv, cfg):
	parser = ArgumentParser(prog=argv.pop(0), description='''show server status (like ^T when building)''')
	parser.add_argument('-s', '--short', action='store_true', negation='not', help="single line summary")
	args = parser.parse_intermixed_args(argv)
	status = call(cfg.url + '/status/full')
	if status.idle:
		print('Idle.')
		if 'last_error_time' in status and not args.short:
			error = call(cfg.url + '/last_error')
			t = datetime.fromtimestamp(error.time).replace(microsecond=0)
			print()
			print('Last error at %s:' % (t,))
			for jobid, method, status in error.last_error:
				e = JobError(Job(jobid, method), method, status)
				print(e.format_msg(), file=sys.stderr)
	else:
		if args.short:
			t = fmttime(status.report_t - status.current[0], True)
			print('%s (%s)' % (status.current[1], t))
		else:
			print_status_stacks(status.status_stacks)

