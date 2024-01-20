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

from accelerator.error import AcceleratorError
from accelerator.job import Job
from accelerator.shell.parser import ArgumentParser

import os
import shutil
import sys

def main(argv, cfg):
	parser = ArgumentParser(prog=argv.pop(0), description='''delete stale and unused jobs''')
	parser.add_argument('-n', '--dry-run', action='store_true', negation='not', help="don't delete anything, just print")
	args = parser.parse_intermixed_args(argv)

	to_delete = []
	def candidate(jid):
		job = Job(jid)
		try:
			job.post
		except (OSError, AcceleratorError):
			to_delete.append(job)

	for name, path in cfg.workdirs.items():
		for jid in os.listdir(path):
			if '-' in jid:
				wd, num = jid.rsplit('-', 1)
				if wd == name and num.isdigit():
					candidate(jid)
	if not to_delete:
		print("Nothing to do.")
		return 0
	if args.dry_run:
		print("Would have deleted %d jobs" % (len(to_delete),))
		return 0
	print("Deleting %d jobs" % (len(to_delete),))
	for job in to_delete:
		shutil.rmtree(job.path)
