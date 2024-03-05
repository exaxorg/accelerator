# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2024 Carl Drougge                                          #
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

from datetime import datetime
import os
import sys

from accelerator.colourwrapper import colour
from accelerator.job import Job
from accelerator.metadata import extract_metadata, b64hash_setup
from .parser import ArgumentParser


def validate(data):
	warnings = []
	job = data.job.rsplit('/', 1)[-1]
	try:
		job = Job(job)
		job.path # may trigger NoSuchWorkdirError
	except Exception:
		job = None
	if job:
		if job.path != data.job:
			warnings.append("path mismatch (%r != %r)" % (job.path, data.job,))
	else:
		warnings.append("unknown job")
	h = b64hash_setup(data.job + '/setup.json')
	if h != data.setup_hash:
		if h:
			warnings.append("does not match job on disk")
		else:
			warnings.append("does not exist on disk")
	return warnings

def main(argv, cfg):
	descr = "show which job produced a file, for files downloaded through board."
	parser = ArgumentParser(
		prog=argv.pop(0),
		description=descr,
	)
	parser.add_argument('-v', '--verbose', action='store_true', negation='not', help='more output')
	parser.add_argument('filename', nargs='+')
	args = parser.parse_intermixed_args(argv)
	res = 0
	os.chdir(cfg.user_cwd)
	for filename in args.filename:
		if len(args.filename) > 1:
			prefix = filename + ': '
		else:
			prefix = ''
		with open(filename, 'rb') as fh:
			found = extract_metadata(filename, fh)
			if not found:
				res = 1
				print(prefix + colour('UNSUPPORTED FORMAT', 'sherlock/warning'), file=sys.stderr)
				continue
			try:
				found = list(found)
			except Exception:
				found = [None]
			if not found:
				res = 1
				if args.verbose or len(args.filename) > 1:
					print(prefix + colour('no data found', 'sherlock/warning'), file=sys.stderr)
				continue
			for data in found:
				if data is None:
					res = 1
					print(prefix + colour('decoding error', 'sherlock/warning'), file=sys.stderr)
					continue
				if args.verbose:
					print('%s%s' % (prefix, data.job,), end='')
				else:
					print('%s%s' % (prefix, data.job.rsplit('/', 1)[-1],), end='')
				warnings = validate(data)
				if warnings:
					print(' ' + colour(', '.join(warnings), 'sherlock/warning'), end='')
				if args.verbose:
					ts = datetime.fromtimestamp(data.time)
					print(' (%s at %s on %s)' % (data.method, ts, data.host), end='')
				print()
	return res
