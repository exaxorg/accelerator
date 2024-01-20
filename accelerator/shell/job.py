# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2020-2024 Carl Drougge                                     #
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

from traceback import print_exc
from datetime import datetime
import errno
import json
from argparse import RawTextHelpFormatter
import os
import sys

from accelerator.build import fmttime
from accelerator.colourwrapper import colour
from accelerator.error import NoSuchJobError
from accelerator.setupfile import encode_setup
from accelerator.compat import FileNotFoundError, url_quote, urlencode
from accelerator.unixhttp import call
from .parser import name2job, ArgumentParser

def show(url, job, verbose, show_output):
	setup = job.json_load('setup.json')
	is_build = setup.get('is_build', False)
	if verbose:
		print(colour(job.path, 'job/header'))
		print(colour('=' * len(job.path), 'job/header'))
		setup.pop('_typing', None)
		setup.starttime = str(datetime.fromtimestamp(setup.starttime))
		if 'endtime' in setup:
			setup.endtime = str(datetime.fromtimestamp(setup.endtime))
		print(encode_setup(setup, as_str=True))
	else:
		starttime = datetime.fromtimestamp(setup.starttime).replace(microsecond=0)
		hdr = '%s (%s) at %s' % (job, job.method, starttime,)
		if 'exectime' in setup:
			hdr = '%s in %s' % (hdr, fmttime(setup.exectime.total),)
		print(colour(hdr, 'job/header'))
		if job.is_build:
			print(colour('  build job', 'job/highlight'))
		if job.parent:
			built_from = "  built from %s (%s)" % (job.parent, job.parent.method,)
			if job.build_job and job.parent != job.build_job:
				built_from = "%s, build job %s (%s)" % (built_from, job.build_job, job.build_job.method,)
			print(colour(built_from, 'job/highlight'))
		things = []
		def opt_thing(name):
			value = setup[name]
			if value:
				if isinstance(value, dict) and len(value) > 1:
					value = json.dumps(value, indent=4, sort_keys=True).replace('\n', '\n    ')
				else:
					value = json.dumps(value)
				things.append((name, value,))
		opt_thing('caption')
		opt_thing('options')
		opt_thing('datasets')
		opt_thing('jobs')
		for k, v in things:
			print('    %s: %s' % (k, v,))
	def list_of_things(name, things):
		total = len(things)
		if total > 5 and not verbose:
			things = things[:3]
		print()
		print(colour('%s:' % (name,), 'job/header'))
		for thing in things:
			print('   ', thing)
		if total > len(things):
			print('    ... and %d more' % (total - len(things),))
	if job.datasets:
		list_of_things('datasets', [ds.quoted for ds in job.datasets])
	try:
		post = job.json_load('post.json')
	except FileNotFoundError:
		print(colour('WARNING: Job did not finish', 'job/warning'))
		post = None
	if post and post.subjobs:
		postdata = urlencode({'jobs': '\0'.join(post.subjobs)}).encode('utf-8')
		subjobs = call(url + '/jobs_are_current', data=postdata)
		fmted_subjobs = []
		for sj, is_current in sorted(subjobs.items()):
			if not is_current:
				sj += ' ' + colour('(not current)', 'job/info')
			fmted_subjobs.append(sj)
		list_of_things('subjobs', fmted_subjobs)
	if post and post.files:
		files = sorted(post.files)
		if verbose:
			files = [job.filename(fn) for fn in files]
		list_of_things('files', files)
	if post and not is_build and not call(url + '/job_is_current/' + url_quote(job)):
		print(colour('Job is not current', 'job/info'))
	print()
	out = job.output('parts')
	if show_output:
		if out:
			if not verbose: # verbose prints section headers in show_output_d()
				print(colour('output:', 'job/header'))
			show_output_d(out, verbose)
		else:
			print(job, 'produced no output')
			print()
	elif out:
		print('%s produced %d bytes of output, use --output/-o to see it' % (job, sum(len(v) for v in out.values()),))
		print()

def show_source(job, pattern='*'):
	import tarfile
	from fnmatch import fnmatch
	with tarfile.open(job.filename('method.tar.gz'), 'r:gz') as tar:
		all_members = [info for info in tar.getmembers() if info.isfile()]
		members = [info for info in all_members if fnmatch(info.path, pattern)]
		if not members:
			if pattern:
				print(colour('No sources matching %r in %s.' % (pattern, job,), 'job/warning'), file=sys.stderr)
				fh = sys.stderr
				res = 1
			else:
				fh = sys.stdout
				res = 0
			print('Available sources:', file=fh)
			for info in all_members:
				print('    ' + info.path, file=fh)
			return res
		for ix, info in enumerate(members, 1):
			if len(members) > 1:
				print(colour(info.path, 'job/header'))
				print(colour('=' * len(info.path), 'job/header'))
			data = tar.extractfile(info).read()
			has_nl = data.endswith(b'\n')
			while data:
				data = data[os.write(1, data):]
			if not has_nl:
				os.write(1, b'\n')
			if ix < len(members):
				os.write(1, b'\n')
	return 0

def show_file(job, pattern):
	try:
		files = job.files(pattern)
	except FileNotFoundError:
		# Probably the job didn't finish, let's not explode
		files = []
	if not files and pattern and os.path.exists(job.filename(pattern)):
		# special case so you can name unregistered files
		files = [pattern]
	if not files:
		if pattern:
			fh = sys.stderr
			print(colour('No files matching %r in %s.' % (pattern, job,), 'job/warning'), file=fh)
			res = 1
		else:
			fh = sys.stdout
			res = 0
		try:
			files = job.files()
		except FileNotFoundError:
			print(colour('Job did not finish - unable to list files', 'job/warning'), file=sys.stderr)
			return 1
		print('Available files:', file=fh)
		for fn in files:
			print('    ' + fn, file=fh)
		return res
	for ix, fn in enumerate(files, 1):
		if len(files) > 1:
			print(colour(fn, 'job/header'))
			print(colour('=' * len(fn), 'job/header'))
		with job.open(fn, 'rb') as fh:
			data = fh.read()
		has_nl = data.endswith(b'\n')
		while data:
			data = data[os.write(1, data):]
		if not has_nl:
			os.write(1, b'\n')
		if ix < len(files):
			os.write(1, b'\n')
	return 0

def show_output_d(d, verbose):
	first = True
	for k, out in d.items():
		if out:
			if verbose:
				if first:
					first = False
				else:
					print()
				if isinstance(k, int):
					k = 'analysis(%d)' % (k,)
				print(colour(k, 'job/header'))
				print(colour('=' * len(k), 'job/header'))
			print(out, end='' if out.endswith('\n') else '\n')

def main(argv, cfg):
	descr = 'show setup.json, dataset list, etc for jobs'
	parser = ArgumentParser(
		prog=argv.pop(0),
		description=descr,
		formatter_class=RawTextHelpFormatter,
	)
	parser.add_argument('-v', '--verbose', action='store_true', negation='not', help='more output (e.g the whole setup.json)')
	group = parser.add_mutually_exclusive_group()
	group.add_argument('-o', '--output', action='store_true', help='show job output')
	group.add_argument('-O', '--just-output', action='store_true', help='show only job output')
	group.add_argument('-P', '--just-path', action='store_true', help='show only job path')
	group.add_argument('-s', '--source', action='store_true', help='show source used to produce job')
	group.add_argument('-S', '--source-file', metavar='PATTERN', nargs='?', const='', help='show specified file(s) from source used to produce job')
	group.add_argument('-f', '--file', metavar='PATTERN', nargs='?', const='', help='show specified file(s) produced by job')
	parser.add_argument(
		'jobid',
		nargs='+', metavar='jobid/jobspec',
		help='jobid is just a jobid.\n' +
		     'you can also use path, method or :urdlist:[entry].\n' +
		     'path is to a jobdir (with setup.json in it).\n' +
		     'method is the latest job with that method.\n' +
		     ':urdlist:[entry] looks up jobs in urd. details are in the\n' +
		     'urd help, except here entry defaults to -1 and you can\'t\n' +
		     'list things (no .../ or .../since/x).\n' +
		     'you can use spec~ or spec~N to go back N jobs\n' +
		     'with that method or spec^ or spec^N to follow .previous\n' +
		     'use spec! to only consider current jobs.\n' +
		     'you can also do things like spec.source.'
	)
	args = parser.parse_intermixed_args(argv)
	res = 0
	for path in args.jobid:
		try:
			job = name2job(cfg, path)
			if args.just_output:
				show_output_d(job.output('parts'), args.verbose)
			elif args.just_path:
				print(job.path)
			elif args.source:
				res |= show_source(job)
			elif args.source_file is not None:
				res |= show_source(job, args.source_file)
			elif args.file is not None:
				res |= show_file(job, args.file)
			else:
				show(cfg.url, job, args.verbose, args.output)
		except NoSuchJobError as e:
			print(e)
			res = 1
		except Exception as e:
			if isinstance(e, OSError) and e.errno == errno.EPIPE:
				raise
			print_exc(file=sys.stderr)
			print("Failed to show %r" % (path,), file=sys.stderr)
			res = 1
	return res
