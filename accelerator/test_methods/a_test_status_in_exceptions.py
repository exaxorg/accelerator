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
Test that the JobError exception contains the correct status stack
and line number from dataset iteration.
'''

options = dict(
	die_in=str,
	die_on_lines=[int],
	status1=str,
	status2=str,
)

jobs = ('source',)


from accelerator import subjobs, status, JobError
import re

def iterate(line_a):
	stop_v = line_a[0]
	line_a = line_a[1:]
	for v in jobs.source.dataset('dont_see_this').iterate(0, 'a'):
		pass
	for v in jobs.source.dataset(str(len(line_a))).iterate(0, 'a'):
		if v == stop_v:
			if line_a:
				return iterate(line_a)
			else:
				if options.status2:
					with status(options.status2):
						1/0
				else:
					1/0

def die_here():
	if options.die_on_lines:
		if options.status1:
			with status(options.status1):
				iterate(options.die_on_lines)
		else:
			iterate(options.die_on_lines)
	else:
		with status(options.status1):
			1/0

def mk_ds(job, name):
	dw = job.datasetwriter(name=name, columns={'a': 'int32'}, allow_missing_slices=True)
	dw.set_slice(0)
	for v in range(1, 1000):
		dw.write(v)
	dw.finish()

def prepare(job):
	if not options.die_in:
		mk_ds(job, '0')
		mk_ds(job, '1')
		mk_ds(job, '2')
		mk_ds(job, 'dont_see_this')
	elif options.die_in == 'prepare':
		die_here()

def analysis(sliceno, job):
	if options.die_in == 'analysis' and sliceno == 1:
		die_here()

def synthesis(job):
	if not options.die_in:
		for where, on_line, status1, status2 in (
			('prepare', [], 'status without datasets', None),
			('prepare', [1], None, None),
			('analysis', [643, 84], 'status with two datasets', None),
			('synthesis', [999, 57, 350], None, 'status inside three datasets'),
			('analysis', [811], None, None),
			('prepare', [99, 999], 'before', 'after')
		):
			try:
				jid = subjobs.build(
					'test_status_in_exceptions',
					die_in=where,
					die_on_lines=on_line,
					status1=status1,
					status2=status2,
					source=job,
				)
				raise Exception('Should not have built job ' + jid)
			except JobError as e:
				got = e.format_msg()
				assert 'dont_see_this' not in got, got
				want_re = 'Status when the exception occurred:.*' + where
				if status1:
					want_re += '.*' + status1
				for ix, want_line in enumerate(on_line, 1):
					ds = job.dataset(str(len(on_line) - ix))
					want_re += '.*Iterating %s:0 reached line %d\n' % (re.escape(ds.quoted), want_line,)
				if status2:
					want_re += '.*' + status2
				assert re.search(want_re, got, re.DOTALL), got
				if not on_line:
					assert 'Iterating ' not in got, got
	elif options.die_in == 'synthesis':
		die_here()
