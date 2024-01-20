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

description = r'''
Test that job.finish_early() works in all three stages.
'''

options = dict(
	when = str,
)

from accelerator import subjobs

def run_test():
	for when, want_res, want_ds in (
		('prepare', 'foo', ['foo']),
		('analysis', None, ['a']),
		('synthesis', 'baz', ['bar']),
		('finish', 'finished', ['bar']),
	):
		sj = subjobs.build('test_finish_early', when=when)
		try:
			got_res = sj.load()
		except IOError:
			got_res = None
		assert want_res == got_res
		got_ds = list(sj.dataset().iterate(0, 'thing'))
		assert want_ds == got_ds

def prepare(job):
	if not options.when:
		run_test()
		job.finish_early()
		assert not 'reached'
	dw = job.datasetwriter()
	dw.add('thing', 'ascii')
	if options.when == 'prepare':
		dw.get_split_write()('foo')
		job.finish_early('foo')
	return dw

def analysis(sliceno, prepare_res, job):
	if options.when == 'analysis':
		prepare_res.write('a')
		job.finish_early()
	prepare_res.write('bar')

def synthesis(job):
	if options.when == 'synthesis':
		job.finish_early('baz')
	return 'finished'
