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
from __future__ import division
from __future__ import unicode_literals

description = r'''
Test a stand-alone urd for compatibility with old logs and some calls.
'''

options = dict(
	command_prefix=['ax', '--config', '/some/path/here'],
)

from accelerator.compat import open, url_quote_more
from accelerator.error import UrdPermissionError, UrdConflictError
from accelerator.unixhttp import call
from subprocess import Popen
import json
import os
import time

TEST_LOG_ing = r'''
3|2023-01-01T00:00:00|add|2023-01|test/ing|{}|[["something", "job-0"]]||it's a caption
3|2023-02-01T00:00:01|add|2023-02|test/ing|{"test/ing": {"caption": "it's a caption", "joblist": [["something", "job-0"]], "timestamp": "2023-01"}, "test/two": {"caption": "\"caption\\nwithout escaping\"", "joblist": [["something", "job-1"]], "timestamp": "2023-02"}}|[["something", "job-2"]]||
4	2023-06-19T00:00:00	add	2023-06	test/ing	{}	[["something", "job-3"]]		"caption\nwith lines\nand\ttab\nand \"quotes\" and | too"
4	2024-03-11T00:00:00	add	2024-03	test/ing	{}	[["something", "job-4"]]		"this one has a build job"	"build_job-0"
'''.lstrip()

TEST_LOG_two = r'''
3|2023-02-01T00:00:00|add|2023-02|test/two|{}|[["something", "job-1"]]||"caption\nwithout escaping"
'''.lstrip()

def synthesis(job):
	os.mkdir('urd.db')
	os.mkdir('urd.db/test')
	with open('urd.db/passwd', 'w', encoding='ascii') as fh:
		fh.write('test:pass\n')
	with open('urd.db/test/ing.urd', 'w', encoding='ascii') as fh:
		fh.write(TEST_LOG_ing)
	with open('urd.db/test/two.urd', 'w', encoding='ascii') as fh:
		fh.write(TEST_LOG_two)
	db_path = os.path.realpath('urd.db')
	socket_path = os.path.realpath('socket')
	p = Popen(options.command_prefix + ['urd-server', '--listen', socket_path, '--path', db_path])

	# Allow some time for urd-server to start
	for _ in range(32):
		if os.path.exists(socket_path):
			break
		time.sleep(0.1)

	url = 'unixhttp://' + url_quote_more(socket_path) + '/'
	headers = {'Content-Type': 'application/json', 'Authorization': 'Basic dGVzdDpwYXNz'}
	def check(url_part, want, post_data=None):
		got = call(url + url_part, server_name='urd', data=post_data, headers=headers, fmt=json.loads)
		assert want == got, '\nWanted %r,\ngot    %r' % (want, got,)

	check('list', ['test/ing', 'test/two'])
	check('test/ing/since/0', ['2023-01', '2023-02', '2023-06', '2024-03'])
	check('test/ing/since/2023-01-01', ['2023-02', '2023-06', '2024-03'])
	check('test/ing/since/2023-02', ['2023-06', '2024-03'])
	check('test/ing/first'  , dict(timestamp='2023-01', user='test', build='ing', build_job=None, joblist=[['something', 'job-0']], caption="it's a caption", deps={}))
	check('test/ing/2023-01', dict(timestamp='2023-01', user='test', build='ing', build_job=None, joblist=[['something', 'job-0']], caption="it's a caption", deps={}))
	check('test/two/2023-02', dict(timestamp='2023-02', user='test', build='two', build_job=None, joblist=[['something', 'job-1']], caption=r'"caption\nwithout escaping"', deps={}))
	check('test/ing/2023-02', dict(timestamp='2023-02', user='test', build='ing', build_job=None, joblist=[['something', 'job-2']], caption='', deps={'test/ing': dict(caption="it's a caption", joblist=[['something', 'job-0']], timestamp='2023-01'), 'test/two': dict(caption=r'"caption\nwithout escaping"', joblist=[['something', 'job-1']], timestamp='2023-02')}))
	check('test/ing/2023-06', dict(timestamp='2023-06', user='test', build='ing', build_job=None, joblist=[['something', 'job-3']], caption="caption\nwith lines\nand\ttab\nand \"quotes\" and | too", deps={}))
	check('test/ing/latest', dict(timestamp='2024-03', user='test', build='ing', build_job='build_job-0', joblist=[['something', 'job-4']], caption="this one has a build job", deps={}))
	# Test not overriding, with keys in different order and a build_job.
	check('add', dict(new=False, changed=False, is_ghost=False), b'{"timestamp": "2023-01", "user": "test", "build": "ing", "deps": {}, "joblist": [["something", "job-0"]], "caption": "it\'s a caption", "build_job": "build_job-0"}')
	check('add', dict(new=False, changed=False, is_ghost=False), b'{"user": "test", "build": "ing", "timestamp": "2023-01", "caption": "it\'s a caption", "deps": {}, "joblist": [["something", "job-0"]], "build_job": "build_job-0"}')
	# And not overriding, with only build_job being different.
	check('add', dict(new=False, changed=False, is_ghost=False), b'{"user": "test", "build": "ing", "timestamp": "2024-03", "caption": "this one has a build job", "deps": {}, "joblist": [["something", "job-4"]], "build_job": "changed-0"}')
	# Changing something should not be allowed.
	try:
		check('add', None, b'{"user": "test", "build": "ing", "timestamp": "2023-01", "caption": "new caption", "deps": {}, "joblist": [["something else", "job-0"]], "build_job": "build_job-1"}')
		raise Exception("Changing should not be allowed without the update flag")
	except UrdConflictError:
		pass
	# And now change something with the update flag
	check('add', dict(new=False, changed=True, is_ghost=False, deps=1), b'{"user": "test", "build": "ing", "timestamp": "2023-01", "caption": "new caption", "deps": {}, "joblist": [["something else", "job-0"]], "flags": ["update"], "build_job": "build_job-1"}')
	# And that should ghost the 2023-02 entry that depended on the old version
	check('test/ing/since/0', ['2023-01', '2023-06', '2024-03'])

	# Test truncation
	check('truncate/test/ing/2023-02', dict(count=2, deps=0), '')
	check('test/ing/since/0', ['2023-01'])

	# Try with a user mismatch
	try:
		check('add', dict(new=True, changed=False, is_ghost=False), b'{"user": "wrong", "build": "ing", "timestamp": "2023-06-01", "caption": "", "deps": {}, "joblist": [["something", "job-0"]], "build_job": "build_job-1"}')
		raise Exception("Wrong user was accepted")
	except UrdPermissionError:
		pass
	# And with the wrong password
	headers['Authorization'] = 'Basic dGVzdDpwYXxx'
	try:
		check('add', dict(new=True, changed=False, is_ghost=False), b'{"user": "test", "build": "ing", "timestamp": "2023-06-01", "caption": "", "deps": {}, "joblist": [["something", "job-0"]], "build_job": "build_job-1"}')
		raise Exception("Wrong password was accepted")
	except UrdPermissionError:
		pass

	# And finally verify that we got what we expected in the log.
	with open('urd.db/test/two.urd', 'r', encoding='ascii') as fh:
		assert fh.read() == TEST_LOG_two
	with open('urd.db/test/ing.urd', 'r', encoding='ascii') as fh:
		assert fh.read(len(TEST_LOG_ing)) == TEST_LOG_ing
		want_it = iter([
			'add\t2023-01\ttest/ing\t{}\t[["something else", "job-0"]]\tupdate\t"new caption"\t"build_job-1"\n',
			'truncate\t2023-02\ttest/ing\n',
			'END'
		])
		for got, want in zip(fh, want_it):
			assert got.startswith('4\t'), got
			got = got.split('\t', 2)[2]
			assert want == got, '\nWanted %r,\ngot    %r' % (want, got,)
		assert next(want_it) == 'END'

	p.terminate()
	p.wait()
