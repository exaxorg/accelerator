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
from __future__ import division
from __future__ import unicode_literals

from accelerator.compat import PY2
import os

description = r'''
Test that the expected files are registered (and not registered) when
using job.open(), job.register_file() and job.register_files().

This one registers some things, and also does not register some files.
'''

def analysis(sliceno, job):
	if sliceno == 0:
		with open('analysis slice 0.txt', 'w') as fh:
			fh.write('written in analysis slice 0')
	if sliceno == 1:
		with job.open('temporary analysis slice 1.txt', 'w', temp=True) as fh:
			fh.write('written as temporary in analysis slice 1')
	if sliceno == 2:
		with job.open('analysis slice 2.txt', 'w') as fh:
			fh.write('written and registered in analysis slice 2')

def synthesis(job):
	with open('synthesis file.txt', 'w') as fh:
		fh.write('written in synthesis')
	with job.open('temp file.txt', 'w', temp=True) as fh:
		fh.write('temporary file, should not be registered by job.register_files()')
	with job.open('registered temp file.txt', 'w', temp=True) as fh:
		fh.write('temporary file, but manually registered after creation.')
	# The slice 1 file is already registered, and should not appear here.
	# And the temp files can also not be registered by register_files.
	assert job.register_files('*.txt') == {'analysis slice 0.txt', 'synthesis file.txt'}
	# But register_file can register a temp file (and make it no longer temporary).
	job.register_file('registered temp file.txt')

	# Produce two datasets, to make sure their files don't end up registered.
	job.datasetwriter(columns={'dummy': 'ascii'}).get_split_write()('dummy')
	job.datasetwriter(name='another dataset', columns={'dummy': 'ascii'}).get_split_write()('dummy')
	os.mkdir('subdir')
	os.mkdir('subdir/deep')
	with open('subdir/deep/file.txt', 'w') as fh:
		fh.write('written in a subdir, then registered with job.register_files() without a pattern.')
	if PY2:
		# No recursive support in glob in python 2.
		assert job.register_files() == set()
		assert job.register_files('*/*/*') == {'subdir/deep/file.txt'}
	else:
		assert job.register_files() == {'subdir/deep/file.txt'}
