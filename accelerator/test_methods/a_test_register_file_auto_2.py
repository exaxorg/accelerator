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

import os

description = r'''
Test that the expected files are registered (and not registered) when
using job.open(), job.register_file() and job.register_files().

This one registers nothing, but produces temp files, and a subdirectory.
'''

def analysis(sliceno, job):
	if sliceno == 0:
		with open('analysis slice 0.txt', 'w') as fh:
			fh.write('written in analysis slice 0')
	if sliceno == 1:
		with job.open('temporary analysis slice 1.txt', 'w', temp=True) as fh:
			fh.write('written as temporary in analysis slice 1')

def synthesis(job):
	with open('synthesis file.txt', 'w') as fh:
		fh.write('written in synthesis')
	with job.open('temporary synthesis file.txt', 'w', temp=True) as fh:
		fh.write('written as temporary in synthesis')
	os.mkdir('subdir')
	with open('subdir/file.txt', 'w') as fh:
		fh.write('written in a subdir, not automatically registered.')
