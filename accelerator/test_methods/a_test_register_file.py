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

from accelerator import subjobs

description = r'''
Test that the expected files are registered (and not registered) when
using job.open(), job.register_file() and job.register_files().

This is the top job, which runs the file producing methods.
'''

def synthesis():
	def test(method, *want_files):
		want_files = set(want_files)
		job = subjobs.build(method)
		got_files = job.files()
		if want_files != got_files:
			extra_files = got_files - want_files
			missing_files = want_files - got_files
			msg = f"Got the wrong files from {method}: {got_files!r}"
			if extra_files:
				msg += f", did not expect {extra_files!r}"
			if missing_files:
				msg += f", also wanted {missing_files!r}"
			raise Exception(msg)

	test('test_register_file_auto', 'analysis slice 0.txt', 'synthesis file.txt', 'result.pickle')
	test('test_register_file_auto_2', 'analysis slice 0.txt', 'synthesis file.txt')
	test('test_register_file_manual', 'analysis slice 0.txt', 'analysis slice 2.txt', 'registered temp file.txt', 'synthesis file.txt', 'subdir/deep/file.txt')
