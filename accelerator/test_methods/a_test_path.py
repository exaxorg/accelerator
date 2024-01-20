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

description = r'''
Test the pathlib types in options.
'''

from pathlib import Path, PurePath, PosixPath, PurePosixPath

from accelerator import subjobs

options = dict(
	inner=False,
	plainPath=Path,
	plainPurePath=PurePath,
	plainPosixPath=PosixPath,
	plainPurePosixPath=PurePosixPath,
	valuedPath=Path('/valued/path'), # Actually a PosixPath
	valuedPurePath=PurePath('valued/pure/path'),
	valuedPosixPath=PosixPath('/valued/posix/path'), # Actually a PurePosixPath
	valuedPurePosixPath=PurePosixPath('valued/pure/posix/path'),
)

def synthesis():
	if options.inner:
		parent = lambda p: p.parent if p else None
		return (
			parent(options.plainPath),
			parent(options.plainPurePath),
			parent(options.plainPosixPath),
			parent(options.plainPurePosixPath),
			parent(options.valuedPath),
			parent(options.valuedPurePath),
			parent(options.valuedPosixPath),
			parent(options.valuedPurePosixPath),
		)

	job = subjobs.build(
		'test_path',
		inner=True,
	)
	assert job.load() == (
		None, None,
		None, None,
		Path('/valued'), PurePath('valued/pure'),
		PosixPath('/valued/posix'), PurePosixPath('valued/pure/posix'),
	)

	job = subjobs.build(
		'test_path',
		inner=True,
		plainPath=Path('hello/there'),
		plainPurePath=Path('over/here'),
		plainPosixPath=PosixPath('/some/where'),
		plainPurePosixPath=PosixPath('there'),
		valuedPath=None,
		valuedPurePath=None,
		valuedPosixPath=None,
		valuedPurePosixPath=None,
	)
	assert job.load() == (
		Path('hello'), PurePath('over'),
		PosixPath('/some'), PurePosixPath('.'),
		None, None,
		None, None,
	)

	# Same again, but with the options as strings.
	str_job = subjobs.build(
		'test_path',
		inner=True,
		plainPath='hello/there',
		plainPurePath='over/here',
		plainPosixPath='/some/where',
		plainPurePosixPath='there',
		valuedPath=None,
		valuedPurePath=None,
		valuedPosixPath=None,
		valuedPurePosixPath=None,
	)
	assert job == str_job
