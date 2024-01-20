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
Verify that datasets can not be overwritten.
'''

from accelerator.error import DatasetUsageError

def synthesis(job):
	dw = job.datasetwriter(name='a', columns={'a': 'ascii'})
	try:
		job.datasetwriter(name='a')
		raise Exception("Allowed creating a duplicate datasetwriter")
	except DatasetUsageError:
		pass
	dw.get_split_write()
	a = dw.finish()
	try:
		job.datasetwriter(name='a')
		raise Exception("Allowed creating a datasetwriter for an existing dataset")
	except DatasetUsageError:
		pass
	dw = job.datasetwriter(name='b', columns={'b': 'ascii'})
	try:
		a.link_to_here('b')
		raise Exception("Allowed link_to_here over a datasetwriter")
	except DatasetUsageError:
		pass
	dw.get_split_write()
	dw.finish()
	try:
		a.link_to_here('b')
		raise Exception("Allowed link_to_here over an existing dataset")
	except DatasetUsageError:
		pass
