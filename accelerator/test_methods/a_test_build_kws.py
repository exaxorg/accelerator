# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2019-2024 Carl Drougge                                     #
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
This method is called to test that urd.build finds the correct target for
keywords, and also that it does not accept ambiguous keywords.
'''

options = {'foo': 'foo', 'a': 'a'}
datasets = ('foo', 'b')
jobs = ('foo', 'c')

def synthesis():
	return (options, datasets, jobs)
