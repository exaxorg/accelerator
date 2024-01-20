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

description = """
Test that dicts in options retain their order, in two levels.
"""

options = dict(
	dict={},
	key_order=[],
)

def synthesis():
	assert set(options.dict) == set(options.key_order), 'Bad options'
	assert set(options.dict.inner) == set(options.key_order), 'Bad options'
	assert list(options.dict) == options.key_order, 'Order was not preserved'
	assert list(options.dict.inner) == options.key_order, 'Order was not preserved at lower level'
