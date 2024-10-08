# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2024 Pablo Correa Gómez                                    #
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
Test returning and pickling defaultdicts. Some are not picklable by default
and the accelerator needs to do some mangling when returning them from analysis.
So create a mix of picklable and non-picklable defaultdicts, fill them with some
random data, and pass them between analysis and synthesis to test that they
work as expected
'''

from collections import defaultdict

def fill(d, sliceno):
	if isinstance(d, defaultdict):
		d[sliceno * sliceno] = fill(d[sliceno * sliceno], sliceno * sliceno)
		return d
	return sliceno

def analysis(sliceno):
	d = defaultdict(int)
	dd = defaultdict(lambda: defaultdict(int))
	ddd = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
	return (fill(d, sliceno), fill(dd, sliceno), fill(ddd, sliceno))


def synthesis(analysis_res):
	res = ()
	for i in analysis_res.merge_auto():
		res += (i,)
	return res
