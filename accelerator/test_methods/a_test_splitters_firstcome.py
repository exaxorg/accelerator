# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2025 Carl Drougge                                          #
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
Test the non-deterministic FirstComeSplitter sequence splitter.

As it's not deterministic, this test can fail under high load.
It probably sleeps long enough for that not to happen in practice.
'''


from accelerator.error import AcceleratorError
from accelerator.splitters import FirstComeSplitter
from time import sleep


def prepare(slices):
	data_a = range(slices * 2 + 2)
	class Unpicklable:
		"This can't be pickled, because it's local to the function."
	data_b = [Unpicklable(), Unpicklable()]
	a = FirstComeSplitter(data_a, continue_in_synthesis=True)
	a_partial = FirstComeSplitter(data_a, continue_in_synthesis=True) # will only be partially consumed in analysis
	a_repeated = FirstComeSplitter(data_a)
	b = FirstComeSplitter(data_b)
	return a, a_partial, a_repeated, b, data_b


def analysis(sliceno, slices, prepare_res):
	a, a_partial, a_repeated, b, data_b = prepare_res
	if sliceno == 0:
		assert next(a) == 0
		assert next(a_partial) == 0
		# These should come out as the same objects, therefore test with is.
		assert next(b) is data_b[0]
	elif sliceno == 1:
		sleep(0.15)
		assert next(a) == 1
		# Leave the rest of a_partial to be consumed in synthesis.
		assert next(b) is data_b[1]
	elif sliceno == 2:
		assert list(a_repeated) == list(range(slices * 2 + 2))
		sleep(0.30)
		assert list(a) == list(range(2, slices * 2 + 2))
		assert list(b) == []


def synthesis(slices, prepare_res):
	a, a_partial, a_repeated, b, _ = prepare_res
	try:
		a_partial[1]
		raise Exception("a_partial set continue_in_synthesis, but allowed item access anyway.")
	except AcceleratorError:
		pass
	assert list(a) == []
	assert list(a_partial) == list(range(1, slices * 2 + 2))
	assert list(a_repeated) == list(range(slices * 2 + 2))
	assert a_repeated[1] == 1
