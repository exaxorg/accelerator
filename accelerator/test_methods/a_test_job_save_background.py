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

description = """
Test job.save() and job.json_save() with background=True
using silly objects that sleep to make saving slow without a lot of IO.

Quite similar to test_job_save other than that.

Tests that they can be loaded in the next execution steps, and that the
save call returned faster than the sleep time.

The current implementation waits between the execution steps, but the API
does not promise that. This test is intended to remain valid if that is
fixed, and tests that the returned objects handle having .wait() called in
a different step (and also that .load() works without .wait()).

Should not take much more than 3 * sleeptime to run.
"""

options = {'sleeptime': 0.5}

from accelerator.compat import monotonic
import time

# This pickles as a string, but slowly.
# It's a hack to make pickle encoding slow.
class SlowToPickle(object):
	__slots__ = ('text', 'sleeptime',)

	def __init__(self, text, sleeptime):
		self.text = text
		self.sleeptime = sleeptime

	def __reduce__(self):
		time.sleep(self.sleeptime)
		return str, (str(self.text),)

# This is a True boolean that takes a long time to evaluate.
# It's a hack to make json encoding slow.
class SlowTrue(object):
	__slots__ = ('sleeptime',)

	def __init__(self, sleeptime):
		self.sleeptime = sleeptime

	# python2 version
	def __nonzero__(self):
		time.sleep(self.sleeptime)
		return True

	# python3 version
	def __bool__(self):
		time.sleep(self.sleeptime)
		return True

def save(job, name, sliceno):
	before = monotonic()
	p = job.save(f'contents of {name} {sliceno}', name + '.pickle', sliceno=sliceno)
	j = job.json_save({name: sliceno}, name + '.json', sliceno=sliceno)
	name = 'background ' + name
	bp = job.save(SlowToPickle(f'contents of {name} {sliceno}', options.sleeptime), name + '.pickle', sliceno=sliceno, background=True)
	bj = job.json_save({name: sliceno}, name + '.json', sliceno=sliceno, sort_keys=SlowTrue(options.sleeptime), background=True)
	save_time = monotonic() - before
	max_time = options.sleeptime * 2 # two slow files
	assert save_time < max_time, f"Saving took {save_time} seconds, should have been less than {max_time}"
	return p, j, bp, bj

def check(job, name, sliceno, p, j, bp, bj, do_background=True, do_wait=False):
	todo = [(name, p, j)]
	if do_background:
		todo.append(('background ' + name, bp, bj))
	for name, p, j in todo:
		if do_wait:
			# Do explicit waiting sometimes
			p.wait()
			j.wait()
		assert p.load() == f'contents of {name} {sliceno}'
		assert j.load() == {name: sliceno}
		for obj, filename in [(p, name + '.pickle'), (j, name + '.json')]:
			path = job.filename(filename, sliceno=sliceno)
			assert obj.path == path
			assert obj.filename == path.split('/')[-1]
			assert obj.jobwithfile().filename(sliceno) == path

def prepare(job):
	res = save(job, 'prepare', None)
	check(job, 'prepare', None, *res, do_background=False)

	# Test that the slowdowns work. (While the stuff we saved above is waiting)
	# Use sleeptime / 2.5 so this takes no extra time.
	checktime = options.sleeptime / 2.5
	before = monotonic()
	p = job.save(SlowToPickle('', checktime), 'test.pickle', background=True)
	p.wait()
	pickle_time = monotonic() - before
	assert pickle_time > checktime, f"Saving a slow pickle took {pickle_time} seconds, should have taken more than {checktime}"
	before = monotonic()
	j = job.json_save({}, 'test.json', sort_keys=SlowTrue(checktime), background=True)
	j.wait()
	json_time = monotonic() - before
	assert json_time > checktime, f"Saving a slow json took {json_time} seconds, should have taken more than {checktime}"

	return res

def analysis(sliceno, job, prepare_res):
	check(job, 'prepare', None, *prepare_res, do_wait=(sliceno % 2 == 0))
	res = save(job, 'analysis', sliceno)
	check(job, 'analysis', sliceno, *res, do_background=False)
	return res

def synthesis(job, prepare_res, analysis_res):
	check(job, 'prepare', None, *prepare_res, do_wait=True)
	for sliceno, res in enumerate(analysis_res):
		check(job, 'analysis', sliceno, *res, do_wait=(sliceno % 3 == 0))
	res = save(job, 'synthesis', None)
	check(job, 'synthesis', None, *res, do_wait=True)
