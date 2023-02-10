############################################################################
#                                                                          #
# Copyright (c) 2023 Carl Drougge                                          #
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
Test using the returned object from job.save() and job.json_save()
"""

def save(job, name, sliceno):
	p = job.save('contents of %s %s' % (name, sliceno,), name + '.pickle', sliceno=sliceno)
	j = job.json_save({name: sliceno}, name + '.json', sliceno=sliceno)
	return p, j

def check(job, name, sliceno, p, j):
	assert p.load() == 'contents of %s %s' % (name, sliceno,)
	assert j.load() == {name: sliceno}
	for obj, filename in [(p, name + '.pickle'), (j, name + '.json')]:
		path = job.filename(filename, sliceno=sliceno)
		assert obj.path == path
		assert obj.filename == path.split('/')[-1]
		assert obj.jobwithfile().filename(sliceno) == path

def prepare(job):
	res = save(job, 'prepare', None)
	check(job, 'prepare', None, *res)
	return res

def analysis(sliceno, job, prepare_res):
	check(job, 'prepare', None, *prepare_res)
	res = save(job, 'analysis', sliceno)
	check(job, 'analysis', sliceno, *res)
	return res

def synthesis(job, prepare_res, analysis_res):
	check(job, 'prepare', None, *prepare_res)
	for sliceno, res in enumerate(analysis_res):
		check(job, 'analysis', sliceno, *res)
	res = save(job, 'synthesis', None)
	check(job, 'synthesis', None, *res)
