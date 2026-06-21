############################################################################
#                                                                          #
# Copyright (c) 2026 Carl Drougge                                          #
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
Test job building timeout, first in a subjob and also in the call to this.
'''

from accelerator import subjobs
from accelerator.error import JobTimeoutError
from time import monotonic, sleep

options = dict(
	inner=False,
)

def synthesis(job):
	t0 = monotonic()
	def p(txt):
		print(f'[{job} {monotonic() - t0 :02.1f}s] {txt}')
	if options.inner:
		p('before sleep')
		sleep(10)
		p('after sleep')
	else:
		try:
			subjobs.build('test_job_timeout', inner=True, job_timeout=1, force_build=True)
		except JobTimeoutError as e:
			output = e.job.output('synthesis')
			assert 'before sleep' in output
			assert 'after sleep' not in output
			p('timed out as expected')
		# Run it again without a timeout, we will be killed before this finishes.
		p('second run')
		subjobs.build('test_job_timeout', inner=True, force_build=True)
		p('second run finished')
