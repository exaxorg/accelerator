# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2020-2024 Carl Drougge                                     #
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
Produce a summary that build_tests can put in the result directory.
'''

from accelerator.job import Job
from accelerator.build import fmttime
from accelerator.compat import url_quote

options = {'joblist': []}

def html_quote(s):
	return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

def synthesis(job):
	joblist = [Job(j) for j in options.joblist]
	total = sum(j.params.exectime.total for j in joblist)
	with job.open('summary.html', 'w', encoding='utf-8') as fh:
		fh.write('<h2>Test built these jobs in ' + fmttime(total) + '</h2>\n')
		fh.write('<ol>\n')
		list_item = '<li><a href="/job/%s" target="_blank">%s</a> %s %s</li>\n'
		for j in joblist:
			fh.write(list_item % (url_quote(j), html_quote(j), html_quote(j.method), fmttime(j.params.exectime.total)))
		fh.write('</ol>\n')
