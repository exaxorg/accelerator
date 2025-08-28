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

description = r'''
Test some strange choices for separators in csvexport.
'''

import gzip

from accelerator import subjobs

def synthesis(job):
	dw = job.datasetwriter()
	dw.add('a', 'ascii')
	dw.add('b', 'unicode')
	w = dw.get_split_write()
	w('A', 'B')
	w('\0', '\xe4')
	ds = dw.finish()
	def verify(data, filename):
		want = []
		for line in [['a', 'b']] + data:
			want.append(separator.join(quote + item + quote for item in line))
			want.append(line_separator)
		want = ''.join(want).encode('utf-8')
		if ext == '.gz':
			open_func = gzip.open
		else:
			open_func = open
		with open_func(j.filename(filename), 'rb') as fh:
			got = fh.read()
		assert want == got, f"Expected {j}/{filename} to contain {want!r}, but contained {got!r}"
	for separator in ('', '\0', 'wheeee'):
		for line_separator in ('', '\0', 'woooooo'):
			for quote in ('', 'qqq'):
				for ext in ('.csv', '.gz'):
					for sliced, filename in ((False, 'out' + ext), (True, 'out.%d' + ext)):
						j = subjobs.build(
							'csvexport',
							filename=filename,
							separator=separator,
							line_separator=line_separator,
							quote_fields=quote,
							lazy_quotes=False,
							sliced=sliced,
							source=ds,
						)
						if sliced:
							for sliceno, data in ((0, ['A', 'B']), (1, ['\0', '\xe4'])):
								verify([data], filename % (sliceno,))
						else:
							verify([['A', 'B'], ['\0', '\xe4']], filename)
