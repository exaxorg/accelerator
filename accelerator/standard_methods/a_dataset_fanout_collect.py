# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2021-2024 Carl Drougge                                     #
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
Collects values for dataset_fanout.

You probably don't want to call this yourself.
'''

from accelerator import OptionString
from accelerator.compat import unicode, imap


options = {
	'column': OptionString,
	'length': -1,
}

datasets = ('source',)

jobs = ('previous',)


def analysis(sliceno):
	chain = datasets.source.chain(stop_ds={jobs.previous: 'source'}, length=options.length)
	return set(imap(unicode, chain.iterate(sliceno, options.column)))

def synthesis(analysis_res):
	return analysis_res.merge_auto()
