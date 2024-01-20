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

from __future__ import unicode_literals

description = r'''
Verify that csvimport puts lines in the expected slice.

Both the actual data and the skipped lines should roundrobin independantly
starting from slice 0.

Does not test allow_bad, as that does not currenctly have a useful rule
for how data is sliced.
'''

from accelerator import subjobs
from itertools import cycle
from random import randint

def check(job, name, labels_txt=[], initial_skipped=[], sometimes='', comments=False, **kw):
	want_linenos = [[] for _ in range(job.params.slices)]
	lineno = [1]
	want_comments = [[] for _ in range(job.params.slices)]
	comments_cycle = cycle(want_comments)
	def add_comment(line):
		next(comments_cycle).append((lineno[0], line.encode('ascii')))
		fh.write(line + '\n')
		lineno[0] += 1
	with job.open(name, 'wt') as fh:
		for line in initial_skipped:
			add_comment(line)
		for line in labels_txt:
			fh.write(line + '\n')
			lineno[0] += 1
		for ix in range(10):
			for sliceno in range(job.params.slices):
				if sometimes and randint(0, 9) == 3:
					add_comment(sometimes % (ix, sliceno,))
				if comments and sliceno % comments == 0:
					add_comment('# line %d, before %d,%d' % (lineno[0], ix, sliceno,))
				fh.write('%d,%d\n' % (ix, sliceno,))
				want_linenos[sliceno].append(lineno[0])
				lineno[0] += 1
	job = subjobs.build(
		'csvimport',
		filename=job.filename(name),
		lineno_label='lineno',
		comment='#' if comments or sometimes else '',
		skip_lines=len(initial_skipped),
		**kw
	)

	ds = job.dataset()
	for sliceno in range(job.params.slices):
		got = [int(x) for x in ds.iterate(sliceno, 'sliceno')]
		assert got == [sliceno] * 10, "Slice %d has wrong slices in %s: %r" % (sliceno, ds.quoted, got,)
		got = [int(x) for x in ds.iterate(sliceno, 'ix')]
		assert got == list(range(10)), "Slice %d has wrong ixes in %s: %r" % (sliceno, ds.quoted, got,)
		got = list(ds.iterate(sliceno, 'lineno'))
		assert got == want_linenos[sliceno], "Slice %d has wrong lines in %s:\n    wanted %r\n    got    %r" % (sliceno, ds.quoted, want_linenos[sliceno], got,)
	if comments or sometimes or initial_skipped:
		ds = job.dataset('skipped')
		for sliceno in range(job.params.slices):
			got = list(ds.iterate(sliceno, ('lineno', 'data')))
			assert got == want_comments[sliceno], "Slice %d has wrong skipped lines in %s:\n    wanted %r\n    got    %r" % (sliceno, ds.quoted, want_comments[sliceno], got,)


def synthesis(job):
	check(job, 'simple.txt', labels=['ix', 'sliceno'])
	check(job, 'labeled.txt', labels_txt=['ix,sliceno'])
	check(job, 'comments.txt', labels_txt=['ix,sliceno'], comments=True)
	check(job, 'some comments.txt', labels_txt=['ix,sliceno'], comments=3)
	check(job, 'random comments.txt', labels_txt=['ix,sliceno'], sometimes='# a comment before %d,%d')
	check(job, 'two label lines.txt', labels_txt=['i,slice', 'x,no'], label_lines=2, rename={'i x': 'ix', 'slice no': 'sliceno'})
	check(job, 'two label lines after comments.txt', labels_txt=['i,slice', 'x,no'], label_lines=2, rename={'i x': 'ix', 'slice no': 'sliceno'}, initial_skipped=['this is some text', 'this is more text'], comments=2)
