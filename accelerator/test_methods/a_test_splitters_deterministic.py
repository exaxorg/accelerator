# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2025-2026 Carl Drougge                                     #
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
Test the roundrobin and chunked sequence splitters.
'''

from accelerator.splitters import RoundRobinSplitter, ChunkSplitter


def prepare(slices):
	class Unpicklable(str):
		"This can't be pickled, because it's local to the function."
	def populate(typ):
		res = {}
		# One with each number of left over elements
		# after dividing them evenly across slices.
		res['left_overs'] = [
			typ(range(slices * 2 + left_over))
			for left_over in range(slices)
		]
		# These all make ['a', 'b', 'c'] in various ways,
		# to make sure modifications work correctly.
		abcds = []
		abcds.append(typ('abcd'))
		abcds.append(typ('ab'))
		abcds[-1].append('c')
		abcds[-1].append('d')
		abcds.append(typ('aXXcd'))
		abcds[-1][1:3] = 'b'
		abcds.append(typ('a'))
		abcds[-1].extend('bcd')
		abcds.append(typ('aXcd'))
		abcds[-1][1] = 'b'
		assert len(abcds) == 5 and all(el == ['a', 'b', 'c', 'd'] for el in abcds)
		res['abcds'] = abcds
		res['unpicklable'] = [typ([Unpicklable('foo'), Unpicklable('bar')])]
		return res
	def split(d):
		# Split each entry as it would be for each slice.
		return {
			k: [
				[inner.for_slice(sliceno) for sliceno in range(slices)]
				for inner in v
			]
			for k, v in d.items()
		}
	overlapped = [
		(
			ChunkSplitter(range(slices * 6 - 2)),
			ChunkSplitter(range(slices * 6 - 2), overlap_start=n),
			ChunkSplitter(range(slices * 6 - 2), overlap_end=n),
			ChunkSplitter(range(slices * 6 - 2), overlap_start=n, overlap_end=n),
			ChunkSplitter(range(slices * 6 - 2), overlap_start=n, overlap_end=4),
			ChunkSplitter(range(slices), overlap_start=n, overlap_end=n),
		)
		for n in (1, 2, 5)
	]
	return (
		populate(list), # One without any magic, for comparing to
		populate(RoundRobinSplitter),
		split(populate(RoundRobinSplitter)),
		populate(ChunkSplitter),
		split(populate(ChunkSplitter)),
		overlapped,
		Unpicklable,
	)


# This one is trivial, so we use the same implementation as in the splitter
def reimplemented_rr(slices, sliceno, lst):
	return lst[sliceno::slices]

# This one is trickier. We replicate the actual decision code, as we must,
# but then use a different (easier to understand but slower) offset calculation.
def reimplemented_chunk(slices, sliceno, lst):
	per_slice = len(lst) // slices
	left_over = len(lst) % slices
	# This is the sliceno where left_over elements start being added.
	# A reasonably large prime gives quite even distribution.
	extra_pos = (left_over * 27644437) % slices
	extra = [0] * slices
	for extra_pos in range(extra_pos, extra_pos + left_over):
		extra[extra_pos % slices] = 1
	start = per_slice * sliceno + sum(extra[:sliceno])
	end = start + per_slice + extra[sliceno]
	return lst[start:end]


def analysis(sliceno, slices, prepare_res):
	normal_lists, rrs, presplit_rrs, chunks, presplit_chunks, overlapped, unpicklable_type = prepare_res
	for d in (normal_lists, rrs, chunks):
		assert all(type(item) is unpicklable_type for item in d['unpicklable'][0])
	def chk(err_prefix, d, presplit_d, split_func):
		for k, got in d.items():
			want = [split_func(slices, sliceno, v) for v in normal_lists[k]]
			assert got == want, "%s:%s had wrong values in slice %d:\nwanted %r\ngot    %r" % (err_prefix, k, sliceno, want, got,)
			got_presplit = [v[sliceno] for v in presplit_d[k]]
			assert got_presplit == want, "%s:%s had wrong presplit values in slice %d:\nwanted %r\ngot    %r" % (err_prefix, k, sliceno, want, got_presplit,)
	chk("rrs", rrs, presplit_rrs, reimplemented_rr)
	chk("chunks", chunks, presplit_chunks, reimplemented_chunk)

	def sequential(lst):
		return lst == list(range(lst[0], lst[0] + len(lst)))
	for n, versions in zip((1, 2, 5), overlapped):
		assert all(sequential(v) for v in versions)
		std, ostart, oend, oboth, oboth4, short_on = versions
		n_start = n
		n_end = -n
		if sliceno == 0:
			n_start = 0
		if sliceno == slices - 1:
			n_end = None
		o_end = -(n_end or 0)
		assert ostart[n_start:] == std
		assert ostart.overlap_start == n_start
		assert ostart.overlap_end == 0
		assert oend[:n_end] == std
		assert oend.overlap_start == 0
		assert oend.overlap_end == o_end
		assert oboth[n_start:n_end] == std
		assert oboth.overlap_start == n_start
		assert oboth.overlap_end == o_end
		n_end = -4 if n_end else None
		o_end = -(n_end or 0)
		assert oboth4[n_start:n_end] == std
		assert oboth4.overlap_start == n_start
		assert oboth4.overlap_end == o_end
		o_start = min(sliceno, n)
		o_end = min(slices - sliceno - 1, n)
		want_short = list(range(slices))[sliceno - o_start:sliceno + o_end + 1]
		assert short_on == want_short
		assert short_on.overlap_start == o_start
		assert short_on.overlap_end == o_end

	# Make sure for_slice() isn't allowed in analysis.
	try:
		rrs['left_overs'][0].for_slice(0)
	except AssertionError:
		pass
	try:
		chunks['left_overs'][0].for_slice(0)
	except AssertionError:
		pass

	# left_overs covers all cases for how they split,
	# so simplify synthesis and only check those.
	return rrs['left_overs'], chunks['left_overs']


def synthesis(slices, prepare_res, analysis_res):
	want = {
		'left_overs': [
			list(range(slices * 2 + left_over))
			for left_over in range(slices)
		],
		'abcds': 5 * [list('abcd')],
		'unpicklable': [['foo', 'bar']],
	}
	def chk(err_prefix, d):
		for k, v in want.items():
			assert d[k] == v, "%s:%s had wrong values in synthesis:\nwanted %r\ngot    %r" % (err_prefix, k, v, d[k],)

	# They should all contain the full data in synthesis
	normal_lists, rrs, _, chunks, _, _, _ = prepare_res
	chk("prepare:normal_lists", normal_lists)
	chk("prepare:rrs", rrs)
	chk("prepare:chunks", chunks)

	# Check that the data seen in analysis is actually all of the data.
	# Since left_overs covers all cases for how they split, only check that.
	want_chunks = want['left_overs']
	want_rrs = []
	for sliceno in range(slices):
		want_rrs.append([lst[sliceno::slices] for lst in want_chunks])
		assert [lst.for_slice(sliceno) for lst in rrs['left_overs']] == want_rrs[-1]
	rrs, chunks = next(analysis_res)
	rrs = [rrs]
	for r, c in analysis_res:
		rrs.append(r)
		for dst, src in zip(chunks, c):
			dst.extend(src)
	assert chunks == want_chunks
	assert rrs == want_rrs
