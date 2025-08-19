# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2019-2024 Carl Drougge                                     #
# Modifications copyright (c) 2020 Anders Berkeman                         #
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
Verify that the output from methods is captured correctly for all valid
combinations of prepare/analysis/synthesis, both in OUTPUT dir and in
status stacks.
'''

from random import randint
import sys
import os

from accelerator import subjobs
from accelerator.build import Automata
from accelerator import g

def test(params, p=False, a=False, s=False):
	prefix = "A bit of text."
	opts = {'prefix': prefix}
	name = 'test_output_'
	cookie = randint(10000, 99999)
	if p:
		name += 'p'
		opts['p'] = f"Some words\nfrom prepare\nwith {cookie} in them."
	if a:
		name += 'a'
		opts['a'] = "A few words\nfrom analysis(%%d)\nwith the cookie %d in them." % (cookie,)
	if s:
		name += 's'
		opts['s'] = f"Words\nfrom synthesis\ncookie is {cookie}."
	jid = subjobs.build(name, options=opts)
	d = jid.filename('OUTPUT/')
	chked = set()
	all = []
	def chk(part):
		output = jid.output(part)
		if isinstance(part, int):
			data = opts['a'] % (part,)
			part = str(part)
		else:
			data = opts[part[0]]
		chked.add(part)
		with open(d +  part, 'r') as fh:
			got = fh.read().replace('\r\n', '\n')
		want = prefix + '\n' + data + '\n'
		assert got == prefix + '\n' + data + '\n', f"{jid} produced {got!r} in {part}, expected {want!r}"
		assert output == got, f'job.output disagrees with manual file reading for {part} in {jid}. {output!r} != {got!r}'
		all.append(got)
	if p:
		chk('prepare')
	if a:
		for sliceno in range(params.slices):
			chk(sliceno)
	if s:
		chk('synthesis')
	unchked = set(os.listdir(d)) - chked
	assert not unchked, f"Unexpected OUTPUT files from {jid}: {unchked!r}"
	output = jid.output()
	got = ''.join(all)
	assert output == got, f'job.output disagrees with manual file reading for <all> in {jid}. {output!r} != {got!r}'

def synthesis(params):
	test(params, s=True)
	test(params, p=True, s=True)
	test(params, p=True, a=True, s=True)
	test(params, p=True, a=True)
	test(params, a=True, s=True)
	test(params, a=True)


# This is run in all parts in the subjobs.
# The code is here so it's not repeated.

def sub_part(sliceno, opts):
	a = Automata(g.server_url, verbose=True)
	pid = os.getpid()
	def verify(want):
		timeout = 0
		got = None
		for _ in range(25):
			status_stacks = a._server_idle(timeout)[2]
			for line in status_stacks:
				if line[0] == pid and line[1] < 0:
					# this is our tail
					got = line[2].replace('\r\n', '\n')
					if got == want:
						return
			# it might not have reached the server yet
			timeout += 0.01
		# we've given it 3 seconds, it's not going to happen.
		raise Exception(f"Wanted to see tail output of {want!r}, but saw {got!r}")
	print(opts.prefix, file=sys.stderr)
	verify(opts.prefix + '\n')
	if isinstance(sliceno, int):
		msg = opts.a % (sliceno,)
	else:
		msg = opts[sliceno]
	print(msg)
	verify(opts.prefix + '\n' + msg + '\n')
