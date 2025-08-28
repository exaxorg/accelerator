# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2019-2024 Carl Drougge                                     #
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
Test writing datasets with strange column names, column names whose cleaned
names collide and column names used in the generated split_write function.
'''

from accelerator.dataset import DatasetWriter

internal_names_analysis = ['a', 'b', 'c', 'w1', 'write']

def mk_dw(name, cols, **kw):
	dw = DatasetWriter(name=name, **kw)
	for colname in cols:
		dw.add(colname, "unicode")
	return dw

def prepare():
	return mk_dw("internal_names_analysis", internal_names_analysis)

def analysis(sliceno, prepare_res):
	prepare_res.write(*[f'a {sliceno}'] * 5)
	prepare_res.write_list([f'b {sliceno}'] * 5)
	prepare_res.write_dict(dict(zip(internal_names_analysis, [f'c {sliceno}'] * 5)))

def synthesis(prepare_res, slices):
	ds = prepare_res.finish()
	for sliceno in range(slices):
		assert list(ds.iterate(sliceno, internal_names_analysis)) == [(f'a {sliceno}',) * 5, (f'b {sliceno}',) *5, (f'c {sliceno}',) *5]

	in_parent = [ # list because order matters
		"-",      # becomes _ because everything must be a valid python identifier.
		"a b",    # becomes a_b because everything must be a valid python identifier.
		"42",     # becomes _42 because everything must be a valid python identifier.
		"print",  # becomes print_ because print is a keyword (in py2).
		"print@", # becomes print__ because print_ is taken.
		"None",   # becomes None_ because None is a keyword (in py3).
	]
	dw = mk_dw("parent", in_parent)
	w = dw.get_split_write()
	w(_="- 1", a_b="a b 1", _42="42 1", print_="print 1", None_="None 1", print__="Will be overwritten 1")
	w(_="- 2", a_b="a b 2", _42="42 2", print_="print 2", None_="None 2", print__="Will be overwritten 2")
	parent = dw.finish()
	in_child = [ # order still matters
		"print_*", # becomes print__ (no collision).
		"print_",  # no collision.
		"normal",  # no collision.
		"Normal",  # no collision.
		"print@",  # becomes print___ because all shorter are taken.
	]
	dw = mk_dw("child", in_child, parent=parent)
	w = dw.get_split_write()
	w(print___="print@ 1", print__="print_* 1", print_="print_ 1", normal="normal 1", Normal="Normal 1")
	w(print___="print@ 2", print__="print_* 2", print_="print_ 2", normal="normal 2", Normal="Normal 2")
	child = dw.finish()
	for colname in in_parent + in_child:
		data = set(child.iterate(None, colname))
		assert data == {colname + " 1", colname + " 2"}, f"Bad data for {colname}: {data!r}"

	def chk_internal(name, **kw):
		internal = ("writers", "w_l", "cyc", "hsh", "next",)
		dw = mk_dw(name, internal, **kw)
		dw.get_split_write()(*internal)
		dw.get_split_write_list()(internal)
		dw.get_split_write_dict()(dict(zip(internal, internal)))
		got = list(dw.finish().iterate(None, internal))
		assert got == [internal] * 3
	chk_internal(name="internal_names")
	chk_internal(name="internal_names_hashed", hashlabel="hsh")
