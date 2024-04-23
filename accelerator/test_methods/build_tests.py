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

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals

from accelerator.dataset import Dataset
from accelerator.build import JobError
from accelerator.compat import monotonic

from collections import OrderedDict
from datetime import date, datetime, timedelta
from sys import exit
import os
import sys

description = '''
Run the tests. Needs at least 3 slices to work.
'''

def main(urd):
	assert urd.info.slices >= 3, "The tests don't work with less than 3 slices (you have %d)." % (urd.info.slices,)

	from accelerator.shell import cfg
	# sys.argv[0] is absolute for unqualified commands ("ax"), but exactly what
	# the user wrote otherwise ("./venv/bin/ax"). This makes either absolute.
	command_prefix = [os.path.join(cfg.user_cwd, sys.argv[0]), '--config', cfg.config_filename]

	print()
	print("Testing urd.build and job.load")
	want = ({'foo': 'foo', 'a': 'a'}, {'foo': '', 'b': ''}, {'foo': '', 'c': ''})
	job = urd.build("test_build_kws")
	assert job.load() == want
	bad = None
	try:
		urd.build("test_build_kws", options=dict(foo='bar'), foo='baz')
		bad = 'Allowed ambiguous keyword "foo"'
	except Exception:
		pass
	assert not bad, bad
	want[0]['foo'] = 'bar'
	want[0]['a'] = 'A'
	job = urd.build("test_build_kws", options=dict(foo='bar'), a='A')
	assert job.load() == want
	assert urd.build("test_build_kws", options=dict(foo='bar'), a='A', b=None, c=None) == job
	want[2]['c'] = job
	job = urd.build("test_build_kws", options=dict(foo='bar', a='override this from kw'), a='A', c=job)
	assert job.load() == want
	want[0]['foo'] = 'foo'
	want[2]['c'] = job
	job = urd.build("test_build_kws", a='A', b=None, c=job, datasets=dict(b='overridden'))
	assert job.load() == want

	print()
	print("Testing urd.begin/end/truncate/get/peek/latest/first/since")
	urd.build('test_urd', command_prefix=command_prefix)
	urd.truncate("tests.urd", 0)
	assert not urd.peek_latest("tests.urd").joblist
	urd.begin("tests.urd", 1, caption="first")
	urd.build("test_build_kws")
	fin = urd.finish("tests.urd")
	assert fin == {'new': True, 'changed': False, 'is_ghost': False}, fin
	urd.begin("tests.urd", 1)
	job = urd.build("test_build_kws")
	fin = urd.finish("tests.urd", caption="first")
	assert fin == {'new': False, 'changed': False, 'is_ghost': False}, fin
	urd.begin("tests.urd", 1) # will be overridden to 2 in finish
	jl = urd.latest("tests.urd").joblist
	assert jl == [job], '%r != [%r]' % (jl, job,)
	urd.build("test_build_kws", options=dict(foo='bar', a='A'))
	urd.finish("tests.urd", 2, caption="second")
	u = urd.peek_latest("tests.urd")
	assert u.caption == "second"
	dep0 = list(u.deps.values())[0]
	assert dep0.caption == "first", dep0.caption
	assert dep0.joblist == jl, '%r != %r' % (dep0.joblist, jl,)
	assert urd.since("tests.urd", 0) == ['1', '2']
	urd.truncate("tests.urd", 2)
	assert urd.since("tests.urd", 0) == ['1']
	urd.truncate("tests.urd", 0)
	assert urd.since("tests.urd", 0) == []
	ordered_ts = [1, 2, 1000000000, '1978-01-01', '1978-01-01+0', '1978-01-01+2', '1978-01-01 00:00', '1978-01-01T00:00+42', '2017-06-27', '2017-06-27T17:00:00', '2017-06-27 17:00:00+42']
	for ts in ordered_ts:
		urd.begin("tests.urd")
		if ts == 1000000000:
			urd.get("tests.urd", '1')
		urd.build("test_build_kws")
		urd.finish("tests.urd", ts)
	urd.begin("tests.urd")
	urd.build("test_build_kws")
	urd.finish("tests.urd", ('2019-12', 3))
	ordered_ts.append('2019-12+3')
	ordered_ts = [str(v).replace(' ', 'T') for v in ordered_ts]
	assert urd.since("tests.urd", 0) == ordered_ts
	assert urd.since("tests.urd", '1978-01-01') == ordered_ts[4:]
	assert urd.peek_first("tests.urd").timestamp == '1'
	assert not urd.peek("tests.urd", 2).deps
	dep_jl = list(urd.peek("tests.urd", 1000000000).deps.values())[0].joblist
	assert dep_jl == [job]
	assert urd.peek("tests.urd", ('2017-06-27 17:00:00', 42)).timestamp == '2017-06-27T17:00:00+42'
	while ordered_ts:
		urd.truncate("tests.urd", ordered_ts.pop())
		assert urd.since("tests.urd", 0) == ordered_ts, ordered_ts
	want = [date.today() - timedelta(10), datetime.utcnow()]
	for ts in want:
		urd.begin("tests.urd", ts)
		urd.build("test_build_kws")
		urd.finish("tests.urd")
	assert urd.since("tests.urd", 0) == [str(ts).replace(' ', 'T') for ts in want]
	urd.truncate("tests.urd", 0)

	for how in ("exiting", "dying",):
		print()
		print("Verifying that an analysis process %s kills the job" % (how,))
		time_before = monotonic()
		try:
			job = urd.build("test_analysis_died", how=how)
			print("test_analysis_died completed successfully (%s), that shouldn't happen" % (job,))
			exit(1)
		except JobError:
			time_after = monotonic()
		time_to_die = time_after - time_before
		if time_to_die > 13:
			print("test_analysis_died took %d seconds to die, it should be faster" % (time_to_die,))
			exit(1)
		elif time_to_die > 2:
			print("test_analysis_died took %d seconds to die, so death detection is slow, but works" % (time_to_die,))
		else:
			print("test_analysis_died took %.1f seconds to die, so death detection works" % (time_to_die,))

	print()
	print("Testing dataset creation, export, import")
	source = urd.build("test_datasetwriter")
	urd.build("test_datasetwriter_verify", source=source)
	source = urd.build("test_datasetwriter_copy", source=source)
	urd.build("test_datasetwriter_verify", source=source)
	urd.build("test_datasetwriter_parent")
	urd.build("test_datasetwriter_missing_slices")
	urd.build("test_datasetwriter_default")
	urd.build("test_datasetwriter_parsed")
	urd.build("test_dataset_in_prepare")
	ds = Dataset(source, "passed")
	csvname = "out.csv.gz"
	csvname_uncompressed = "out.csv"
	csv = urd.build("csvexport", filename=csvname, separator="\t", source=ds)
	csv_uncompressed = urd.build("csvexport", filename=csvname_uncompressed, separator="\t", source=ds)
	csv_quoted = urd.build("csvexport", filename=csvname, quote_fields='"', source=ds)
	urd.build("csvexport", filename='slice%d.csv', sliced=True, source=ds) # unused
	reimp_csv = urd.build("csvimport", filename=csv.filename(csvname), separator="\t")
	reimp_csv_uncompressed = urd.build("csvimport", filename=csv_uncompressed.filename(csvname_uncompressed), separator="\t")
	reimp_csv_quoted = urd.build("csvimport", filename=csv_quoted.filename(csvname), quotes=True)
	urd.build("test_compare_datasets", a=reimp_csv, b=reimp_csv_uncompressed)
	urd.build("test_compare_datasets", a=reimp_csv, b=reimp_csv_quoted)

	print()
	print("Testing job related functions")
	urd.build("test_subjobs_type", typed=ds, untyped=reimp_csv)
	urd.build("test_subjobs_nesting")
	urd.build('test_finish_early')
	urd.build('test_register_file')

	print()
	print("Testing datasets more")
	dsnamejob = urd.build("test_dataset_names")
	# make sure .datasets works with these names (only possible after job finishes)
	assert [ds.name for ds in dsnamejob.datasets] == dsnamejob.load()
	urd.build("test_dataset_column_names")
	mergejob = urd.build("test_dataset_merge")
	urd.build('test_dataset_fanout')
	urd.build("test_dataset_filter_columns")
	urd.build("test_dataset_empty_colname")
	urd.build("test_dataset_nan")
	urd.build('test_dataset_parsing_writer')
	urd.build('test_dataset_overwrite')
	urd.build('test_dataset_rename_columns')
	urd.build('test_dataset_concat')

	print()
	print("Testing order preservation in dicts in options")
	for key_order in (['a', 'c', 'b', 'z', 'd', 'inner'], ['foo', 'bar', 'inner', 'aaa']):
		d = OrderedDict((k, 0) for k in key_order)
		d['inner'] = OrderedDict((k, 1) for k in key_order)
		urd.build("test_options_dict_order", dict=d, key_order=key_order)
	print("And that changing order rebuilds when expected")
	chars4rebuild_test = 'bfhnaeolkgjdmci' # same order as the default in the method
	rb_noopt  = urd.build('test_options_rebuild')
	rb_same   = urd.build('test_options_rebuild', dict=OrderedDict([(c, c) for c in chars4rebuild_test]))
	rb_unord  = urd.build('test_options_rebuild', dict={c: c for c in chars4rebuild_test}) # not ordered, so will be sorted
	rb_sorted = urd.build('test_options_rebuild', dict=OrderedDict([(c, c) for c in sorted(chars4rebuild_test)]))
	rb_other  = urd.build('test_options_rebuild', dict=OrderedDict([(c, c) for c in reversed(chars4rebuild_test)]))
	assert rb_noopt == rb_same  , 'OrderedDict rebuilt job equivalent to default'
	assert rb_unord == rb_sorted, 'OrderedDict rebuilt equivalent non-ordered job'
	assert rb_noopt != rb_unord , 'OrderedDict did not rebuild job with different order'
	assert rb_noopt != rb_other , 'OrderedDict did not rebuild job with different order'

	print()
	print("Testing ['lists'] of datasets and jobs")
	urd.build(
		"test_arg_lists",
		dslist=[
			'',
			mergejob.dataset('a0'),
			mergejob.dataset('a1'),
			mergejob.dataset('b0'),
			mergejob.dataset('b1'),
			mergejob.dataset('c0'),
			mergejob.dataset('c1'),
			None,
		],
		joblist=[None, source, ''],
	)

	print()
	print("Testing csvimport with more difficult files")
	urd.build("test_csvimport_corner_cases")
	urd.build("test_csvimport_separators")
	urd.build("test_csvimport_slicing")

	print()
	print("Testing csvexport with all column types, strange separators, ...")
	urd.build("test_csvexport_naming")
	urd.build("test_csvexport_all_coltypes")
	urd.build("test_csvexport_separators")
	urd.build("test_csvexport_chains")
	urd.build("test_csvexport_quoting")

	print()
	print("Testing dataset typing")
	try:
		# Test if numeric_comma is broken (presumably because no suitable locale
		# was found, since there are not actually any commas in the source dataset.)
		urd.build("dataset_type", source=source, numeric_comma=True, column2type=dict(b="float64"), defaults=dict(b="0"))
		comma_broken = False
	except JobError as e:
		comma_broken = True
		urd.warn()
		urd.warn('SKIPPED NUMERIC COMMA TESTS')
		urd.warn('Follow the instructions in this error to enable numeric comma:')
		urd.warn()
		urd.warn(e.format_msg())
	urd.build("test_dataset_type_None")
	urd.build("test_dataset_type_corner_cases", numeric_comma=not comma_broken)
	urd.build("test_dataset_type_minmax")

	print()
	print("Testing dataset chaining, filtering, callbacks and rechaining")
	selfchain = urd.build("test_selfchain", previous=source)
	urd.build("test_rechain", jobs=dict(selfchain=selfchain))
	urd.build("test_dataset_callbacks")
	urd.build("test_dataset_range")

	print()
	print("Testing dataset sorting and rehashing (with subjobs again)")
	urd.build("test_sorting")
	urd.build("test_sort_stability")
	urd.build("test_sort_chaining")
	urd.build("test_sort_trigger")
	urd.build("test_hashpart")
	urd.build("test_dataset_type_hashing")
	urd.build("test_dataset_type_chaining")

	print()
	print("Test hashlabels")
	urd.build("test_hashlabel")

	print()
	print("Test dataset roundrobin iteration and slicing")
	urd.build("test_dataset_roundrobin")
	urd.build("test_dataset_slice")
	urd.build("test_dataset_unroundrobin")
	urd.build("test_dataset_unroundrobin_trigger")
	urd.build("test_number")
	urd.build("test_nan")

	print()
	print("Test dataset_checksum")
	urd.build("test_dataset_checksum")

	print()
	print("Test csvimport_zip")
	urd.build("test_csvimport_zip")

	print()
	print("Test output handling")
	urd.build("test_output")
	urd.build("test_output_on_error")

	print()
	print("Test special types in options")
	urd.build("test_datetime")
	urd.build("test_path")

	print()
	print("Test various utility functions")
	urd.build("test_optionenum")
	urd.build("test_json")
	urd.build("test_job_save")
	bgsave = urd.build("test_job_save_background")
	assert len(bgsave.files()) == 10 + 4 * urd.info.slices
	urd.build("test_jobwithfile")
	urd.build("test_jobchain")

	print()
	print("Testing that the status stack is correct in exceptions from job building")
	urd.build("test_status_in_exceptions")

	print()
	print("Test shell commands")
	urd.truncate("tests.urd", 0)
	# These have to be rebuilt every time, or the resolving might give other jobs.
	urd.begin("tests.urd", 1)
	a = urd.build('test_shell_data', force_build=True)
	b = urd.build('test_shell_data', force_build=True)
	c = urd.build('test_shell_data', datasets={'previous': a})
	urd.finish("tests.urd")
	urd.begin("tests.urd", "2021-09-27T03:14")
	d = urd.build('test_shell_data', datasets={'previous': c, 'parent': a + '/j'}, jobs={'previous': b, 'extra': c})
	urd.finish("tests.urd")
	urd.begin("tests.urd", "2021-09-27T03:14+1")
	e = urd.build('test_shell_data', jobs={'previous': d, 'extra': a})
	urd.finish("tests.urd")
	urd.build('test_shell_commands', command_prefix=command_prefix)
	# ~ finds earlier jobs with that method, ^ follows jobs.previous falling back to datasets.previous.
	want = {
		'test_shell_data': e, # just the plain method -> job resolution.
		c + '~~': a, # not using .previous, just going back jobs
		'test_shell_data~3': b, # numbered tildes
		'test_shell_data~2^': a, # ~~ goes to c, ^ follows .previous to a.
		d + '^': b, # prefers jobs.previous to .datasets.previous
		d + '.parent': a, # will be different as a ds
		':tests.urd:': e,
		':tests.urd/2021-09-27T03:14:': d,
		':tests.urd/1:1': b, # 1 is the second entry
		':tests.urd/1:-3': a, # third entry from the end
		':tests.urd:^': d,
		':tests.urd/2021-09-27T03:14+1^^:0': a, # ^ in :: goes to earlier entries
		':tests.urd/1~:': d, # ~ in :: goes to later entries
		':tests.urd/2021-09-27T03:14:.extra': c,
		':tests.urd/2021-09-27T03:14+1:.extra': a,
		':tests.urd/2021-09-27T03:14+1:.jobs.previous': d,
		':tests.urd/2021-09-27T03:14+1:.jobs.previous.extra': c,
		':tests.urd/2021-09-27T03:14+1:.jobs.previous.jobs.extra.datasets.previous': a,
		':tests.urd/2021-09-27T03:14+1:.jobs.previous.parent': a, # will be different as a ds
		':tests.urd/2021-09-27T03:14+1:.jobs.previous~.datasets.previous': a,
	}
	urd.build('test_shell_job', command_prefix=command_prefix, want=want)
	# most of the old specs give the same results
	want = {spec: job + '/default' for spec, job in want.items()}
	want.update({
		d + '.parent': a + '/j', # this changes as a ds
		':tests.urd/2021-09-27T03:14+1:.jobs.previous.parent': a + '/j', # and this too
		# below here are new things, not overrides.
		d + '/j^': a + '/j', # .parent
		d + '/j~': b + '/j', # .previous
		'test_shell_data~/j^': a + '/j', # both job and ds movement
		e + '/j~^': a + '/j', # .previous.parent
		# some urdlist ones with datasets on
		':tests.urd:/j': e + '/j',
		':tests.urd/1:1/j': b + '/j',
		':tests.urd:^/j': d + '/j',
		':tests.urd/2021-09-27T03:14:/j': d + '/j',
		# finally one with : in the list and / in the ds name
		':tests.urd/2021-09-27T03:14+1:0/name/with/slash': e + '/name/with/slash',
	})
	urd.build('test_shell_ds', command_prefix=command_prefix, want=want)
	urd.truncate("tests.urd", 0)
	urd.build('test_shell_grep', command_prefix=command_prefix)
	urd.build('test_shell_config', command_prefix=command_prefix)

	print()
	print("Test board")
	urd.build('test_board_metadata', command_prefix=command_prefix)

	summary = urd.build("test_summary", joblist=urd.joblist_all)
	summary.link_result('summary.html')
