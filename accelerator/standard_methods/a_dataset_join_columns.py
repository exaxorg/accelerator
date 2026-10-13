############################################################################
#                                                                          #
# Copyright (c) 2025 Carl Drougge                                          #
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
Joins two or more columns with an optional separator.

Supports string-like types and also date + time joining into a datetime column.
If any of the joined values are None, the whole joined value is None.
'''


from datetime import datetime
from itertools import starmap
from functools import partial


datasets = ('source', 'previous',)

options = dict(
	columns=[str],
	colname='joined column',
	discard_source_columns=False,
	separator='',
)


def prepare(job):
	if len(options.columns) < 2:
		raise Exception("Specify at least two columns")
	chain = datasets.source.chain(stop_ds={datasets.previous: 'source'})
	allowed = [
		{'ascii'},
		{'bytes'},
		{'unicode'},
		{'ascii', 'unicode'},
	]
	if len(options.columns) == 2:
		allowed.append({'date', 'time'})
	for ds in chain:
		types = set()
		for col in options.columns:
			if col not in ds.columns:
				raise Exception(f"Column {col !r} doesn't exist in {ds.quoted}")
			types.add(ds.columns[col].type)
		if types not in allowed:
			if types == {'date', 'time'}:
				raise Exception(f"Can't join {len(options.columns)} colums of date and time (in {ds.quoted})")
			raise Exception(f"Unjoinable column types {types} in {ds.quoted}")
	names = list(map(str, range(len(chain))))
	names[-1] = 'default'
	previous = datasets.previous
	writers = []
	columns = dict.fromkeys(options.columns) if options.discard_source_columns else {}
	for ds, name in zip(chain, names):
		# unicode sorts after ascii, so this is the the right type.
		desttype = sorted(ds.columns[n].type for n in options.columns)[-1]
		if desttype == 'time':
			desttype = 'datetime'
		none_support = any(ds.columns[colname].none_support for colname in options.columns)
		columns[options.colname] = (desttype, none_support)
		dw = job.datasetwriter(name=name, previous=previous, parent=ds, columns=columns)
		writers.append(dw)
		previous = dw
	return writers


def analysis(sliceno, prepare_res):
	joiners = {
		'ascii'  : partial(map, options.separator.join),
		'bytes'  : partial(map, options.separator.encode('utf-8').join),
		'date'   : partial(starmap, datetime.combine),
		'unicode': partial(map, options.separator.join),
	}
	for writer in prepare_res:
		ds = writer.parent
		write = writer.write
		columns = options.columns
		typ = ds.columns[columns[0]].type
		if typ == 'time':
			typ = 'date'
			columns = [columns[1], columns[0]]
		joiner = joiners[typ]
		if any(ds.columns[colname].none_support for colname in columns):
			joiner = noner(joiner)
		for value in joiner(ds.iterate(sliceno, columns)):
			write(value)


def noner(joiner):
	f = joiner.args[0]
	def nonefilter(*a):
		try:
			return f(*a)
		except TypeError:
			return None
	return partial(joiner.func, nonefilter)
