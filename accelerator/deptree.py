# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2024 Carl Drougge                       #
# Modifications copyright (c) 2023 Pablo Correa Gómez                      #
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

from traceback import print_exc
from collections import OrderedDict
from datetime import datetime, date, time, timedelta
from pathlib import Path, PosixPath, PurePath, PurePosixPath
import sys

from accelerator.compat import iteritems, itervalues, first_value, str_types, int_types, num_types

from accelerator.extras import OptionEnum, OptionEnumValue, _OptionString, OptionDefault, RequiredOption, typing_conv
from accelerator.job import JobWithFile

class OptionException(Exception):
	__slots__ = ()
	pass

_date_types = (datetime, date, time, timedelta)

class DepTree:

	def __init__(self, methods, setup):
		self.methods = methods
		m = self.method = setup.method
		self.params = {
			'options': setup.options,
			'datasets': setup.datasets,
			'jobs': setup.jobs,
		}
		self.item = {
			'method': m,
			'params': {m: self.params},
			'make': False,
			'link': False,
			'uid': 0,
		}
		self._fix_options(False)
		self._fix_jobids('jobs')
		self._fix_jobids('datasets')

	def get_reqlist(self):
		params = {k: dict(v) for k, v in iteritems(self.methods.params[self.method].defaults)}
		for k, v in iteritems(self.params):
			params[k].update(v)
		return [(self.method, 0, self.methods.params2optset({self.method: params}))]

	def fill_in_default_options(self):
		self._fix_options(True)

	def _fix_jobids(self, key):
		method = self.method
		data = self.params[key]
		method_wants = self.methods.params[method][key]
		res = {}
		for jobid_name in method_wants:
			if isinstance(jobid_name, str_types):
				value = data.get(jobid_name)
				assert value is None or isinstance(value, str), f'Input {jobid_name} on {method} not a string as required'
			elif isinstance(jobid_name, list):
				if len(jobid_name) != 1 or not isinstance(jobid_name[0], str_types):
					raise OptionException(f'Bad {key} item on {method}: {jobid_name!r}')
				jobid_name = jobid_name[0]
				value = data.get(jobid_name)
				if value:
					if isinstance(value, str_types):
						value = [e.strip() for e in value.split(',')]
				else:
					value = []
				assert isinstance(value, list), f'Input {jobid_name} on {method} not a list or string as required'
			else:
				raise OptionException(f'{key} item of unknown type {type(jobid_name)} on {method}: {jobid_name!r}')
			res[jobid_name] = value
		self.params[key] = res
		spill = set(data) - set(res)
		if spill:
			raise OptionException(f"Unknown {key} on {method}: {', '.join(sorted(spill))}")

	def _fix_options(self, fill_in):
		method = self.method
		options = self.methods.params[method].options
		res_options = {}
		def typefuzz(t):
			if issubclass(t, str_types):
				return str_types
			if issubclass(t, int_types):
				return int_types
			return t
		def convert(default_v, v):
			if isinstance(default_v, RequiredOption):
				if v is None and not default_v.none_ok:
					raise OptionException(f'Option {k} on method {method} requires a non-None value ({default_v.value!r})')
				default_v = default_v.value
			if default_v is None or v is None:
				if isinstance(default_v, _OptionString):
					raise OptionException(f'Option {k} on method {method} requires a non-empty string value')
				if hasattr(default_v, '_valid') and v not in default_v._valid:
					raise OptionException(f'Option {k} on method {method} requires a value in {default_v._valid}')
				if isinstance(default_v, OptionDefault):
					v = default_v.default
				return v
			if isinstance(default_v, OptionDefault):
				default_v = default_v.value
			if isinstance(default_v, dict) and isinstance(v, dict):
				if default_v:
					sample_v = first_value(default_v)
					for chk_v in itervalues(default_v):
						assert isinstance(chk_v, type(sample_v))
					assert isinstance(v, OrderedDict)
					return OrderedDict((k, convert(sample_v, v)) for k, v in iteritems(v))
				else:
					return v
			if isinstance(default_v, (list, set, tuple,)) and isinstance(v, str_types + (list, set, tuple,)):
				if isinstance(v, str_types):
					v = (e.strip() for e in v.split(','))
				if default_v:
					sample_v = first_value(default_v)
					for chk_v in default_v:
						assert isinstance(chk_v, type(sample_v))
					v = (convert(sample_v, e) for e in v)
				return type(default_v)(v)
			if isinstance(default_v, (OptionEnum, OptionEnumValue,)):
				if not (v or None) in default_v._valid:
					ok = False
					for cand_prefix in default_v._prefixes:
						if v.startswith(cand_prefix):
							ok = True
							break
					if not ok:
						raise OptionException(f'{v!r} not a permitted value for option {k} on method {method} ({default_v._valid})')
				return v or None
			if isinstance(default_v, str_types + num_types) and isinstance(v, str_types + num_types):
				if isinstance(default_v, _OptionString):
					v = str(v)
					if not v:
						raise OptionException(f'Option {k} on method {method} requires a non-empty string value')
					return v
				if isinstance(default_v, str) and isinstance(v, bytes):
					return v.decode('utf-8')
				return type(default_v)(v)
			if (isinstance(default_v, type) and isinstance(v, typefuzz(default_v))) or isinstance(v, typefuzz(type(default_v))):
				return v
			if isinstance(default_v, bool) and isinstance(v, (str, int)):
				lv = str(v).lower()
				if lv in ('true', '1', 't', 'yes', 'on',):
					return True
				if lv in ('false', '0', 'f', 'no', 'off', '',):
					return False
			if isinstance(default_v, _date_types):
				default_v = type(default_v)
			if isinstance(default_v, PosixPath) or default_v is PosixPath:
				default_v = Path
			if isinstance(default_v, PurePosixPath) or default_v is PurePosixPath:
				default_v = PurePath
			if default_v in _date_types + (Path, PurePath,):
				try:
					return typing_conv[default_v.__name__](v)
				except Exception:
					raise OptionException(f'Failed to convert option {k} {v!r} to {default_v} on method {method}')
			if isinstance(v, str_types) and not v:
				return type(default_v)()
			if isinstance(default_v, JobWithFile) or default_v is JobWithFile:
				defaults = ('', '', False, None,)
				if default_v is JobWithFile:
					default_v = defaults
				if not isinstance(v, (list, tuple,)) or not (2 <= len(v) <= 4):
					raise OptionException(f'Option {k} ({v!r}) on method {method} is not {type(default_v)} compatible')
				v = tuple(v) + defaults[len(v):] # so all of default_v gets convert()ed.
				v = [convert(dv, vv) for dv, vv in zip(default_v, v)]
				return JobWithFile(*v)
			if type(default_v) != type:
				default_v = type(default_v)
			raise OptionException(f'Failed to convert option {k} of {type(v)} to {default_v} on method {method}')
		for k, v in iteritems(self.params['options']):
			if k in options:
				try:
					res_options[k] = convert(options[k], v)
				except OptionException:
					raise
				except Exception:
					print_exc(file=sys.stderr)
					raise OptionException(f'Failed to convert option {k} on method {method}')
			else:
				raise OptionException(f'Unknown option {k} on method {method}')
		if fill_in:
			missing = set(options) - set(res_options)
			missing_required = missing & self.methods.params[method].required
			if missing_required:
				raise OptionException('Missing required options {%s} on method %s' % (', '.join(sorted(missing_required)), method,))
			defaults = self.methods.params[method].defaults
			res_options.update({k: defaults.options[k] for k in missing})
		self.params['options'] = res_options

	def set_link(self, uid, job):
		self.item['link'] = job.id
		self.item['total_time'] = job.total

	def propagate_make(self):
		self.item['make'] = not self.item['link']

	def get_sorted_joblist(self):
		return [self.item]
