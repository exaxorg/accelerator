# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2024 Carl Drougge                       #
# Modifications copyright (c) 2020-2021 Anders Berkeman                    #
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

from threading import Thread
import multiprocessing
import signal
from os import unlink
from os.path import join
import time

from accelerator import dependency
from accelerator import dispatch

from accelerator import workspace
from accelerator import database
from accelerator import methods
from accelerator.colourwrapper import colour
from accelerator.compat import FileNotFoundError
from accelerator.setupfile import update_setup
from accelerator.job import WORKDIRS, Job
from accelerator.extras import json_save, DotDict
from accelerator.error import BuildError




class Main:
	""" This is the main controller behind the server. """

	def __init__(self, config, options, server_url):
		"""
		Setup objects:

		  Methods

		  WorkSpaces

		"""
		self.config = config
		self.keep_temp_files = options.keep_temp_files
		self.server_url = server_url
		self._update_methods()
		self.target_workdir = self.config['target_workdir']
		self.workspaces = {}
		for name, path in self.config.workdirs.items():
			self.workspaces[name] = workspace.WorkSpace(name, path, config.slices)
		WORKDIRS.clear()
		WORKDIRS.update({k: v.path for k, v in self.workspaces.items()})
		self.DataBase = database.DataBase(self)
		print('Scanning all workdirs...')
		self.update_database()
		print('\x1b[A\x1b[24Cdone.')
		self.broken = False

	def _update_methods(self):
		print('Update methods')
		# initialise methods class looking in method_directories from config file
		self.Methods = methods.Methods(self.config)

	def update_methods(self):
		try:
			self._update_methods()
			self.update_database()
			self.broken = False
		except methods.MethodLoadException as e:
			self.broken = e.module_list
			return {'broken': e.module_list}


	def get_workspace_details(self):
		""" Some information about main workspace, some parts of config """
		return dict(
			[(key, getattr(self.workspaces[self.target_workdir], key),) for key in ('slices',)] +
			[(key, self.config.get(key),) for key in ('input_directory', 'result_directory', 'urd',)]
		)


	def list_workdirs(self):
		""" Return list of all initiated workdirs """
		return self.workspaces


	def print_workdirs(self):
		namelen = max(len(n) for n in self.workspaces)
		templ = "    %%s %%%ds: %%s %s(%%d)" % (namelen, colour.RESET,)
		print("Available workdirs:")
		names = sorted(self.workspaces)
		names.remove(self.target_workdir)
		names.insert(0, self.target_workdir)
		for n in names:
			if n == self.target_workdir:
				prefix = 'TARGET  ' + colour.BOLD
			else:
				prefix = 'SOURCE  '
			w = self.workspaces[n]
			print(templ % (prefix, n, w.path, w.slices,))


	def add_single_jobid(self, jobid):
		ws = self.workspaces[jobid.rsplit('-', 1)[0]]
		ws.add_single_jobid(jobid)
		return self.DataBase.add_single_jobid(jobid)

	def update_database(self):
		"""Insert all new jobids (from all workdirs) in database,
		discard all deleted or with incorrect hash.
		"""
		if hasattr(multiprocessing, 'get_context') and not running_under_wsl():
			# forkserver is apparently broken under (some versions of) WSL1
			ctx = multiprocessing.get_context('forkserver')
			Pool = ctx.Pool
		else:
			Pool = multiprocessing.Pool
		pool = Pool(initializer=_pool_init, initargs=(WORKDIRS,))
		try:
			self.DataBase._update_begin()
			def update(name):
				ws = self.workspaces[name]
				ws.update(pool)
				self.DataBase._update_workspace(ws, pool)
				failed.remove(name)
			failed = set(self.workspaces)
			t_l = []
			for name in self.workspaces:
				# Run all updates in parallel. This gets all (sync) listdir calls
				# running at the same time. Then each workspace will use the same
				# process pool to do the post.json checking.
				t = Thread(
					target=update,
					args=(name,),
					name='Update ' + name,
				)
				t.daemon = True
				t.start()
				t_l.append(t)
			for t in t_l:
				t.join()
			assert not failed, f"{', '.join(failed)} failed to update"
		finally:
			pool.close()
		self.DataBase._update_finish(self.Methods.hash)


	def initialise_jobs(self, setup, workdir=None):
		""" Update database, check deps, create jobids. """
		ws = workdir or self.target_workdir
		if ws not in self.workspaces:
			raise BuildError(f"Workdir {ws} does not exist")
		return dependency.initialise_jobs(
			setup,
			self.workspaces[ws],
			self.DataBase,
			self.Methods,
			self.config,
		)


	def run_job(self, jobid, subjob_cookie=None, parent_pid=0, concurrency=None):
		W = self.workspaces[Job(jobid).workdir]
		#
		active_workdirs = {name: ws.path for name, ws in self.workspaces.items()}
		slices = self.workspaces[self.target_workdir].slices

		t0 = time.time()
		setup = update_setup(jobid, starttime=t0)
		prof = setup.get('exectime', DotDict())
		new_prof, files, subjobs = dispatch.launch(W.path, setup, self.config, self.Methods, active_workdirs, slices, concurrency, self.server_url, subjob_cookie, parent_pid)
		files = finish_job_files(jobid, files, self.keep_temp_files)
		prof.update(new_prof)
		prof.total = 0
		prof.total = sum(v for v in prof.values() if isinstance(v, (float, int)))
		if concurrency:
			prof.concurrency = concurrency
		data = dict(
			starttime=t0,
			endtime=time.time(),
			exectime=prof,
		)
		update_setup(jobid, **data)
		data['files'] = files
		data['subjobs'] = subjobs
		data['version'] = 1
		json_save(data, jobid.filename('post.json'))


	def get_methods(self):
		return {k: self.method_info(k) for k in self.Methods.db}


	def method_info(self, method):
		d = self.Methods.db.get(method, None)
		if d:
			d = dict(d)
			p = self.Methods.params[method]
			for k in ('options', 'datasets', 'jobs'):
				d[k] = [v[0] if isinstance(v, (list, tuple)) else v for v in p[k]]
			d['description'] = self.Methods.descriptions[method]
			return d


def finish_job_files(job, files, keep_temp_files=False):
	prefix = job.path + '/'
	if not keep_temp_files:
		for filename, temp in list(files.items()):
			if temp:
				try:
					unlink(join(prefix, filename))
				except FileNotFoundError:
					pass
				del files[filename]
	return sorted(fn[len(prefix):] if fn.startswith(prefix) else fn for fn in files)


def _pool_init(workdirs):
	# The pool system will send SIGTERM when the pool is closed, so
	# restore the original behaviour for that.
	signal.signal(signal.SIGTERM, signal.SIG_DFL)
	WORKDIRS.update(workdirs)


# This can't distinguish between WSL1 and WSL2.
_running_under_wsl = None
def running_under_wsl():
	global _running_under_wsl
	if _running_under_wsl is None:
		try:
			with open('/proc/sys/kernel/osrelease', 'rb') as fh:
				osrelease = fh.read().lower()
			_running_under_wsl = (b'microsoft' in osrelease or b'wsl' in osrelease)
		except IOError:
			_running_under_wsl = False
	return _running_under_wsl
