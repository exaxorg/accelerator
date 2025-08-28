# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2024 Carl Drougge                       #
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

import os
from signal import SIGTERM, SIGKILL

from accelerator.compat import monotonic

from accelerator.statmsg import children, statmsg_endwait
from accelerator.build import JobError


valid_fds = []
def update_valid_fds():
	# Collect all valid fds, so we can close them in job processes
	global valid_fds
	valid_fds = []
	from fcntl import fcntl, F_GETFD
	from resource import getrlimit, RLIMIT_NOFILE
	for fd in range(3, getrlimit(RLIMIT_NOFILE)[0]):
		try:
			fcntl(fd, F_GETFD)
			valid_fds.append(fd)
		except Exception:
			pass

def close_fds(keep):
	for fd in valid_fds:
		# Apparently sometimes one of them has gone away.
		# That's a little worrying, so try to protect our stuff (and ignore errors).
		try:
			if fd not in keep:
				os.close(fd)
		except OSError:
			pass

def run(cmd, close_in_child, keep_in_child, no_stdin=True):
	child = os.fork()
	if child:
		return child
	for fd in close_in_child:
		os.close(fd)
	keep_in_child = set(keep_in_child)
	keep_in_child.add(int(os.getenv('BD_STATUS_FD')))
	keep_in_child.add(int(os.getenv('BD_TERM_FD')))
	close_fds(keep_in_child)
	if no_stdin:
		# unreadable stdin - less risk of stuck jobs
		devnull = os.open('/dev/null', os.O_RDONLY)
		os.dup2(devnull, 0)
		os.close(devnull)
	keep_in_child.update([1, 2])
	for fd in keep_in_child:
		os.set_inheritable(fd, True)
	os.execv(cmd[0], cmd)
	os._exit()

def launch(workdir, setup, config, Methods, active_workdirs, slices, concurrency, server_url, subjob_cookie, parent_pid):
	starttime = monotonic()
	jobid = setup.jobid
	method = setup.method
	if subjob_cookie:
		print_prefix = ''
	else:
		print_prefix = '    '
	print(f'{print_prefix}| {jobid} [{method}] |')
	args = dict(
		workdir=workdir,
		slices=slices,
		concurrency=concurrency,
		jobid=jobid,
		workdirs=active_workdirs,
		server_url=server_url,
		subjob_cookie=subjob_cookie,
		parent_pid=parent_pid,
		debuggable=config.debuggable,
	)
	from accelerator.runner import runners
	runner = runners[Methods.db[method].version]
	child, prof_r = runner.launch_start(args)
	# There's a race where if we get interrupted right after fork this is not recorded
	# (the launched job could continue running)
	try:
		children.add(child)
		status, data = runner.launch_finish(child, prof_r, workdir, jobid, method)
		if status:
			try:
				os.killpg(child, SIGTERM) # give it a chance to exit gracefully
			except OSError:
				# On macOS you get EPERM if the child has exited (tested on 12.6.1)
				pass
			# The dying process won't have sent an end message, so it has
			# the endwait time until we SIGKILL it.
			print(f'{print_prefix}| {jobid} [{method}]  failed!    ({monotonic() - starttime:5.1f}s) |')
		# There is a race where stuff on the status socket has not arrived when
		# the sending process exits. This is basically benign, but let's give
		# it a chance to arrive to cut down on confusing warnings.
		statmsg_endwait(child, 0.1)
	finally:
		try:
			os.killpg(child, SIGKILL) # this should normally be a no-op, but in case it left anything.
		except Exception:
			pass
		try:
			children.remove(child)
		except Exception:
			pass
		try:
			# won't block long (we just killed it, plus it had probably already exited)
			runner.launch_waitpid(child)
		except Exception:
			pass
	if status:
		raise JobError(jobid, method, status)
	print(f'{print_prefix}| {jobid} [{method}]  completed. ({monotonic() - starttime:5.1f}s) |')
	return data
