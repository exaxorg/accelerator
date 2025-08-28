# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2018-2024 Carl Drougge                       #
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

# In methods you only use the status function from this module.
# It works like this:
#
# with status("some text") as update:
#   long running code here
#
# then you can see how long your code has been doing that part in ^T.
# If you want to change the status text (without resetting the time,
# and possibly with other statuses nested below it) you can call the
# update function with the new text. If you don't need to update the
# text you can of course use just "with status(...):".
#
# Several built in functions will call this for you, notably dataset
# iterators and pickle load/save functions.

from contextlib import contextmanager
from errno import ENOTCONN
from functools import partial
from time import sleep
from traceback import print_exc
from threading import Lock
from weakref import WeakValueDictionary
import socket
import os
import sys

from accelerator.compat import str_types, iteritems, monotonic
from accelerator.colourwrapper import colour
from accelerator import g


status_tree = {}
status_all = WeakValueDictionary()
status_stacks_lock = Lock()


# all currently (or recently) running launch.py PIDs
class Children(set):
	__slots__ = ()
	def add(self, pid):
		with status_stacks_lock:
			set.add(self, pid)
	def remove(self, pid):
		with status_stacks_lock:
			d = status_all.pop(pid, None)
			if d and d.parent_pid in status_all:
				status_all[d.parent_pid].children.pop(pid, None)
			status_tree.pop(pid, None)
			set.remove(self, pid)
children = Children()


_cookie = 0

_local_status = []
_exc_status = ()

@contextmanager
def status(msg):
	if g.running in ('server', 'build', 'shell',):
		yield lambda _: None
		return
	global _cookie
	global _exc_status
	_exc_status = ()
	_cookie += 1
	cookie = str(_cookie)
	t = str(monotonic())
	typ = 'push'
	# capture the PID here, because update might be called in a different process
	pid = os.getpid()
	update_local = _local_status.append
	def update(msg):
		assert msg and isinstance(msg, str_types) and '\0' not in msg
		update_local((msg, update._line_report))
		_send(typ, '\0'.join((msg, t, cookie)), pid=pid)
	update._line_report = [None] # a list so it can be updated without access to the update object
	update(msg)
	update_local = partial(_local_status.__setitem__, len(_local_status) - 1)
	typ = 'update'
	# Exceptions work a bit strangely in context managers, so we may not
	# actually see the exception, but the finally block still runs.
	missed_exception = True
	try:
		yield update
		missed_exception = False
		_exc_status = ()
	finally:
		if missed_exception and not _exc_status:
			# only the innermost one saves, so we preserve the whole list
			_exc_status = list(_local_status)
		_local_status.pop()
		_send('pop', cookie)

# Same interface as status, but without the actual reporting
@contextmanager
def dummy_status(msg):
	assert msg and isinstance(msg, str_types) and '\0' not in msg
	yield lambda _: None

def _start(msg, parent_pid, is_analysis=False):
	global _cookie
	if is_analysis:
		_cookie += 1
		analysis_cookie = str(_cookie)
	else:
		analysis_cookie = ''
	_send('start', f'{parent_pid}\x00{analysis_cookie}\x00{msg}\x00{monotonic():f}')
	def update(msg):
		_send('update', f'{msg}\x00\x00{analysis_cookie}')
	return update

def _end(pid=None):
	_send('end', '', pid=pid)

def _output(pid, msg):
	_send('output', f'{monotonic():f}\x00{msg}', pid=pid)

def _clear_output(pid):
	_send('output', '', pid=pid)


def status_stacks_export():
	res = []
	last = [None]
	current = None
	def fmt(tree, start_indent=0):
		for pid, d in sorted(iteritems(tree), key=lambda i: (i[1].stack or ((0,),))[0][0]):
			last[0] = d
			indent = start_indent
			for msg, t, _ in d.stack:
				res.append((pid, indent, msg, t))
				indent += 1
			fmt(d.children, indent)
			if d.output:
				res.append((pid, -1,) + d.output)
	try:
		with status_stacks_lock:
			fmt(status_tree)
		if last[0]:
			current = last[0].summary
			if len(last[0].stack) > 1 and not current[1].endswith('analysis'):
				msg, t, _ = last[0].stack[1]
				current = (current[0], f'{current[1]} {msg}', t,)
	except Exception:
		print_exc(file=sys.stderr)
		res.append((0, 0, 'ERROR', monotonic()))
	return res, current

def print_status_stacks(stacks=None):
	if stacks == None:
		stacks, _ = status_stacks_export()
	report_t = monotonic()
	for pid, indent, msg, t in stacks:
		if indent < 0:
			print(f"{pid:6} TAIL OF OUTPUT: ({report_t - t:.1f} seconds ago)")
			msgs = list(filter(None, msg.split('\n')))[-3:]
			for msg in msgs:
				print("       " + colour.green(msg))
		else:
			print(f"{pid:6} STATUS: {'    ' * indent}{msg} ({report_t - t:.1f} seconds)")


def _find(pid, cookie):
	stack = status_all[pid].stack
	# should normally be the last one, or at least close to it.
	for ix in range(len(stack) -1, -1, -1):
		if stack[ix][2] == cookie:
			return stack, ix
	return stack, None

def statmsg_sink(sock):
	from accelerator.extras import DotDict
	wrong_pops = 0
	while True:
		data = None
		try:
			data = sock.recv(1500)
			typ, pid, msg = data.decode('utf-8').split('\0', 2)
			pid = int(pid)
			with status_stacks_lock:
				if typ == 'push':
					msg, t, cookie = msg.split('\0', 3)
					t = float(t)
					status_all[pid].stack.append((msg, t, cookie))
				elif typ == 'pop':
					stack, ix = _find(pid, msg)
					if ix == len(stack) - 1:
						stack.pop()
					else:
						print(f'POP OF WRONG STATUS: {pid}:{msg} (index {ix} of {len(stack)})')
						wrong_pops += 1
						if wrong_pops == 3:
							print('Getting a lot of these? Are you interleaving dataset iterators? Set status_reporting=False on all but one.')
							wrong_pops = 0
				elif typ == 'update':
					msg, _, cookie = msg.split('\0', 3)
					stack, ix = _find(pid, cookie)
					if ix is None:
						print(f'UPDATE TO UNKNOWN STATUS {pid}:{cookie}: {msg}')
					else:
						stack[ix] = (msg, stack[ix][1], cookie)
				elif typ == 'output':
					if msg:
						t, msg = msg.split('\0', 1)
						t = float(t)
						status_all[pid].output = (msg, t,)
					else:
						status_all[pid].output = None
				elif typ == 'start':
					parent_pid, is_analysis, msg, t = msg.split('\0', 3)
					parent_pid = int(parent_pid)
					t = float(t)
					d = DotDict()
					d.parent_pid = parent_pid
					d.children   = {}
					d.stack      = [(msg, t, is_analysis or None)]
					d.summary    = (t, msg, t,)
					d.output     = None
					if parent_pid in status_all:
						if is_analysis:
							msg, parent_t, _ = status_all[parent_pid].stack[0]
							d.summary = (parent_t, msg + ' analysis', t,)
						status_all[parent_pid].children[pid] = d
					else:
						status_tree[pid] = d
					status_all[pid] = d
					del d
				elif typ == 'end':
					d = status_all.pop(pid, None)
					if d:
						if d.parent_pid in status_all:
							status_all[d.parent_pid].children.pop(pid, None)
						del d
					status_tree.pop(pid, None)
				else:
					print(f'UNKNOWN MESSAGE: {data!r}')
		except Exception:
			print(f'Failed to process {data!r}:', file=sys.stderr)
			print_exc(file=sys.stderr)


def statmsg_endwait(pid, timeout):
	"""Wait for pid to be removed from status_stacks (to send 'end')"""
	for _ in range(10):
		with status_stacks_lock:
			d = status_all.get(pid)
			if not d:
				return
		sleep(timeout / 10)


_send_sock = None

def _send(typ, message, pid=None):
	global _send_sock
	if not _send_sock:
		fd = int(os.getenv('BD_STATUS_FD'))
		_send_sock = socket.fromfd(fd, socket.AF_UNIX, socket.SOCK_DGRAM)
	header = f'{typ}\x00{pid or os.getpid()}\x00'.encode('utf-8')
	message = message.encode('utf-8')
	if len(message) > 1450:
		message = message[:300] + b'\n....\n' + message[-1100:]
		# Make sure we don't have any partial characters.
		message = message.decode('utf-8', 'ignore').encode('utf-8')
	msg = header + message
	for ix in range(5):
		try:
			_send_sock.send(msg)
			return
		except socket.error as e:
			if e.errno == ENOTCONN:
				# The server is dead, no use retrying.
				return
			print(f'Failed to send statmsg (type {typ}, try {ix}): {e}')
			sleep(0.1 + ix)
