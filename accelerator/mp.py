# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2021-2024 Carl Drougge                                     #
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

import os
import sys
import select
import signal
import fcntl
import multiprocessing
import struct
import pickle
import errno
from traceback import print_exc
from accelerator.compat import QueueEmpty, monotonic, selectors


assert select.PIPE_BUF >= 512, "POSIX says PIPE_BUF is at least 512, you have %d" % (select.PIPE_BUF,)

PIPE_BUF = min(select.PIPE_BUF, 65536)
MAX_PART = PIPE_BUF - 6


def _nb(fd):
	fl = fcntl.fcntl(fd, fcntl.F_GETFL)
	fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)


def _mk_sel(fd, ev):
	sel = selectors.DefaultSelector()
	sel.register(fd, ev)
	return sel


# This provides some of the functionality of multiprocessing.Queue but
# without using any locks, instead relying on writes up to PIPE_BUF being
# atomic. No threads are used.
#
# Only one process can .get() (after .make_reader()). .get() can take a timeout.
# (There are no checks to enforce a single reader, so be careful.)
# Any number of processeses can .put(). .put() blocks until the pipe will take
# all the data.
# A dying writer will never corrupt or deadlock the queue, but can cause a
# memory leak in the reading process (until the queue object is destroyed).
# Large messages are probably quite inefficient.
# You can call .make_*() before forking, but once you have put anything in the
# queue it is no longer fork safe. (Forking is still fine, but not using the
# queue from the new process.)
#
# In short, you often can't use this. And when you do, you have to be careful.

class QueueClosedError(Exception):
	__slots__ = ()

class LockFreeQueue:
	__slots__ = ('r', 'w', '_r_sel', '_w_sel', '_pid', '_buf', '_partial', '_local')

	def __init__(self):
		self._r_sel = self._w_sel = None
		self.r, self.w = os.pipe()
		self._buf = b''
		self._partial = {}
		self._local = []

	def make_writer(self):
		os.close(self.r)
		self.r = -1

	def make_reader(self):
		os.close(self.w)
		self.w = -1

	def close(self):
		if self._r_sel:
			self._r_sel.close()
			self._r_sel = None
		if self._w_sel:
			self._w_sel.close()
			self._w_sel = None
		if self.r != -1:
			os.close(self.r)
			self.r = -1
		if self.w != -1:
			os.close(self.w)
			self.w = -1

	__del__ = close

	def _late_setup(self):
		# The selector must be constructed late so it happens in the right
		# process. Also probably saves an fd before fork (in the selector).
		pid = os.getpid()
		if self._r_sel or self._w_sel:
			assert self._pid == pid, "Can't fork after using"
		else:
			self._pid = pid
			if self.r != -1:
				_nb(self.r)
				self._r_sel = _mk_sel(self.r, selectors.EVENT_READ)
			if self.w != -1:
				_nb(self.w)
				self._w_sel = _mk_sel(self.w, selectors.EVENT_WRITE)

	def get(self, block=True, timeout=0):
		assert self.w == -1, "call make_reader first"
		if self._local:
			return self._local.pop(0)
		self._late_setup()
		if timeout:
			deadline = monotonic() + timeout
		need_data = False
		eof = (self.r == -1)
		while True:
			if not eof:
				try:
					data = os.read(self.r, PIPE_BUF)
					if not data:
						eof = True
					self._buf += data
					need_data = False
				except OSError:
					pass
			if len(self._buf) >= 6:
				z, pid = struct.unpack('<HI', self._buf[:6])
				assert pid, "all is lost"
				if len(self._buf) < 6 + z:
					need_data = True
				else:
					data = self._buf[6:6 + z]
					self._buf = self._buf[6 + z:]
					if pid not in self._partial:
						want_len = struct.unpack("<I", data[:4])[0]
						have = [data[4:]]
						have_len = len(have[0])
					else:
						want_len, have, have_len = self._partial[pid]
						have.append(data)
						have_len += len(data)
					if have_len == want_len:
						self._partial.pop(pid, None)
						data = b''.join(have)
						return pickle.loads(data)
					else:
						self._partial[pid] = (want_len, have, have_len)
			if len(self._buf) < 6 or need_data:
				if eof:
					raise QueueEmpty()
				if not block:
					if not self._r_sel.select(0):
						raise QueueEmpty()
				elif timeout:
					time_left = deadline - monotonic()
					if time_left <= 0:
						raise QueueEmpty()
					self._r_sel.select(time_left)
				else:
					self._r_sel.select()

	def put_local(self, msg):
		self._local.append(msg)

	def put(self, msg):
		assert self.r == -1, "call make_writer first"
		if self.w == -1:
			raise QueueClosedError()
		self._late_setup()
		msg = pickle.dumps(msg, pickle.HIGHEST_PROTOCOL)
		msg = struct.pack('<I', len(msg)) + msg
		offset = 0
		while offset < len(msg):
			part = msg[offset:offset + MAX_PART]
			part = struct.pack('<HI', len(part), self._pid) + part
			offset += MAX_PART
			while True:
				self._w_sel.select()
				try:
					wlen = os.write(self.w, part)
				except OSError as e:
					if e.errno == errno.EAGAIN:
						wlen = 0
					else:
						self.close()
						raise QueueClosedError()
				if wlen:
					if wlen != len(part):
						print("OS violates PIPE_BUF guarantees, all is lost.", file=sys.stderr)
						while True:
							try:
								# this should eventually make the other side read PID 0
								os.write(self.w, b'\0')
							except OSError:
								pass
					break

	def try_notify(self):
		pid = os.getpid()
		msg = struct.pack('<HII', 6, pid, 2) + b'N.' # pickled None
		try:
			os.write(self.w, msg)
			return True
		except OSError:
			return False


# This is a partial replacement for multiprocessing.Process.
# It doesn't work if you use the rest of the mp machinery (like Queues)
# and always uses os.fork(). It exists because multiprocessing.Process
# has scaling issues with many children.

class SimplifiedProcess:
	__slots__ = ('pid', 'name', '_alive', 'exitcode')

	def __init__(self, target, args=(), kwargs={}, name=None, stdin=None, ignore_EPIPE=False):
		sys.stdout.flush()
		sys.stderr.flush()
		self.pid = os.fork()
		if self.pid:
			self.name = name
			self._alive = True
			return
		rc = 1
		try:
			if stdin:
				os.dup2(stdin, 0)
				os.close(stdin)
			target(*args, **kwargs)
			rc = 0
		except KeyboardInterrupt:
			signal.signal(signal.SIGINT, signal.SIG_DFL)
			os.kill(os.getpid(), signal.SIGINT)
		except Exception as e:
			if not isinstance(e, OSError) or e.errno != errno.EPIPE:
				print("Exception in %d %r:" % (os.getpid(), name), file=sys.stderr)
				print_exc(file=sys.stderr)
			elif ignore_EPIPE:
				rc = 0
		except SystemExit as e:
			if e.code is None:
				rc = 0
			elif isinstance(e.code, int):
				rc = e.code
			else:
				print(e.code, file=sys.stderr)
		finally:
			os._exit(rc)

	def is_alive(self):
		self._wait(False)
		return self._alive

	def _wait(self, block):
		if not self._alive:
			return
		pid, status = os.waitpid(self.pid, 0 if block else os.WNOHANG)
		if pid:
			assert pid == self.pid
			self._alive = False
			if os.WIFEXITED(status):
				self.exitcode = os.WEXITSTATUS(status)
			elif os.WIFSIGNALED(status):
				self.exitcode = -os.WTERMSIG(status)
			else:
				self.exitcode = -999

	def join(self):
		self._wait(True)


class MpSet:
	__slots__ = ('_lock', '_q_r', '_q_w', '_p', '_broken')

	def __init__(self, initial=(), _set_cls=set):
		self._lock = multiprocessing.Lock()
		self._q_r = LockFreeQueue()
		self._q_w = LockFreeQueue()
		self._p = SimplifiedProcess(target=self._process, args=(_set_cls(initial),), name='MpSet')
		self._q_r.make_reader()
		self._q_w.make_writer()
		self._broken = False

	def _process(self, s):
		fd_keep = {self._q_r.r, self._q_r.w, self._q_w.r, self._q_w.w}
		fd = 0
		fd_end = fd + 20
		while fd < fd_end:
			if fd not in fd_keep:
				try:
					os.close(fd)
					fd_end = fd + 20
				except OSError:
					pass
			fd += 1
		self._q_r.make_writer()
		self._q_w.make_reader()
		while True:
			try:
				funcname, a = self._q_w.get()
			except QueueEmpty:
				return
			try:
				res = getattr(s, funcname)(*a)
				err = None
			except Exception as e:
				res = None
				err = e
			self._q_r.put((res, err))

	def _call(self, funcname, *a):
		assert not self._broken
		self._lock.acquire()
		try:
			self._q_w.put((funcname, a))
			res, err = self._q_r.get()
		except:
			self._broken = True
			raise
		finally:
			self._lock.release()
		if err:
			raise err
		return res

	def add(self, value):
		return self._call('add', value)

	def clear(self):
		return self._call('clear')

	def pop(self):
		return self._call('pop')

	def remove(self, value):
		return self._call('remove', value)

	def __contains__(self, value):
		return self._call('__contains__', value)

	def __len__(self):
		return self._call('__len__')
