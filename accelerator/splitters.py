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


from accelerator.error import AcceleratorError
import weakref


_analysis_todo = []
_synthesis_todo = []

def _analysis_start(sliceno):
	for obj in _analysis_todo:
		obj = obj() # dereference weakref
		if obj:
			obj._analysis_start(sliceno)

def _synthesis_start():
	for obj in _synthesis_todo:
		obj = obj() # dereference weakref
		if obj:
			obj._synthesis_start()


class _BaseSplitterList(list):
	__slots__ = ('_sliceno', '__weakref__',)

	def __init__(self, iterable=(), *, _register=True):
		list.__init__(self, iterable)
		self._sliceno = None
		if _register:
			_analysis_todo.append(weakref.ref(self))

	def __repr__(self):
		return f'{self.__class__.__name__}({list.__repr__(self)})'

	def _analysis_start(self, sliceno):
		self[:] = self.for_slice(sliceno)
		self._sliceno = sliceno

	def __reduce__(self):
		return list, (), None, iter(self)


class ChunkSplitter(_BaseSplitterList):
	"""
	Splits a sequence of elements with one contiguous chunk for each slice.
	The split is fully deterministic, for a give slice count and number
	of elements the split is always the same, but in an attempt to balance
	as perfectly as possible over slices it's not easily predictable.

	You can set overlap_start and/or overlap_end to overlap that many items
	between slices. Both are applied independently.

	In analysis() (and from .for_slice()) the actual overlap is stored
	in .overlap_start and .overlap_end.

	This splitter can be used as a list.
	"""

	__slots__ = ('_overlap_start', '_overlap_end',)

	def __init__(self, iterable=(), *, overlap_start=0, overlap_end=0, _register=True):
		assert isinstance(overlap_start, int) and overlap_start >= 0
		assert isinstance(overlap_end, int) and overlap_end >= 0
		self._overlap_start = overlap_start
		self._overlap_end = overlap_end
		_BaseSplitterList.__init__(self, iterable, _register=_register)

	def _ranges_for_slice(self, sliceno):
		from accelerator.g import slices
		per_slice = len(self) // slices
		left_over = len(self) % slices
		# This is the sliceno where left_over elements start being added.
		# A reasonably large prime gives quite even distribution.
		extra_pos = (left_over * 27644437) % slices
		# First assume no left_over.
		start = per_slice * sliceno
		length = per_slice
		# Inside or after the extra range, add one for each previous slice in the range.
		if sliceno >= extra_pos:
			start += min(sliceno - extra_pos, left_over)
			if sliceno < extra_pos + left_over:
				# One longer because we are inside the range.
				length += 1
		# Same thing but for wrap around to the early slices.
		if extra_pos + left_over > slices:
			extra_at_start = (extra_pos + left_over) % slices
			start += min(sliceno, extra_at_start)
			if sliceno < extra_at_start:
				length += 1
		end = start + length
		full_end = min(len(self), end + self._overlap_end)
		full_start = max(0, start - self._overlap_start)
		return full_start, full_end, start - full_start, full_end - end

	@property
	def overlap_start(self):
		return self._overlap_start

	@property
	def overlap_end(self):
		return self._overlap_end

	def _analysis_start(self, sliceno):
		start, end, self._overlap_start, self._overlap_end = self._ranges_for_slice(sliceno)
		self._sliceno = sliceno
		self[:] = self[start:end]

	def for_slice(self, sliceno):
		assert self._sliceno is None, "for_slice() doesn't work in analysis"
		start, end, overlap_start, overlap_end = self._ranges_for_slice(sliceno)
		res = self.__class__(self[start:end], overlap_start=overlap_start, overlap_end=overlap_end, _register=False)
		res._sliceno = sliceno
		return res


class RoundRobinSplitter(_BaseSplitterList):
	"""
	Splits a sequence of elements across slices giving one element to each
	slice in turn. The split is fully deterministic, but will on average give
	more elements to earlier slices (at most one per sequence).

	This splitter can be used as a list.
	"""

	__slots__ = ()

	def for_slice(self, sliceno):
		assert self._sliceno is None, "for_slice() doesn't work in analysis"
		from accelerator.g import slices
		return self[sliceno::slices]


class FirstComeSplitter:
	"""
	Splits a sequence of elements across slices, giving out each element
	to whichever slice is ready to recieve it. Thus, the split is not at
	all deterministic, but should balance work as equally as possible.

	If you know the approximate cost of processing each item, putting
	more expensive items earlier in the sequence increases the evenness
	of the split (in time).

	As there is non-neglible overhead to fetch each item from this splitter
	cheap elements should be grouped in a single element, i.e. instead of
	FirstComeSplitter(range(1000000))
	something like
	FirstComeSplitter(range(n, n + 100) for n in range(0, 1000000, 100))
	is better for quickly processed elements.

	Set continue_in_synthesis if you want to continue the (possibly
	exhausted) iteration in synthesis.
	The default is to provide all the data in synthesis, like the other
	splitters.

	This splitter can only be used as an iterator. If you don't set
	continue_in_synthesis it is mostly tuple compatible in synthesis.
	"""

	__slots__ = ('_items', '_keys', '_repeating_in_synthesis', '__weakref__',)

	def __init__(self, items, continue_in_synthesis=False):
		from accelerator.mp import MpSet
		self._items = tuple(items)
		self._keys = MpSet(initial=range(len(self._items) - 1, -1, -1), _set_cls=list)
		self._repeating_in_synthesis = False
		if not continue_in_synthesis:
			_synthesis_todo.append(weakref.ref(self))

	def __repr__(self):
		return f'{self.__class__.__name__}({self._items !r})'

	def _synthesis_start(self):
		self._repeating_in_synthesis = True

	def __iter__(self):
		if self._repeating_in_synthesis:
			return iter(self._items)
		else:
			return self

	def __next__(self):
		try:
			return self._items[self._keys.pop()]
		except IndexError:
			pass
		raise StopIteration
	next = __next__

	def __getitem__(self, item):
		if not self._repeating_in_synthesis:
			raise AcceleratorError("Item access only works without continue_in_synthesis (and only ever in synthesis)")
		return self._items[item]

	def __len__(self):
		return len(self._items)

	def __reduce__(self):
		from pickle import PicklingError
		raise PicklingError(f"Can't pickle {self.__class__.__name__}")


class HashSplitter(_BaseSplitterList):
	"""
	Splits a sequence of elements across slices based on element hashes.
	You can provide your own hash function or you can provide the name of
	a dataset type, to hash as writing to a dataset would.

	Using a dataset type name is a more convenient way to do something like:
	    for items in lst:
	        if dw.hashcheck(items[0]):
	            ....

	If your items are tuples and you don't provide your own hash function,
	the first item is passed to the hash function.

	This splitter can be used as a list.
	"""

	__slots__ = ('append', 'extend', '_custom_hashfunc', '_hashfunc',)

	def __init__(self, iterable=(), hashfunc=None):
		if callable(hashfunc):
			self._custom_hashfunc = True
			self._hashfunc = hashfunc
		else:
			self._custom_hashfunc = False
			from accelerator.dsutil import _convfuncs
			if hashfunc not in _convfuncs:
				raise AcceleratorError(f'Unknown column type {hashfunc !r}')
			self._hashfunc = _convfuncs[hashfunc].hash
		self.append = self._append
		self.extend = self._extend
		_BaseSplitterList.__init__(self, iterable)
		self._check()

	def _append(self, item):
		_BaseSplitterList.append(self, item)
		self._check()

	def _extend(self, items):
		_BaseSplitterList.extend(self, items)
		self._check()

	def _check(self):
		if self:
			try:
				value = self[0]
				if not self._custom_hashfunc and isinstance(value, tuple):
					value = value[0]
				assert isinstance(self._hashfunc(value), int)
				# This should only run once, replace functions with non-checking versions.
				self.append = _BaseSplitterList.append.__get__(self)
				self.extend = _BaseSplitterList.extend.__get__(self)
			except Exception:
				# In case the program continues, nothing should happen in _analysis_start
				self.clear()
				raise

	def for_slice(self, sliceno):
		assert self._sliceno is None, "for_slice() doesn't work in analysis"
		from accelerator.g import slices
		hashfunc = self._hashfunc
		# Put this if outside the loop for performance.
		if not self._custom_hashfunc and self and isinstance(self[0], tuple):
			for item in self:
				if hashfunc(item[0]) % slices == sliceno:
					yield item
		else:
			for item in self:
				if hashfunc(item) % slices == sliceno:
					yield item
