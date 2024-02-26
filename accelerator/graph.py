# -*- coding: utf-8 -*-
############################################################################
#                                                                          #
# Copyright (c) 2024 Anders Berkeman                                       #
# Modifications copyright (c) 2024 Carl Drougge                            #
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

from math import sin, pi, tan, atan
from collections import defaultdict
from datetime import datetime
from accelerator import JobWithFile, Job
from accelerator.dataset import Dataset
from accelerator.compat import unicode, FileNotFoundError

MAXANGLE = 45 * pi / 180


def expand_to_set(what, fun=lambda x: x):
	if what:
		if not isinstance(what, (list, tuple)):
			what = [what, ]
		return set(fun(item) for item in what)
	return set()


def jobdeps(job):
	"""return all the job's dependencies"""
	res = defaultdict(set)
	for key, value in job.params.jobs.items():
		if value:
			value = expand_to_set(value)
			value = tuple(x for x in value if type(x) == Job)
			res['jobs.' + key].update(expand_to_set(value))
	for key, value in job.params.datasets.items():
		if value:
			value = expand_to_set(value)
			value = tuple(x for x in value if type(x) == Dataset)
			res['datasets.' + key].update(expand_to_set(value, lambda x: x.job))
	# options JobWithFile
	def recurse(options, name=''):
		if isinstance(options, JobWithFile):  # must happen before tuple
			res['jwf' + name].add(options.job)
		elif isinstance(options, dict):
			for key, val in options.items():
				recurse(val, '.'.join((name, key)))
		elif isinstance(options, (list, tuple, set)):
			for item in options:
				recurse(item, name)
	recurse(job.params.options)
	return res


def dsdeps(ds):
	"""return all the dataset's parents and previous"""
	res = defaultdict(set)
	if ds:
		if ds.parent:
			res['parent'].update(expand_to_set(ds.parent))
		if ds.previous:
			res['previous'].add(ds.previous)
	return res


class WrapperNode(unicode):
	def __init__(self, payload):
		self.payload = payload
		self.level = 0
		self.not_in_urdlist = False
		self.deps = set()
		self.subjobs = set()
		self.contextual_deps = set()
		self.depdict = {}
		self.done = False
		self.in_degrees = 0
		self.top_job = False
		self.neighbour_nodes = set()
		self.neighbour_edges = set()
		self.atmaxdepth = False  # Implemented in frontend but not in current backend.
		self.is_build = False

def try_subjobs(n):
	try:
		return n.payload.post.subjobs
	except FileNotFoundError:
		return {}

class Graph:
	def __init__(self, depsfun):
		self.count = 0
		self.nodes = {}
		self.edges = set()
		self.subjob_edges = set()
		self.depsfun = depsfun

	def get_or_create_node(self, item):
		"""get existing from item or create a new WrapperNode"""
		if item in self.nodes:
			return self.nodes[item]
		else:
			n = WrapperNode(item)
			n.safename = 'node' + str(self.count)
			n.depdict = self.depsfun(item)
			self.nodes[item] = n
			self.count += 1
			return n

	def create_edge(self, current, dep, relation=None):
		"""create a new edge (from WrapperNode to WrapperNode)"""
		if relation is not None:
			self.edges.add((current, dep, relation))
		else:
			self.subjob_edges.add((current, dep))

	def level2nodes(self):
		"""return {level: set_of_WrapperNodes_at_level}"""
		ret = defaultdict(set)
		for n in self.nodes.values():
			ret[n.level].add(n)
		return ret

	def populate_with_neighbours(self):
		"""add neighbour information to all WrapperNodes"""
		for src, dst, rel in self.edges:
			edgekey = src.safename + dst.safename
			src.neighbour_nodes.add(dst)
			dst.neighbour_nodes.add(src)
			src.neighbour_edges.add(edgekey)
			dst.neighbour_edges.add(edgekey)

	def convert_to_safe_strings(self):
		"""collapse neighbour_nodes and edges to strings, for later JSON output"""
		for n in self.nodes.values():
			n.neighbour_nodes = tuple(x.safename for x in sorted(n.neighbour_nodes))
		edges = defaultdict(set)
		for src, dst, rel in self.edges:
			edges[(src.safename, dst.safename)].add(rel)  # there can be more than one relation between the same two nodes
		self.edges = tuple((src, dst, ', '.join(sorted(v))) for (src, dst), v in edges.items())
		self.subjob_edges = tuple((src.safename, dst.safename) for (src, dst) in self.subjob_edges)
		self.nodes = {n.safename: n for n in self.nodes.values()}

	def depth_first(self, stack):
		"""
		Jobs and Datasets (but not JobLists) do a depth-first
		search to discover all nodes and number of inputs to each node
		"""
		while stack:
			current = stack.pop()
			if current.done:
				continue
			for relation, deps in current.depdict.items():
				for dep in deps:
					dep = self.get_or_create_node(dep)
					if dep != current:
						self.create_edge(current, dep, relation)
						current.deps.add(dep)
			for dep in sorted(current.deps):
				stack.append(dep)
				dep.in_degrees += 1
			if isinstance(current.payload, Job):
				current.subjobs = set(self.get_or_create_node(Job(jid)) for jid in try_subjobs(current).keys())
				for sjob in sorted(current.subjobs):
					self.create_edge(current, sjob)
					sjob.contextual_deps.add(current)
					stack.append(sjob)
					current.in_degrees += 1
			current.done = True

	def breadth_first(self, stack):
		"""breadth-first search to get the correct rendering level of each node."""
		while stack:
			current = stack.pop()
			if current.done:
				continue
			current.in_degrees -= 1
			if current.in_degrees <= 0:
				for dep in sorted(current.deps):
					dep.level = max(dep.level, current.level + 1)
					stack.append(dep)
				current.done = True


def create_graph(inputitem, urdinfo=()):
	if isinstance(inputitem, tuple):
		# Was JobList. Create and populate WrapperNodes for all jobs in input
		graph = Graph(jobdeps)
		inputitem = [graph.get_or_create_node(x) for x in inputitem]
		for n in inputitem:
			for relation, deps in n.depdict.items():
				for c in deps:
					c = graph.get_or_create_node(c)
					graph.create_edge(n, c, relation)
					c.in_degrees += 1
					n.deps.add(c)
		stack = [n for n in inputitem if n.in_degrees == 0]
	else:
		# Is Job or Dataset. Do depth-first to find in_degrees for all WrapperNodes.
		# in_degrees is then used later to compute each node's rendering level.
		graph = Graph(jobdeps if isinstance(inputitem, Job) else dsdeps)
		inputitem = graph.get_or_create_node(inputitem)
		inputitem.top_job = True
		stack = [inputitem, ]
		graph.depth_first(stack)
		# Reset the done-flag so it can be re-used in a second recursion.
		# Also add contextual_deps to deps.
		for n in graph.nodes.values():
			n.done = False
			for c in n.contextual_deps:
				if c not in n.deps:
					n.deps.add(c)
				else:
					c.in_degrees -= 1  # because it is already counted in the "original" deps
		reached_by_deps = set.union(*(n.deps for n in graph.nodes.values()))
		starters = set(graph.nodes.values()) - reached_by_deps
		stack = sorted(starters)
	graph.breadth_first(stack)

	# add parameters from payload (job/ds) to all WrapperNodes
	inputitem_set = set(inputitem)
	for n in graph.nodes.values():
		njob = n.payload if isinstance(n.payload, Job) else n.payload.job
		n.jobid = njob
		n.method = njob.method
		n.timestamp = datetime.fromtimestamp(njob.params.starttime).strftime("%Y-%m-%d %H:%M:%S")
		n.name = n.method
		if isinstance(n.payload, Job):
			n.files = sorted(n.payload.files())
			n.datasets = sorted(n.payload.datasets)
			n.subjobs = tuple((jid, Job(jid).method) for jid in try_subjobs(n))
			n.parent = njob.params.get('parent')
			n.is_build = n.payload.params.get('is_build', False)
			if urdinfo:
				jobid2urddep, jobid2name = urdinfo
				if n not in inputitem_set:
					n.not_in_urdlist = jobid2urddep.get(n.jobid, True)
				n.name = jobid2name.get(n.jobid, None)
		else:
			# dataset
			n.columns = sorted((colname, dscol.type) for colname, dscol in n.payload.columns.items())
			n.lines = "%d x % s" % (len(n.payload.columns), '{:,}'.format(sum(n.payload.lines)).replace(',', '_'))
			n.ds = n.payload
	graph.populate_with_neighbours()
	return graph


def placement(graph):
	class Ordering:
		"""The init function takes the first level of nodes as input.
		The update function takes each consecutive level of nodes as
		input.  It returns a list of the nodes in order.
		"""
		def __init__(self, nodes):
			self.order = {x: str(ix) for ix, x in enumerate(sorted(nodes))}
		def update(self, nodes):
			nodes = sorted(nodes, key=lambda x: self.order[x])
			for n in nodes:
				for ix, c in enumerate(sorted(n.deps)):
					if c not in self.order:
						self.order[c] = self.order[n] + str(ix)
				self.order.pop(n)
			for ix, (key, val) in enumerate(sorted(self.order.items(), key=lambda x: x[1])):
				self.order[key] = str(ix)
			return nodes
	# determine x and y coordinates
	level2nodes = graph.level2nodes()
	order = Ordering(level2nodes[0])
	for level, nodesatlevel in sorted(level2nodes.items()):
		nodesatlevel = order.update(nodesatlevel)
		for ix, n in enumerate(nodesatlevel):
			n.x = -160 * level + 32 * sin(ix)
			n.y =  140 * ix    + 50 * sin(level)
	# limit angles by adjusting x positions
	offset = {}
	for level, nodesatlevel in sorted(level2nodes.items()):
		maxangle = MAXANGLE
		xoffset = 0
		for n in nodesatlevel:
			for m in n.deps:
				dx = abs(n.x - m.x)
				dy = abs(n.y - m.y)
				angle = abs(atan(dy / dx))
				if angle > maxangle:
					maxangle = angle
					xoffset = (dy / tan(MAXANGLE)) - dx
		offset[level + 1] = xoffset
	totoffset = 0
	for level, xoffset in sorted(offset.items()):
		totoffset += xoffset
		for n in level2nodes[level]:
			n.x -= totoffset
	graph.convert_to_safe_strings()
	return dict(
		nodes=graph.nodes,
		edges=graph.edges,
		subjob_edges=graph.subjob_edges,
	)


def graph(inp, gtype):
	if gtype == 'urd':
		# jobid2urddep is {jobid: "urdlist/timestamp"} for all jobids in depending urdlist
		jobid2urddep = defaultdict(list)
		for key, urditem in inp.deps.items():
			for _, jid in urditem.joblist:
				jobid2urddep[jid].append("%s/%s" % (key, urditem.timestamp))
		jobid2urddep = {key: sorted(val) for key, val in jobid2urddep.items()}
		jlist = inp.joblist
		inp = tuple(Job(jid) for _, jid in jlist)
		jobid2name = {jid: name for name, jid in jlist}
		graph = create_graph(inp, urdinfo=(jobid2urddep, jobid2name))
	else:
		graph = create_graph(inp)
	ret = placement(graph)
	ret['type'] = 'job' if gtype in ('urd', 'job') else 'dataset'
	return ret
