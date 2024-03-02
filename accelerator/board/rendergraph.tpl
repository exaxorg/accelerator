% from math import atan2, sin, cos, pi
% from json import dumps
%
% arrowlen = 15
% arrowangle = pi / 8
% smallnodesize = 20
% defaultnodesize = 30
% topnodesize = 39
% textrowspace = 15
% fontsize = 12
% edgetextcolor = 'var(--graph-edgetext)'
% edgefontsize = 'var(--graph-edgefontsize)'
%
% it = iter(nodes.values())
% n = next(it)
% bbox = [n.x, n.y, n.x, n.y]
% for n in it:
%	for i, (fun, var) in enumerate(((min, n.x), (min, n.y), (max, n.x), (max, n.y))):
%		bbox[i] = fun(bbox[i], var)
%	end
% end
% bbox = [bbox[0] - 50, bbox[1] - 50, max(bbox[2] - bbox[0] + 100, 200), max(bbox[3] - bbox[1] + 120, 200)]

% def rendernode(item, type):
	<circle
		id="{{ item.safename }}"
		class="hovernode"
		cx="{{ item.x }}"
		cy="{{ item.y }}"
		r="{{ item.size }}"
		stroke="var(--graph-fg)"
		stroke-width="2"
		fill="var({{ item.color }})"
		fill-opacity="100%"
		data-origfill="var({{ item.color }})"
		data-neighbour_nodes="{{ dumps(list(item.neighbour_nodes)) }}"
		data-neighbour_edges="{{ dumps(list(item.neighbour_edges)) }}"
	/>
	% if item.top_job:
	<circle
		id="{{ item.safename + '_bonusring' }}"
		class="hovernode"
		cx="{{ item.x }}"
		cy="{{ item.y }}"
		fill="none"
		r="{{ item.size - 3.4 }}"
		stroke="var(--graph-fg)"
		stroke-width="2"
		fill-opacity="100%"
	/>
	% end
	% if type == 'job':
	<text x="{{ item.x }}" y="{{ item.y + 5 }}" font-weight="bold" font-size="{{ fontsize }}" text-anchor="middle" fill="var(--graph-fg)">
		{{ ''.join(('D' if item.datasets else '', 'F' if item.files else '', 'S' if item.subjobs else '')) }}
	</text>
	% end
	<circle
		id="{{ item.safename + '_transparent' }}"
		cx="{{ item.x }}"
		cy="{{ item.y }}"
		r="{{ item.size }}"
		fill-opacity="0%"
		onmouseover="highlight_nodes(document.getElementById('{{ item.safename }}'), true)"
		onmouseout="highlight_nodes(document.getElementById('{{ item.safename }}'), false)"
	% if type == 'job':
		onclick="graphpopup_job(
			event,
			{{ dumps(item.jobid) }},
			{{ dumps(item.files) }},
			{{ dumps(item.datasets) }},
			{{ dumps(item.subjobs) }},
			{{ dumps(item.method) }},
			{{ dumps(item.name) }},
			{{ dumps(item.atmaxdepth) }},
			{{ dumps(item.timestamp) }},
			{{ dumps(item.not_in_urdlist) }},
			{{ dumps(item.parent) }},
			{{ dumps(item.is_build) }}
		)"
	/>
	<text x="{{ item.x }}" y="{{ item.y + item.size + 1 * textrowspace }}" font-weight="bold" font-size="{{ fontsize }}" text-anchor="middle" fill="var(--graph-fg)">
		<a href="{{ '/job/' + url_quote(item.jobid) }}">{{ item.jobid }}</a>
	</text>
	<text x="{{ item.x }}" y="{{ item.y + item.size + 2 * textrowspace }}" font-weight="normal" font-size="{{ fontsize }}" text-anchor="middle" fill="var(--graph-fg)">
		<a href="{{ '/job/' + url_quote(item.jobid) +'/method.tar.gz/' }}">{{ item.name }}</a>
	</text>
	% else:
		onclick="graphpopup_ds(
			event,
			{{ dumps(item.jobid) }},
			{{ dumps(item.method) }},
			{{ dumps(item.ds) }},
			{{ dumps(item.columns) }},
			{{ dumps(item.atmaxdepth) }},
			{{ dumps(item.timestamp) }},
			{{ dumps(item.is_build) }}
		)"
	/>
	<text x="{{ item.x }}" y="{{ item.y + item.size + 1 * textrowspace }}" font-weight="bold" font-size="{{ fontsize }}" text-anchor="middle" fill="var(--graph-fg)">
		<a href="{{ '/dataset/' + url_quote(item.ds) }}">{{ item.ds }}</a>
	</text>
	<text x="{{ item.x }}" y="{{ item.y + item.size + 2 * textrowspace }}" font-weight="normal" font-size="{{ fontsize }}" text-anchor="middle" fill="var(--graph-fg)">
		<a href="{{ '/job/' + url_quote(item.jobid) +'/method.tar.gz/' }}">{{ item.name }}</a>
	</text>
	<text x="{{ item.x }}" y="{{ item.y + item.size + 3 * textrowspace }}" font-weight="normal" font-size="{{ fontsize }}" text-anchor="middle" fill="var(--graph-fg)">
		{{ item.lines }}
	</text>
	% end
% end

% def renderedge(src, dst, stroke='var(--graph-edge)', width='2'):
%	srcnode = nodes[src]
%	dstnode = nodes[dst]
%	srcx, srcy = srcnode.x, srcnode.y
%	dstx, dsty = dstnode.x, dstnode.y
%	a = atan2(dsty - srcy, dstx - srcx)
%	srcx = srcx + srcnode.size * cos(a)
%	srcy = srcy + srcnode.size * sin(a)
%	dstx = dstx - dstnode.size * cos(a)
%	dsty = dsty - dstnode.size * sin(a)
		<line x1="{{ srcx }}" x2="{{ dstx }}" y1="{{ srcy }}" y2="{{ dsty }}" stroke="{{ stroke }}" stroke-width="{{ width }}"/>
%	x1 = dstx - arrowlen * cos(a + arrowangle)
%	y1 = dsty - arrowlen * sin(a + arrowangle)
%	x2 = dstx - arrowlen * cos(a - arrowangle)
%	y2 = dsty - arrowlen * sin(a - arrowangle)
		<polygon points="{{ dstx }},{{ dsty }} {{ x1 }},{{ y1 }} {{ x2 }},{{ y2 }}" stroke="{{ stroke }}" fill="{{ stroke }}" stroke-width="{{ width }}"/>
%	return srcx, srcy, a
% end

<svg id="svg" version="1.1" xmlns="http://www.w3.org/2000/svg" viewBox="{{ ' '.join(map(str, bbox)) }}" width="100%" height="400px">
%	# first, set some visual related attributes
%	if type == 'job':
%		for name, item in nodes.items():
%			if item.top_job:
%				item.size = topnodesize
%			elif item.not_in_urdlist:
%				item.size = smallnodesize
%			else:
%				item.size = defaultnodesize
%			end
%			if item.atmaxdepth:
%				item.color = '--graphnode-atmaxdepth'
%			elif item.not_in_urdlist is True:
%				item.color = '--graphnode-nourdlist'
%			elif isinstance(item.not_in_urdlist, list):
%				item.color = '--graphnode-inanotherurdlist'
%			elif item.top_job:
%				item.color = '--graphnode-job-top'
%			else:
%				item.color = '--graphnode-job-default'
%			end
%			if item.name is None or item.name == item.method:
%				item.name = item.method
%			else:
%				item.name = '%s (%s)' % (item.method, item.name)
%			end
%		end
%	else:
%		assert type == 'dataset'
%		for name, item in nodes.items():
%			item.size = defaultnodesize
%			if item.atmaxdepth:
%				item.color = '--graphnode-atmaxdepth'
%			elif item.top_job:
%				item.color = '--graphnode-ds-top'
%				item.size = topnodesize
%			else:
%				item.color = '--graphnode-ds-default'
%			end
%		end
%	end
%

%	# subjob edges
%	for src, dst in subjob_edges:
%		key = src + dst
	<g id="{{ key + '_subjob' }}">
%		renderedge(src, dst, "var(--graph-subjobedge)", "8")
	</g>
%	end

%	# dependency edges
%	for src, dst, relation in edges:
%		key = src + dst
	<g id="{{ key }}">
%		srcx, srcy, a = renderedge(src, dst)
%		mx = srcx + 4 * cos(a) - 6 * sin(a)
%		my = srcy + 4 * sin(a) + 6 * cos(a)
		<text x="{{ mx }}" y="{{ my }}" transform="rotate({{ a * 180 / pi + 180 }}, {{ mx }}, {{ my }})" text-anchor="end" font-size="{{ edgefontsize }}" fill="{{ edgetextcolor }}">
		{{ relation }}
		</text>
	</g>
%	end

%	# nodes
%	for name, item in nodes.items():
%		rendernode(item, type)
%	end
</svg>
