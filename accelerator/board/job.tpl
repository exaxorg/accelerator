{{ ! template('head', title=job) }}

% from datetime import datetime
% def paramspart(name):
	% thing = params.get(name)
	% if thing:
		<h3>{{ name }}</h3>
		<div class="box">
			{
			<table>
			% for k, v in sorted(thing.items()):
				<tr><td>{{ k }}</td><td>=</td><td>
					{{ ! ax_link(v) }}
				</td></tr>
			% end
			</table>
			}
		</div>
	% end
% end

	<div class="prevnext">
		<h1>{{ job }}</h1>
		% if job.number > 0:
			<a accesskey="p" href="/job/{{ url_quote('%s-%d' % (job.workdir, job.number - 1,)) }}">⇦ prev</a>
		% end
		<a accesskey="n" href="/job/{{ url_quote('%s-%d' % (job.workdir, job.number + 1,)) }}">next ⇨</a>
		<a accesskey="l" href="/job/{{ url_quote('%s-LATEST' % (job.workdir,)) }}">LATEST ⇉</a>
	</div>
	% if aborted:
		<div class="warning">WARNING: Job didn't finish, information may be incomplete.</div>
	% elif not current and not job.is_build:
		<div class="warning">Job is not current.</div>
	% end
	<h2>job graph</h2>
	% include('graph', mode='job', key=job)
	<h2>setup</h2>
	<div class="box">
		% if job.is_build:
			{{ params.method }}
		% else:
			<a href="/method/{{ url_quote(params.method) }}">{{ params.package }}.{{ params.method }}</a>
		% end
		<br>
		<a href="/job/{{ url_quote(job) }}/method.tar.gz/">Source</a>
		<div class="box" id="other-params">
			% blacklist = {
			%     'package', 'method', 'options', 'datasets', 'jobs', 'params',
			%     'starttime', 'endtime', 'exectime', '_typing', 'versions',
			% }
			<table>
				<tr><td>starttime</td><td>=</td><td>{{ datetime.fromtimestamp(params['starttime']) }}</td></tr>
				% if not aborted:
					<tr><td>endtime</td><td>=</td><td>{{ datetime.fromtimestamp(params['endtime']) }}</td></tr>
					% exectime = params['exectime']
					% for k in sorted(exectime):
						<tr><td>exectime.{{ k }}</td><td>=</td><td>{{ ! ax_repr(exectime[k]) }}</td></tr>
					% end
				% end
				% for k in sorted(set(params) - blacklist):
					<tr><td>{{ k }}</td><td>=</td><td>{{ ! ax_repr(params[k]) }}</td></tr>
				% end
				% versions = params['versions']
				% for k in sorted(versions):
					<tr><td>versions.{{ k }}</td><td>=</td><td>{{ ! ax_repr(versions[k]) }}</td></tr>
				% end
			</table>
		</div>
		% if params.options:
			<h3>options</h3>
			<div class="box">
				{
				<table>
				% for k, v in sorted(params.options.items()):
					<tr><td>{{ k }}</td><td>=</td><td>{{ ! ax_repr(v) }}</td></tr>
				% end
				</table>
				}
			</div>
		%end
		% paramspart('datasets')
		% paramspart('jobs')
	</div>
	% if datasets:
		<h2>datasets</h2>
		<div class="box">
			<ul>
				% for ds in datasets:
					<li><a href="/dataset/{{ url_quote(ds) }}">{{ ds }}</a> {{ '%d columns, %d lines' % ds.shape }}</li>
				% end
			</ul>
		</div>
	% end
	% if subjobs:
		<h2>subjobs</h2>
		<div class="box">
			<ul>
				% for j, is_current in subjobs:
					<li><a href="/job/{{ url_quote(j) }}">{{ j }}</a> {{ j.method }}
					% if not is_current:
						<span class="warning">not current</span>
					% end
					</li>
				% end
			</ul>
		</div>
	% end
	% if files:
		<h2>files</h2>
		<div class="box">
			<ul>
				% for fn in sorted(files):
					<li><a target="_blank" href="/job/{{ url_quote(job) }}/{{ url_quote(fn) }}">{{ fn }}</a></li>
				% end
			</ul>
		</div>
	% end
	% if output:
		<h2>output</h2>
		<div class="box" id="output">
			<div class="spinner"></div>
		</div>
		<script language="javascript">
			(function () {
				const output = document.getElementById('output');
				const spinner = output.querySelector('.spinner');
				const create = function (name, displayname) {
					const el = document.createElement('DIV');
					el.id = displayname;
					el.className = 'spinner';
					output.appendChild(el);
					const h3 = document.createElement('H3');
					h3.innerText = displayname;
					const pre = document.createElement('PRE');
					fetch('/job/{{! url_quote(job) }}/OUTPUT/' + name, {headers: {Accept: 'text/plain'}})
					.then(res => {
						if (res.status == 404) {
							el.remove();
						} else if (!res.ok) {
							throw new Error(displayname + ' got ' + res.status)
						} else {
							return res.text();
						}
					})
					.then(text => {
						el.appendChild(h3);
						el.appendChild(pre);
						parseANSI(pre, text);
						el.className = '';
					})
					.catch(error => {
						console.log(error);
						el.appendChild(h3);
						pre.innerText = 'FETCH ERROR';
						el.appendChild(pre);
						el.className = 'error';
					});
				};
				create('prepare', 'prepare');
				for (let sliceno = 0; sliceno < {{ job.params.slices }}; sliceno++) {
					create(sliceno, 'analysis-' + sliceno);
				}
				create('synthesis', 'synthesis');
				spinner.remove();
			})();
		</script>
	% end
</body>
