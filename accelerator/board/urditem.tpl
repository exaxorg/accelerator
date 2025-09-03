{{ ! template('head', title=key) }}

	<h1>{{ ! urd_ts_html(key) }}</h1>
	<table class="urd-table">
		% saved_caption = entry.caption
		<tr><td>timestamp</td><td id="urd-timestamp">{{ ! urd_ts_html(entry.pop('timestamp')) }}</td></tr>
		% for thing in ('user', 'build', 'caption',):
			<tr><td>{{ thing }}</td><td id="urd-{{ thing }}">{{ entry.pop(thing) }}</td></tr>
		% end
		% if entry.build_job:
			<tr><td>build_job</td><td id="urd-build_job"><a href="/job/{{ url_quote(entry.build_job) }}">{{ entry.build_job }}</a></td></tr>
		% end
		% build_job = entry.pop('build_job')
		% for thing, value in sorted(entry.items()):
			% if thing not in ('joblist', 'deps',):
				<tr><td>{{ thing }}</td><td>{{ value }}</td></tr>
			% end
		% end
	</table>
	<h2>urd item graph</h2>
	% include('graph', mode='urd', key=key)
	<table class="urd-table">
		<tr><td>deps</td><td>
			% for dep, depentry in sorted(entry.deps.items()):
				<a href="/urd/{{ url_quote(dep) }}/{{ ! urd_ts_html(depentry.timestamp) }}">
					{{ dep }}/{{ ! urd_ts_html(depentry.timestamp) }}
				</a>
				<ol>
					% for method, job in depentry.joblist:
						<li>{{ method }} <a href="/job/{{ url_quote(job) }}">{{ job }}</a></li>
					% end
				</ol>
			% end
		</td></tr>
		<tr><td>joblist</td><td>
			<ol>
				% for method, job in entry.joblist:
					<li>{{ method }} <a href="/job/{{ url_quote(job) }}">{{ job }}</a></li>
				% end
			</ol>
		</td></tr>
	</table>
	<script language="javascript">
	(function() {
		const el = document.getElementById('urd-caption');
		parseANSI(el, {{! js_quote(saved_caption) }});
	})();
	</script>
	% if build_job and results:
		<h2>Results (from build job <a href="/job/{{ url_quote(build_job) }}">{{ build_job }}</a>)</h2>
		<input type="submit" value="show all" id="show-all" disabled>
		<div class="box" id="results">
		<script>
			(function () {
				const show_all = document.getElementById('show-all');
				const resultfiles = document.getElementById('results');
				show_all.onclick = function() {
					show_all.disabled = true;
					for (const el of document.querySelectorAll('.result.hidden')) {
						el.classList.remove('hidden');
					}
				}
				const results = {{! results }};
				dirsandfiles(results, resultfiles);
			})();
		</script>
		</div>
	% end
</body>
