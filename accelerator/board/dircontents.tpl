{{ ! template('head', title='%s/%s' % (job, dirname,)) }}

	<h1>{{ job }}/{{ dirname }}</h1>

	% if files:
		<h3>files</h3>
		<ul>
		% for fname in files:
			<li>
			   <a href="/job/{{ url_quote(job) }}/{{ url_quote(dirname) }}/{{ url_quote(fname) }}">
			   {{ fname }}
			   </a>
			</li>
		% end
		</ul>
	% end
	% if dirs:
		<h3>directories</h3>
		<div id="dirs">
			<ul>
			% for fname in dirs:
				<li>
				   <a href="/job/{{ url_quote(job) }}/{{ url_quote(dirname) }}/{{ url_quote(fname) }}">
				   {{ fname }}
				   </a>
				</li>
			% end
			</ul>
		</div>
	% end
</body>
