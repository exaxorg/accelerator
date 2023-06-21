{{ ! template('head', title=job) }}

	<h1>{{ job }}/method.tar.gz</h1>
	<ul>
	% for info in members:
		<li><a href="/job/{{ url_quote(job) }}/method.tar.gz/{{ url_quote(info.path) }}">{{ info.path }}</a></li>
	% end
	</ul>
</body>
