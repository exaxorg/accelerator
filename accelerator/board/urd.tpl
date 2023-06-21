{{ ! template('head', title='urd for ' + project) }}

	<h1>urd for {{ project }}</h1>
	<ul class="urdlist">
	% for thing in lists:
		<li>
			<a href="/urd/{{ url_quote(thing) }}">{{ thing }}</a>
			<a href="/urd/{{ url_quote(thing) }}/first">first</a>
			<a href="/urd/{{ url_quote(thing) }}/latest">latest</a>
		</li>
	% end
	</ul>
</body>
