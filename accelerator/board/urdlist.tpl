{{ ! template('head', title=key) }}

	<h1>{{ key }}</h1>
	<ul class="urdlist">
	% for ts, caption in timestamps:
		<li><a href="/urd/{{ url_quote(key) }}/{{ url_quote(ts) }}">{{ ts }}</a> <span class="urd-caption">{{ caption[:70] + '....' if len(caption) > 80 else caption }}</span></li>
	% end
	</ul>
<script language="javascript">
// This does not preserve newlines (the HTML parser will have turned them into spaces),
// and that's intentional. Huge captions aren't great in a list like this, so they are
// both truncated (above) and don't care about newlines here.
(function() {
	for (const el of document.querySelectorAll('.urd-caption')) {
		parseANSI(el, el.innerText);
	}
})();
</script>
</body>
