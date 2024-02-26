<script language="javascript" src="{{ name2hashed['graph.js'] }}"></script>


<div id="graph" class="box">
	<script>
		(function () {
			const e = document.querySelector('#graph');
			fetch('/graph/{{ mode }}/{{ url_quote(key) }}')
			.then(res => {
				if (res.ok) return res.text();
				throw new Error('error response');
			})
			.then(res => {
				e.innerHTML = res;
				setTimeout(panzoom, 0);
			})
			.catch(error => {
				console.log(error);
				e.innerText = 'Failed to fetch graph';
			});
		})();
	</script>
</div>


<div id="graphpopup" class="box">
	<div id="gp-method"></div>
	<hr><br>
	<div id="gp-atmaxdepth" style="display: none"><font color="var(--popup-atmaxdepth)">
		<b>Affected by recursion limit,<br>not all edges drawn!<br>&nbsp</b>
	</font></div>
	% if mode in ('job', 'urd'):
		<div id="gp-notinurdlist" style="display: none" class="gp-notice">
			Job not in this urdlist or<br>any of its dependencies.<br>&nbsp;
		</div>
		Job: <a id="gp-jobid" href=""></a><br>
		<div id="gp-parent" style="display: none">
			<br>Parent: <a href="" id="gp-parenthref"></a><br>
		</div>
		<div id="gp-inthisurdlist" style="display: none" class="gp-notice">
			<br>Job in dependency urdlist:
			<table></table>
		</div>
		<div id="gp-files" style="display: none">
			<h1>Files:</h1>
			<table></table>
		</div>
		<div id="gp-datasets" style="display: none">
			<h1>Datasets:</h1>
			<table></table>
		</div>
		<div id="gp-subjobs" style="display: none">
			<h1>Subjobs:</h1>
			<table></table>
		</div>
	% else:
		Dataset: <a id="gp-dataset" href=""></a><br>
		Job: <a id="gp-jobid" href=""></a><br>
		<div id="gp-columns" style="display: none">
			<h1>Columns:</h1>
			<table></table>
		</div>
	% end
	<br>
	<hr>
	<a id="gp-source">Source</a> &nbsp; <a id="gp-help">Documentation</a>
	<div id="gp-timestamp"></div>
</div>
