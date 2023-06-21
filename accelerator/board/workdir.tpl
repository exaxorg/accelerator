{{ ! template('head', title=name, bodyclass='workdir') }}

<h1>{{ name }}</h1>
<div class="filter">
	<h1>Filter</h1>
	<table>
		<tr><td>Method</td><td><input type="text" id="f-method" autocapitalize="off" disabled></td></tr>
		<tr>
			<td>State</td>
			<td>
				% for state in ('current', 'old', 'unfinished'):
				<input id="f-{{ state }}" value="{{ state }}" type="checkbox" checked disabled>
				<label for="f-{{ state }}"> {{ state }}</label><br>
				% end
			</td>
		</tr>
	</table>
</div>
<table class="job-table">
	<tr>
		<th data-reverse="t">jobid</th>
		<th>method</th>
		<th>time</th>
	</tr>
	% for job, data in reversed(jobs.items()):
		<tr class="{{ data.klass }}">
			<td data-jid="{{ job }}"><a href="/job/{{ url_quote(job) }}">{{ job }}</a></td>
			<td>{{ data.method }}</td>
			% if data.totaltime is None:
				<td data-s="-1">DID NOT FINISH</td>
			% else:
				<td data-s="{{ data.totaltime }}">{{ data.humantime }}</td>
			% end
		</tr>
	% end
</table>
<script language="javascript">
(function () {
	const filter_change = function () {
		const want = f_method.value.toLowerCase().split(/\s+/).filter(v => !!v).map(v => {
			try {
				return new RegExp(v);
			} catch (e) {
				console.log('Failed to parse ' + JSON.stringify(v) + ' as regexp: ' + e.message);
			}
		});
		if (want.includes(undefined)) {
			f_method.className = 'error';
			return;
		}
		f_method.className = '';
		if (want.length == 0) want.push(/./); // nothing -> all
		const classes = Array.from(
			document.querySelectorAll('.filter input[type="checkbox"]:checked'),
			el => el.value,
		);
		const filtered = function(el) {
			if (!classes.some(cls => el.classList.contains(cls))) return true;
			// innerText is '' when collapsed (at least in FF), so use innerHTML.
			const method = el.querySelector('td ~ td').innerHTML.toLowerCase();
			return !want.some(re => re.test(method));
		}
		for (const el of document.querySelectorAll('.job-table tr[class]')) {
			if (filtered(el)) {
				el.classList.add('filtered');
			} else {
				el.classList.remove('filtered');
			}
		}
	};
	const f_method = document.getElementById('f-method');
	filter_change();
	for (const el of document.querySelectorAll('.filter input[type="checkbox"]')) {
		el.oninput = filter_change;
		el.disabled = false;
	}
	f_method.oninput = filter_change;
	f_method.disabled = false;

	const split_job = function (jid) {
		const a = jid.split('-');
		return [a.slice(0, -1).join('-'), a[a.length - 1]];
	}
	const cmp_jobid = function (a, b) {
		const [a_wd, a_n] = split_job(a.dataset.jid);
		const [b_wd, b_n] = split_job(b.dataset.jid);
		return a_wd.localeCompare(b_wd) || parseInt(b_n) - parseInt(a_n);
	};
	const cmp_method = function (a, b) {
		return a.innerHTML.localeCompare(b.innerHTML);
	}
	const cmp_time = function (a, b) {
		return parseFloat(b.dataset.s) - parseFloat(a.dataset.s);
	}
	const comparers = [null, cmp_jobid, cmp_method, cmp_time];
	const job_table = document.querySelector('.job-table');
	const sort = function () {
		const tds = Array.from(job_table.querySelectorAll('tr[class] td:nth-child(' + this.dataset.ix + ')'));
		tds.sort(comparers[this.dataset.ix]);
		if (this.dataset.reverse) {
			tds.reverse();
			this.dataset.reverse = '';
		} else {
			this.dataset.reverse = 't';
		}
		for (const el of tds) {
			job_table.appendChild(el.parentNode);
		}
	};
	job_table.querySelectorAll('th').forEach((el, ix) => {
		el.dataset.ix = ix + 1;
		el.onclick = sort;
		el.accessKey = el.innerText.slice(0, 1);
	});
})();
</script>
</body>
