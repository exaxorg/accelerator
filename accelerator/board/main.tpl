{{ ! template('head', title='') }}

	<div id="bonus-info">
		<table id="workdirs">
			% for workdir in sorted(workdirs):
				<tr>
					<td><a target="_blank" href="/workdir/{{ url_quote(workdir) }}">{{ workdir }}</a></td>
					<td><a target="_blank" href="/job/{{ url_quote(workdir) }}-LATEST">latest</a></td>
				</tr>
			% end
			<tr>
				<td><a target="_blank" href="/workdir/">ALL</a></td>
			</tr>
		</table>
		<ul>
			<li><a target="_blank" href="/methods">methods</a></li>
			<li><a target="_blank" href="/urd">urd</a></li>
		</ul>
		<input type="submit" value="show all" id="show-all" disabled>
	</div>
	<h1 id="header">
		ax board: {{ project }}
		<span id="path">{{ path[8:] }}</span>
	</h1>
	<div id="status">
		<a target="_blank" href="/status">status</a>: <span></span>
	</div>
	<div id="dirs"><ul></ul></div>
	<div id="missing"></div>
	<div id="waiting"><div class="spinner"></div></div>
<script language="javascript">
(function () {
	const waitingEl = document.getElementById('waiting');
	const statusEl = document.querySelector('#status span');
	const show_all = document.getElementById('show-all');
	show_all.onclick = function() {
		show_all.disabled = true;
		for (const el of document.querySelectorAll('.result.hidden')) {
			el.classList.remove('hidden');
		}
	}
	const status = function () {
		if (document.body.className === 'error') {
			setTimeout(status, 1500);
			return;
		}
		fetch('/status?short', {headers: {Accept: 'text/plain'}})
		.then(res => {
			if (res.ok) return res.text();
			throw new Error('error response');
		})
		.then(res => {
			statusEl.innerText = res;
			setTimeout(status, 1500);
		})
		.catch(error => {
			console.log(error);
			statusEl.innerText = '???';
			setTimeout(status, 1500);
		});
	};
	const update = function (try_num) {
		fetch('{{ url_path }}', {headers: {Accept: 'application/json'}})
		.then(res => {
			if (res.ok) return res.json();
			throw new Error('error response');
		})
		.then(res => {
			const missing = document.getElementById('missing');
			if (res.missing) {
				missing.className = 'error';
				missing.innerText = res.missing
			} else {
				missing.className = '';
			}
			const existing = {};
			for (const el of document.querySelectorAll('.result')) {
				if (el.dataset.name) existing[el.dataset.name] = el;
			};

			const dirs_ul = document.querySelector('#dirs ul');
			const dirs_els = {};
			for (const el of dirs_ul.querySelectorAll('li')) {
				dirs_els[el.dataset.name] = el;
				el.remove();
			}
			const dirs = Object.entries(res.dirs).sort();
			for (const [name, href] of dirs) {
				let el = dirs_els[name];
				if (!el) {
					el = document.createElement('LI');
					el.dataset.name = name;
					const a = document.createElement('A');
					a.innerText = name;
					a.href = encodeURI(href);
					el.appendChild(a);
				}
				dirs_ul.appendChild(el);
			}

			const items = Object.entries(res.files);
			if (items.length) {
				waitingEl.style.display = 'none';
			} else {
				waitingEl.style.display = 'block';
			}
			// sort files on ts, but fall back to (link) name for files with the same time
			items.sort((a, b) => b[1].ts - a[1].ts || a[0].localeCompare(b[0]));
			let prev = waitingEl;
			for (const [name, data] of items) {
				const oldEl = existing[name];
				if (oldEl) {
					delete existing[name];
					if (oldEl.dataset.ts == data.ts) {
						update_date(oldEl);
						prev = oldEl;
						continue;
					}
					remove(oldEl);
				}
				const resultEl = resultItem(name, data, '{{ url_path }}');
				prev.after(resultEl);
				prev = resultEl;
			}
			for (const el of Object.values(existing)) {
				remove(el);
			}
			setTimeout(update, 1500);
		})
		.catch(error => {
			console.log(error);
			if (try_num === 4) {
				document.body.className = 'error';
				waitingEl.style.display = 'none';
				const header = document.getElementById('header');
				const goodHTML = header.innerHTML;
				header.innerText = 'ERROR - updates stopped at ' + fmtdate();
				const btn = document.createElement('INPUT');
				btn.type = 'button';
				btn.value = 'restart';
				btn.id = 'restart';
				btn.onclick = function () {
					document.body.className = '';
					waitingEl.style.display = 'block';
					header.innerHTML = goodHTML;
					update();
				};
				header.appendChild(btn);
			} else {
				waitingEl.style.display = 'block';
				setTimeout(() => update((try_num || 0) + 1), 1500);
			}
		});
	};
	const remove = function (el) {
		if (el.classList.contains('hidden')) {
			el.remove();
		} else {
			setTimeout(el.remove, 1400);
			el.classList.add('hidden');
			el.dataset.name = '';
		}
	};
	update();
	status();
})();
</script>
</body>
