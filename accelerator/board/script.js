const resultItem = (function () {
	const imageExts = new Set(['jpg', 'jpeg', 'gif', 'png', 'apng', 'svg', 'bmp', 'webp']);
	const videoExts = new Set(['mp4', 'mov', 'mpg', 'mpeg', 'mkv', 'avi', 'webm']);
	const resultItem = function (name, data, url_path) {
		const resultEl = document.createElement('DIV');
		metahdr = document.createElement('SPAN');
		const txt = text => metahdr.appendChild(document.createTextNode(text));
		const a = function (text, ...parts) {
			const a = document.createElement('A');
			a.innerText = text;
			let href = '/job'
			for (const part of parts) {
				href = href + '/' + encodeURIComponent(part);
			}
			a.href = href;
			a.target = '_blank';
			metahdr.appendChild(a);
		}
		resultEl.className = 'result';
		resultEl.dataset.name = name;
		resultEl.dataset.ts = data.ts;
		const size = document.createElement('INPUT');
		size.type = 'submit';
		size.value = 'big';
		size.disabled = true;
		resultEl.appendChild(size);
		const hide = document.createElement('INPUT');
		hide.type = 'submit';
		hide.value = 'hide';
		hide.onclick = function () {
			document.getElementById('show-all').disabled = false;
			resultEl.classList.add('hidden');
		}
		resultEl.appendChild(hide);
		if (data.is_build) {
			metahdr.className = 'metahdr';
			const el = document.createElement('SPAN');
			el.className = 'resultheader';
			resultEl.appendChild(el);
			if (data.header) {
				el.innerText = data.header;
			} else {
				el.innerText = data.method;
			}
		}
		if (data.jobid) {
			a(name, data.jobid, data.name);
			txt(' from ');
			a(data.jobid, data.jobid);
			txt(' (');
			const methodEl = document.createElement('SPAN');
			methodEl.className = 'method';
			metahdr.appendChild(methodEl);
			txt(')');
			fetch('/job/' + encodeURIComponent(data.jobid), {headers: {Accept: 'application/json'}})
			.then(res => {
				if (res.ok) return res.json();
				throw new Error('error response');
			})
			.then(res => {
				const a = document.createElement('A');
				a.innerText = res.params.method;
				a.href = '/method/' + encodeURIComponent(res.params.method);
				a.target = '_blank';
				methodEl.appendChild(a);
			});
		} else {
			txt(name + ' ');
			const el = document.createElement('SPAN');
			el.className = 'unknown';
			el.appendChild(document.createTextNode('from UNKNOWN'));
			metahdr.appendChild(el);
		}
		txt(' ');
		const dateEl = document.createElement('SPAN');
		dateEl.className = 'date';
		metahdr.appendChild(dateEl);
		resultEl.appendChild(metahdr);
		update_date(resultEl);
		let child;
		if (data.isdir) {
			child = document.createElement('DIV');
			const ul = document.createElement('UL');
			const li = document.createElement('LI');
			const a = document.createElement('A');
			a.innerText = data.name;
			a.href = encodeURI('/job/' + data.jobid + '/' + data.name);
			child.id = 'dirs';
			child.appendChild(ul);
			li.appendChild(a);
			ul.appendChild(li);
		} else {
			child = sizewrap(data.linkname ? data.name : name, data, size, url_path);
		}
		resultEl.appendChild(child);
		if (data.description) {
			const tail = document.createElement('DIV');
			tail.className = 'description';
			tail.innerText = data.description;
			resultEl.appendChild(tail);
		}
		return resultEl;
	};
	const sizewrap = function (name, data, size, url_path) {
		if (data.size < 5000000) return load(name, data, size, url_path);
		const clickEl = document.createElement('DIV');
		clickEl.className = 'clickme';
		clickEl.innerText = 'Click to load ' + data.size + ' bytes';
		clickEl.onclick = function () {
			clickEl.parentNode.replaceChild(load(name, data, size, url_path), clickEl);
		};
		return clickEl;
	};
	const name2ext = function (name) {
		const parts = name.split('.');
		let ext = parts.pop().toLowerCase();
		if (ext === 'gz' && parts.length > 1) {
			ext = parts.pop().toLowerCase();
		}
		return ext;
	}
	const load = function (name, data, size, url_path) {
		const fileUrl = url_path + '/' + encodeURIComponent(name) + '?ts=' + data.ts;
		const ext = name2ext(name);
		const container = document.createElement('DIV');
		const spinner = document.createElement('DIV');
		spinner.className = 'spinner';
		container.appendChild(spinner);
		const onerror = function () {
			spinner.remove();
			container.className = 'error';
			container.innerText = 'ERROR';
		};
		let fileEl;
		let stdhandling = false;
		size.disabled = false;
		size.onclick = function () {
			if (container.className) {
				size.value = 'big';
				container.className = '';
			} else {
				size.value = 'small';
				container.className = 'big';
				container.scrollIntoView({behavior: 'smooth', block: 'end'});
			}
		};
		if (imageExts.has(ext)) {
			fileEl = document.createElement('IMG');
			fileEl.onclick = function () {
				if (fileEl.naturalHeight > fileEl.height) {
					if (container.className) {
						container.className = 'full';
						size.value = 'small';
						fileEl.scrollIntoView({behavior: 'smooth', block: 'nearest'});
					} else {
						container.className = 'big';
						container.scrollIntoView({behavior: 'smooth', block: 'nearest'});
						if (fileEl.naturalHeight > fileEl.height) {
							size.value = 'bigger';
						} else {
							size.value = 'small';
						}
					}
				} else {
					size.value = 'big';
					container.className = '';
					fileEl.className = '';
				}
			};
			size.onclick = fileEl.onclick;
			stdhandling = true;
		} else if (videoExts.has(ext)) {
			fileEl = document.createElement('VIDEO');
			fileEl.src = fileUrl;
			fileEl.controls = true;
			spinner.remove(); // shows a video UI immediately anyway
		} else if (ext === 'pdf') {
			fileEl = document.createElement('EMBED');
			fileEl.type = 'application/pdf';
			stdhandling = true;
		} else {
			fileEl = document.createElement('DIV');
			fileEl.className = 'textfile';
			const pre = document.createElement('PRE');
			fileEl.appendChild(pre);
			fetch(fileUrl, {headers: {Accept: 'text/plain'}})
			.then(res => {
				if (res.ok) return res.text();
				throw new Error('error response');
			})
			.then(res => {
				if (ext === 'html') {
					fileEl.innerHTML = res;
				} else {
					parseANSI(pre, res);
				}
				spinner.remove();
			})
			.catch(error => {
				console.log(error);
				onerror();
			});
		}
		if (stdhandling) {
			fileEl.onload = () => spinner.remove();
			fileEl.onerror = onerror;
			fileEl.src = fileUrl;
		}
		container.appendChild(fileEl);
		return container;
	};
	return resultItem;
})();
const dirsandfiles = function (results, resultfiles) {
	if (results.some(data => data.isdir)) {
		const h3 = document.createElement('H3');
		h3.innerText = 'directories';
		resultfiles.appendChild(h3);
		const div = document.createElement('DIV');
		div.id = "dirs";
		const ul = document.createElement('UL');
		for (const data of results) {
			if (data.isdir) {
				let li = document.createElement('LI');
				let a = document.createElement('A');
				a.innerText = data.name;
				a.href = encodeURI('/job/' + data.jobid + '/' + data.name);
				li.appendChild(a);
				li.appendChild(document.createTextNode(" (" + data.method + ") "));
				a = document.createElement('A');
				a.innerText = data.jobid;
				a.href = encodeURI('/job/' + data.jobid);
				li.appendChild(a);
				ul.appendChild(li);
			}
		}
		div.appendChild(ul);
		resultfiles.appendChild(div);
	}
	for (const data of results) {
		if (!data.isdir) {
			resultfiles.appendChild(resultItem(data.linkname, data, '/job/' + data.jobid));
		}
	}
}
const units = [['minute', 60], ['hour', 24], ['day', 365.25], ['year', 0]];
const fmtdate_ago = function (date) {
	const now = new Date();
	let ago = (now - date) / 60000;
	for (const [unit, size] of units) {
		if (size === 0 || ago < size) {
			ago = ago.toFixed(0);
			let s = (ago == 1) ? '' : 's';
			return fmtdate(date) + ', ' + ago + ' ' + unit + s + ' ago';
		}
		ago = ago / size;
	}
};
const update_date = function(el) {
	const date = new Date(el.dataset.ts * 1000);
	el.querySelector('.date').innerText = fmtdate_ago(date);
};
const fmtdate = function(date) {
	if (!date) date = new Date();
	return date.toISOString().substring(0, 19).replace('T', ' ') + 'Z';
};


const parseANSI = (function () {
	// Two digit hex str
	const hex2 = (n) => ('0' + n.toString(16)).slice(-2);

	// Make a CSS colour from an index (or return string untouched).
	function idx2colour(idx) {
		if (typeof idx === 'string') return idx;
		if (idx < 16) {
			return 'var(--ansi-' + idx + ')';
		} else if (idx < 232) {
			const c2xx = ['00', '5f', '87', 'af', 'd7', 'ff'];
			const rgb = (idx - 16);
			const r = Math.trunc(rgb / 36);
			const g = Math.trunc((rgb % 36) / 6);
			const b = rgb % 6;
			return '#' + c2xx[r] + c2xx[g] + c2xx[b];
		} else {
			const hex = hex2((idx - 232) * 10 + 8);
			return '#' + hex + hex + hex;
		}
	}

	// SGR arguments get collected into something like [[1], [2, 3]] (for "1;2:3").
	// This iterates the values and can count and skip to the next ";".
	function groups_iter(arr) {
		let idx = 0;
		let inner_idx = 0;
		return {
			inner_left: () => {
				if (inner_idx) return arr[idx].length - inner_idx;
				return 0;
			},
			next: () => {
				if (idx >= arr.length) return null;
				if (inner_idx < arr[idx].length) {
					inner_idx += 1;
					return arr[idx][inner_idx - 1];
				} else {
					idx += 1;
					inner_idx = 0;
					if (idx >= arr.length) return null;
					return arr[idx][0];
				}
			},
			finish_group: () => {
				idx += 1;
				inner_idx = 0;
			}
		};
	}

	// Parse the 38 and 48 arguments
	function parse_extended_colour(groups) {
		const colour_type = groups.next();
		if (colour_type === 2) {
			if (groups.inner_left() > 3) groups.next();
			const r = groups.next() & 255;
			const g = groups.next() & 255;
			const b = groups.next();
			if (b === null) return null;
			return '#' + hex2(r) + hex2(g) + hex2(b & 255);
		} else if (colour_type === 5) {
			return groups.next() & 255;
		}
		return null;
	}

	function split_params(s) {
		let group = [];
		const groups = [group];
		let num = 0;
		let ix = 0;
		collect: for (const c of s) {
			if (c >= '0' && c <= '9') {
				num = num * 10 + parseInt(c, 10);
			} else {
				group.push(num);
				num = 0;
				switch (c) {
					case ':':
						break;
					case ';':
						group = [];
						groups.push(group);
						break;
					default:
						break collect;
				}
			}
			ix += c.length; // of course a character can have length > 1
		}
		return [groups, ix];
	}

	class Sixel {
		constructor(sixeldata, trailing_text) {
			this.sixeldata = sixeldata;
			this.trailing_text = trailing_text;
		}
		draw2(dest) {
			let xpos = 0;
			let maxx = 0;
			let ypos = 0;
			let repeat_cnt = 1;
			let repeat_mode = false;
			const palette = {};
			let colour_mode = false;
			let colour_collect;
			let r = 0, g = 0, b = 0;
			for (const c of this.sixeldata) {
				const cc = c.charCodeAt(0);
				if (cc < 48 || cc > 57) {
					repeat_mode = false;
					if (colour_mode && c !== ';') {
						colour_mode = false;
						const ix = colour_collect[0];
						if (colour_collect.length >= 5 && colour_collect[1] === 2) {
							palette[ix] = [
								(colour_collect[2] * 2.55) & 255,
								(colour_collect[3] * 2.55) & 255,
								(colour_collect[4] * 2.55) & 255,
							];
						}
						if (palette[ix]) [r, g, b] = palette[ix];
					}
				}
				if (repeat_mode) {
					repeat_cnt = (repeat_cnt * 10) + cc - 48;
				} else if (colour_mode) {
					if (c === ';') {
						colour_collect.push(0);
					} else {
						colour_collect[colour_collect.length - 1] = (colour_collect[colour_collect.length - 1] * 10) + cc - 48;
					}
				} else if (cc >= 63 && cc <= 126) {
					if (dest && xpos < dest.width) {
						const bits = cc - 63;
						let offset = dest.width * ypos * 4;
						for (let y = 0; y < 6; y++) {
							if (bits & (1 << y)) {
								const start = xpos * 4 + offset;
								const xend = Math.min(xpos + repeat_cnt, dest.width);
								const end = xend * 4 + offset;
								for (let off = start; off < end; ) {
									dest.data[off++] = r;
									dest.data[off++] = g;
									dest.data[off++] = b;
									dest.data[off++] = 255;
								}
							}
							offset += dest.width * 4;
						}
					}
					xpos += repeat_cnt;
					repeat_cnt = 1;
				} else if (c === '!') {
					repeat_mode = true;
					repeat_cnt = 0;
				} else if (c === '#') {
					colour_collect = [0];
					colour_mode = true;
				} else if (c === '$' || c === '-') {
					maxx = Math.max(maxx, xpos);
					xpos = 0;
					if (c === '-') {
						ypos += 6;
						if (dest && dest.height <= ypos) break;
					}
				}
			}
			if (xpos) ypos += 6;
			return [maxx, ypos];
		}
		draw() {
			const canvas = document.createElement('CANVAS');
			let width = 0;
			let height = 0;
			if (this.sixeldata[0] === '"') { // Raster Attributes
				const groups = split_params(this.sixeldata.slice(1))[0];
				if (groups.length >= 4) {
					// We will truncate anything outside the defined size,
					// because that works with how these are used these days.
					width = groups[2][0] || 0;
					height = groups[3][0] || 0;
				}
			}
			if (width < 1 || height < 1) {
				// No size from Raster Attributes, dummy decode the image to find out the size.
				[width, height] = this.draw2(null);
			}
			canvas.width = width;
			canvas.height = height;
			const ctx = canvas.getContext('2d');
			const dest = ctx.createImageData(width, height);
			this.draw2(dest);
			ctx.putImageData(dest, 0, 0);
			return canvas;
		}
	}

	function split_sixel(s) {
		let pos = 0;
		const end = s.indexOf('\x1b\\');
		if (end === -1) return ['', s];
		// Skip initial (useless) parameters, up until "q" or failure.
		for (const c of s) {
			pos++;
			if (c === 'q') return [s.slice(pos, end), s.slice(end + 2)];
			if (c === ';' || '0123456789'.indexOf(c) >= 0) continue;
			break; // Unknown character, give up.
		}
		return ['', s];
	}

	function find_sixels(a) {
		const res = [];
		for (const s of a) {
			const parts = s.split('\x1bP');
			res.push(parts[0]);
			for (const sixeldata of parts.slice(1)) {
				const [sixel_part, tail_part] = split_sixel(sixeldata);
				if (sixel_part) {
					res.push(new Sixel(sixel_part, tail_part))
				} else {
					res.push(tail_part);
				}
			}
		}
		return res;
	}

	// Parse SGR sequences in text, replace el contents with results.
	// May add extra elements after el, so el must have a parent.
	function parseANSI(el, text) {
		if (!text) return;
		const parts = find_sixels(text.split('\x1b['));
		el.innerText = parts[0];
		const attr2name = [null, 'bold', 'faint', 'italic', 'underline', 'blink-slow', 'blink-fast', 'invert', 'hide', 'strike'];
		const reset_extra = {1: 2, 2: 1, 5: 6, 6: 5};
		const settings = {fg: null, bg: null, attr: new Set()};
		const apply = (groups) => {
			while (true) {
				const num = groups.next();
				if (num === null) return;
				if (num === 0) {
					settings.fg = settings.bg = null;
					settings.attr.clear();
				} else if (num < 10) {
					settings.attr.add(num);
					settings.attr.delete(reset_extra[num]);
				} else if (num > 20 && num < 30) {
					settings.attr.delete(num - 20);
					settings.attr.delete(reset_extra[num - 20]);
				} else if (num >= 30 && num < 38) {
					settings.fg = num - 30;
				} else if (num >= 90 && num < 98) {
					settings.fg = num - 82;
				} else if (num === 38) {
					settings.fg = parse_extended_colour(groups);
				} else if (num === 39) {
					settings.fg = null;
				} else if (num >= 40 && num < 48) {
					settings.bg = num - 40;
				} else if (num >= 100 && num < 108) {
					settings.bg = num - 92;
				} else if (num === 48) {
					settings.bg = parse_extended_colour(groups);
				} else if (num === 49) {
					settings.bg = null;
				}
				groups.finish_group();
			}
		};
		const make_span = (text) => {
			const span = document.createElement('SPAN');
			if (settings.fg !== null || settings.bg !== null || settings.attr.size) {
				let style = '';
				let fg = settings.fg;
				let bg = settings.bg;
				// I couldn't find a way to do this in CSS, so workaround it is.
				// I can't even find what the actual background is unless set here.
				if (settings.attr.has(7)) {
					bg = settings.fg;
					if (bg === null) bg = 'var(--ansi-invert-bg)';
					fg = settings.bg;
					if (fg === null) fg = 'var(--ansi-invert-fg)';
				}
				if (fg !== null) style += 'color: ' + idx2colour(fg) + ';';
				if (bg !== null) style += 'background: ' + idx2colour(bg) + ';';
				if (style) span.style = style;
				for (const a of settings.attr) {
					span.classList.add('ansi-' + attr2name[a]);
				}
			}
			span.innerText = text;
			el.appendChild(span);
		};
		for (const part of parts.slice(1)) {
			if (part instanceof Sixel) {
				const sixel_el = part.draw();
				el.insertAdjacentElement('afterend', sixel_el);
				el = document.createElement('PRE');
				sixel_el.insertAdjacentElement('afterend', el);
				if (part.trailing_text) make_span(part.trailing_text);
			} else {
				let [groups, ix] = split_params(part);
				if (part[ix] === 'm') {
					apply(groups_iter(groups));
					ix++;
				}
				if (ix < part.length) make_span(part.slice(ix));
			}
		}
	}
	return parseANSI;
})();
