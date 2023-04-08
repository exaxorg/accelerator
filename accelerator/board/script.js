const parseANSI = (function () {
	// Two digit hex str
	const hex2 = (n) => ('0' + n.toString(16)).slice(-2);

	// Make a CSS colour from an index or (24bit RGB << 8)
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

	// Parse SGR sequences in text, replace el contents with results.
	function parseANSI(el, text) {
		if (!text) return;
		const parts = text.split('\x1b[');
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
		for (const part of parts.slice(1)) {
			let group = [];
			const groups = [group];
			let num = 0;
			let ix = 0;
			collect: for (const c of part) {
				ix += c.length; // of course a character can have length > 1
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
						case 'm':
							apply(groups_iter(groups));
							// fallthrough
						default:
							break collect;
					}
				}
			}
			if (ix < part.length) {
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
				span.innerText = part.slice(ix);
				el.appendChild(span);
			}
		}
	}
	return parseANSI;
})();
