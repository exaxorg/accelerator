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
