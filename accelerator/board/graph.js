function getWidth(element) {
	var styles = window.getComputedStyle(element);
	var padding = parseFloat(styles.paddingLeft) + parseFloat(styles.paddingRight);
	return element.clientWidth - padding;
}

function positionpopup(popup, e) {
	if (e.clientX > getWidth(document.querySelector('#svg')) / 2) {
		const x = Math.max(0, e.clientX - getWidth(popup));
		popup.style.left = x + 'px'
	} else {
		popup.style.left = e.clientX + 'px';
	}
	popup.style.top = e.clientY + 'px';
}

function highlight_nodes(thisnode, onoff) {
	if (onoff) {
		thisnode.setAttribute('fill', 'var(--graphnode-highlight)');
		thisnode.setAttribute('stroke-width', '5');
	} else {
		thisnode.setAttribute('fill', thisnode.dataset.origfill);
		thisnode.setAttribute('stroke-width', '2');
	}
	const neighbour_nodes = JSON.parse(thisnode.dataset.neighbour_nodes);
	for (const jobid of neighbour_nodes) {
		const n = document.querySelector('#' + jobid);
		if (onoff) {
			n.setAttribute('fill', 'var(--graphnode-highlight2)');
		} else {
			n.setAttribute('fill', n.dataset.origfill);
		}
	}
	const neighbour_edges = JSON.parse(thisnode.dataset.neighbour_edges);
	const width = (onoff ? 6 : 2);
	for (const edge of neighbour_edges) {
		const group = document.getElementById(edge);
		for (const n of Array.from(group.children)) {
			n.setAttribute('stroke-width', width);
		}
	}
}

function panzoom () {
	const svg = document.querySelector('#svg');
	let move_init_epage = {x: null, y: null};
	let move_init_viewboxposition = {x: null, y: null};
	let pt = svg.createSVGPoint();
	let viewboxScale = 1;
	const init = svg.viewBox.baseVal;
	let viewboxPosition = {x: init.x, y: init.y};
	let viewboxSize = {x: init.width, y: init.height};
	let actualscale = Math.max(init.width / getWidth(svg), init.height / svg.clientHeight); // svg-pixels per screen-pixels
	svg.addEventListener('mousedown', mousedown);
	svg.addEventListener('wheel', wheel);
	function mousetosvgcoords(e) {
		// mouse pointer in svg coordinate system
		pt.x = e.pageX;
		pt.y = e.pageY;
		return pt.matrixTransform(svg.getScreenCTM().inverse());
	}
	function mousedown(e) {
		if (e.which === 1) {
			move_init_epage.x = e.pageX;
			move_init_epage.y = e.pageY;
			move_init_viewboxposition.x = viewboxPosition.x;
			move_init_viewboxposition.y = viewboxPosition.y;
			window.addEventListener('mouseup', mouseup);
			window.addEventListener('mousemove', mousemove);
			e.preventDefault();
			graphpopup_off();
		}
	}
	function mouseup(e) {
		window.removeEventListener('mousemove', mousemove);
		window.removeEventListener('mouseup', mouseup);
		e.preventDefault();
	}
	function mousemove(e) {
		viewboxPosition.x = move_init_viewboxposition.x + (move_init_epage.x - e.pageX) * actualscale;
		viewboxPosition.y = move_init_viewboxposition.y + (move_init_epage.y - e.pageY) * actualscale;
		setviewbox();
		e.preventDefault();
	}
	function wheel(e) {
		if (!e.ctrlKey) return;
		const scale = (e.deltaY < 0) ? 0.90 : 1/0.90;
		if ((viewboxScale * scale < 8.) && (viewboxScale * scale > 1./1024.))
		{
			const mpos = mousetosvgcoords(e);
			viewboxPosition.x = mpos.x * (1 - scale) + scale * viewboxPosition.x;
			viewboxPosition.y = mpos.y * (1 - scale) + scale * viewboxPosition.y;
			viewboxScale *= scale;
			actualscale *= scale;
			setviewbox();
		}
		e.preventDefault();
	}
	function setviewbox() {
		const pos_x = viewboxPosition.x;
		const pos_y = viewboxPosition.y;
		const scale_x = viewboxSize.x * viewboxScale;
		const scale_y = viewboxSize.y * viewboxScale;
		svg.setAttribute('viewBox', pos_x + ' ' + pos_y + ' ' + scale_x + ' ' + scale_y);
	}
}

function populatelist(jobid, items, location, maxitems=5) {
	const thelist = document.querySelector(location);
	thelist.style.display = 'none';
	if (items.length) {
		thelist.style.display = 'block';
		const thetable = document.querySelector(location + ' table');
		thetable.innerHTML = '';
		ix = 0;
		for (const item of items) {
			if (location === '#gp-columns') {
				const tr = document.createElement('tr');
				thetable.appendChild(tr);
				const td1 = document.createElement('td');
				tr.appendChild(td1);
				const td2 = document.createElement('td');
				tr.appendChild(td2);
				td1.textContent = item[0];
				td2.textContent = item[1];
			} else {
				const a = document.createElement('a');
				if (location === '#gp-files') {
					a.href = '/job/' + encodeURIComponent(jobid) + '/' + item;
					a.textContent = item;
				} else if (location === '#gp-datasets') {
					a.href = '/dataset/' + encodeURIComponent(item);
					a.textContent = item;
				} else if (location === '#gp-subjobs') {
					a.href = '/job/' + encodeURIComponent(item[0]);
					a.textContent = item[0] + '    (' + item[1] + ')';
				} else if (location === '#gp-inthisurdlist') {
					a.href = '/urd/' + encodeURIComponent(item);
					a.textContent = item;
				}
				const tr = document.createElement('tr');
				const td = document.createElement('td');
				thetable.appendChild(tr);
				tr.appendChild(td);
				td.appendChild(a);
			}
			ix += 1;
			if (items.length > maxitems && ix === maxitems) {
				const sublen = items.length - maxitems;
				const txt = document.createTextNode('... and ' + sublen + ' more.');
				const tr = document.createElement('tr');
				const td = document.createElement('td');
				td.appendChild(txt);
				tr.appendChild(td);
				thetable.appendChild(tr);
				break;
			}
		}
	}
}

function graphpopup_job(e, jobid, files, datasets, subjobs, method, name, atmaxdepth, timestamp, notinurdlist, parent, is_build) {
	const popup = document.querySelector('#graphpopup');
	popup.style.display = 'block';
	if (parent) {
		popup.children['gp-parent'].style.display = 'block';
		popup.children['gp-parent'].children['gp-parenthref'].textContent = parent;
		popup.children['gp-parent'].children['gp-parenthref'].href = '/job/' + encodeURIComponent(parent);
	} else {
		popup.children['gp-parent'].style.display = 'none';
	}
	popup.children['gp-jobid'].textContent = jobid;
	popup.children['gp-jobid'].href =  '/job/' + encodeURIComponent(jobid);
	popup.children['gp-method'].textContent = name;
	popup.children['gp-help'].href =  '/method/' + method;
	popup.children['gp-source'].href ='/job/' + encodeURIComponent(jobid) + '/method.tar.gz' + '/';
	popup.children['gp-timestamp'].textContent = '[' + timestamp + ']';
	if (atmaxdepth === true) {
		popup.children['gp-atmaxdepth'].style.display = 'block';
	} else {
		popup.children['gp-atmaxdepth'].style.display = 'none';
	}
	if (notinurdlist === false ) {
		popup.children['gp-notinurdlist'].style.display = 'none';
		popup.children['gp-inthisurdlist'].style.display = 'none';
	} else if (notinurdlist === true) {
		popup.children['gp-notinurdlist'].style.display = 'block';
		popup.children['gp-inthisurdlist'].style.display = 'none';
	} else {
		popup.children['gp-notinurdlist'].style.display = 'none';
		popup.children['gp-inthisurdlist'].style.display = 'block';
		populatelist(jobid, notinurdlist, '#gp-inthisurdlist');
	}
	if (is_build === true) {
		popup.children['gp-help'].style.display='none';
	} else {
		popup.children['gp-help'].style.display='block';
	}
	populatelist(jobid, files, '#gp-files');
	populatelist(jobid, datasets, '#gp-datasets');
	populatelist(jobid, subjobs, '#gp-subjobs');
	positionpopup(popup, e);
}

function graphpopup_ds(e, jobid, method, ds, columns, atmaxdepth, timestamp, is_build) {
	const popup = document.querySelector('#graphpopup');
	popup.style.display = 'block';
	popup.children['gp-dataset'].textContent = ds;
	popup.children['gp-dataset'].href = '/dataset/' + encodeURIComponent(ds);
	popup.children['gp-jobid'].textContent = jobid;
	popup.children['gp-jobid'].href =  '/job/' + encodeURIComponent(jobid);
	popup.children['gp-method'].textContent = method;
	popup.children['gp-help'].href = '/method/' + method;
	popup.children['gp-source'].href ='/job/' + encodeURIComponent(jobid) + '/method.tar.gz' + '/';
	popup.children['gp-timestamp'].textContent = '[' + timestamp + ']';
	if (atmaxdepth === true) {
		popup.children['gp-atmaxdepth'].style.display = 'block';
	} else {
		popup.children['gp-atmaxdepth'].style.display = 'none';
	}
	if (is_build === true) {
		popup.children['gp-help'].style.display='none';
	} else {
		popup.children['gp-help'].style.display='block';
	}
	populatelist(jobid, columns, '#gp-columns');
	positionpopup(popup, e);
}

function graphpopup_off() {
	const popup = document.querySelector('#graphpopup');
	popup.style.display = 'none';
}
