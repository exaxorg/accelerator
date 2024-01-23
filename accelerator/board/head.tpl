<!DOCTYPE html>
<head>
	<title>ax board{{ title and ' - ' + title }}</title>
	<link href="{{ name2hashed['style.css'] }}" rel="stylesheet" />
	<script language="javascript" src="{{ name2hashed['script.js'] }}"></script>
	<meta name="robots" content="noindex, nofollow">
</head>
<body{{ ! get('bodyclass', '') and ' class="' + bodyclass + '"' }}>
	<div id="ax-version">version {{ ax_version }}</div>
% if title:
	<a href="/" id="main-link">main</a>
% end
