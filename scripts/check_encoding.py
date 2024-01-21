#!/opt/python/cp39-cp39/bin/python3
# -*- coding: utf-8 -*-
#
# Make sure all (relevant) python files have the expected encoding header.

import glob
import os
import sys

res = 0
for fn in sorted(glob.glob('/tmp/accelerator/**/*.py', recursive=True)):
	if '/accelerator/examples/' in fn:
		continue
	if '/accelerator/scripts/' in fn:
		continue
	if os.stat(fn).st_size == 0:
		continue
	with open(fn, 'rt', encoding='utf-8') as fh:
		a = next(fh)
		b = next(fh)
		if '# -*- coding: utf-8 -*-\n' not in (a, b):
			res = 1
			print(fn, "does not have a correct coding header")
sys.exit(res)
