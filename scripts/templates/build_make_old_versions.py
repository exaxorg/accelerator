def main(urd):
	jid = urd.build(
		'csvimport',
		options=dict(filename='a.csv'),
	)
	jid = urd.build(
		'dataset_type',
		datasets=dict(source=jid),
		options=dict(column2type={'a': 'number', 'b': 'ascii'}),
	)
	previous = None
	for _ in range(50):
		previous = urd.build(
			'dataset_$HASHPART',
			datasets=dict(source=jid, previous=previous),
			options=dict(hashlabel='b'),
		)
