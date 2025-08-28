description = "Write a sliced file in analysis, one file in synthesis, and return some data."


def analysis(sliceno, job):
	# save one file per analysis process...
	filename = 'myfile1'
	data = f'This is job {job} analysis slice {sliceno}.'
	job.save(data, filename, sliceno=sliceno)


def synthesis(job):
	# ...and one file in the synthesis process...
	filename = 'myfile2'
	data = f'this is job {job} synthesis'
	job.save(data, filename)

	# ...and let's return some data too.
	returndata = f'this is job {job} return value'
	return returndata
