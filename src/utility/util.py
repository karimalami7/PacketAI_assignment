#utility functions

import re

def wordClean(word):
	"""
	Gets clean words from a line

	>>> wordClean('Hello,')
	'hello'

	>>> wordClean('you?r')
	'your'

	>>> wordClean('A12')
	'a12'

	"""
	return re.sub('[^a-z0-9]+', '', word.lower()) 

def breakLine(line):
	"""
	Break line by space separator

	>>> breakLine("Hello HI ")
	['Hello', 'HI', '']

	"""
	return re.split(' +', line)


def saveToCsv(data_frame, output):

	try:
		data_frame.write.save(output, format="csv", header=True)
		print("Saved to csv")
	except Exception as e:
		print("ERROR!!")
		raise e

def saveToMongo(data_frame):

	try:
		data_frame.write.format("mongo").mode("append").save()
		print("Saved to mongo")
	except Exception as e:
		print("ERROR!!")
		raise e