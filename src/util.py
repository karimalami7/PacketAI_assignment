#utility functions

import re

def word_clean(word):
	"""
	Gets clean words from a line

	>>> word_clean('Hello,')
	'hello'

	>>> word_clean('you?r')
	'your'

	>>> word_clean('A12')
	'a12'

	"""
	return re.sub('[^A-Za-z0-9]+', '', word.lower()) 

def break_line(line):
	
	"""
	Break line by space separator

	>>> break_line("Hello HI ")
	['Hello', 'HI', '']


	"""

	return re.split(' +', line)