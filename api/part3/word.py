from random import choice

class Word:

	def __init__(self,word, next_words_list):
		self.word=word
		self.next_words_list=next_words_list

	def possibleNextWord(self):

		words=list(map(lambda obj: obj["nw"], self.next_words_list))
		probability=list(map(lambda obj: obj["p"], self.next_words_list))
		return {"words": words, "probability": probability}

	def guessNextWord(self):

		guess=choice(list(map(lambda obj: obj["nw"], self.next_words_list)))
		return {"guess": guess}