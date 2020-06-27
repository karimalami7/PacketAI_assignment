from word import Word
from pymongo import MongoClient

class MongoConnector:

	def __init__(self, host, port, db, collection):
		try:
			self.words_collection=MongoClient(host, port)[db][collection]
		except Exception as e:
			print("The collection is not accessible")
		

	def get_document_by_word(self, word):
		try:
			next_words_list=self.words_collection.find_one({"word" : word.lower()})["next"]
			return Word(word, next_words_list)
		except Exception as e:
			print ("The word you are looking for does not exist in the database")
		
		