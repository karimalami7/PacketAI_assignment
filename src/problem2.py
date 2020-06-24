##################
#
#	Part 2: Compute probabilty of followers of a given word
#
#	Input: a random sample of 5000 books in gutenberg-sample/_
#
# 	Output: store results in json and push to mongodb

from pyspark.sql import Row, SparkSession
import re
import sys
	

class wordFollwers:

	"""
	This class takes a set of text files and computes the followers of each word.
	"""

	def __init__(self, source_directory, spark_session):

		self.spark_session=spark_session
		self.source_directory=source_directory

		sc=self.spark_session.sparkContext

		""" 1/ Create RDD of words"""

		words = sc.textFile(self.source_directory+"/*") \
			.flatMap(lambda line: line.split(" ")) \
			.map(lambda word: re.sub('[^A-Za-z0-9]+', '', word)) \
			.filter(lambda word: word!="") \
			.map(lambda word: word.lower())

		""" 2/ Creating a RDD of pairs (word,frequency)"""

		words_count= words.map(lambda word: (word,1)) \
			.reduceByKey(lambda v1,v2: v1+v2)


		""" 3/ Creating a RDD of pairs (word,(follower,frequency)
		RDDs keep the order of elements. I zip each element with its index.
		I create another RDD of words but I shift the index to the left. 
		Hence a word and its follower will share the same index and I join the 2 RDDs
		"""

		words_indexed= words.zipWithIndex() \
			.map(lambda pair:(pair[1],pair[0]))

		shifted_words= words_indexed \
			.map(lambda pair: (pair[0]-1,pair[1]))

		word_and_follower=words_indexed.join(shifted_words) \
			.map(lambda pair: pair[1]) \
			.map(lambda pair: (pair,1)) \
			.reduceByKey( lambda v1,v2: v1+v2) \
			.map(lambda pair: (pair[0][0], (pair[0][1], pair[1])))

		""" 4/ Join RDDs words_count & word_and_follower
		"""		

		word_register= words_count.join(word_and_follower) \
			.map(lambda tuple: (tuple[0], Row(nw=tuple[1][1][0],p=tuple[1][1][1]/tuple[1][0]))) \
			.groupByKey() \
			.map(lambda tuple: Row(word=tuple[0],next=tuple[1]))

		
		word_register_df=self.spark_session.createDataFrame(word_register)
		
		word_register_df.write.format("mongo").mode("append").save()


def Main():

	if len(sys.argv) != 2:
		print("Usage: problem2.py <source_directory>")
		sys.exit(-1)

	spark = SparkSession.builder \
	    .appName("Word followers") \
	    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/gutenberg.words") \
	    .getOrCreate()

	wordfollowers= wordFollwers(sys.argv[1],spark)


if __name__=="__main__":
	Main()
