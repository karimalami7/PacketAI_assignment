##################
#
#	Part 2: Compute probabilty of followers of a given word
#
#	Input: a random sample of 5000 books in gutenberg-sample/_
#
# 	Output: store results in json and push to mongodb

from pyspark.sql import SparkSession
from word_followers import WordFollowers
import sys


def Main():

	if len(sys.argv) != 2:
		print("Usage: problem2.py <source_directory>")
		sys.exit(-1)

	spark_session = SparkSession.builder \
	    .appName("Word followers") \
	    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/gutenberg.words") \
	    .getOrCreate()

	spark_session.sparkContext.addPyFile("src/util.py")
	spark_session.sparkContext.addPyFile("src/word_followers.py")

	word_followers= WordFollowers(sys.argv[1],spark_session)
	word_followers.compute()
	word_followers.store()


if __name__=="__main__":
	Main()
