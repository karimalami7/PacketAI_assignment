##################
#
#	Part 1: Test zipf's law on words contained on the books of the Gutenberg Project
#
#	Input: a random sample of 5000 books in gutenberg-sample/_
#
# 	Output: a single csv-file contained in resources/output/zipf

from pyspark.sql import SparkSession
from word_distribution import WordDistribution
import sys

def Main():

	if len(sys.argv) != 2:
		print("Usage: problem1.py <source_directory>")
		sys.exit(-1)

	spark_session = SparkSession.builder \
	    .appName("Word distribution") \
	    .getOrCreate()

	spark_session.sparkContext.addPyFile("src/util.py")
	spark_session.sparkContext.addPyFile("src/word_distribution.py")

	word_distribution= WordDistribution(spark_session, sys.argv[1])
	word_distribution.compute()
	word_distribution.store_csv("output/zipf")


if __name__=="__main__":
	Main()
