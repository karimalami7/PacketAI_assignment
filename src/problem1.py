##################
#
#	Part 1: Test zipf's law on words contained on the books of the Gutenberg Project
#
#	Input: a random sample of 5000 books in gutenberg-sample/_
#
# 	Output: a single csv-file contained in resources/output/zipf

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import re
import sys
	
class wordDistribution:

	"""
	This class takes a set of text files and computes the frequency of each word.
	"""

	def __init__(self, source_directory, spark_session):

		self.spark_session=spark_session
		self.source_directory=source_directory

		sc=self.spark_session.sparkContext

		""" 1/ Counting occurency of words """
		word_count_rdd = sc.textFile(source_directory).flatMap(lambda line: line.split(" ")) \
			.map(lambda word: re.sub('[^A-Za-z0-9]+', '', word)) \
			.map(lambda word: word.lower()) \
			.map(lambda word: (word,1)) \
			.reduceByKey(lambda v1, v2: v1+v2)

		""" 2/ Transforming RDD to DF """
		word_count_df = self.spark_session.createDataFrame(word_count_rdd.map(lambda pair: Row(word=pair[0], count_=pair[1])))
		word_count_df.orderBy("count_", ascending=False)

		""" 3/ Adding columns """
		total_unique_words=word_count_df.count()
		word_count_df= word_count_df.withColumn("frequency",word_count_df.count_/total_unique_words)
		word_count_df= word_count_df.withColumn("rank", dense_rank().over(Window.orderBy(desc("count_"))))
		word_count_df= word_count_df.withColumn("zipf",1/word_count_df.rank)

		""" 4/ Saving output into csv format """
		#word_count_df.write.csv("../resources/output/zipf/output.csv")
		word_count_df.show()


def Main():

	if len(sys.argv) != 2:
		print("Usage: problem1.py <source_directory>")
		sys.exit(-1)

	spark_session = SparkSession.builder \
	    .appName("Word Distribution") \
	    .getOrCreate()

	wordistribution= wordDistribution(sys.argv[1],spark_session)


if __name__=="__main__":
	Main()
