##############################
# 
#	Part 1: Test zipf's law on words contained on the books of the Gutenberg Project
#
#	Input: a directory containing text files
#
# 	Output: a single csv file saved in output/zipf


from pyspark.sql import SparkSession
from word_frequency import WordFrequency
import sys
sys.path.append('src/utility')
from util import saveToCsv


def main():

	if len(sys.argv) != 2:
		print("Usage: part1.py <source_directory>")
		sys.exit(-1)

	spark_session = SparkSession.builder \
	    .appName("Word frequency") \
	    .getOrCreate()

	spark_session.sparkContext.addPyFile("src/utility/util.py")
	spark_session.sparkContext.addPyFile("src/part1/word_frequency.py")

	word_frequency = WordFrequency(spark_session, sys.argv[1])
	word_frequency.compute()
	saveToCsv(word_frequency.data, "output/part1/")


if __name__ == "__main__":
	main()
