##################
#
#	Part 2: Compute probabilty of followers of a given word
#
#	Input: a directory containing text files
#
# 	Output: documents stored in mongo


from pyspark.sql import SparkSession
from word_followers import WordFollowers
import sys
sys.path.append('src/utility')
from util import saveToMongo


def main():

	if len(sys.argv) != 2:
		print("Usage: part2.py <source_directory>")
		sys.exit(-1)

	spark_session = SparkSession.builder \
	    .appName("Word followers") \
	    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/gutenberg.words") \
	    .getOrCreate()

	spark_session.sparkContext.addPyFile("src/utility/util.py")
	spark_session.sparkContext.addPyFile("src/part2/word_followers.py")

	word_followers = WordFollowers(sys.argv[1],spark_session)
	word_followers.compute()
	saveToMongo(word_followers.data)


if __name__ == "__main__":
	main()
