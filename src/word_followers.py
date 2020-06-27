from util import word_clean, break_line
from pyspark.sql import Row
import pyspark.sql.functions as F

class WordFollowers:

	"""
	This class takes a set of text files and computes the followers of each word.
	"""

	def __init__(self, source_directory, spark_session):

		self.spark_session=spark_session
		self.source_directory=source_directory


	def compute(self):

		### get spark_context from spark_session
		spark_context=self.spark_session.sparkContext

		""" 1/ Create RDD of words"""

		words_rdd = spark_context.textFile(self.source_directory+"/*",4) \
			.flatMap(break_line) \
			.map(word_clean) \
			.filter(lambda word: word!="")


		""" 2/ Creating the DataFrame (word,count)

		+--------+---------+
		|   word |  count  |  
		+--------+---------+
		"""

		words_count_df= words_rdd.map(lambda word: (word,1)) \
			.reduceByKey(lambda v1,v2: v1+v2) \
			.map(lambda pair: Row(word=pair[0], count=pair[1])) \
			.toDF()


		""" 3/ Creating the DataFrame word_and_follower_df

		+--------+-----------------+
		|   word |   word_follower |
		+--------+-----------------+
		"""

		words_indexed_df=words_rdd.zipWithIndex().map(lambda pair: Row(word=pair[0],pos=pair[1])) \
			.toDF()

		shifted_words_df=words_indexed_df.select((words_indexed_df['word']).alias('word_follower'), (words_indexed_df['pos']-1).alias('pos'))

		word_and_follower_df=words_indexed_df.join(shifted_words_df, 'pos', 'inner') \
			.select('word', 'word_follower')


		""" 4/ Group by (word, word_follower) to  computer the compute the count of the following word
		
		+--------+-----------------+---------------------+
		|   word |   word_follower |  follower_count     |
		+--------+-----------------+---------------------+
		"""		

		word_and_follower_df=word_and_follower_df.groupBy('word', 'word_follower') \
			.count()

		word_and_follower_df=word_and_follower_df.withColumnRenamed('count', 'follower_count')


		""" 5/ Join the DataFrame word_and_follower_df with word_count_df

		+--------+-----------------+---------------------+---------+
		|   word |   word_follower |  follower_count | count   |
		+--------+-----------------+---------------------+---------+
		"""			

		word_and_follower_df = word_and_follower_df.join(words_count_df, 'word', 'inner')


		""" 6/ Creating the DataFrame

		+--------+-------+-----+
		|   word |   nw  |  p  |
		+--------+-------+-----+
		"""			

		word_and_follower_df = word_and_follower_df.withColumn('probability', word_and_follower_df['follower_count']/word_and_follower_df['count'])
		word_and_follower_df = word_and_follower_df.select('word', word_and_follower_df['word_follower'].alias('nw'), word_and_follower_df['probability'].alias('p'))


		""" 7/ Grouping by word and creating a struct column next=(nw,p)

		+--------+-------------+
		|   word |   next   |
		+--------+-------------+
		"""			

		self.data=word_and_follower_df.groupBy('word') \
			.agg(F.collect_list(F.struct('nw', 'p')).alias('next'))


	def store(self):
		
		self.data.write.format("mongo").mode("append").save()