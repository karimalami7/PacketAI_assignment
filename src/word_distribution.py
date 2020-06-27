from util import word_clean, break_line
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window


class WordDistribution:

	"""
	Describe the class
	"""

	def __init__(self, spark_session, source_directory):

		self.spark_session=spark_session
		self.source_directory=source_directory


	def compute(self):

		### get spark_context from spark_session
		spark_context=self.spark_session.sparkContext


		""" 1/ Create RDD of words """
		word_rdd = spark_context.textFile(self.source_directory+"/*",4) \
			.flatMap(break_line) \
			.map(word_clean) \
			.filter(lambda word: word!="") \


		""" 2/ Creating the DataFrame (word,count)

		+--------+---------+
		|   word |  count  |  
		+--------+---------+
		"""
		word_count_df =	word_rdd.map(lambda word: (word,1)) \
			.reduceByKey(lambda v1, v2: v1+v2) \
			.map(lambda pair: Row(word=pair[0], count=pair[1])) \
			.toDF()

		total_unique_words = word_count_df.count()


		""" 3/ Adding the column frequency

		+--------+---------+--------------+
		|   word |  count  |  frequency   |
		+--------+---------+--------------+
		"""
		
		word_count_df = word_count_df.withColumn("frequency",word_count_df["count"]/total_unique_words)


		""" 4/ Adding the column rank

		+--------+---------+---------------+------------+
		|   word |  count  |   frequency   |    rank    |
		+--------+---------+---------------+------------+
		"""

		word_count_df = word_count_df.withColumn("rank", dense_rank().over(Window.orderBy(desc("count"))))


		""" 5/ Adding the column zipf

		+--------+---------+---------------+------------+----------+
		|   word |  count  |   frequency   |    rank    |   zipf   |
		+--------+---------+---------------+------------+----------+
		"""

		self.data = word_count_df.withColumn("zipf",1/word_count_df.rank)


	def store_csv(self, output):

		"""  Saving output data into csv format """
		try:
			self.data.write.save(output, format="csv", header=True)
		except Exception as e:
			raise e