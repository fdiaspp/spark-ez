from pyspark.sql import SparkSession
import os
import json


INPUT_FILE_PATH = "data/input/users/load.csv"
OUTPUT_FILE_PATH = "data/output/"
DATATYPES_FILE_PATH = "config/types_mapping.json"
APP_NAME = "PipelineTesteCognitivoAi"
SPARK_CLUSTER = "local"


def datatypes():
	"""Loads fields configurations settings"""
	
	return json.loads(open(DATATYPES_FILE_PATH, 'r').read())


if __name__ == '__main__':
	"""Job aims to deduplicate data and convert types of data fields previous specified."""


	# Acquire Spark Session
	spark = SparkSession.builder \
		.master(SPARK_CLUSTER) \
		.appName(APP_NAME) \
		.getOrCreate()


	# Loads the data from Local File System since this job
	# is not using any kind of cloud service
	df = spark.read.format("csv") \
				.option("header", "true") \
				.load(INPUT_FILE_PATH)


	# Eliminates duplicated data rows based on `update_date`
	# To acomplish this, first is necessary to get only the pair
	# `id` and `update_date` is valid.
	df_grouped = df \
				.groupby("id") \
				.agg({"update_date":"max"}) \
				.withColumnRenamed("max(update_date)","update_date")


	# And, in second place, proceed with a INNER JOIN operation to 
	# effectively retrieve the right data
	df_transformed = df.join(df_grouped, on=["id", "update_date"], how="inner")

	
	# Since the data needed was found, checkpoint the data
	# for further operations
	df_transformed.cache()

	
	# Transforms data fields (this task is strategically here because now
	# we have our main data reduced)
	dt_configs = datatypes()
	for dt in dt_configs:
		df_transformed = df_transformed 	\
							.withColumn(
								dt, 
								df_transformed[dt] \
									.cast(dt_configs[dt])
							)	
	
	
	# Persist data in the File System with PARQUET
	# PARQUET is a COLUMN based file, which helps in operations that 
	# using only some fields of a row is needed. Since there is none especification
	# related to the way that data will be used, I think PARQUET is best suited
	# for general purpose.
	df_transformed.write.parquet(OUTPUT_FILE_PATH + "output.parquet")
	
	
	# Free the memory once all the operation is 
	# finished	
	spark.catalog.clearCache()
		
