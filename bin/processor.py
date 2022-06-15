#!/usr/bin/env python

""" 
Python file used to read messages real time from a kafka topic named "transactions" and compute analytical queries in the transactions. 
The result of this queries is sent again to kafka, as different topics.
"""

from argparse import ArgumentParser
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery


def main():
    """ Main function where we instanciate the spark session, read the messages from kafka, define and execute the queries. Write back to kafka. """
    # Define possible arguments
    parser = ArgumentParser(description="Spark processor processing credit cards transactions. Reads from / write to Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:29092", help="Kafka bootstrap servers", type=str)
    args = parser.parse_args()

    # Instanciate the spark session
    spark = SparkSession \
        .builder \
        .appName("processor") \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

    # Reads messages from kafka topic "transactions"
    transactions = read_transactions(args, spark)

    # Create a temporary view
    transactions.createOrReplaceTempView("trans")

    # QUERY DEFINITION
    # QUERY 1
    trans_per_gender = spark.sql("""
        SELECT 'trans_per_gender' AS topic,
                    gender AS key,
                    to_json(named_struct(
                    'gender', gender,
                    'transactions', COUNT(*)
                    )) AS value
        FROM trans 
        GROUP BY gender
        """)

    # QUERY 2
    trans_per_city = spark.sql("""
        SELECT 'trans_per_city' AS topic,
                    city AS key,
                    to_json(named_struct(
                    'city', city,
                    'transactions', COUNT(*)
                    )) AS value
        FROM trans 
        GROUP BY city
        """)

    # QUERY 3
    trans_per_cat = spark.sql("""
        SELECT 'trans_per_cat' AS topic,
                    category AS key,
                    to_json(named_struct(
                    'category', category,
                    'transactions', COUNT(*)
                    )) AS value
        FROM trans 
        GROUP BY category
        """)

    # QUERY 4
    trans_last_minute = spark.sql("""
        SELECT 'trans_last_minute' AS topic,
                    '' AS key,
                    to_json(named_struct(
                    'category', category,
                    'start', window.start,
                    'end', window.end,
                    'transactions', COUNT(*),
                    'amount', SUM(amount)
                    )) AS value
        FROM trans
        GROUP BY category, WINDOW(ts, '1 minute', '5 seconds')
        """)

    # QUERY 5
    avg_per_cat = spark.sql("""
        SELECT 'avg_amount_cat' AS topic,
                    category AS key,
                    to_json(named_struct(
                    'category', category,
                    'amount', round(AVG(amount), 3)
                    )) AS value
        FROM trans 
        GROUP BY category
        """)

    # QUERY 6, not used in the dashboard
    avg_per_job = spark.sql("""
        SELECT 'avg_amount_job' AS topic,
                    job AS key,
                    to_json(named_struct(
                    'job', job,
                    'amount', round(AVG(amount), 3)
                    )) AS value
        FROM trans 
        GROUP BY job
        """)

    # QUERY 7
    total_trans_amount = spark.sql("""
        SELECT 'total_trans_amount' AS topic,
                    '' AS key,
                    to_json(named_struct(
                    'tot_amount', round(SUM(amount), 3)
                    )) AS value
        FROM trans
        """)

    # QUERY 8
    total_trans_number = spark.sql("""
        SELECT 'total_trans_number' AS topic,
                    '' AS key,
                    to_json(named_struct(
                    'tot_trans', count(*)
                    )) AS value
        FROM trans
        """)

    # QUERY 9
    min_amount = spark.sql("""
    SELECT 'min_amount' AS topic,
                '' AS key,
                to_json(named_struct(
                'min_amount', round(MIN(amount), 3)
                )) AS value
    FROM trans
    """)

    # QUERY 10
    max_amount = spark.sql("""
        SELECT 'max_amount' AS topic,
                    '' AS key,
                    to_json(named_struct(
                    'max_amount', round(MAX(amount), 3)
                    )) AS value
        FROM trans
        """)

    # Creates a list of queries
    queries: List[StreamingQuery] = []

    # Add queries to the list and execute
    queries.append(write(args, trans_per_gender, mode="update"))
    queries.append(write(args, trans_per_city, mode="update"))
    queries.append(write(args, avg_per_cat, mode="update"))
    queries.append(write(args,trans_per_cat, mode="update"))
    queries.append(write(args,trans_last_minute, mode="append"))
    queries.append(write(args,avg_per_job, mode="update"))
    queries.append(write(args,total_trans_amount, mode="update"))
    queries.append(write(args,total_trans_number, mode="update"))
    queries.append(write(args,min_amount, mode="update"))
    queries.append(write(args,max_amount, mode="update"))

    # Wait for termination
    for q in queries:
        q.awaitTermination()


def read_transactions(args, spark: SparkSession):
    """ Method used to read messages about transactions from kafka topic "transactions" """

    changes = (spark
               .readStream
               .format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap_servers)
               .option("subscribe", "transactions")
               .option("startingOffsets", "earliest")
               .option("failOnDataLoss", "False")
               .load()
               .selectExpr("CAST(value AS STRING)")
               .selectExpr(""" from_json(value, '
            `id` INTEGER,
            `timestamp` LONG,
            `category` STRING,
            `amount` FLOAT,
            `gender` STRING,
            `city` STRING,
            `state` STRING,
            `job` STRING,
            `city_pop` LONG
        ') AS value""")
               .selectExpr("value.*")
               .selectExpr(
                   "id",
                   "CAST(from_unixtime(timestamp) AS TIMESTAMP) AS ts",
                   "category",
                   "amount",
                   "gender",
                   "city",
                   "state",
                   "job",
                   "city_pop"
               )
               .withWatermark("ts", "1 MINUTE")
               )

    print("'changes' schema:")
    changes.printSchema()

    return changes



def write(args, df: DataFrame, mode="append", trigger="3 seconds") -> StreamingQuery:
    """ Method used to write the query result to a new topic in kafka """
    query_to_wait_for = df \
        .writeStream \
        .outputMode(mode) \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.bootstrap_servers) \
        .trigger(processingTime=trigger) \
        .start()

    return query_to_wait_for


if __name__ == "__main__":
    main()
