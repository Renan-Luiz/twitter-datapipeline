from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum, to_date, date_format, countDistinct
import argparse
from os.path import join

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description = "Spark Twitter Insights"
    )

    parser.add_argument("--src", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--process_date", required=True)
    args = parser.parse_args()

    spark = SparkSession\
        .builder\
        .appName("twitter_insights")\
        .getOrCreate()

    tweet = spark.read.json(args.src)

    alura = tweet\
        .where("author_id = '1566580880'")\
        .select("author_id", "conversation_id")  

    tweet = tweet.alias("tweet")\
        .join(
            alura.alias("alura"),
            [
                alura.author_id != tweet.author_id,
                alura.conversation_id == tweet.conversation_id
            ],
            "left"
        )\
        .withColumn(
            "alura_conversation",
            when(col("alura.conversation_id").isNotNull(), 1).otherwise(0)
        ).withColumn(
            "reply_alura",
            when(col("tweet.in_reply_to_user_id") == '1566580880', 1).otherwise(0)
        ).groupBy(to_date("created_at").alias("created_date"))\
        .agg(
            countDistinct("id").alias("n_tweets"),
            countDistinct("tweet.conversation_id").alias("n_conversations"),
            sum("alura_conversation").alias("alura_conversation"),
            sum("reply_alura").alias("reply_alura")
        ).withColumn("weekday", date_format("created_date","E"))

    path = join(args.path, f"process_date={args.process_date}")

    tweet.coalesce(1)\
        .write\
        .json(path)

