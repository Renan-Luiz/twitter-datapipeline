from os.path import join
import argparse
from pyspark.sql import SparkSession, functions as f

def get_tweets_data(df):
    return df\
        .select(
            f.explode("data").alias("tweets")
        ).select(
            "tweets.author_id",
            "tweets.conversation_id",
            "tweets.created_at",
            "tweets.id",
            "tweets.in_reply_to_user_id",
            "tweets.public_metrics.*",
            "tweets.text"
        )

def get_users_data(df):
    return df\
        .select(
            f.explode("includes.users").alias("users")
        ).select(
            "users.*"
        )

def export_df(df,path):
    df.coalesce(1).write.mode("overwrite").json(path)

def twitter_transform(spark, src, path, process_date):
    df = spark.read.json(src)
    tweets_df = get_tweets_data(df)
    users_df = get_users_data(df)
    table_path = join(path, "{table_name}", f"process_date={process_date}")
    export_df(tweets_df, table_path.format(table_name = "tweets"))
    export_df(users_df, table_path.format(table_name = "users"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = "Spark Twitter Transformation"
    )

    parser.add_argument("--src", required=True)
    parser.add_argument("--path", required=True)
    parser.add_argument("--process_date", required=True)
    args = parser.parse_args()
    
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    twitter_transform(spark, args.src, args.path, args.process_date)

    
