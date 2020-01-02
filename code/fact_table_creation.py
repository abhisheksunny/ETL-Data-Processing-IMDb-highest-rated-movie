import sys
from pyspark.sql.types import *
from pyspark.sql.functions import lit, max, col, udf

from custom_logger import log_print

MAX_RATING = 10.0

class FactTableCreation:
    
    def movie_rating_fact(self, spark, crew_partitioned, 
                          titles_processed, ratings_processed, 
                          genres, partition, fact_dir, table_name):
        
        crew_partitioned_df = spark.read.parquet(crew_partitioned).drop('data_date')
        titles_processed_df = spark.read.parquet(titles_processed).drop('data_date')
        ratings_processed_df = spark.read.parquet(ratings_processed).drop('data_date')
        genres_df = spark.read.parquet(genres).drop('data_date')
              
        joined_df = titles_processed_df.select('title_const', 'primary_title')\
        .join(ratings_processed_df, ['title_const'])\
        .join(genres_df, ['title_const'])
        
        crew_partitioned_df = crew_partitioned_df.select('name_const', 'primary_name', 'title_const' ,'profession')
        df = joined_df.join(crew_partitioned_df, ['title_const'])\
        .withColumnRenamed('primary_title', 'movie_name')\
        .withColumnRenamed('avg_rating', 'movie_rating')
        
        rounding_udf = udf(lambda x: round(x, 4), DoubleType())
        
        df = df.withColumn('calculated_metric_movie',rounding_udf(df.movie_rating * df.num_votes))\
        .withColumn('movie_rating',rounding_udf(df.movie_rating))\
        .withColumn('data_date', lit(partition))
        
        df = df.select('name_const', 'primary_name', 'title_const', 'movie_name', 'movie_rating', 
                       'num_votes', 'calculated_metric_movie', 'data_date', 'profession', 'genres')\
        .repartition('data_date', 'profession', 'genres')
        
        #max_votes = ratings_processed_df.select(max(df.num_votes).alias('max_num_votes')).collect()[0].max_num_votes
        max_votes = ratings_processed_df.select(max(df.num_votes)).head()[0]
        log_print('Max rating: {}, Max number of votes: {}.'.format(MAX_RATING, max_votes))
        
        output_dir=fact_dir+table_name+'/' 
        df.write.mode('overwrite').partitionBy('data_date', 'profession', 'genres').parquet(output_dir)
        
        df.show(5)
        
        return output_dir
    
    
if __name__ == "__main__":
    log_print("Could not run "+sys.argv[0]+" script. Try running main.py.")