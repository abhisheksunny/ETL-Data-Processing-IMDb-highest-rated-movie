import sys
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from custom_logger import log_print

class ReportingTableCreation:
    def highest_rated_movie_report(self, spark, movie_rating_fact, partition, reporting_dir, table_name):
        
        movie_rating_fact = spark.read.parquet(movie_rating_fact).drop('data_date')
        
        df = movie_rating_fact.repartition('name_const')
        
        df_window = Window.partitionBy(df.profession, df.genres, df.name_const)
        df_window_pn = Window.partitionBy(df.profession, df.name_const)
        
        df = df.withColumn('calculated_metric_HRM',max(df.calculated_metric_movie).over(df_window))\
        .withColumn('rating_AHRM',max(df.movie_rating).over(df_window))\
        .withColumn('total_movies',count(df.title_const).over(df_window_pn))
        
        df = df.withColumn('is_HRM', when(df.calculated_metric_HRM==df.calculated_metric_movie, 'Y'))\
        .withColumn('is_AHRM', when(df.rating_AHRM==df.movie_rating, 'Y'))
        
        df = df.filter((df.is_HRM == 'Y') | (df.is_AHRM == 'Y'))\
        .withColumn('is_same', when(df.is_HRM==df.is_AHRM, 'Yes').otherwise('No'))
                
        df = df.groupBy(df.name_const, df.primary_name, df.profession, df.genres, df.total_movies, df.is_same)\
        .agg(
            collect_list(df.title_const).alias('title_const'), 
            collect_list(df.movie_name).alias('movie_name'), 
            collect_list(df.num_votes).alias('num_votes'),
            collect_list(df.calculated_metric_movie).alias('calculated_metric_movie'), 
            collect_list(df.movie_rating).alias('movie_rating')
        ).repartition(df.is_same)
        
        df_no = df.filter(df.is_same=='No')\
        .withColumn(
            "ahrm_index", 
            lit(df.calculated_metric_movie[0]>=df.calculated_metric_movie[1]).cast(IntegerType())
        ).withColumn(
            "hrm_index", 
            lit(df.calculated_metric_movie[0]<df.calculated_metric_movie[1]).cast(IntegerType())
        )
        
        df_yes = df.filter(df.is_same=='Yes')\
        .withColumn("ahrm_index", lit(0))\
        .withColumn("hrm_index", lit(0))
        
        df = df_yes.union(df_no)
        
        df = df.withColumn("name_HRM", df.movie_name[df.hrm_index])\
        .withColumn("rating_HRM", df.movie_rating[df.hrm_index])\
        .withColumn("votes_HRM", df.num_votes[df.hrm_index])\
        .withColumn("calculated_metric_HRM", df.calculated_metric_movie[df.hrm_index])\
        .withColumn("name_AHRM", df.movie_name[df.ahrm_index])\
        .withColumn("rating_AHRM", df.movie_rating[df.ahrm_index])\
        .selectExpr('primary_name as name', 'total_movies', 'name_HRM', 'rating_HRM', 
                    'votes_HRM', 'calculated_metric_HRM', 'name_AHRM', 
                    'rating_AHRM', 'is_same', 'profession', 'genres')\
        .withColumn('data_date', lit(partition))
        
        df.show(5)
        output_dir=reporting_dir+table_name+'/' 
        df.write.mode('overwrite').partitionBy('data_date', 'profession').parquet(output_dir)
        
        
        
if __name__ == "__main__":
    log_print("Could not run "+sys.argv[0]+" script. Try running main.py.")