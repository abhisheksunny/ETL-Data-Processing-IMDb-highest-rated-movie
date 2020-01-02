import sys
from pyspark.sql.types import *
from pyspark.sql.functions import lit, explode

from custom_logger import log_print

class DimensionTableCreation:
    
    def crew_partitioned(self, spark, name_basics, title_crew, partition_dir, partition, output_location, output_table):
        name_basics+=partition_dir
        title_crew+=partition_dir
        output_dir=output_location+output_table+'/'
        
        name_basics_df = spark.read.parquet(name_basics)
        title_crew_df = spark.read.parquet(title_crew)

        title_crew_df = title_crew_df.select('tconst','directors') \
        .filter(title_crew_df.directors.isNotNull()) \
        .withColumnRenamed('tconst', 'title_const') \
        .withColumnRenamed('directors' ,'name_const') \
        .withColumn('profession', lit('directors')) \
        .union(
            title_crew_df.select('tconst','writers')\
            .filter(title_crew_df.writers.isNotNull()) \
            .withColumnRenamed('tconst', 'title_const')\
            .withColumnRenamed('writers', 'name_const')\
            .withColumn('profession', lit('writers'))
        )

        df = name_basics_df.select('nconst','primaryName') \
        .withColumnRenamed('nconst', 'name_const') \
        .withColumnRenamed('primaryName', 'primary_name') \
        .join(title_crew_df, ['name_const'])
        
        df = df.withColumn('data_date', lit(partition))
        df.write.mode('overwrite').partitionBy('data_date', 'profession').parquet(output_dir)

        self.local_log(output_dir,output_table,df)
        return output_dir+partition_dir
        
        
    def titles_processed(self, spark, title_basics, partition_dir, partition, output_location, output_table):
        title_basics+=partition_dir
        output_dir=output_location+output_table+'/'     
        
        df = spark.read.parquet(title_basics)
        
        df = df.select('tconst', 'primaryTitle', 'titleType', 'runtimeMinutes') \
        .filter(df.titleType == 'movie') \
        .filter(df.runtimeMinutes > 60) \
        .withColumnRenamed('tconst', 'title_const') \
        .withColumnRenamed('primaryTitle', 'primary_title') \
        .withColumnRenamed('titleType', 'title_type') \
        .withColumnRenamed('runtimeMinutes', 'runtime')
        
        df = df.withColumn('data_date', lit(partition))
        df.write.mode('overwrite').partitionBy('data_date').parquet(output_dir)
        
        self.local_log(output_dir,output_table,df)
        return output_dir+partition_dir
    
    
    def ratings_processed(self, spark, title_ratings, partition_dir, partition, output_location, output_table):
        title_ratings+=partition_dir
        output_dir=output_location+output_table+'/'     
        
        df = spark.read.parquet(title_ratings)
        
        df = df.select('tconst', 'averageRating', 'numVotes') \
        .filter(df.averageRating > 0.0) \
        .withColumnRenamed('tconst', 'title_const') \
        .withColumnRenamed('averageRating', 'avg_rating') \
        .withColumnRenamed('numVotes', 'num_votes')
        
        df = df.withColumn('data_date', lit(partition))
        df.write.mode('overwrite').partitionBy('data_date').parquet(output_dir)
        
        self.local_log(output_dir,output_table,df)
        return output_dir+partition_dir
    
    
    def genres(self, spark, title_basics, partition_dir, partition, output_location, output_table):
        title_basics+=partition_dir
        output_dir=output_location+output_table+'/'     
        
        df = spark.read.parquet(title_basics)
        
        df = df.select('tconst', 'genres') \
        .withColumnRenamed('tconst', 'title_const') \
        .withColumn("genres", explode(df.genres))
        
        df = df.withColumn('data_date', lit(partition))
        df.write.mode('overwrite').partitionBy('data_date').parquet(output_dir)
        
        self.local_log(output_dir,output_table,df)
        return output_dir+partition_dir
    
    
    def local_log(self, output_dir, table_name, df):
        if(False):
            print('Table - '+ table_name+':: ')
            print(df.dtypes)
            df.show(2,False)
        log_print("Dimension Table created - '{}', at location - '{}'.".format(table_name, output_dir))

