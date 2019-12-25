import sys
from pyspark.sql.types import *
from pyspark.sql.functions import when, lit, split, union input_file_name

from custom_logger import log_print

class DimensionTableCreation:

    def crew_partitioned(spark, name_basics, title_crew, partition_dir, partition, output_location, output_table):
        name_basics+=partition_dir
        title_crew+=partition_dir
        output_dir=output_location+output_table+'/'
        
        name_basics_df = spark.read.parquet(name_basics)
        title_crew_df = spark.read.parquet(title_crew)
        
        title_crew_directors_df = title_crew_df.select('tconst','directors')\
        .withColumn('title_const', title_crew_df.tconst)\
        .withColumn('name_const', title_crew_df.directors)\
        .withColumn('profession', lit('directors'))
        title_crew_writers_df = title_crew_df.select('tconst','directors')\
        .withColumn('title_const', title_crew_df.tconst)\
        .withColumn('name_const', title_crew_df.writers)\
        .withColumn('profession', lit('writers'))
        title_crew_df = union(title_crew_directors_df, title_crew_writers_df)
        
        df = df.withColumn('data_date', lit(self.partition))
        df.write.mode('overwrite').partitionBy('data_date').parquet(output_dir)
"""
        print(df.dtypes)
        df.show(2)
        df.select(input_file_name()).limit(1).show(20, False)
"""