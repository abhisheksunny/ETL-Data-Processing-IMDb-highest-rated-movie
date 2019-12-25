import sys

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from load_staging_table import LoadStagingTable 
from dimension_table_creation import DimensionTableCreation
from args import Args
from custom_logger import log_print


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.ui.port", "3000") \
        .getOrCreate()
    return spark

def main():
    args=Args(sys.argv)
    spark = create_spark_session()
    
    name_basics = LoadStagingTable(spark, args.source_dir, 'name.basics.tsv.gz', 
                                   args.partition, args.staging_dir, 'name_basics').execute()
    title_basics = LoadStagingTable(spark, args.source_dir, 'title.basics.tsv.gz', 
                                    args.partition, args.staging_dir, 'title_basics').execute()
    title_crew = LoadStagingTable(spark, args.source_dir, 'title.crew.tsv.gz', 
                                  args.partition, args.staging_dir, 'title_crew').execute()
    title_ratings = LoadStagingTable(spark, args.source_dir, 'title.ratings.tsv.gz', 
                                     args.partition, args.staging_dir, 'title_ratings').execute()
  
    
    crew_partitioned = DimensionTableCreation() \
    .crew_partitioned(spark, name_basics, title_crew, args.partition_dir, args.partition, dimension_dir, crew_partitioned)
    
    

if __name__ == "__main__":
    main()