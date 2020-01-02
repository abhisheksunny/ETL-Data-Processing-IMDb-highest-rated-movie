import sys
import os

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from load_staging_table import LoadStagingTable 
from dimension_table_creation import DimensionTableCreation
from fact_table_creation import FactTableCreation
from reporting_table_creation import ReportingTableCreation
from data_quality import DataQualityCheck
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
    
    time_note = time.asctime(time.localtime())
    log_print('Start Time: '+time_note)
    
    if(args.runmode=='S3'):
        os.system('sh source_data_copy.sh {} {} {}'.format(args.source_dir,args.source_dir_archived,args.partition))
        log_print('Source Data Loaded at : '+time.asctime(time.localtime()))
    
    name_basics = LoadStagingTable(spark, args.source_dir, 'name.basics.tsv.gz', 
                                   args.partition, args.staging_dir, 'name_basics').execute()
    title_basics = LoadStagingTable(spark, args.source_dir, 'title.basics.tsv.gz', 
                                    args.partition, args.staging_dir, 'title_basics').execute()
    title_crew = LoadStagingTable(spark, args.source_dir, 'title.crew.tsv.gz', 
                                  args.partition, args.staging_dir, 'title_crew').execute()
    title_ratings = LoadStagingTable(spark, args.source_dir, 'title.ratings.tsv.gz', 
                                     args.partition, args.staging_dir, 'title_ratings').execute()
    time_note = time.asctime(time.localtime())
    log_print('Loading completion Time of Staging Tables: '+time_note)
    
    crew_partitioned = DimensionTableCreation() \
    .crew_partitioned(spark, name_basics, title_crew, args.partition_dir, args.partition, args.dimension_dir, 'crew_partitioned')
    titles_processed = DimensionTableCreation() \
    .titles_processed(spark, title_basics, args.partition_dir, args.partition, args.dimension_dir, 'titles_processed')
    ratings_processed = DimensionTableCreation() \
    .ratings_processed(spark, title_ratings, args.partition_dir, args.partition, args.dimension_dir, 'title_ratings')
    genres = DimensionTableCreation() \
    .genres(spark, title_basics, args.partition_dir, args.partition, args.dimension_dir, 'genres')
    time_note = time.asctime(time.localtime())
    log_print('Loading completion Time of Dimension Tables: '+time_note)
    
    movie_rating_fact = FactTableCreation()\
    .movie_rating_fact(spark, crew_partitioned, titles_processed, ratings_processed, 
                       genres, args.partition, args.fact_dir, 'movie_rating_fact')
    time_note = time.asctime(time.localtime())
    log_print('Loading completion Time of Fact Tables: '+time_note)
    
    highest_rated_movie_report = ReportingTableCreation()\
    .highest_rated_movie_report(spark, movie_rating_fact, args.partition, args.reporting_dir, 'highest_rated_movie_report')
    time_note = time.asctime(time.localtime())
    log_print('Loading completion Time of Reporting Tables: '+time_note)
    
    #Data Quality Checks
    staging_tables = [name_basics, title_basics, title_crew, title_ratings]
    dimension_tables = [crew_partitioned, titles_processed, ratings_processed, genres]
    fact_tables = [movie_rating_fact]
    reporting_tables = [highest_rated_movie_report]
    all_tables = [staging_tables, dimension_tables, fact_tables, reporting_tables]
    
    log_print("Starting Data Quality Checks -- ")
    DataQualityCheck.generic_checks(spark, all_tables, args.partition_dir)
    DataQualityCheck.check_reporting_tables(spark, reporting_tables, args.partition_dir, args.data_quality_dir)
    log_print('Completion Time of Data Quality Checks: ' + time.asctime(time.localtime()))
    
    print(" ** ENDS ** ")
    
if __name__ == "__main__":
    main()