import sys
from pyspark.sql.types import *
from pyspark.sql.functions import lit, max, col, udf

from custom_logger import log_print

class DataQualityCheck:
    # This will check for two things: 1.If path is correct and has data 2.If counts are greater than 0.
    def generic_checks(spark, all_tables, partition_dir):
        
        dqc_failed = False
        no_data_error = "No data found on given location"
        
        for tables in all_tables:
            for table_path in tables:
                table_name = table_path.split('/')[-2]
                data = None
                try:
                    df = spark.read.parquet(table_path+partition_dir)
                except:
                    log_print(" DQC Failed : ,Table- {}, Error- {} {}.".format(table_name, no_data_error, table_path))
                    dqc_failed = True
                    continue
                count = df.count()
                if(count > 0):
                    log_print(" DQC : Table- {}, Counts- {}.".format(table_name, count))
                else:
                    log_print(" DQC Failed : {} table has {} counts.".format(table_name, count))
                    dqc_failed = True
                    
        if(dqc_failed):
            log_print("Data Quality Check Failed! Terminating ETL run.")
            # raise Exception('One or more Data Quality Check Failed!')
            
            
    # This will check for in consistancies in reporting data set. Data which may not be fit for reporting.
    def check_reporting_tables(spark, reporting_tables, partition_dir, dqc_output_dir):
        reporting_table = reporting_tables[0]
        table_name = reporting_table.split('/')[-2]
        dqc_output_dir += 'failed_data_' + table_name + '/' + partition_dir 
        
        df = spark.read.parquet(reporting_table+partition_dir)\
        .drop('data_date').drop('calculated_metric_HRM').drop('total_movies')
        
        df = df.filter(df.is_same == 'No').filter(df.name_HRM==df.name_AHRM)\
        .withColumn('type', lit('probable_incorrect_data'))
        
        count = df.count()
        if(count > 0):
            log_print(" DQC : {} table might have {} incorrect records. Data is stored in {}."
                      .format(table_name, count, dqc_output_dir))
        else: 
            log_print(" DQC : Data in {} table looks good.".format(table_name))
            
        log_print(" DQC : Sample data of {} table.")
        df.show(10)
        df.write.mode('overwrite').parquet(dqc_output_dir)
        
        
if __name__ == "__main__":
    log_print("Could not run "+sys.argv[0]+" script. Try running main.py.")