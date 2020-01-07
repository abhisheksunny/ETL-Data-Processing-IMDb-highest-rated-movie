import sys
from pyspark.sql.types import *
from pyspark.sql.functions import when, lit, split, input_file_name

from custom_logger import log_print
from args import Args

class LoadStagingTable:
    
    def __init__(self, spark, input_location, input_filename, partition, output_location, output_table):
        self.spark = spark
        self.input_location = input_location
        self.input_filename = input_filename
        self.partition = partition
        self.output_location = output_location
        self.output_table = output_table 
        
    def execute(self):
        spark = self.spark
        
        df = spark.read\
        .option('delimiter', '\t')\
        .option('header', 'true')\
        .csv(self.input_location+self.input_filename)
        
        #Use output_table aka Table Name as function name for calling them
        df = getattr(LoadStagingTable, self.output_table)(df)
        
        #locals()["myfunction"]() # --> Another way
        
        df = df.withColumn('data_date', lit(self.partition))
        output_dir = self.output_location+self.output_table+'/'
        df.write.mode('overwrite').partitionBy('data_date').parquet(output_dir)
        
        if(False):
            print(df.dtypes)
            df.show(2, False)
            df.select(input_file_name()).limit(1).show(1, False)
        
        log_print("Staging Table loaded - '{}', at location - '{}'.".format(self.output_table, output_dir))
        return output_dir

    def name_basics(df):
        df = df.withColumn('birthYear', when(df.birthYear=='\\N',lit(-1)).otherwise(df.birthYear))\
        .withColumn('deathYear', when(df.deathYear=='\\N',lit(-1)).otherwise(df.deathYear))
        
        df = df.withColumn('birthYear', df.birthYear.cast(IntegerType()))\
        .withColumn('deathYear', df.deathYear.cast(IntegerType()))\
        .withColumn('primaryProfession',split(df.primaryProfession, ',').cast(ArrayType(StringType())))\
        .withColumn('knownForTitles',split(df.knownForTitles, ',').cast(ArrayType(StringType())))    
        
        return df
                
    def title_basics(df):
        df = df.withColumn('startYear', when(df.startYear=='\\N',lit(-1)).otherwise(df.startYear))\
        .withColumn('endYear', when(df.endYear=='\\N',lit(-1)).otherwise(df.endYear))\
        .withColumn('runtimeMinutes', when(df.runtimeMinutes=='\\N',lit(-1)).otherwise(df.runtimeMinutes))\
        .withColumn('genres', when(df.genres=='\\N',lit(None)).otherwise(df.genres))\
        .withColumn('isAdult', when(df.isAdult=='0',lit('No')).otherwise(lit('Yes')))

        df = df.withColumn('startYear', df.startYear.cast(IntegerType()))\
        .withColumn('endYear', df.endYear.cast(IntegerType()))\
        .withColumn('runtimeMinutes', df.runtimeMinutes.cast(IntegerType()))\
        .withColumn('genres',split(df.genres, ',').cast(ArrayType(StringType())))   
        
        return df
    
    def title_crew(df):
        df = df.withColumn('directors', when(df.directors=='\\N',lit(None)).otherwise(df.directors))\
        .withColumn('writers', when(df.writers=='\\N',lit(None)).otherwise(df.writers))
        return df
    
    def title_ratings(df):
        df = df.withColumn('averageRating', df.averageRating.cast(FloatType())) \
        .withColumn('numVotes', df.numVotes.cast(IntegerType()))
        return df
    
    
    
if __name__ == "__main__":
    log_print("Could not run "+sys.argv[0]+" script. Try running main.py.")