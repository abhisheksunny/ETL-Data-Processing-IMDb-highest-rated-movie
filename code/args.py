import configparser
import sys
import os
from datetime import date
from custom_logger import log_print

config = configparser.ConfigParser()
config.read('properties.cfg')

class Args:
    
    runmode = ''
    partition = ''
    partition_dir = ''
    source_dir = ''
    source_dir_archived = ''
    staging_dir = ''
    dimension_dir = ''
    fact_dir = ''
    reporting_dir = ''
    data_quality_dir = ''

    def __init__(self,run_args=[]):
             
        self.runmode = Args.get_run_mode()
        log_print("Running in {} mode. Actual - {}.".format(self.runmode, config['code_config']['run_mode']))
        
        os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

        self.partition = str(date.today() if config['code_config']['partition']=='' else config['code_config']['partition'])
        partition_dir = 'data_date='+self.partition+'/'
        log_print("Running for data date partition - {}. Data date today is - {}.".format(self.partition,date.today()))
        
        self.source_dir = config[self.runmode]['source_dir'] 
        self.staging_dir = config[self.runmode]['staging_dir']
        self.dimension_dir = config[self.runmode]['dimension_dir']
        self.fact_dir = config[self.runmode]['fact_dir']
        self.reporting_dir = config[self.runmode]['reporting_dir']
        self.data_quality_dir = config[self.runmode]['data_quality_dir']
        
        if self.runmode == "S3":
            self.source_dir += self.partition+'/'
            self.source_dir_archived = config[self.runmode]['source_dir_archived']
        
        
    def get_run_mode():
        runmode = config['code_config']['run_mode']
        if (runmode != "S3" and  runmode != "local"):
            runmode = "local"
            log_print("Runmode not recognized. Available options: local | S3.")
        return runmode
    
    
if __name__ == "__main__":
    log_print("Could not run "+sys.argv[0]+" script. Try running main.py.")