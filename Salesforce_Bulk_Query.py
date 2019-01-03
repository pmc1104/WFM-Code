import pandas as pd
from time import sleep
import salesforce_bulk as sf
import configparser



###############################################################################
###############################################################################



class Salesforce_Query():
    
    def __init__(self):
        config = configparser.ConfigParser()  
        config.read('C:/Users/pmc1104/OneDrive - The Home Depot/Documents/Projects/salesforce_config.ini')

        self.username = config['Info']['Username']
        self.password = config['Info']['Password']
        self.security_token = config['Info']['Security_Token']
        self.client = config['Info']['Client']
        self.secret = config['Info']['Secret']
    
    
    
    def SF_Dataframe(self, query_list, object_list):
        print('Establishing Connection to Salesforce')
        try:
            bulk = sf.SalesforceBulk(username = self.username, password = self.password,
                                     security_token = self.security_token)
        except:
            bulk = sf.SalesforceBulk(username = self.username, password = self.password,
                                     security_token = self.security_token)
            
            
        print('Connection Established')
        
        def Run_SF_Query(query, object_name):
            job_id = bulk.create_query_job(object_name = object_name)
            
            batch_id = bulk.query(job_id = job_id, soql = query)
            
            bulk.close_job(job_id)
            
            while not bulk.is_batch_done(batch_id):
                sleep(1)
            
            results = bulk.get_all_results_for_query_batch(batch_id)
            
            for result in results:
                df = pd.read_csv(result)
                break
            
            return df
        
        dfs = []
        for query, obj in zip(query_list, object_list):
            df = Run_SF_Query(query, obj)
            print('-------------------------------------------')
            print('%s object has been queried.' % obj)
            print('-------------------------------------------')
            print()
            
            df.drop_duplicates(inplace = True)
            dfs.append(df)
        
        return dfs