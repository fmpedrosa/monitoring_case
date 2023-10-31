import pandas as pd
import pickle
from adtk.data import validate_series
import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)



class ModelProcessorOperator:

    ''' Class variables'''

    '''All statuses'''
    all_statuses = ['reversed', 'approved', 'processing', 'denied', 'backend_reversed', 'refunded', 'failed']

    '''Loading the model dictionaries'''
    local_data = pd.DataFrame()
    model_list = os.listdir('models')
    model_dictionary = []
    for model in model_list:
        with open(f'models/{model}', 'rb') as f:
            entry = {"model_name":model.split(".")[0],
                    "model_object": pickle.load(f),
                    "variable": model.split(".")[0].split("-", maxsplit=1)[0],
                    "type":model.split(".")[0].split("-", maxsplit=1)[1] }
            model_dictionary.append(entry)

    def __init__(self, data_entries, upload_to_bq: False):
        ''' Data entries '''
        self.data_entries = pd.DataFrame(data_entries)

        ''' Upload to bq parameter, default: False'''
        self.upload_to_bq = upload_to_bq


    def upload_data_entries(self):

        ''' 
        This function uploads the entry received from the instance to the transactions_raw.table
        
        It also stores the entry to an local_data class variable, so we can check for historical anomalies without requesting the server
        '''
        self.data_entries['time'] =  pd.to_datetime(self.data_entries['time'] , format = "%Hh %M")#.dt.time 
        ModelProcessorOperator.local_data = ModelProcessorOperator.local_data.append(self.data_entries)

        if self.upload_to_bq == True:
            self.data_entries.to_gbq('monitoring-case.transactions.transactions_raw', if_exists = 'append')

    @classmethod
    def process_local_dataframe(cls):
        
        ''' 
        This function transforms the local_data to a pivoted table, to facilitate calculating the KPI's
        '''
        cls.df_pivoted = cls.local_data.pivot_table(columns='status', values= 'count', index= 'time', fill_value=0, aggfunc= 'sum', margins = True, margins_name= 'totals')

        '''Thats a simple check, to force creating a column for every status in the all_status list in the pivoted table'''
        for status in ModelProcessorOperator.all_statuses:
            if status not in cls.df_pivoted.columns:
                 cls.df_pivoted[status] = 0
        '''Calculating KPIs'''
        cls.df_pivoted['percentage_failed'] =  (cls.df_pivoted['failed']) / cls.df_pivoted['totals']
        cls.df_pivoted['percentage_denied'] =  (cls.df_pivoted['denied']) / cls.df_pivoted['totals']
        cls.df_pivoted['percentage_reversed'] = (cls.df_pivoted['reversed']) / cls.df_pivoted['totals']
        cls.df_pivoted['sucess_rate'] = cls.df_pivoted['approved'] / cls.df_pivoted['totals']
        return cls.df_pivoted

    @classmethod
    def run_single_anomaly_model(cls, entry_model_dictionary):
        ''' 
        This function receives an entry in the model_dictionary and returns a dataframe with anomalies
        '''

        '''To apply the model we must create a dataframe with the timestamp as an index and our variable as a single column'''

        data_series = pd.DataFrame(index = list( cls.df_pivoted.index.drop('totals')), #Droping the toals
                                columns= [entry_model_dictionary.get('variable')], # Getting the corresponding variable to name the column
                                data=list(cls.df_pivoted[entry_model_dictionary.get('variable')].values[0:-1])) # Getting the corresponding variable values 
        '''Validating series'''
        data_series = validate_series(data_series)
        '''Getting the model from the dictionary and using to detect the anomalies in the data'''
        anomalies = entry_model_dictionary.get('model_object').detect(data_series)

        '''Making some adjustements to create the anomalies dataframe'''
        anomalies_true = anomalies.loc[anomalies[entry_model_dictionary.get('variable')] == True].copy()
        anomalies_true['model_name'] = entry_model_dictionary.get('model_name')
        anomalies_true['model_type'] = entry_model_dictionary.get('type')
        anomalies_true = anomalies_true.reset_index().rename(columns = {'index': 'time', entry_model_dictionary.get('variable'): 'status'})
        return anomalies_true

    @classmethod
    def run_all_anomalies(cls):
        '''This function loops through all entries in the models dictionary,
        Running the model -> Appending the data the anomalies dataframe.
        '''
        cls.all_anomalies = pd.DataFrame()
        for model in cls.model_dictionary:
            anomalies = ModelProcessorOperator.run_single_anomaly_model(model)
            cls.all_anomalies = pd.concat([cls.all_anomalies, anomalies], axis=0)

    def upload_anomalies(self):
        '''This function uploads the newly detected anomalies to the monitoring-case.transactions.anomalies_raw table'''
        if self.upload_to_bq == True:
            ModelProcessorOperator.all_anomalies['time'] =  pd.to_datetime(ModelProcessorOperator.all_anomalies['time'], format = "%Hh %M")
            ModelProcessorOperator.all_anomalies['status'] = ModelProcessorOperator.all_anomalies['status'].astype('bool')
            ModelProcessorOperator.all_anomalies.loc[ModelProcessorOperator.all_anomalies['time'] >= min(self.data_entries['time'])].to_gbq('monitoring-case.transactions.anomalies_raw', if_exists = 'append')

    
    def orquestrator(self):
        '''This function orquestrates all processes'''
        print(f"Uploading entries from time {min(self.data_entries['time'])}")
        self.upload_data_entries()
        ModelProcessorOperator.process_local_dataframe()
        ModelProcessorOperator.run_all_anomalies()
        list(ModelProcessorOperator.all_anomalies.model_name)
        print(f"Detected anomalies: {list(ModelProcessorOperator.all_anomalies.loc[ModelProcessorOperator.all_anomalies['time'] >= min(self.data_entries['time'])].model_name.unique())}")
        self.upload_anomalies()

        if self.upload_to_bq == False:
            ModelProcessorOperator.all_anomalies.to_csv('output_files/all_anomalies.csv')
            ModelProcessorOperator.local_data.to_csv('output_files/all_transactions.csv')

        
    

