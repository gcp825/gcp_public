import datetime as dt
from typing import Iterable

from airflow import models
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowException

#################################################################################
# Custom Operator example 1
# Basic override of __init__ only to alter behaviour 
#################################################################################

class CustomBigQueryOperator(BigQueryOperator):
    
    def __init__(self,*args,**kwargs):
        
        super().__init__(*args,**kwargs)
        
        if (self.destination_dataset_table is not None
        and self.sql is not None
        and self.create_disposition.upper() == 'CREATE_NEVER'
        and self.write_disposition.upper() == 'WRITE_TRUNCATE'):
            
            tab = '`' + self.destination_dataset_table + '`'
            self.sql = ['TRUNCATE TABLE ' + tab] + ['INSERT INTO ' + tab + ' ' + self.sql]
            self.destination_dataset_table = self.create_disposition = self.write_disposition = None

#################################################################################
# Custom Operator example 2
# Override of execute to force different behaviour 
# Copy and adapt of the standard BigQueryOperator execute function
# Don't really like this - too much risk of breakage if any of the underlying
#   code used by the execute changes too much
#################################################################################            

class AnotherCustomBigQueryOperator(BigQueryOperator):
    
    def __init__(self,*args,**kwargs):
        
        super().__init__(*args,**kwargs)
        
        if (self.destination_dataset_table is not None
        and self.sql is not None
        and self.create_disposition.upper() == 'CREATE_NEVER'
        and self.write_disposition.upper() == 'WRITE_TRUNCATE'):
            
            self.sql = ['TRUNCATE TABLE ' + '`' + self.destination_dataset_table + '`'] + [self.sql]
            self.write_disposition = 'WRITE_EMPTY'
            
    def execute(self,context):
       
       if self.bq_cursor is None:
           self.log.info('Executing: %s', self.sql)
           hook = BigQueryHook(
               bigquery_conn_id=self.bigquery_conn_id,
               use_legacy_sql=self.use_legacy_sql,
               delegate_to=self.delegate_to,
               location=self.location,
           )
           conn = hook.get_conn()
           self.bq_cursor = conn.cursor()
           
       if isinstance(self.sql, str):
           job_id = self.bq_cursor.run_query(
               sql=self.sql,
               destination_dataset_table=(None if self.sql[0:8].upper() == 'TRUNCATE' else self.destination_dataset_table),
               write_disposition=self.write_disposition,
               allow_large_results=self.allow_large_results,
               flatten_results=self.flatten_results,
               udf_config=self.udf_config,
               maximum_billing_tier=self.maximum_billing_tier,
               maximum_bytes_billed=self.maximum_bytes_billed,
               create_disposition=('CREATE_IF_NEEDED' if self.sql[0:8].upper() == 'TRUNCATE' else self.create_disposition),
               query_params=self.query_params,
               labels=self.labels,
               schema_update_options=self.schema_update_options,
               priority=self.priority,
               time_partitioning=self.time_partitioning,
               api_resource_configs=self.api_resource_configs,
               cluster_fields=self.cluster_fields,
               encryption_configuration=self.encryption_configuration
           )
       elif isinstance(self.sql, Iterable):
           job_id = [
               self.bq_cursor.run_query(
                   sql=s,
                   destination_dataset_table=(None if s[0:8].upper() == 'TRUNCATE' else self.destination_dataset_table),
                   write_disposition=self.write_disposition,
                   allow_large_results=self.allow_large_results,
                   flatten_results=self.flatten_results,
                   udf_config=self.udf_config,
                   maximum_billing_tier=self.maximum_billing_tier,
                   maximum_bytes_billed=self.maximum_bytes_billed,
                   create_disposition=('CREATE_IF_NEEDED' if s[0:8].upper() == 'TRUNCATE' else self.create_disposition),
                   query_params=self.query_params,
                   labels=self.labels,
                   schema_update_options=self.schema_update_options,
                   priority=self.priority,
                   time_partitioning=self.time_partitioning,
                   api_resource_configs=self.api_resource_configs,
                   cluster_fields=self.cluster_fields,
                   encryption_configuration=self.encryption_configuration
               )
               for s in self.sql]
       else:
           raise AirflowException(
               "argument 'sql' of type {} is neither a string nor an iterable".format(type(str)))
        
       context['task_instance'].xcom_push(key='job_id', value=job_id)
       
#################################################################################
# Custom Operator example 3 - Stack multiple executes in one 
# It works, but the logging does not capture everything you've run
# Best to avoid this! Including for reference / an example of what not to do!
#################################################################################

class DodgyCustomBigQueryOperator(BigQueryOperator):
    
    def __init__(self,*args,**kwargs):
        
        super().__init__(*args,**kwargs)
        self.truncate = False
        self.stash = (self.destination_dataset_table, self.sql)
        
        if (self.destination_dataset_table is not None
        and self.sql is not None
        and self.create_disposition.upper() == 'CREATE_NEVER'
        and self.write_disposition.upper() == 'WRITE_TRUNCATE'):        
            
            self.truncate = True
            self.sql = 'TRUNCATE TABLE `' + self.destination_dataset_table + '`'
            self.destination_dataset_table = None
            self.write_disposition = 'WRITE_EMPTY'
            
    def execute(self,context):
        
        if self.truncate is True:
            super().execute(context)
            self.destination_dataset_table, self.sql = self.stash

        super().execute(context)
        
#################################################################################  
# SubDag example - combines truncate and load as 2 seperate steps
#################################################################################

def Load_Subdag(tgt_tab,t,s,d,c,w,l,r,args):

    subdag = models.DAG(dag_id='Skating_ELT.' + t, default_args=args, schedule_interval="@daily")
        
    s01 = BigQueryOperator(
        task_id = 'truncate_' + tgt_tab, sql = 'TRUNCATE TABLE `' + d + '`', use_legacy_sql = l, trigger_rule = r, dag=subdag)         
        
    s02 = BigQueryOperator(
        task_id = 'load_' + tgt_tab,
        sql=s,destination_dataset_table=d,create_disposition=c,write_disposition=w,use_legacy_sql=l,trigger_rule=r,dag=subdag)

    s01 >> s02  
    return subdag  

#######################################################
# Out-of-the-box Operators
#######################################################

def Import_From_File(proj,dset,tab,bucket,path,infile):

    return GoogleCloudStorageToBigQueryOperator(
        task_id = 'import_to_' + tab,
        bucket = bucket,
        source_objects = [path + '/' + infile],
        field_delimiter = ',',
        skip_leading_rows = 1,
        allow_quoted_newlines = True,
        destination_project_dataset_table = proj + '.' + dset + '.' + tab,
        create_disposition = 'CREATE_NEVER',
        write_disposition = 'WRITE_TRUNCATE',
        schema_object = 'schema/' + dset + '/' + tab + '.json',
        aurodetect = False,
        trigger_rule = 'none_failed')

def Truncate_BQ_Table(proj,dset,tgt_tab):

    return BigQueryOperator(
        task_id = 'truncate_' + tgt_tab,
        sql = 'TRUNCATE TABLE `' + proj + '.' + dset + '.' + tgt_tab + '`',
        use_legacy_sql = False,
        trigger_rule = 'none_failed')

def Call_BQ_Load_Proc(proj,dset,tgt_tab):

    return BigQueryOperator(
        task_id = 'load_' + tgt_tab + '_via_sproc',
        sql = 'CALL `' + proj + '.' + dset + '.load_' + tgt_tab + '`()',
        use_legacy_sql = False,
        trigger_rule = 'none_failed')

#################################################################################
# Combine choice of various load options into a single function 
################################################################################

def Load_Within_BQ(mode,proj,dset,tgt_tab,src_tab,src_cols='*'):

    t = 'load_' + tgt_tab + '_from_' + src_tab
#   t = 'load_' + tgt_tab + '_via_' + ('custom_op' + str(mode) if mode in (1,2,3) else 'subdag' if mode == 4 else 'std_op')
    
    d = proj + '.' + dset + '.' + tgt_tab    
    s = 'SELECT ' + src_cols + ' FROM `' + proj + '.' + dset + '.' + src_tab + '`'
  
    c = 'CREATE_NEVER';  w = 'WRITE_TRUNCATE' if mode in (1,2,3) else 'WRITE_EMPTY' ; l = False;  r = 'none_failed'
    
    if mode == 1:
        return CustomBigQueryOperator(
            task_id=t,sql=s,destination_dataset_table=d,create_disposition=c,write_disposition=w,use_legacy_sql=l,trigger_rule=r)
    
    elif mode == 2:
        return AnotherCustomBigQueryOperator(
            task_id=t,sql=s,destination_dataset_table=d,create_disposition=c,write_disposition=w,use_legacy_sql=l,trigger_rule=r)
    
    elif mode == 3:
        return DodgyCustomBigQueryOperator(
            task_id=t,sql=s,destination_dataset_table=d,create_disposition=c,write_disposition=w,use_legacy_sql=l,trigger_rule=r)
    
    elif mode == 4:
        return SubDagOperator(
            subdag = Load_Subdag(tgt_tab,t,s,d,c,w,l,r,dag.default_args), task_id=t, dag=dag)
 
    else:
        return BigQueryOperator(
            task_id=t,sql=s,destination_dataset_table=d,create_disposition=c,write_disposition=w,use_legacy_sql=l,trigger_rule=r)  
    
        
#######################################################
# Run
#######################################################        

start_dt = dt.datetime.combine(dt.datetime.today(),dt.datetime.min.time())
args  = {'start_date' : start_dt, 'project_id' : 'your-project'}

with models.DAG('Skating_ELT', schedule_interval=None, default_args=args) as dag:

    p01 = Import_From_File('your-project','skating','s_performances',
                           'your-bucket','bucket-folder','performances.csv')
                         
    p02 = Import_From_File('your-project','skating','s_judges',
                           'your-bucket','bucket-folder','judges.csv')

    p03 = Import_From_File('your-project','skating','s_judged_aspects',
                           'your-bucket','bucket-folder','judged-aspects.csv')

    p04 = Import_From_File('your-project','skating','s_judge_scores',
                           'your-bucket','bucket-folder','judge-scores.csv')
 
    p10 = Load_Within_BQ(1,'your-project','skating','d_event','s_event_v',
                           'event_id, competition_nm, program_nm, element_nm, entries_ct')

    p20 = Truncate_BQ_Table('your-project','skating','d_person')
    p25 = Load_Within_BQ(5,'your-project','skating','d_person',
                           's_person_v','person_id, forename, surname, gender, nationality')

    p30 = Call_BQ_Load_Proc('your-project','skating','d_aspect')

    p40 = Load_Within_BQ(2,'your-project','skating','f_perf','s_perf_v',
                           'perf_id, event_id, event_perf_seq_nbr, skater1_id, skater2_id, event_perf_rank_nbr, component_score, element_score, deductions')
    
    p50 = Load_Within_BQ(3,'your-project','skating','f_perf_aspect', 's_perf_aspect_v',
                           'perf_aspect_id, perf_id, perf_aspect_seq_nbr, aspect_id, base_difficulty, total_score')
                         
    p60 = Load_Within_BQ(4,'your-project','skating','f_perf_aspect_score','s_perf_aspect_score_v')
                             
    p01
    p02
    p03
    p04
    p10 << [p01,]
    p25 << p20 << [p01, p02]
    p30 << [p03,]
    p40 << [p01, p02]
    p50 << [p01, p02, p03]
    p60 << [p01, p02, p03, p04]
 
