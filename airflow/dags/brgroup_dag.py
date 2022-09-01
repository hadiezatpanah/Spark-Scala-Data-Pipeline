"""BRGroup Dag!
Documenting Formatting Type:  Epytext
This script create a dag in airflow in order to runnig nifi and Spark Application

This script requires that `nipyapi` be installed within the Python
environment you are running this script in.

"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin # for logging in airflow
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
import nipyapi # nifi API
from nipyapi.nifi import ProcessorConfigDTO
from time import sleep
import requests
import json


# sshHook = SSHHook(ssh_conn_id='ssh_spark_submit')
sshHook = SSHHook(remote_host='spark_submit', username='root', password='root')


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 7, 14),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "hadi.ezatpanah@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup" : False
}

# nifi Url
nifi_url = 'http://nifi:8080/nifi/'
# required url's for nifi API
nipyapi.config.nifi_config.host = 'http://nifi:8080/nifi-api'
nipyapi.config.registry_config.host = 'http://nifi:18080/nifi-registry-api'
nifi_resources_api = 'http://nifi:8080/nifi-api/resources'

nifi_file_path = 'nifi/brgroup.xml'

def nifi_status(nifi_url):
    """checking nifi server and APIs.

        Parameters
        ----------
        @type nifi_urls: str
        @param nifi_urls: Base url and port for nifi server.
        Raises
        ------
        Not handled.
        @returns : Nothing
        """
    # we have to wait until Nifi is running.
    # waiting for Nifi server
    
    LoggingMixin().log.info(f"checking Nifi server")
    status = 0
    while(status == 0):
        try:
            res = requests.get(nifi_url)
            status = res.status_code
        except requests.exceptions.ConnectionError:
            LoggingMixin().log.warning(f"Nifi url {nifi_url} is not reachable")
        sleep(2)

    LoggingMixin().log.info(f"checking Nifi flow API")

    # waiting for Nifi API and Nifi flow.
    status = 0
    while(status != 200):
        try:
            res = nipyapi.system.get_nifi_version_info()
            status = 200
        except:
            LoggingMixin().log.warning(f"Nifi API is not reachable")
        sleep(2)
        
    LoggingMixin().log.info(f"Nifi is up and running")


def setup_nifi(template_file):
    """
    importing template file and running the processors.

        Parameters
        ----------
        @type template_file: str
        @param template_file: path to the nifi template file.
        Raises
        ------
        Not handled.
        return : Nothing
        """
    #  check if template is already exist
    response = requests.get(nifi_resources_api)
    jsonObject = json.loads(response.text)

    jsondata = jsonObject["resources"]
    uploaded = False
    for i in jsondata:
        if(i['name'] == 'brgroup'):
            uploaded = True
            LoggingMixin().log.info(f"Nifi template has already been uploaded")

            
    root_pg_id = nipyapi.canvas.get_root_pg_id()
    #uploading template
    if not uploaded:
        res = nipyapi.templates.upload_template(root_pg_id, template_file)
        LoggingMixin().log.info(f"Nifi template has been uploaded")

        #importing template
        templateId = res.id
        nipyapi.templates.deploy_template(pg_id=root_pg_id, template_id= templateId, loc_x=0.0, loc_y=0.0)
        LoggingMixin().log.info(f"Nifi template has been imported")
        get_ftp_proccesor = nipyapi.canvas.get_processor("GetFTP", identifier_type='name', greedy=True)
        sftp_proc_conf = ProcessorConfigDTO (
            properties = dict(Username= "test", Password="test")
        )

        nipyapi.canvas.update_processor(get_ftp_proccesor, sftp_proc_conf, False)

    # starting all Nifi processors.
    LoggingMixin().log.info(f"starting processors")
    allProcessors = nipyapi.canvas.list_all_processors(pg_id=root_pg_id)
    for processor in allProcessors:
        nipyapi.canvas.schedule_processor(processor, True, refresh=True)
        sleep(1)
    
    LoggingMixin().log.info(f"Nifi template is working now!")



# defining 3-step Dag.
with DAG(dag_id="brgroup_etl",
         schedule_interval= None, # trigger task in airflow UI. 
         default_args=default_args,
         template_searchpath=[f"{os.environ['AIRFLOW_HOME']}"],
         catchup=False) as dag:

    is_nifi_available = PythonOperator(
        task_id = "waiting_for_nifi",
        python_callable = nifi_status,
        op_kwargs = {"nifi_url": nifi_url}
    )

    import_nifi_tempalte_and_start = PythonOperator(
        task_id = "setup_nifi",
        python_callable = setup_nifi,
        op_kwargs = {"template_file": nifi_file_path}
    )

    execute_spark_app = SSHOperator(task_id="runing_spark_application",
                         command='sh /opt/bitnami/spark/work/spark-submit.sh ',
                         ssh_hook=sshHook)


    # checking if Nifi is up and then importing and running the flow file.
    is_nifi_available >> import_nifi_tempalte_and_start >> execute_spark_app
    