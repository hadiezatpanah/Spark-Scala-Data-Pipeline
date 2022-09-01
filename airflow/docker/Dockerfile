# Base Image
FROM puckel/docker-airflow

ENV AIRFLOW_HOME=/usr/local/airflow
WORKDIR $AIRFLOW_HOME
COPY start-airflow.sh $AIRFLOW_HOME/start-airflow.sh
COPY requirements.txt $AIRFLOW_HOME/requirements.txt
RUN mkdir -p $AIRFLOW_HOME/nifi
RUN pip install -r requirements.txt
COPY airflow.cfg $AIRFLOW_HOME/airflow.cfg
USER root
RUN chmod +x $AIRFLOW_HOME/start-airflow.sh
RUN chown -R airflow:airflow $AIRFLOW_HOME
USER airflow
CMD [ "./start-airflow.sh" ]