


# import library
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def run_etl():
	for i in range(2):
	    f = open("myfile"+str(i)+".txt", "x")
	    f.write("Woops! I have deleted the content!")
	    f.close()




args = {
    'owner': 'Airflow',
    'start_date': dt.datetime(2020, 7, 29, 0, 0, 0)
}

dag = DAG(
    dag_id='datapipeflask',
    default_args=args,
    schedule_interval='0 5 * * *'
)


activation = PythonOperator(task_id='datapipe_etl', python_callable=run_etl, dag=dag)

activation

