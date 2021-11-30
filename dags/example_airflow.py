import requests
import json
import csv

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


# Extract data from REST API

# num_users is the number of users you can request to extract
def extract(num_users=1):
 """ Sends a HTTP request to endpoint specified and returns fake user data
     from public API
 Parameters
 ----------

 num_users: int
    The number of users you want to extract
 
 """
 url = ""
 if num_users == 1:
 	url = "https://randomuser.me/api/"
 else: 
 	url = "https://randomuser.me/api/?results={}".format(num_users)
 res = requests.get(url)
 with open('user.json', 'w') as outfile:
 	json.dump(res.json(), outfile)

def transform():
 """ Reading in json file and then transforming data """
 results = []
 with open('user.json', 'r') as outfile:
   data = json.load(outfile)
 for i in data["results"]:
  x = []
  x.append(i["gender"])
  x.append(i["name"]["first"])
  x.append(i["name"]["last"])
  x.append(i["email"])
  x.append(i["location"]["country"])
  results.append(x)
  #return results
 with open('user_results.csv', 'w') as f:
 	writer = csv.writer(f)
 	writer.writerow(["Gender", "First_Name", "Last_Name", "email", "country"])
 	writer.writerows(results)



default_args = {
        "owner": 'airflow',
        'depends_on_past': False,
        }

with DAG('User_ETL', default_args=default_args,
        description="Extracting fake user data", schedule_interval = timedelta(days=1),
        start_date=datetime(2020,1,1),
        catchup=False,
        tags=['Users']
) as dags:
	task_1 = PythonOperator(
	 task_id="Extract",
	 python_callable=extract,
	 op_kwargs={'num_users': 1000})
	task_2 = PythonOperator(task_id="Transform", python_callable=transform)

task_1 >> task_2
 

 
