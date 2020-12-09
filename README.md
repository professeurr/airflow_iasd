# ML Pipelining using Airflow

The main goal of this project is to implement an end-to-end machine learning pipeline using Airflow in following steps:
* download dataset from AWS S3 bucket
* training a model using keras framework
* serialize the trained model in pickel format
* upload the trained model into AWS S3 bucket.

# Work to do
* implement the above workflow code (in python) on local machine
* push the code into Github
* synchronize the code on the remote machine (e.g AWS EC2) with cron job (running on remote machine)
* run the task every hours within Airflow (scheduler) push the result (trained model) into AWS S3

# Implementation status
All steps above are completed.
* `dags/model.py` contains te ML model work
* `dags/petroleum.py` contains Airflow DAGS pipeline implementation
* EC2 machine is created and syncs with this Github repo.
* S3 is populated with the model dump


Reference: https://github.com/faouzelfassi/pipelining/blob/master/README.md
