from prefect import task, flow
from data_validation import data_validation

@task
def log_completion():
    print("Complete")

@flow(log_prints=True)
def end_of_run_workflow(stop_doc):
    uid = stop_doc["run_start"]
    data_validation(uid)
    log_completion()
