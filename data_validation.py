import time
import os

from prefect import flow, task, get_run_logger

from bluesky_tiled_plugins.writing.validator import validate
from tiled.client import from_uri
from dotenv import load_dotenv


def get_api_key_from_env(api_key=None):
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task
def get_run(uid, beamline_acronym="arpes", api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl[f"{beamline_acronym}/migration"][uid] # TODO change to raw after migration is complete
    return run


@task(retries=3, retry_delay_seconds=20)
def data_validation_task(uid, beamline_acronym="arpes", api_key=None):
    logger = get_run_logger()

    logger.info(f"Connecting to Tiled client for beamline '{beamline_acronym}'")
    run_client = get_run(uid, beamline_acronym=beamline_acronym, api_key=api_key)

    logger.info(f"Validating uid {uid}")
    start_time = time.monotonic()
    validate(run_client, fix_errors=True, try_reading=True, raise_on_error=True)
    elapsed_time = time.monotonic() - start_time
    logger.info(f"Finished validating data; {elapsed_time = }")


@flow(log_prints=True)
def data_validation_flow(uid, api_key=None):
    data_validation_task(uid, api_key=api_key)
