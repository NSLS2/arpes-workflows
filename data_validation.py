import os
import time as ttime

from prefect import flow, task
from tiled.client import from_profile

BEAMLINE_OR_ENDSTATION = "!!! Set the endstation or beamline_TLA here !!!"


@task(retries=2, retry_delay_seconds=10)
def read_all_streams(uid, beamline_acronym=BEAMLINE_OR_ENDSTATION):
    tiled_client = from_profile("nsls2")
    run = tiled_client[beamline_acronym]["raw"][uid]
    print(f"Validating uid {run.metadata['start']['uid']}")
    start_time = ttime.monotonic()
    for stream in run:
        print(f"{stream}:")
        stream_start_time = ttime.monotonic()
        stream_data = run[stream].read()
        stream_elapsed_time = ttime.monotonic() - stream_start_time
        print(f"{stream} elapsed_time = {stream_elapsed_time}")
        print(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    print(f"{elapsed_time = }")


@flow(log_prints=True)
def data_validation(uid):
    read_all_streams(uid)
