import time

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

from tiled.client import from_profile

BEAMLINE_OR_ENDSTATION = "arpes"




@task(retries=2, retry_delay_seconds=10)
def export_metadata_task(uid, beamline_acronym=BEAMLINE_OR_ENDSTATION):
    logger = get_run_logger()

    api_key = Secret.load(f"tiled-{beamline_acronym}-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2", api_key=api_key)
    run_client = tiled_client[beamline_acronym]["migration"][uid]
    logger.info(f"Exporting metadata for uid {uid}")
    start_time = time.monotonic()

    # Export metadata

    elapsed_time = time.monotonic() - start_time
    logger.info(f"Finished exporting metadata; {elapsed_time = }")

@flow(log_prints=True)
def metadata_export_flow(uid):
    export_metadata_task(uid)