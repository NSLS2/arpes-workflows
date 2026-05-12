import traceback

from prefect import task, flow, get_run_logger
from prefect.blocks.notifications import SlackWebhook
from prefect.context import FlowRunContext
from prefect.settings import PREFECT_UI_URL

from data_validation import data_validation_flow, get_run, get_api_key_from_env
from metadata_exporter import metadata_export_flow

CATALOG_NAME = "arpes"


def slack(func):
    """
    Send a message to mon-prefect and mon-prefect-est slack channels if the flow-run failed.
    Send a message to mon-prefect-arpse slack channel with the flow-run status.
    Send a message to mon-bluesky slack channel if the bluesky-run failed.

    NOTE: the name of this inner function is the same as the real end_of_workflow() function because
    when the decorator is used, Prefect sees the name of this inner function as the name of
    the flow. To keep the naming of workflows consistent, the name of this inner function had to match the expected name.
    """

    def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
        flow_run_name = FlowRunContext.get().flow_run.dict().get("name")

        # Load slack credentials that are saved in Prefect.
        mon_prefect = SlackWebhook.load("mon-prefect")
        mon_bluesky = SlackWebhook.load("mon-bluesky")
        mon_prefect_arpes = SlackWebhook.load("mon-prefect-arpes")
        mon_prefect_est = SlackWebhook.load("mon-prefect-est")

        # Get the uid.
        uid = stop_doc["run_start"]

        # Get Tiled API key, if not set already
        if not api_key:
            api_key = get_api_key_from_env()

        # Get the scan_id.
        run = get_run(uid, api_key=api_key)
        scan_id = run.start["scan_id"]

        # Send a message to mon-bluesky if bluesky-run failed.
        if stop_doc.get("exit_status") == "fail":
            mon_bluesky.notify(
                f":bangbang: {CATALOG_NAME} bluesky-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```reason: {stop_doc.get('reason', 'none')}```"
            )

        try:
            result = func(stop_doc, api_key=api_key, dry_run=dry_run)

            # Send a message to mon-prefect-arpes if flow-run is successful.
            message = f":white_check_mark: {CATALOG_NAME} flow-run successful. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}```"
            mon_prefect_arpes.notify(message)
            return result
        except Exception as e:
            tb = traceback.format_exception_only(e)

            # Send a message to mon-prefect-arpes, mon-prefect if flow-run failed.
            message = f":bangbang: {CATALOG_NAME} flow-run failed. (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            mon_prefect.notify(message)
            mon_prefect_arpes.notify(message)
            flow_run = FlowRunContext.get().flow_run
            # Add link to flow-run for the message to mon-prefect-est.
            program_message = (
                f":bangbang: {CATALOG_NAME} flow-run failed. <https://{PREFECT_UI_URL.value()}/flow-runs/"
                + f"flow-run/{flow_run.id}|the flow run link> (*{flow_run_name}*)\n ```run_start: {uid}\nscan_id: {scan_id}``` ```{tb[-1]}```"
            )
            mon_prefect_est.notify(program_message)
            raise

    return end_of_run_workflow


@task
def log_completion():
    logger = get_run_logger()
    logger.info("Complete")


@flow(log_prints=True)
@slack
def end_of_run_workflow(stop_doc, api_key=None, dry_run=False):
    uid = stop_doc["run_start"]
    data_validation_flow(uid, api_key=api_key)
    metadata_export_flow(uid, api_key=api_key, dry_run=dry_run)
    log_completion()
