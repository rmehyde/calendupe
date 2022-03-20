import json
from typing import Optional
import threading
import logging
from datetime import datetime, timedelta

import flask
from dateutil import parser

import functions_framework
from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2
from googleapiclient import discovery
from google.api_core.exceptions import NotFound
from googleapiclient.errors import HttpError

import config
import gcs
import update
import subscribe


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("calendupe")


LOCK_BLOB_NAME = "calendupe_lock"
NEXT_TOKEN_BLOB_NAME = "next_sync_token"

CALENDUPE_URL = f"https://{config.GCP_REGION}-{config.GCP_PROJECT}.cloudfunctions.net/calendupe"

MAX_CLOUD_TASK_FUTURE = timedelta(days=29, hours=23)


def get_stored_sync_token() -> Optional[str]:
    logger.info("fetching sync token")
    try:
        return gcs.read_text(config.GCS_DATA_BUCKET, NEXT_TOKEN_BLOB_NAME)
    except NotFound:
        return None


def store_next_sync_token(next_sync_token: str) -> None:
    logger.info("storing next sync token")
    gcs.upload_text(config.GCS_DATA_BUCKET, NEXT_TOKEN_BLOB_NAME, next_sync_token)


def perform_update():
    """
    Primary cloud entrypoint. Acquires the global lock and performs any updates needed, since the last sync token if
    one is available.
    """
    events_service = discovery.build('calendar', 'v3').events()

    # acquire lock
    lock = gcs.GCSLock(config.GCS_LOCK_BUCKET, LOCK_BLOB_NAME)
    logger.info("acquiring lock")
    lock.acquire()
    logger.info("lock acquired")
    try:
        sync_token = get_stored_sync_token()
        config.MIN_END_TIME = config.MIN_END_TIME if sync_token is None else None
        next_sync_token = update.duplicate_events(events_service,
                                                  config.SOURCE_CALENDAR_ADDRESS, config.TARGET_CALENDAR_ADDRESS,
                                                  sync_token, min_end_time=config.MIN_END_TIME)
        store_next_sync_token(next_sync_token)

    # release lock
    finally:
        logger.info("releasing lock")
        lock.release()
        logger.info("lock released")


def perform_resubscribe(watched_resource_id: str):
    """
    Unsubscribe and resubscribe calendupe from the source calendar to refresh the expiration. Note that updates will be
    missed for a brief period between unsubscribing and resubscribing
    :param watched_resource_id: the resourceId for the calendar returned from the initial watch
    """
    service = discovery.build('calendar', 'v3')
    channel_service = service.channels()
    events_service = service.events()
    try:
        subscribe.unsubscribe(channel_service, config.CHANNEL_ID, watched_resource_id)
    except HttpError:
        logger.info(f"failed to unsubscribe channel, continuing")
    url = CALENDUPE_URL + "/channel"
    subscribe.subscribe_calendar(events_service, config.SOURCE_CALENDAR_ADDRESS, config.CHANNEL_ID, url,
                                 ttl_seconds=None)


def create_resubscribe_task(watched_resource_id: str, schedule_time: datetime):
    """
    Create a Cloud Task to resubscribe to the calendar.
    :param watched_resource_id: resource ID from the watch
    :param schedule_time: when to schedule the task for
    """
    tasks_client = tasks_v2.CloudTasksClient()
    queue_path = tasks_client.queue_path(config.GCP_PROJECT, config.GCP_REGION, config.TASKS_QUEUE_NAME)
    body = {
        "watched_resource_id": watched_resource_id
    }
    request = {
        "http_method": tasks_v2.HttpMethod.PATCH,
        "url": CALENDUPE_URL + "/subscription",
        "headers": {
            "Content-type": "application/json",
            "X-Goog-Channel-Token": config.TOKEN
        },
        "body": json.dumps(body).encode('utf-8')
    }
    task = {
        "http_request": request,
    }
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(schedule_time)
    task['schedule_time'] = timestamp
    response = tasks_client.create_task(request={"parent": queue_path, "task": task})
    logger.info(f"created resubscription task {response.name} scheduled for {response.schedule_time}")


def schedule_resubscribe(expiration: datetime, watched_resource_id: str):
    """
    Schedule a Cloud Task to resubscribe to the calendar 1 hour before the channel expiration.
    :param expiration: channel expiration
    :param watched_resource_id: resource ID from the watch
    """
    schedule_time = expiration - timedelta(hours=1)
    if schedule_time <= datetime.now(tz=schedule_time.tzinfo):
        logger.error("cannot schedule resubscription in the past!")
    max_time = datetime.now(tz=schedule_time.tzinfo) + MAX_CLOUD_TASK_FUTURE
    create_resubscribe_task(watched_resource_id, min(schedule_time, max_time))


@functions_framework.http
def calendupe(request: flask.Request) -> flask.Response:
    """
    Cloud Function request handler. Parses a push notification from the Calendar API and performs an update. Responds
    immediately with a 401 if the correct token is not provided, or a 202 otherwise. Asynchronously begins working on
    update if the resource state is 'exists'
    """
    # auth
    channel_token = request.headers.get("X-Goog-Channel-Token", None)
    if channel_token == config.TOKEN:
        logger.info("authorization succeeded")
    else:
        logger.warning("unauthorized request")
        return flask.Response(response="unauthorized", status=401)

    # parse path
    path_parts = request.path.strip("/").split("/")
    if len(path_parts) != 1:
        logger.warning(f"'{request.path}' not found")
        return flask.Response(response=f"'{request.path}' not found", status=404)
    path = path_parts[0]

    if path == "subscription":
        if request.json is None or isinstance(request.json, list) or 'watched_resource_id' not in request.json.keys():
            logger.warning(f"bad request body '{request.data}'")
            return flask.Response(response="bad request", status=400)
        perform_resubscribe(request.json['watched_resource_id'])

    elif path == "channel":
        # handle channel
        resource_id = request.headers.get("X-Goog-Resource-ID", None)
        resource_state = request.headers.get("X-Goog-Resource-State", None)
        channel_id = request.headers.get("X-Goog-Channel-ID", None)
        expiry_string = request.headers.get("X-Goog-Channel-Expiration", None)
        expiry = None if expiry_string is None else parser.parse(expiry_string)

        if resource_state == "sync":
            logger.info(f"New subscription channel '{channel_id}' created. Expires {expiry_string}")
            if expiry is not None:
                schedule_resubscribe(expiry, resource_id)
        elif resource_state == "not_exists":
            logger.info("Resource does not exist!")
        elif resource_state == "exists":
            thread = threading.Thread(target=perform_update, name="calendupe-update-thread")
            thread.start()
        else:
            logger.info(f"Resource state was {resource_state}, doing nothing")

    else:
        logger.warning(f"'{request.path}' not found")
        return flask.Response(response=f"'{request.path}' not found", status=404)

    return flask.Response(response="ok", status=202)
