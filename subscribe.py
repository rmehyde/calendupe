import logging
import time
from typing import Optional

from google.oauth2.service_account import Credentials
from googleapiclient import discovery

from config import *


logger = logging.getLogger("subscribe")


def subscribe_calendar(events_service: discovery.Resource, calendar_address: str, channel_id: str, receiving_url: str,
                       ttl_seconds: Optional[int] = 60):

    # build out subscription body
    subscribe_body = {
        "address": receiving_url,
        "id": channel_id,
        "payload": True,
        "token": TOKEN,
        "type": "web_hook"
    }
    if ttl_seconds is not None:
        expiry = int((time.time() + ttl_seconds) * 1000)
        subscribe_body['expiration'] = expiry

    # send request
    request = events_service.watch(calendarId=calendar_address, body=subscribe_body)
    response = request.execute()

    resource_id = response['resourceId']
    actual_expiry_ms = response['expiration']
    actual_expiry = datetime.utcfromtimestamp(int(actual_expiry_ms) / 1000)

    logger.info(f"Successfully watched resource {response['resourceId']} with expiry {actual_expiry}")
    return resource_id


def unsubscribe(channel_service: discovery.Resource, channel_id: str, watched_resource_id: str):
    channel_service.stop(body={'id': channel_id, 'resourceId': watched_resource_id}).execute()
    logger.info(f"Successfully stopped watching resource {watched_resource_id}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # load service account and create service
    creds = Credentials.from_service_account_file("service-account.json",
                                                  scopes=['https://www.googleapis.com/auth/calendar.events.readonly'])
    service = discovery.build('calendar', 'v3', credentials=creds)
    events = service.events()
    channels = service.channels()

    url = f"https://{GCP_REGION}-{GCP_PROJECT}.cloudfunctions.net/calendupe/channel"
    resource_id = subscribe_calendar(events, calendar_address=SOURCE_CALENDAR_ADDRESS,
                                     channel_id=CHANNEL_ID,
                                     receiving_url=url,
                                     # ttl_seconds=None)
                                     ttl_seconds=3690)

    # log watched resource
    with open(f"subscribe_logs/{datetime.now().strftime('%Y%m%d%H%M%S')}.txt", 'w') as f:
        f.write(f"Calendar Address: {SOURCE_CALENDAR_ADDRESS}\n")
        f.write(f"Channel ID: {CHANNEL_ID}\n")
        f.write(f"Receiving URL: {url}\n")
        f.write(f"Resource ID: {resource_id}\n")
