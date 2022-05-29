import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from googleapiclient import discovery
from googleapiclient.errors import HttpError


logger = logging.getLogger("calendupe.update")


MAX_PAGES = 500
DEFAULT_TARGET_EVENT_NAME = "busy (personal)"
DEFAULT_TARGET_EVENT_DESCRIPTION = """created by <a href="https://github.com/rmehyde/calendupe">calendupe</a>"""
CREATED_BY_KEY = "createdBy"
CALENDUPE = "calendupe"
SOURCE_EVENT_KEY = "sourceEventId"


def list_events(events_service: discovery.Resource, calendar_address: str,
                sync_token: Optional[str] = None, min_end_time: Optional[datetime] = None,
                only_calendupe_events=False) -> Tuple[List[Dict[str, Any]], str]:
    """List all matching calendar events
    
    :param events_service: a Google Calendar Events Python Service
    :param calendar_address: address of the calendar to list events for, e.g. 'primary' or 'you@your.domain'
    :param sync_token: a sync token to use, to fetch changes since the previous token
    :param min_end_time: the minimum time (of event end) of events to match
    :param only_calendupe_events: if True, only list events created by Calendupe
    :raises HttpError: if the request executes and gets back a bad response
    """
    if min_end_time is not None and min_end_time.tzinfo is None:
        raise ValueError("'min_end_time' parameter must be timezone-aware")

    # construct args
    kwargs = {'calendarId': calendar_address}
    if min_end_time is not None:
        kwargs['timeMin'] = min_end_time.isoformat()
    if sync_token is not None:
        kwargs['syncToken'] = sync_token
    if only_calendupe_events:
        kwargs['privateExtendedProperty'] = f"{CREATED_BY_KEY}={CALENDUPE}"

    logger.info(f"fetching events from {calendar_address}")
    # first request
    response = events_service.list(**kwargs).execute()
    events = response['items']
    total_pages = 1
    # subsequent pages
    while 'nextPageToken' in response.keys() and total_pages <= MAX_PAGES:
        kwargs['pageToken'] = response['nextPageToken']
        response = events_service.list(**kwargs).execute()
        events.extend(response['items'])
        total_pages += 1
    logger.info(f"fetched {len(events)} events from calendar across {total_pages} pages")
    # return
    next_sync_token = response['nextSyncToken']
    return events, next_sync_token


def create_event(events_service: discovery.Resource, calendar_address: str, event: Dict[str, Any]):
    events_service.insert(calendarId=calendar_address, body=event).execute()


def update_event(events_service: discovery.Resource, calendar_address: str, event: Dict[str, Any]):
    events_service.patch(calendarId=calendar_address, eventId=event['id'], body=event).execute()


def delete_all_calendupe_events(events_service: discovery.Resource, calendar_address: str):
    calendupe_events, _ = list_events(events_service, calendar_address=calendar_address, only_calendupe_events=True)
    logging.info(f"removing {len(calendupe_events)} calendupe events from {calendar_address}")
    deleted = 0
    for event in calendupe_events:
        if event['status'] != "cancelled":
            events_service.delete(calendarId=calendar_address, eventId=event['id']).execute()
            deleted += 1
    logging.info(f"successfully removed {deleted} calendupe events from {calendar_address}")


def source_event_to_target_event(source_event: Dict[str, Any], title=DEFAULT_TARGET_EVENT_NAME,
                                 description=DEFAULT_TARGET_EVENT_DESCRIPTION) -> Dict[str, Any]:
    """Convert an event data for an event on the source calendar into obfuscated event data to be added to the target
    calendar.

    :param source_event: the event data from the source calendar
    :param title: title/summary of the event to return. defaults to "busy (personal)"
    :param description: description of the event to return. defaults to "created by calendupe"
    """
    target_event = {'status': source_event.get('status', "confirmed")}
    if target_event['status'] != "cancelled":
        target_event['start'] = source_event['start']
        target_event['end'] = source_event['end']
        target_event['summary'] = title
        target_event['description'] = description
        target_event['reminders'] = {'useDefault': False, 'overrides': []}
    if 'recurrence' in source_event.keys():
        target_event['recurrence'] = source_event['recurrence']

    extended_properties = {
        CREATED_BY_KEY: CALENDUPE,
        SOURCE_EVENT_KEY: source_event['id']
    }
    target_event['extendedProperties'] = {'private': extended_properties}
    return target_event


def fetch_target_event(events_service: discovery.Resource,
                       calendar_address: str, source_event_id: str) -> Optional[Dict[str, Any]]:
    """Given event ID on the source calendar, retrieve an associated event on the target calendar.

    :param events_service: a Google Calendar Events Python Service
    :param calendar_address: address of the target calendar, e.g. 'primary' or 'you@your.domain'
    :param source_event_id: event ID of the event on the source calendar
    """
    request = events_service.list(calendarId=calendar_address, showDeleted=True,
                                  # would be nice to have the createdBy param too, but can't figure how to pass multiple
                                  privateExtendedProperty=f"{SOURCE_EVENT_KEY}={source_event_id}")
    matches = request.execute()['items']
    if len(matches) == 0:
        return None
    elif len(matches) > 1:
        logger.warning("Found multiple matching target events!! Using first...")

    return matches[0]


def duplicate_events(events_service: discovery.Resource, source_calendar_address: str, target_calendar_address: str,
                     sync_token: Optional[str] = None, min_end_time: Optional[datetime] = None,
                     allow_same_calendar=False) -> str:
    """
    Duplicate events from source to target calendar.

    :param events_service: a Google Calendar Events Python Service
    :param source_calendar_address: address of the calendar to copy events from, e.g. "primary" or "you@your.domain"
    :param target_calendar_address: address of the calendar to copy events into, e.g. "primary" or "you@your.domain"
    :param sync_token: only copy events updated since the sync token indicates
    :param min_end_time: only copy events with end times greater than this
    :param allow_same_calendar: allow source and target calendars to be the same. useful for debugging and local
    testing, prevents infinite loop in production setups
    """
    if source_calendar_address == target_calendar_address and not allow_same_calendar:
        raise ValueError(f"Cannot duplicate events to same calendar unless allow_same_calendar is True")

    try:
        events, next_sync_token = list_events(events_service, source_calendar_address, sync_token,
                                              min_end_time=min_end_time)
    except HttpError as e:
        # this indicates the sync token has been invalided by server (e.g. expired)
        if e.status_code == 410:
            logger.info(f"sync token has been invalidated")
            delete_all_calendupe_events(events_service, target_calendar_address)
            events, next_sync_token = list_events(events_service, source_calendar_address,
                                                  min_end_time=min_end_time)
        else:
            raise e

    # iterate through
    created_count = 0
    updated_count = 0
    for source_event in events:
        expected_target_event = source_event_to_target_event(source_event)
        existing_target_event = fetch_target_event(events_service, calendar_address=target_calendar_address,
                                                   source_event_id=source_event['id'])
        if existing_target_event is None:
            if expected_target_event['status'] == "cancelled":
                # if the source event is cancelled and there's no target event, nothing to do
                continue
            # but if the source event isn't cancelled and there's no target event, create one
            create_event(events_service, calendar_address=target_calendar_address, event=expected_target_event)
            created_count += 1
        else:
            # if it does exist, check if matches
            existing_target_matches = True
            for target_key in expected_target_event.keys():
                if existing_target_event[target_key] != expected_target_event[target_key]:
                    existing_target_matches = False
                    break
            # if they're different, update the existing one
            if not existing_target_matches:
                expected_target_event['id'] = existing_target_event['id']
                update_event(events_service, calendar_address=target_calendar_address,
                             event=expected_target_event)
                updated_count += 1
    logger.info(f"found {len(events)} events in source calendar, created {created_count} and updated {updated_count} "
                f"in target calendar")
    return next_sync_token
