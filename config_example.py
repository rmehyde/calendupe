from datetime import datetime, timezone

# gcp parameters
GCP_REGION = "us-central1"
GCP_PROJECT = "PROJECT"
# bucket in which to store the lock file
GCS_LOCK_BUCKET = "lock-bucket"
# bucket in which to store sync tokens
GCS_DATA_BUCKET = "data-bucket"
# name of the resubscribe tasks queue
TASKS_QUEUE_NAME = "calendupe-resubscribe"

# secret token for auth, e.g. output of `secrets.token_hex(16)`
TOKEN = "abc123"

# unique identifier for the update subscription channel, e.g. output of `uuid.uuid4()`
CHANNEL_ID = "abc123"

# calendar to read events from
SOURCE_CALENDAR_ADDRESS = "you@your.domain"
# calendar to copy events into
TARGET_CALENDAR_ADDRESS = "you@your.domain"
# minimum end time for events to be copied, must be timezone-aware
MIN_END_TIME = datetime(2022, 1, 1, tzinfo=timezone.utc)
# if True, resubscribe to the notification channel before it's set to expire
REMAIN_SUBSCRIBED = False
