import io

import backoff
from google.api_core.exceptions import NotFound, PreconditionFailed
from google.cloud import storage

UTF_8 = "utf-8"


def upload_text(bucket_name: str, blob_name: str, content: str):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    encoded = content.encode(UTF_8)
    data = io.BytesIO(encoded)
    blob.upload_from_file(data)


def read_text(bucket_name: str, blob_name: str) -> str:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content: bytes = blob.download_as_bytes()
    return content.decode(UTF_8)


class GCSLock:
    def __init__(self, bucket_name: str, blob_name: str, creds=None):
        storage_client = storage.Client(credentials=creds)
        self.bucket = storage_client.bucket(bucket_name)
        self.blob_name = blob_name

    @backoff.on_exception(backoff.expo, exception=PreconditionFailed, max_time=300)
    def acquire(self) -> None:
        """
        Synchronously acquire the lock. Uses exponential backoff with up to 5 minute wait
        :raises PreconditionFailed: if the lock cannot be acquired
        """
        blob = self.bucket.blob(self.blob_name)
        data = io.BytesIO(b"")
        blob.upload_from_file(data, if_generation_match=0)

    def release(self, allow_nonexistent=False):
        """
        Synchronously release the lock
        :param allow_nonexistent: do not raise an error if the lock blob doesn't exist
        :raises NotFound: if the lock blob doesn't exist and allow_nonexistent is False
        """
        try:
            self.bucket.delete_blob(self.blob_name)
        except NotFound as e:
            if not allow_nonexistent:
                raise e
