from google.cloud import storage

class StorageService:
    storage_client: storage.Client = storage.Client()