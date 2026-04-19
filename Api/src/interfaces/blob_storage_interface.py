from abc import ABC, abstractmethod

class BlobStorageInterface(ABC):
    @abstractmethod
    def upload(self,file_content, file_name, overwrite):
        pass

    @abstractmethod
    def download_blob(self, file_name):
        pass

    @abstractmethod
    def get_blob_url(self, blob_name: str):
        pass
    
    @abstractmethod
    def list_blobs(self, blob_name: str):
        pass

    @abstractmethod
    def delete_blob(self, file_path: str):
        pass