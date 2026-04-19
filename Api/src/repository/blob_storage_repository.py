import logging
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from src.interfaces.blob_storage_interface import BlobStorageInterface
class BlobStorageRepository(BlobStorageInterface):
    def __init__(self, connection_string: str, container_name: str)->None:
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_name = container_name

    def upload(self, file_content, file_name, overwrite=True):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_name)
            response = blob_client.upload_blob(file_content, overwrite=overwrite)
            logging.info(f"[BlobStorageRepository - upload] - blob_response: {response}")
        except Exception as e:
            logging.error(f"Error when try upload file to blob storage: {str(e)}")
            raise ValueError(f"[BlobStorageRepository - upload] - Error: {str(e)}")

    def download_blob(self, file_name: str):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_name)
            response = blob_client.download_blob()
            logging.info(f"response: {response}")
            return response
        except Exception as e:
            logging.error(f"Error when try download_blob file from blob storage: {str(e)}")
            raise ValueError(f"[BlobStorageRepository - download_blob] - Error: {str(e)}")

    def get_blob_url(self, blob_name: str) -> str:
        try:
            account_name = self.blob_service_client.account_name
            account_key = self.blob_service_client.credential.account_key
            sas_token = generate_blob_sas(
                account_name=account_name,
                container_name=self.container_name,
                blob_name=blob_name,
                account_key=account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=24)  # URL válida por 24 hora
            )
            blob_url = f"https://{account_name}.blob.core.windows.net/{self.container_name}/{blob_name}?{sas_token}"
            return blob_url
        except Exception as e:
            logging.error(f"Error when try get_blob_url file from blob storage: {str(e)}")
            raise ValueError(f"[BlobStorageRepository - get_blob_url] - Error: {str(e)}")

    def list_blobs(self, blob_name: str):
        try:
            container_client = self.blob_service_client.get_container_client(container=self.container_name)
            blobs = container_client.list_blobs(name_starts_with=blob_name)
            return blobs
        except Exception as e:
            logging.exception(f"[BlobStorageRepository - get_folders_and_subfolders] - Error: {str(e)}")
            raise ValueError(f"Error when try get_container_client files to blob storage: {str(e)}")
        
    def delete_blob(self, file_path: str) -> None:
        try:
            get_blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_path)

            if not get_blob_client.exists():
                logging.warning(f"File: {file_path} not found")
                raise ValueError(f"File: {file_path} not found")
            
            get_blob_client.delete_blob()
            logging.info(f"File: {file_path} was deleted successfully")   
        except Exception as e:
            logging.exception(f"[BlobStorageRepository - delete_blob] - Error: {str(e)}")  
            raise ValueError(f"Error when try delete_blob files to blob storage: {str(e)}")