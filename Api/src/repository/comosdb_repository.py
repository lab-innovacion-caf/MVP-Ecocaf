import logging
from azure.cosmos import CosmosClient
from azure.cosmos import exceptions
from src.interfaces.cosmosdb_interface import CosmosdbInterface

class CosmosdbRepository(CosmosdbInterface):
    def __init__(self, connection_string, database_name, container_name):
        self.client = CosmosClient.from_connection_string(connection_string)
        self.container = self.client.get_database_client(database_name).get_container_client(container_name)        

    def save(self):
        pass
                    
    def getAll(self):
        pass

    def get_one(self, id: str):
        logging.info(f"id: {id}")
        try:
            item = self.container.read_item(item=id,partition_key=id)
            return item
        except exceptions.CosmosResourceNotFoundError:
            return {}
        except Exception as e:
            logging.error(f"[CosmosdbRepository - get_one] - Error: {str(e)}")
            raise ValueError(f"[CosmosdbRepository - get_one] - Error: {str(e)}")
        
    def query_items(self, query):
        try:
            item = self.container.query_items(query=query, enable_cross_partition_query=True)
            return list(item)
        except Exception as e:
            logging.error(f"[CosmosdbRepository - query_items] - Error: {str(e)}")
            raise ValueError(f"[CosmosdbRepository - query_items] - Error: {str(e)}")