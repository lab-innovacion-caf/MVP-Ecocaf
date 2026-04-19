# Databricks notebook source
# MAGIC %pip install azure-cosmos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Libraries

# COMMAND ----------

from azure.cosmos import CosmosClient

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Classes

# COMMAND ----------

class CosmosDB:
    def __init__(self, endpoint, key, database_name, container_name):
        self.client = CosmosClient(endpoint, key)
        self.database = self.client.get_database_client(database_name)
        self.container = self.database.get_container_client(container_name)

    def getAliadoItem(self, query, parameters):
        query = "SELECT * FROM c WHERE c.reportType = @reportType ORDER BY c.createdAt DESC"
        parameters = [{"name": "@reportType", "value": reportType}]
        
        items = list(self.container.query_items(
            query=query,
            parameters=parameters,
            enable_cross_partition_query=True
        ))

        return items
