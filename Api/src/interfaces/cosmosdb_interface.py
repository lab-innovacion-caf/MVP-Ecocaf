from abc import ABC, abstractmethod

class CosmosdbInterface(ABC):
    @abstractmethod
    def save(self):
        pass

    @abstractmethod
    def getAll(self):
        pass

    @abstractmethod
    def get_one(self, id: str):
        pass

    @abstractmethod
    def query_items(self, query: str):
        pass