from abc import ABC, abstractmethod

class OpenAIInterface(ABC):
    @abstractmethod
    def chat_completions_create(self, prompt: str):
        pass