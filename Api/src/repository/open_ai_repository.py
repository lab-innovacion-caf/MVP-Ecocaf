import logging
from openai import AzureOpenAI

class OpenAIRepository:
    def __init__(self, open_ai_api_key: str, open_ai_api_url: str, open_ai_api_version: str ):
        self.client =  AzureOpenAI(
            api_key=open_ai_api_key,
            azure_endpoint=open_ai_api_url,
            api_version=open_ai_api_version
        )
    def chat_completions_create(self, prompt: str, model="gpt-4o",temperature=0.2,frequency_penalty= 0, presence_penalty=0):
        try:
            completion = self.client.chat.completions.create(
            model = model,
            messages = prompt,
            temperature = temperature,
            frequency_penalty = frequency_penalty,
            presence_penalty = presence_penalty
        )
            return completion.choices[0].message.content
        except Exception as e:
            logging.error(f"[OpenAI - chat_completions_create] - Error: {str(e)}")
            raise ValueError(f"[OpenAI - chat_completions_create] - Error: {str(e)}")