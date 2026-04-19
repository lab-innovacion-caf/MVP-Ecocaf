import logging
import requests
class ConverterToPDFService:
    def __init__(self, api_url_base: str):
        self.api_url_base = api_url_base

    def convert_to_PDF(self,data):
        try:
            url = f"{self.api_url_base}/base_convert_to_pdf"
            response = requests.post(url, files=data)
            if len(response.content) == 0:
                logging.info("[ConverterToPDFService - convert_to_PDF] - La respuesta de la API no tiene contenido.")
            logging.info(f"[ConverterToPDFService - convert_to_PDF] - Response: successfully!")    
            return response
        except Exception as e:
            logging.exception(f"[ConverterToPDFService - convert_to_PDF] - Error: {e}")            
            raise ValueError(f"[ConverterToPDFService - convert_to_PDF] - Error: {e}")