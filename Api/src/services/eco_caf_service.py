import json
import logging
from src.interfaces.blob_storage_interface import BlobStorageInterface
from src.interfaces.cosmosdb_interface import CosmosdbInterface
from src.services.notifications_service import NotificationsService
from src.interfaces.open_ai_interface import OpenAIInterface
from src.queries.query import GET_ALL_PROJECTS,GET_ONE_PROJECT_BY_ID
from src.prompts.group_by_projects_prompt import get_group_by_projects_prompt
from src.const.const import KPIS_VARIABLES_FOR_GROUP_BY_PROJECTS
from src.services.converter_to_pdf_service import ConverterToPDFService
class EcoCafService:
    def __init__(self, blob_storage_repository: BlobStorageInterface, cosmosdb_repository: CosmosdbInterface, notifications_service: NotificationsService, open_ai_repository: OpenAIInterface, converter_to_PDF_service: ConverterToPDFService) -> None:
        self.blob_storage_repository = blob_storage_repository
        self.cosmosdb_repository = cosmosdb_repository
        self.notifications_service = notifications_service
        self.open_ai_repository = open_ai_repository
        self.converter_to_PDF_service = converter_to_PDF_service
    
    def get_document_url_from_storage(self, path_file: str):
        document_url = self.blob_storage_repository.get_blob_url(path_file)
        return document_url
    

    def is_id_operation_exists(self, id: str) -> bool:
        response = True
        get_number_operation = self.cosmosdb_repository.get_one(id)
        logging.info(f"get_number_operation: {get_number_operation}")
        if not 'id' in get_number_operation:
            response = False
        return response
    
    def send_notifications(self, id:str, project_name: str, is_id_operation_exists: bool) -> None:
        notification = "SUCCESSFUL_PROCESS_UPDATED" if is_id_operation_exists else "SUCCESSFUL_PROCESS_CREATED"
        get_files = self.blob_storage_repository.list_blobs(id)
        blobs = [blob.name.split("/")[1] for blob in get_files if blob.name and blob.name.endswith(".pdf")]
        documents = []
        if len(blobs) > 0:
            for blob in blobs:
                documents.append({
                    "Documentos": blob
                })
        data = {
            "idProject": "ECOCAF",
            "typeNotification": "EMIAL",
            "notification": notification,
            "data": [
                            {
                    "label":"{{projectName}}",
                    "value": project_name
                },
                {
                    "label":"{{id}}",
                    "value": id
                }
            ],
            "table": documents
        }
        logging.warning(data)
        self.notifications_service.send(data=data)

    def prepare_documents(self, paths):
        folder_structure = []
        logging.info(f"paths: {paths}")
        for path in paths:
            parts = path.split("/")
            logging.info(f"parts: {parts}")
            if len(parts)>1:
                folder_structure.append(parts[1])

        return [document for document in folder_structure if document.endswith((".pdf", ".docx", ".pptx"))]

    def get_documents_by_id(self, id):
        blobs =  self.blob_storage_repository.list_blobs(id)
        folders_path = []
        for blob in blobs:
            blob_name = blob.name
            folders_path.append(blob_name)

        folder_structure = self.prepare_documents(folders_path)
        return folder_structure
    
    def upload_documents(self, id:str, files, typeDocument: str):
        uploaded_files_info = []
        for file in files:
            try:
                filename = file.filename
                pdf_bytes = file.stream.read()
                if filename.lower().endswith('.docx') or filename.lower().endswith('.pptx'):
                    file.stream.seek(0) # Reiniciar el puntero antes de enviarlo, debido a que ya se hizo file.stream.read()
                    multipart = {"file": (filename, file.stream,file.content_type)}
                    response = self.converter_to_PDF_service.convert_to_PDF(multipart)
                    pdf_bytes = response.content
                    filename = filename.replace(".docx",".pdf").replace(".DOCX",".pdf").replace(".PPTX",".pdf")
                path_file = f"{id}/{filename}"
                self.blob_storage_repository.upload(pdf_bytes,path_file)
                uploaded_files_info.append({
                        "message": "File uploaded successfully",
                        "path": path_file,
                        "filename": filename,
                        "tipoDocumento": typeDocument
                    }
                )
                logging.info(f"File {path_file} uploaded successfully.")
            except Exception as e:
                logging.error(f"Error uploading file {path_file}: {e}")
                raise ValueError(e)
        return uploaded_files_info
    def delete_document(self, file_path):
        response = {
            "isDeleted": False,
            "pathFile": file_path
        }
        filename = file_path.replace(".pdf", "")
        files_pdf_and_txt = [f"{filename}.pdf",f"{filename}.txt"]
        for file in files_pdf_and_txt:
            try:
                self.blob_storage_repository.delete_blob(file)
                response["isDeleted"] = True
                return response
            except Exception as e:
                logging.error(f"Error deleting file {file_path}: {e}")
                raise ValueError(e)

    def extract_kpis(self,data, variables):
        kpis = {}
        for var in variables:
            valor = 0
            if "originacion" in data and isinstance(data["originacion"], dict) and var in data["originacion"]:
                valor = data["originacion"][var]
            elif "evaluacion" in data and isinstance(data["evaluacion"], dict) and var in data["evaluacion"]:
                valor = data["evaluacion"][var]
            kpis[var] = valor
        return kpis

    def get_kpi_values(self,project_id):
        query = GET_ONE_PROJECT_BY_ID.replace("@project_id",f"'{project_id}'")
        try:
            data = self.cosmosdb_repository.query_items(query)
            if data:
                data = data[0]
                return self.extract_kpis(data, KPIS_VARIABLES_FOR_GROUP_BY_PROJECTS)
            else:
                return {var: 0 for var in KPIS_VARIABLES_FOR_GROUP_BY_PROJECTS}
        except Exception as e:
            print(f"Error al consultar Cosmos DB para el proyecto {project_id}: {e.message}")
            return {var: 0 for var in KPIS_VARIABLES_FOR_GROUP_BY_PROJECTS}
            
    def compare_projects(self):
        get_projects = self.cosmosdb_repository.query_items(GET_ALL_PROJECTS)
        serialize_projects = json.dumps(get_projects, indent=4, ensure_ascii=False)

        prompt = get_group_by_projects_prompt(serialize_projects)
        completion =  self.open_ai_repository.chat_completions_create(prompt)
        serialize_completion = json.loads(completion)
        for group_key in serialize_completion:
            proyectos_list = group_key.get("projects", [])
            for idx, item in enumerate(proyectos_list):
                if isinstance(item, str):
                    pid = item
                    get_values = self.get_kpi_values(pid)
                    proyectos_list[idx] = {
                        "id": pid,
                    }
                    proyectos_list[idx].update(get_values)
                elif isinstance(item, dict):
                    pid = item.get("id")
                    if pid:
                        values = self.get_kpi_values(pid)
                        item.update(values)
            group_key["projects"] = proyectos_list
        return serialize_completion