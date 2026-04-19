import os
import uuid
import json
import logging
import tempfile
import mimetypes
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.cosmos import exceptions
from src.const.const import API_URL_BASE
from src.kpi.datos import item_cosmos, extract_kpis, filters
from src.load_documents.process_documents import extractdocument, savedocumentdata, items_cosmos, update_item_cosmos, insertar_item_cosmos, export_data_to_excel, get_items_by_id
from src.process.model import getresult, getinformegpt, getResumenBiodiversidad
from src.services.eco_caf_service import EcoCafService
from src.repository.blob_storage_repository import BlobStorageRepository
from src.repository.comosdb_repository import CosmosdbRepository
from src.services.notifications_service import NotificationsService
from src.services.logging_service import LoggingService
from src.repository.open_ai_repository import OpenAIRepository
from src.services.converter_to_pdf_service import ConverterToPDFService

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

notifications_api_url_base = os.environ["NOTIFICATIONS_API_URL_BASE"]

blob_connection_string = os.environ["BLOB_STORAGE_CONNECTION_STRING"]
blob_container_name = os.environ["BLOB_SOURCE_CONTAINER_NAME"]

cosmosdb_connection_string = os.environ["COSMOS_DB_CONNECTION_STRING"]
cosmosdb_database_name = os.environ["COSMOS_DB_NAME"]
cosmosdb_container_name = os.environ["COSMOS_DB_CONTAINER_NAME"]


storageaccount = os.environ["BLOB_STORAGE_ACCOUNT_NAME"]
source_container_name = blob_container_name
credential = os.environ["BLOB_STORAGE_KEY"]
blob_service = BlobServiceClient(
        account_url=f"https://{storageaccount}.blob.core.windows.net",
        credential=credential
    )
blob_container = blob_service.get_container_client(source_container_name)

    
open_ai_api_key = os.environ["OPEN_AI_KEY"]
open_ai_api_url = os.environ["OPEN_AI_API_URL"]
open_ai_api_version = os.environ["OPEN_AI_API_VERSION"]

converter_to_pdf_api_url_base = os.environ["CONVERTER_TO_PDF_API_URL_BASE"]

audits_api_url_base = os.environ["AUDTIS_API_URL_BASE"]

blob_storage_repository = BlobStorageRepository(connection_string=blob_connection_string, container_name=blob_container_name)
cosmosdb_repository = CosmosdbRepository(connection_string=cosmosdb_connection_string, database_name=cosmosdb_database_name, container_name=cosmosdb_container_name)
notifications_service = NotificationsService(notifications_api_url_base)
open_ai_repository = OpenAIRepository(open_ai_api_key=open_ai_api_key, open_ai_api_url=open_ai_api_url, open_ai_api_version=open_ai_api_version)

converter_to_PDF_service = ConverterToPDFService(converter_to_pdf_api_url_base)
eco_caf_service = EcoCafService(blob_storage_repository, cosmosdb_repository, notifications_service, open_ai_repository, converter_to_PDF_service)

logging_service = LoggingService(audits_api_url_base)

@app.route("uploadDocuments", methods=['POST'])
def process_files(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")  
    log = {
            "user": user,
            "action": "UPLOAD",
            "api": f"{API_URL_BASE}/uploadDocuments",
            "request": json.dumps(req.form)
        }    
    try:
        logging.info("Procesando process_files")
        # Get the files from the request
        files = req.files.getlist('files')
        if not files:
            return func.HttpResponse(
                json.dumps({"error": "No files selected for uploading"}),
                mimetype="application/json",
                status_code=400
            )

        # Get form data
        form_data = req.form
        tipo_documento = form_data.get('tipoDocumento')
        if not tipo_documento:
            return func.HttpResponse(
                json.dumps({"error": "No document type provided"}),
                mimetype="application/json",
                status_code=400
            )

        id = form_data.get('id')

        if not id:
            return func.HttpResponse(
                json.dumps({"error": "un ID es requerido."}),
                mimetype="application/json",
                status_code=400
            )
        response  = eco_caf_service.upload_documents(id=id,files=files,typeDocument=tipo_documento)
        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log)
        return func.HttpResponse(
            json.dumps(response),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.exception(f"[process_files] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@app.route("content/{file_path}/{file_name}", methods=['GET'])
def content_file(req: func.HttpRequest) -> func.HttpResponse:

    try:
        # Get file_path from route parameters
        file_path = req.route_params.get('file_path')
        file_name = req.route_params.get('file_name')
        logging.info(f"file_path: {file_path}")

        blob = blob_container.get_blob_client(f"{file_path}/{file_name}").download_blob()
        mime_type = blob.properties["content_settings"]["content_type"]

        if mime_type == "application/octet-stream":
            mime_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"

        headers = {
            "Content-Type": mime_type,
            "Content-Disposition": f"inline; filename={file_path}"
        }

        return func.HttpResponse(
            body=blob.readall(),
            headers=headers,
            status_code=200
        )
    except Exception as e:
        logging.exception(f"[content_file] - Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@app.route("save", methods=['POST'])
def save_data(req: func.HttpRequest) -> func.HttpResponse:
    try:
        filedata = req.get_json()
        # Cargar el archivo al Blob Storage
        result = savedocumentdata(filedata)
       
        return func.HttpResponse(
            json.dumps({"ok": "ok"}),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        logging.exception(f"[save_data] - Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@app.route('kpis', methods=['GET'])
def get_kpis(req: func.HttpRequest) -> func.HttpResponse:
    try:
        params = req.params
        pais = params.get('pais')
        proyecto = params.get('proyecto')
        tipoProyecto = params.get('tipoProyecto')
        fechaInicio = params.get('fechaInicio')
        fechaFin = params.get('fechaFin')
        criterio = params.get('criterio')
        operacion = params.get('operacion')
        financiado = params.get('financiado')
        actividad = params.get('actividades')
        nombreProyecto = params.get('nombreProyecto')
        categoriaP = params.get('categoriaP')
        categoria = params.get('categoria')
        subCategoria = params.get('subCategoria')
        fase = params.get('fase')
        indicadores = params.get('indicadores')
        subIndicadores = params.get('subIndicadores')
        criterios = params.get('criterios')

        # Obtener datos filtrados
        data = item_cosmos(nombreProyecto=nombreProyecto, pais=pais, proyecto=proyecto, tipoProyecto=tipoProyecto, fechaInicio=fechaInicio, criterio=criterio, operacion=operacion, financiado=financiado, fechaFin=fechaFin, actividad=actividad, categoriaP=categoriaP, categoria=categoria, subCategoria=subCategoria, fase=fase, indicadores=indicadores, subIndicadores=subIndicadores, criterios=criterios)
        if isinstance(data, str):
            return func.HttpResponse(json.dumps({"error": data}), mimetype="application/json", status_code=500)

        # Filtrado adicional por país
        if pais:
            data = [project for project in data if project.get("PAIS") == pais]

        # if actividad:
        #     data = [project for project in data if project.get("ACTIVIDADES ELEGIBLES QUE APLICAN AL PROYECTO") == actividad]

        kpis = extract_kpis(data)

        if actividad:
            # Filtra el objeto ACTIVIDADES_PROYECTO en el kpi
            actividades_proyecto = kpis.get("ACTIVIDADES_PROYECTO", {})
            logging.info(actividades_proyecto)
            # Filtra solo las actividades que coinciden con 'actividad'
            actividades_filtradas = {actividad: actividades_proyecto.get(actividad.strip(), 1)}
            logging.info(f"Actividades filtradas: {actividades_filtradas}")

            # Asigna las actividades filtradas al objeto de kpis
            kpis["ACTIVIDADES_PROYECTO"] = actividades_filtradas

        return func.HttpResponse(json.dumps(kpis), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[get_kpis] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route('list', methods=['GET'])
def get_paises(req: func.HttpRequest) -> func.HttpResponse:
    try:
        list = filters()
        return func.HttpResponse(json.dumps(list), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[get_paises] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route("JSON", methods=['GET'])
def get_dictionary(req: func.HttpRequest) -> func.HttpResponse:
    try:
        blob = blob_container.get_blob_client(f"Parametrizaciones/Dict.json").download_blob().readall()
        json_data = json.loads(blob)
        return func.HttpResponse(json.dumps(json_data), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[get_dictionary] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route("JSON_UPLOAD", methods=['POST'])
def update_dictionary(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")     
    updated_json = req.get_json()
    log = {
            "user": user,
            "action": "UPLOAD",
            "api": f"{API_URL_BASE}/JSON_UPLOAD",
            "request": json.dumps(updated_json)
        }    
    try:
        dumped_json = json.dumps(updated_json)
        logging.info(dumped_json)
        # Leer el archivo JSON desde el Blob Storage
        blob_client = blob_container.get_blob_client("Parametrizaciones/Dict.json")
        blob_data = blob_client.download_blob().readall()
        # json_data = json.loads(blob_data)

        # Obtener el JSON actualizado del cuerpo de la solicitud

        # Aquí puedes realizar cualquier actualización en `updated_json`
        # Por ejemplo, podemos reemplazar el contenido del JSON con los datos nuevos

        # Sobreescribir el archivo JSON en el Blob Storage con el contenido actualizado
        blob_client.upload_blob(dumped_json, overwrite=True)
        response = {"message": "JSON actualizado con éxito"}
        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log)
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[get_dictionary] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route("JSON_DELETE", methods=['DELETE'])
def delete_variable(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")     
    request = req.get_json()
    log = {
            "user": user,
            "action": "DELETE",
            "api": f"{API_URL_BASE}/JSON_DELETE",
            "request": json.dumps(request)
        }     
    try:
        # Obtener el nombre de la variable a eliminar desde la solicitud
        type_document = request.get("typeDocument")
        variable_name = request.get("variableName")
        if not type_document:
            return func.HttpResponse(json.dumps({"message": "El nombre del tipo de documento es requerido."}), mimetype="application/json", status_code=400)

        # Leer el archivo JSON desde el Blob Storage
        blob_client = blob_container.get_blob_client("Parametrizaciones/Dict.json")
        blob_data = blob_client.download_blob().readall()
        json_data = json.loads(blob_data)
        
        if not type_document in json_data:
            response = json.dumps({"message": "El tipo de documento no existe en el JSON."})
            log["response"] = response
            log["isSuccess"] = True
            logging_service.save_log(log)            
            return func.HttpResponse(response, mimetype="application/json", status_code=404)            

        # Eliminar tipo de documento del JSON
        if variable_name and type_document:
            for index, variable in enumerate(json_data[type_document]['variables']):
                if variable["nombre"].upper() == variable_name.upper():
                    del json_data[type_document]['variables'][index]
                    continue

        if not variable_name:
            del json_data[type_document]

        # Sobreescribir el archivo JSON en el Blob Storage con el contenido actualizado
        updated_json = json.dumps(json_data)
        blob_client.upload_blob(updated_json, overwrite=True)
        response = {"message": "Variable eliminada con éxito."}
        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log) 
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[delete_variable] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)


@app.route('extract_fields', methods=['POST'])
def extract_fields(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")  
    request = req.get_json()
    log = {
            "user": user,
            "action": "EXTRACT_TEXT",
            "api": f"{API_URL_BASE}/extract_fields",
            "request": json.dumps(request)
        }
    try:
        # Obtener el path del archivo desde la solicitud
        filename = request.get('path')
        # Obtiene el tipo de documento
        tipo_documento = request.get('tipoDocumento')
        id = request.get('id')
        if not filename:
            return func.HttpResponse(json.dumps({'error': 'Path del archivo no proporcionado'}), mimetype="application/json", status_code=400)

        # Llamar a la función extractdocument con el path proporcionado
        fields = extractdocument(filename, tipo_documento)

        # Comprobar si los campos fueron extraídos correctamente
        if fields is None:
            return func.HttpResponse(json.dumps({'error': 'No se pudieron extraer los campos del documento'}), mimetype="application/json", status_code=400)

        url = eco_caf_service.get_document_url_from_storage(filename)
        # Crear la respuesta en el formato deseado
        response = {
            "id": id,
            "Nombre": filename.split('/')[-1],
            "Path": filename,
            "url": url,
            "Tipodoc": tipo_documento,
            "Variables": fields
        }
        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log) 
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[extract_fields] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(json.dumps({'error': 'Ocurrió un error en el servidor'}), mimetype="application/json", status_code=500)


@app.route('readProyect', methods=['GET'])
def leer_items(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Extraer parámetros opcionales de la solicitud
        params = req.params
        pais = params.get('pais', None)
        proyecto = params.get('proyecto', None)
        tipoProyecto = params.get('tipoProyecto', None)
        fechaInicio = params.get('fechaInicio', None)
        fechaFin = params.get('fechaFin', None)
        criterio = params.get('criterio', None)
        operacion = params.get('operacion', None)
        financiado = params.get('financiado', None)
        fase = params.get('fase', None)
        actividad = params.get('actividades', None)
        categoriaP = params.get('categoriaP', None)
        categoria = params.get('categoria', None)
        subCategoria = params.get('subCategoria', None)
        indicadores = params.get('indicadores', None)
        subIndicadores = params.get('subIndicadores', None)
        criterios = params.get('criterios', None)

        # Llamar a items_cosmos con los parámetros opcionales
        resultado = items_cosmos(
            pais=pais,
            proyecto=proyecto,
            tipoProyecto=tipoProyecto,
            fechaInicio=fechaInicio,
            criterio=criterio,
            operacion=operacion,
            fase=fase,
            fechaFin=fechaFin,
            financiado=financiado,
            actividad=actividad,
            categoria=categoria,
            subCategoria=subCategoria,
            categoriaP=categoriaP,
            indicadores=indicadores,
            subIndicadores=subIndicadores,
            criterios=criterios
        )

        # Comprobar si el resultado es una lista y devolverlo como JSON
        if isinstance(resultado, list):
            return func.HttpResponse(json.dumps({'items': resultado}), mimetype="application/json", status_code=200)
        else:
            return func.HttpResponse(json.dumps({'mensaje': resultado}), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[leer_items] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({'error': 'Ocurrió un error en el servidor'}), mimetype="application/json", status_code=500)


@app.route('insertProyect', methods=['POST'])
def insertar_item(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")  
    request = req.get_json()
    log = {
            "user": user,
            "action": "CREATE",
            "api": f"{API_URL_BASE}/insertProyect",
            "request": json.dumps(request)
        }    
    try:

        if 'id' not in request:
            return func.HttpResponse("Error: El ítem debe tener un campo 'id'", mimetype="application/json", status_code=404)

        is_exists = eco_caf_service.is_id_operation_exists(request["id"])

        request["RESUMEN DE REPORTE PROYECTO"] = request.get('RESUMEN', None)
        request["NOMBRE DEL PROYECTO"] = request.get("NOMBRE DEL PROYECTO", None)
        
        resultado = insertar_item_cosmos(request)

        if "Error" in resultado:
            status_code = 400
        else:
            eco_caf_service.send_notifications(request["id"], request["NOMBRE DEL PROYECTO"], is_exists)
            status_code = 200
        
        response = {'mensaje': resultado}
        
        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log)
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=status_code)
    except Exception as e:
        logging.exception(f"[insertar_item] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(json.dumps({'error': 'Ocurrió un error en el servidor'}), mimetype="application/json", status_code=500)


@app.route('download', methods=['GET'])
def download_file(req: func.HttpRequest) -> func.HttpResponse:
    params = req.params
    anio = params.get('anio')

    if not anio:
        return func.HttpResponse(json.dumps({'error': 'Año no proporcionado'}), mimetype="application/json", status_code=400)

    try:
        xlsx_file = export_data_to_excel(anio)
        headers = {
            'Content-Disposition': f'attachment; filename="reporte_{anio}.xlsx"',
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }

        return func.HttpResponse(
            body=xlsx_file.getvalue(),
            headers=headers,
            status_code=200
        )
    except exceptions.CosmosHttpResponseError as e:
        logging.exception(f"[download_file] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({'error': f'Error en la consulta a la base de datos: {e.message}'}), mimetype="application/json", status_code=500)
    except Exception as e:
        logging.exception(f"[download_file] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({'error': str(e)}), mimetype="application/json", status_code=500)


@app.route('Model', methods=['POST'])
def getmodel(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")  
    data = req.get_json()
    log = {
            "user": user,
            "action": "PROCESS_MODEL",
            "api": f"{API_URL_BASE}/Model",
            "request": json.dumps(data)
        }         
    try:
        resultado = getresult(data)
        response = {'Variables': resultado}

        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log)
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[download_file] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(json.dumps({'error': str(e)}), mimetype="application/json", status_code=500)


@app.route('estimate_extract_time', methods=['POST'])
def estimate_extract_time(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        id = data.get('id')  
        filename = data.get('path')  # Path del archivo en el Blob Storage
        tipo_documento = data.get('tipoDocumento')

        if not filename:
            return func.HttpResponse(json.dumps({'error': 'Path del archivo no proporcionado'}), mimetype="application/json", status_code=400)
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, filename.split('/')[-1])

            blob_client = blob_service.get_blob_client(container=source_container_name, blob=filename)
            with open(file_path, "wb") as temp_file:
                blob_data = blob_client.download_blob()
                blob_data.readinto(temp_file)

            file_size = os.path.getsize(file_path)

            file_size_mb = file_size / (1024 * 1024)
            estimated_time = file_size_mb * 30

            estimated_time = round(estimated_time, 2)
            return func.HttpResponse(json.dumps({
                'id': id,
                'filename': filename,
                'tipoDocumento': tipo_documento,
                'file_size_mb': round(file_size_mb, 2),
                'estimated_time_seconds': estimated_time
            }), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception(f"[estimate_extract_time] - Error: {str(e)}")
        return func.HttpResponse(json.dumps({'error': 'Ocurrió un error en el servidor'}), mimetype="application/json", status_code=500)


@app.route('fases/{id}', methods=['GET'])
def get_item(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Get item_id from route parameters
        item_id = req.route_params.get('id')
        result = get_items_by_id(item_id)
        if result:
            return func.HttpResponse(
                json.dumps(result),
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                json.dumps({"error": "Item no encontrado"}),
                mimetype="application/json",
                status_code=404
            )
    except Exception as e:
        logging.exception(f"[get_item] - Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@app.route('informetext', methods=['POST'])
def getinforme(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        result = getinformegpt(data)

        if result:
            return func.HttpResponse(
                json.dumps(result),
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                json.dumps({"error": "Item no encontrado"}),
                mimetype="application/json",
                status_code=404
            )
    except Exception as e:
        logging.exception(f"[getinforme] - Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@app.route('biodiversidad', methods=['POST'])
def getbiodiversidad(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        result = getResumenBiodiversidad(data)

        if result:
            return func.HttpResponse(
                json.dumps(result),
                mimetype="application/json",
                status_code=200
            )
        else:
            return func.HttpResponse(
                json.dumps({"error": "Item no encontrado"}),
                mimetype="application/json",
                status_code=404
            )
    except Exception as e:
        logging.exception(f"[getbiodiversidad] - Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )


@app.route('document-url', methods=['GET'])
def get_document_url(req: func.HttpRequest) -> func.HttpResponse:
    try:
        params = req.params
        path_file = params.get('path_file')
        url = eco_caf_service.get_document_url_from_storage(path_file)
        return func.HttpResponse(json.dumps({'documentUrl': url}), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[get_document_url] - Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

@app.route('generate-biodiversity', methods=['POST'])
def generate_biodiversity(req: func.HttpRequest)-> func.HttpResponse:
    user = req.headers.get("user")  
    request = req.get_json()
    log = {
            "user": user,
            "action": "UPDATE",
            "api": f"{API_URL_BASE}/generate-biodiversity",
            "request": json.dumps(request)
        }    
    try:
        if not "id" in request:
            return func.HttpResponse(
                json.dumps("'id' is required"),
                mimetype="application/json",
                status_code=404
            )
        
        data = get_items_by_id(request["id"])
        
        biodiversity = getResumenBiodiversidad(data)

        if not biodiversity:
            return func.HttpResponse(
                json.dumps({"error": "Biodiversidad no encontrado"}),
                mimetype="application/json",
                status_code=404
            )

        biodiversity = json.loads(biodiversity)
        for item in biodiversity:
            id = uuid.uuid4()
            item["idBiodiversidad"] = str(id)
  
        data['biodiversidad'] = biodiversity
        update_item_cosmos(data)

        log["response"] = json.dumps(data)
        log["isSuccess"] = True
        logging_service.save_log(log)       
        return func.HttpResponse(
                json.dumps(data),
                mimetype="application/json",
                status_code=200
            )
    except Exception as e:
        logging.exception(f"[generate_biodiversity] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )
    
@app.route('items/{item_id}', methods=['PUT'])
def update(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")  
    item_id = req.route_params.get('item_id')
    request = req.get_json()
    log = {
            "user": user,
            "action": "UPDATE",
            "api": f"{API_URL_BASE}/items/{item_id}",
            "request": json.dumps(request)
        }      
    try:
        
        if not item_id:
            return func.HttpResponse("ID es requerido", mimetype="application/json", status_code=404)
        
        request["id"] = item_id
        update_item_cosmos(request)
        log["response"] = json.dumps(request)
        log["isSuccess"] = True
        logging_service.save_log(log)         
        return func.HttpResponse(json.dumps(request), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[update] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(
            json.dumps({"error": f"[update] - Error: {str(e)}"}),
            mimetype="application/json",
            status_code=500
        )
    
@app.route('documents/{id}', methods=['GET'])
def get_documents_by_id(req: func.HttpRequest) -> func.HttpResponse:
    try:
        id = req.route_params.get('id')
        documents = eco_caf_service.get_documents_by_id(id)
        return func.HttpResponse(json.dumps(documents), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[get_documents_by_id] - Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

@app.route(route="documents/{id}", methods=["DELETE"])
def delete_document_by_id(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")  
    id = req.route_params.get('id')
    request = req.get_json()
    log = {
            "user": user,
            "action": "DELETE",
            "api": f"{API_URL_BASE}/documents/{id}",
            "request": json.dumps(request)
        }    
    try:
        filename = request.get("filename")
        if not id:
            return func.HttpResponse("ID es requerido.", mimetype="application/json", status_code=404)
        
        if not filename:
            return func.HttpResponse("'filename' es requerido.", mimetype="application/json", status_code=404)

        document_path = f"{id}/{filename}"
        logging.info(f"document_path: {document_path}")
        response = eco_caf_service.delete_document(document_path)

        log["response"] = json.dumps(response)
        log["isSuccess"] = True
        logging_service.save_log(log)
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[delete_file] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
    
@app.route(route="grouping-projects", methods=["POST"])
def grouping_projects(req: func.HttpRequest) -> func.HttpResponse:
    user = req.headers.get("user")  
    log = {
            "user": user,
            "action": "PROCESS_MODEL",
            "api": f"{API_URL_BASE}/grouping-projects",
        }    
    try:
        compare_projects_response = eco_caf_service.compare_projects()
        response = json.dumps(compare_projects_response)
        logging.info(f"[compare_projects] - Response: {response}")
        log["response"] = response
        log["isSuccess"] = True
        logging_service.save_log(log)        
        return func.HttpResponse(response, mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception(f"[compare_projects] - Error: {str(e)}")
        log["isSuccess"] = False
        log["error"] = str(e)
        logging_service.save_log(log)        
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=500)
    
