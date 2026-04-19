import logging
import os
import io
from io import BytesIO
from azure.core.credentials import AzureKeyCredential
from azure.storage.blob import BlobServiceClient
from openai import AzureOpenAI
import json
import pandas as pd
from docx import Document
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.ai.documentintelligence import DocumentIntelligenceClient

removeall = False
remove = False
localpdfparser = False
storageaccount = os.environ["BLOB_STORAGE_ACCOUNT_NAME"]
credential = os.environ["BLOB_STORAGE_KEY"]
formrecognizerservice = os.environ["DOCUMENT_INTELLIGENCE_NAME"]
credentialformrecognizer = os.environ["DOCUMENT_INTELLIGENCE_KEY"]
source_container_name = os.environ["BLOB_SOURCE_CONTAINER_NAME"]
target_container_name = os.environ["BLOB_TARGET_CONTAINER_NAME"]
verbose = False
AZURE_OPENAI_SERVICE = os.environ["OPEN_AI_NAME"]
OPENAI_API_KEY = os.environ["OPEN_AI_KEY"]

# Azure OpenAI cliente y configuración
client = AzureOpenAI(
    api_key=OPENAI_API_KEY,
    azure_endpoint=f"https://{AZURE_OPENAI_SERVICE}.openai.azure.com",
    api_version="2023-03-15-preview"
)


# Conexión blob storage
blob_service = BlobServiceClient(
        account_url=f"https://{storageaccount}.blob.core.windows.net",
        credential=credential
    )
source_container_client = blob_service.get_container_client(source_container_name)


def get_document_text(file_path, blob_service, source_container_client, formrecognizerservice, credentialformrecognizer):
    # Descargar el archivo desde Azure Blob Storage
    blob_client = source_container_client.get_blob_client(file_path)
    file_stream = io.BytesIO()
    file_stream.write(blob_client.download_blob().readall())
    file_stream.seek(0)
    logging.info(f"Loaded {file_stream.getbuffer().nbytes} bytes from {file_path} in Blob Storage.")

    # Detectar el tipo de archivo basado en la extensión
    file_extension = os.path.splitext(file_path)[1].lower()
    output_txt_filename = f"{os.path.splitext(file_path)[0]}.txt"

    if file_extension == '.pdf':
        # Usar Azure Form Recognizer para extraer texto de archivos PDF
        form_recognizer_client = DocumentAnalysisClient(
            endpoint=f"https://{formrecognizerservice}.cognitiveservices.azure.com/",
            credential=AzureKeyCredential(credentialformrecognizer)
        )

        poller = form_recognizer_client.begin_analyze_document("prebuilt-document", file_stream)
        result = poller.result()

        # Procesar resultados del PDF
        document_text = ""
        for page in result.pages:
            document_text += f"--- Page {page.page_number} ---\n"
            for line in page.lines:
                document_text += f"{line.content}\n"

    elif file_extension == '.docx':
        # Procesar el archivo DOCX
        doc = Document(file_stream)
        document_text = ""
        for paragraph in doc.paragraphs:
            document_text += paragraph.text + "\n"

    elif file_extension in ['.xls', '.xlsx']:
        # Procesar el archivo Excel
        df = pd.read_excel(file_stream)
        # Convertir el DataFrame a texto
        document_text = df.to_string(index=False)

    else:
        raise ValueError(f"Formato de archivo no soportado: {file_extension}")

    logging.info(f"Saved extracted text to {output_txt_filename}")
    return document_text


# path archivos
file_path_pdf_criterios = "Parametrizaciones/resumen_criterios.txt"
file_path_pdf_gef = "Parametrizaciones/resumen_gef.txt"
resumen_criterios = source_container_client.get_blob_client(file_path_pdf_criterios).download_blob().readall().decode('utf-8')

resumen_gef = source_container_client.get_blob_client(file_path_pdf_gef).download_blob().readall().decode('utf-8')

print("RESUMEN CRITERIOS: ", resumen_criterios)
print("RESUMEN GEF: ", resumen_gef)


variables_gef = {
    "CLIENTE": "Nombre del cliente, el dato debe ser proporcionado como tipo string.",
    "PAIS": "País ejecutor del proyecto, el dato debe ser proporcionado como tipo string.",
    "INDICADORES": "Indicador o indicadores principales los cuales se encuentran en el documento brindado como contexto a los que aplica el proyecto evaluado los cuales se encuentran el documento brindado como contexto. El dato debe ser proporcionado como tipo string.",
    "SUBINDICADORES": "Subindicador o subindicadores  que hacen parte del indicador principal los cuales se encuentran en el documento brindado como contexto al que aplica el proyecto evaluado . El dato debe ser proporcionado como tipo string.",
    "CRITERIOS DE ELEGIBILIDAD": "Criterio o criterios los cuales se encuentran en el documento brindado como contexto a los que aplica el proyecto evaluado . El dato debe ser proporcionado como tipo string, (sino se encuentra o no hay datos poner como 'N/A').",
    "MEDIDA DEL INDICADOR": "Unidad de medida de los indicadores que aplican los cuales se encuentran en el documento brindado como contexto. El dato debe ser proporcionado como tipo string,.",
    "¿POR QUÉ APLICA ESOS INDICADORES?": "Explicación detallada del por qué se eligieron esos indicadores, subindicadores y criterios. El dato debe ser proporcionado como tipo string."
}

variables_criterios = {
    "CLIENTE": "Nombre del cliente, el dato debe ser proporcionado como tipo string.",
    "PAIS": "País ejecutor del proyecto, el dato debe ser proporcionado como tipo string.",
    "¿CUMPLE LOS CRITERIOS DE FINANCIAMIENTO VERDE?": "Evaluación de si el proyecto cumple o no los criterios de financiamiento verde. La respuesta debe ser 'Sí' o 'No'. El dato debe ser proporcionado como tipo string.",
    "CLASIFICACIÓN DEL PROYECTO": "Clasificación del proyecto como 'Elegible' o 'No elegible', dependiendo de si cumple o no los criterios de financiamiento verde los cuales se encuentran en el documento brindado como contexto. El dato debe ser proporcionado como tipo string.",
    "¿POR QUÉ CUMPLE CON LOS CRITERIOS DE FINANCIAMIENTO VERDE?": "Descripción detallada de por qué cumple los criterios de financiamiento verde los cuales se encuentran en el documento brindado como contexto. En caso de que no cumpla los criterios de financiamiento, debes responder 'N/A'. El dato debe ser proporcionado como tipo string.",
    "¿POR QUÉ NO CUMPLE CON LOS CRITERIOS DE FINANCIAMIENTO VERDE?": "Descripción detallada de por qué no cumple los criterios de financiamiento verde y cuáles son los criterios que no se cumplen para que sea clasificado como proyecto verde. En caso de que si cumpla los criterios de financiamiento, debes responder 'N/A'. El dato debe ser proporcionado como tipo string.",
    "CATEGORÍAS PRINCIPALES DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA": "Lista de categoría o categoríaslos cuales se encuentran en el documento brindado como contexto en las que clasifica el proyecto evaluado. Las categorías son: 'Financiamiento para la mitigación del cambio climático', 'Financiamiento para la adaptación al cambio climático', 'Financiamiento para el medio ambiente y la biodiversidad'. En caso de que no cumpla los criterios de financiamiento verde, debes responder 'N/A'. El dato debe ser proporcionado como tipo string.",
    "CATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA": "Lista de categoría o categoríaslos cuales se encuentran en el documento brindado como contexto en las que clasifica el proyecto evaluado. Las categorías están descritas en el documento de contexto como 'CATEGORÍA'. En caso de que no cumpla los criterios de financiamiento verde, debes responder 'N/A'. El dato debe ser proporcionado como tipo string. Considerar solo texto como por ejemplo si se tiene el texto '3.1. Puesta en valor del capital natural y sostenibilidad de actividades productivas' solo considerar como valor 'Puesta en valor del capital natural y sostenibilidad de actividades productivas'",
    "SUBCATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA": "Lista de subcategoría o subcategorías los cuales se encuentran en el documento brindado como contexto en las que clasifica el proyecto evaluado. Las subcategorías están descritas en el documento de contexto como 'SUBCATEGORÍA' y forman parte de una categoría. En caso de que no cumpla los criterios de financiamiento verde, debes responder 'N/A'. El dato debe ser proporcionado como tipo string. Considerar solo texto como por ejemplo si se tiene el texto '3.1.1. Agua' solo considerar como valor 'Agua'.",
    "ACTIVIDADES ELEGIBLES QUE APLICAN AL PROYECTO": "Lista de actividad o actividades los cuales se encuentran en el documento brindado como contexto en las que clasifica el proyecto evaluado. Las actividades elegibles están descritas en el documento de contexto como 'ACTIVIDADES ELEGIBLES' y forman parte de una subcategoría. En caso de que no cumpla los criterios de financiamiento verde, debes responder 'N/A'. El dato debe ser proporcionado como tipo string. Considerar solo texto como por ejemplo si se tiene el texto '3.1.1. Restauración y gestión integral de cuencas' solo considerar como valor 'Restauración y gestión integral de cuencas'.",
    "CLASIFICACIÓN DE RIESGO AMBIENTAL, CLIMáTICO Y SOCIAL DE LA OPERACIÓN": "str"
}


def getresult(project):
    logging.info(f"Project: {project}")
    diccionario_entrada = {}
    for i, dic in enumerate(project):
        diccionario_entrada.update(dic["Variables"])

    lista_prompt = [
        f"""Basándote en el siguiente documento como contexto y traduciéndolo previamente al español si es necesario {resumen_gef}. Analiza el proyecto  {project} teniendo en cuenta que el proyecto incluye un campo RESUMEN importante para el analisis teniendo en cuenta las siguientes instrucciones:
        Realiza un análisis de los indicadores y subindicadores que aplican para el proyecto, la medida  y él porque aplica esos indicadores.
        La respuesta debe ser en español, clara, concisa y  en formato json, si el campo no existe, dejarás una string vacía como valor, UNICAMENTE RESPONDE EL JSON, SIN SALTOS DE LINEA NI NADA ADICIONAL, NO COLOQUES json al principio, unicamente abre y cierra las llaves, LAS COMILLAS DEBEN SER DOBLES Y NO SENCILLAS, ASEGURATE QUE EL JSON ESTE EN EL FORMATO CORRECTO. Este es el formato de JSON con el que debes generar la respuesta:{variables_gef} Si tienes que listar datos, usa el símbolo "|" como separador en lugar de una coma ",".| 
        """,
        f"""Basándote en el siguiente documento como contexto {resumen_criterios}. Analiza el proyecto  {project} teniendo en cuenta que el proyecto incluye un campo RESUMEN importante para el analisis teniendo en cuenta las siguientes instrucciones:
        La respuesta debe ser en formato JSON, en español, clara y concisa.
        Solamente responde con la información que se te compartió sin incluir información adicional.
        Realiza una clasificación del proyecto indicando si cumple los criterios de financiamiento verde y en que categoría(s) clasifica.
        La respuesta debe ser en español, clara, concisa y  en formato json, si el campo no existe, dejarás una string vacía como valor, UNICAMENTE RESPONDE EL JSON, SIN SALTOS DE LINEA NI NADA ADICIONAL, NO COLOQUES json al principio, unicamente abre y cierra las llaves, LAS COMILLAS DEBEN SER DOBLES Y NO SENCILLAS, ASEGURATE QUE EL JSON ESTE EN EL FORMATO CORRECTO. Este es el formato de JSON con el que debes generar la respuesta:{variables_criterios}Si tienes que listar datos, usa el símbolo "|" como separador en lugar de una coma ",".
        """
    ]

    resultados_gef = []
    resultados_criterios = []

    logging.info(f"LISTA PROMPT:  {lista_prompt}")

    for i, prompt in enumerate(lista_prompt):
        # Formatea el prompt con los datos del proyecto
        # formatted_prompt = prompt.format(project=project, resultados_gef=resultados_gef, resultado_criterios=resultado_criterios)

        chat_prompt = [
            {"role": "system", "content": "Eres un asistente experto en el análisis y aprobación de financiamiento en proyectos ecológicos"},
            {"role": "user", "content": f"{prompt}"}
        ]

        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=chat_prompt,
            temperature=0.2,
            frequency_penalty=0,
            presence_penalty=0
        )

        logging.info("OpenAI completado")
        answer = completion.choices[0].message.content

        # Guardamos la respuesta en la lista correspondiente
        if i == 0:  # Asumimos que el primer prompt es para resultados GEF
            resultados_gef.append(answer)
            json_string_gef = answer.strip('```json\n')
            result_gef = json.loads(json_string_gef)
            df_gef = pd.DataFrame([result_gef])
        else:  # Los demás prompts se almacenan en resultados_criterios
            resultados_criterios.append(answer)
            json_string_criterios = answer.strip('```json\n')
            logging.info(f"RESULT TO PASE: {json_string_criterios}")
            result_criterios = json.loads(json_string_criterios)
            df_criterios = pd.DataFrame([result_criterios])
    # Opcionalmente, puedes imprimir o procesar los resultados aquí
    logging.info(f"Resultados GEF: {len(resultados_gef)}")
    logging.info(f"Resultados Criterios: {len(resultados_criterios)}")
    dict_final = {**result_gef, **result_criterios, **diccionario_entrada}
    dict_final["ELEGIBLE / NO ELEGIBLE"] = dict_final["CLASIFICACIÓN DEL PROYECTO"]
    # for i, dic in enumerate(project):
    #     dict_final.update(dic["Variables"])
    return dict_final


def getinformegpt(data):

    chat_prompt = [
        {"role": "system", "content": """Eres un asistente experto en el análisis y aprobación de financiamiento en proyectos ecológicos, se requiere que generes un informe IET, con la información que te proporcione el usuario, retorna el resultado unicamente como html, utiliza tablas y graficos en lo posible; utiliza el siguiente ejemplo de contenido:
             Informe número
DATBC-GRISALES-2024-01
País
República Dominicana, Provincia Valverde, Municipio de Mao
Representante país
Daniel Cabrales
Ejecutivo país
Roberto Agostini
Ejecutivo DATBC
Cristian Grisales


Fecha del informe
23/02/2024


Nombre de la operación
Proyecto Construcción del Canal Alto Mao, Provincia Valverde


Resumen de la operación
El Proyecto de Construcción del Canal Alto Mao, ubicado en la Provincia Valverde, tiene como objetivo mejorar la productividad y competitividad de la producción agrícola bajo riego. Además, busca elevar las condiciones de vida de los productores agrícolas y sus familias. Para lograr esto, se implementarán intervenciones estructurales, como la construcción del canal de riego “Alto Mao” y su infraestructura complementaria. También se llevarán a cabo medidas no estructurales, como la conservación de cuencas, la remediación ambiental, acciones de saneamiento y el fortalecimiento de capacidades de las juntas de regantes y del INDRHI. 

Componente 1: Desarrollo de infraestructura
Componente 2: Fortalecimiebto de capacidades
Componente 3: Conservación de cuencas
Componente 4: Administración del proyecto


Contexto de biodiversidad y clima relevante para la operación
3.1 Marco de políticas y objetivos de política (Biodiversidad y clima)
Mediante la Ley 202 del 2004, la República Dominicana crea el Sistema Nacional de Áreas Protegidas, que es un conjunto de zonas naturales, coordinadas dentro de sus propias categorías de manejo, y que poseen características, objetivos y manejos muy precisos y especializados.
Dentro de los límites territoriales de la provincia Valverde inciden siete (7) áreas protegidas, agrupadas en tres (3) categorías de manejo: Paisaje Protegido, Parque Nacional y Reserva Natural, Ocupando una superficie de 128.45 km², equivalente al 16% de la superficie total que están dentro del Sistema Nacional de Áreas Protegidas. Las principales áreas protegidas del Municipio de Mao son:
Área Protegida Furnia de Gurabo
Vía Panorámica Entrada de Mao
Reserva Forestal del río Cana
Parque Ecológico Cincuentenario
Parque Nacional Amina
Parque Nacional Piki Lora
Corredor ecológico de la Autopista Duarte
El principal río que pasa por el Municipio de Mao y toda la Provincia es el Yaque del Norte (35 km en la provincia), el cual atraviesa la provincia de sureste a noroeste. El otro río importante es el Mao (15 km en la provincia), tributario del Yaque del Norte. Otros ríos, de menor caudal y también tributario del Yaque del Norte, son el Ámina (11 km en la provincia) y el Gurabo (16 km), que constituye parcialmente el límite con la provincia Santiago Rodríguez. Otras cuencas hidrográficas que aportan agua a la provincia Valverde, son la cuenca del río Mao, que recarga la presa de Monción, que retroalimenta el Contraembalse y sus afluentes.
Los factores determinantes del clima de la Línea Noroeste son los mismos que afectan a toda la República Dominicana, con modificaciones que introducen la Cordillera Central y la Cordillera Septentrional. El régimen de lluvias está determinado en forma regular por el desplazamiento del frente intertropical que genera los vientos alisios, y por la influencia irregular de los dos sistemas anticiclónicos del Atlántico Norte y del continente norteamericano.  La temperatura muestra variaciones moderadas a lo largo del año. Las temperaturas extremas en general tampoco son exageradas, salvo en áreas bajas (Mao-Montecristi), que en ocasiones suelen sufrir los efectos del calentamiento adiabático de los vientos que proceden del otro lado de la Cordillera (efecto foehn).
3.2 Perfil de emisiones de GEI del país / región
República Dominicana tiene una emisión neta anual de 35,50 MtCO2e para el año 2020, reportada en el último inventario GEI oficial.
De acuerdo con la Secretaría de la CMNUCC, las emisiones del país representan un 0,075% de las emisiones globales, y en ese sentido representan un 0,91% de las emisiones de los países CAF, siendo el 11° país de CAF en orden de emisiones.
Las emisiones per cápita son 2,1 tCO2e/persona lo que representa un 15° país de CAF en orden de emisiones per capita.
Las emisiones por PBI son 450,21 tCO2e/MUSD, lo que representa un 16° país de CAF en orden de emisiones por PBI.
El perfil sectorial de emisiones del país corresponde a: 60.42% Energía; 26.31% Agricultura; -4.39% AFOLU; 9.89%; Procesos Industriales y Uso de Productos y 7.75%; Residuos
El perfil de la matriz energética del país es: 11,8% limpia y 88,2% fósil
El perfil de la matriz eléctrica del país es:  17,3% limpia y 82,7% fósil.
3.3 Perfil de vulnerabilidades, riesgos e impactos climáticos del país /región
La República Dominicana es uno de los países más vulnerables al cambio climático. Recientemente ha soportado severas temporadas de sequía y lluvias y el cambio climático está aumentando la probabilidad de desastres naturales como huracanes y otros peligros hidrometeorológicos. También corre el riesgo de sufrir peligros geológicos como terremotos. Es fundamental que el país fortalezca su capacidad de respuesta y recuperación de este tipo de situaciones. El bajo puntaje de vulnerabilidad y el bajo puntaje de preparación de República Dominicana lo ubican en el cuadrante inferior izquierdo de la Matriz ND-GAIN. En comparación con otros países, sus vulnerabilidades actuales son manejables, pero las mejoras en su preparación le ayudarán a adaptarse mejor a los desafíos futuros. La República Dominicana es el 93º país más vulnerable y el 115º país más preparado.

3.4 Perfil de vulnerabilidades, riesgos e impactos de otros desastres del país /región
Referente a la vulnerabilidad y riesgos a eventos no relacionados con el cambio climático, para el Municipio de Mao, Valverde, el peligro de terremoto se clasifica como medio de acuerdo con la información actualmente disponible. Esto significa que hay un 10 % de probabilidad de que en los próximos 50 años se produzca un terremoto potencialmente dañino en la zona de su proyecto. 




3.5 Perfil de conservación de biodiversidad, ecosistemas vulnerables y áreas protegidas
Ubicada en el punto de acceso de las islas del Caribe, la República Dominicana comparte la isla Hispaniola con Haití. Junto con Cuba, estas 2 islas son las que más contribuyen a la biodiversidad del Caribe. La República Dominicana se caracteriza por un alto nivel de endemismo, particularmente en lo que respecta a especies de reptiles (hay 5 especies por cada 2.000 km2), plantas vasculares y especies de aves. El país también cuenta con ecosistemas exclusivos del Caribe, como el lago Enriquillo (el lago más grande y de menor elevación de la región) y el pico montañoso más alto de la región, que se eleva a 2.000 metros sobre el nivel del mar. Los ecosistemas del país se están perdiendo debido a la destrucción del hábitat en su mayor parte, influenciada por la expansión de la producción agrícola y ganadera, el desarrollo del turismo (principalmente de playa) y la minería. Entre 1993 y 1997, los sectores agrícola y turístico crecieron a una tasa anual promedio del 5% y el 15%, respectivamente. Estas tasas han seguido aumentando. Los ecosistemas marinos costeros, especialmente las playas del país, son de gran importancia económica. El turismo representa una importante fuente de divisas, estimada en 4.400 millones de dólares estadounidenses al año y contribuye con más del 8,4% del PIB.


Elementos identificados en la operación que contribuyen a financiamiento verde
Los elementos incluidos en esta sección son una identificación preliminar que deberá ser analizadas en la etapa de evaluación y que para ser contabilizadas en el % de financiamiento verde, deberán estar debidamente integradas a la operación, y así reflejadas en los componentes, actividades y/o presupuesto.
4.1 Mitigación del cambio climático

Categoría
1.4. Agricultura, silvicultura y uso del suelo
Subcategoría
1.4.1. Agricultura
Actividad elegible
1.4.1.1. Reducción del consumo de energía en la tracción (por ejemplo, labranza eficiente), irrigación, y otros procesos agrícolas.
Evaluación
La provisión de agua por gravedad a través del canal permitirá que los usuarios del recurso puedan eliminar el uso de sistemas de bombeo forzado alimentados con combustible fósil, reduciendo las emisiones de CO2 derivadas de su quema.


Categoría
1.9. Otros temas transversales de la mitigación de GEI
Subcategoría
1.9.3.Capacidades y gestión del conocimiento
Actividad elegible
1.9.3.1. Educación, formación, capacitación y sensibilización sobre la mitigación del cambio climático / energía sostenible / transporte sostenible; la investigación de mitigación.
Evaluación
El proyecto contempla acciones no-estructurales complementarias a la inversión en infraestructura, como la capacitación a los agricultores para lograr un uso racional de las aguas y un buen manejo (operación y mantenimiento) de las obras a construir, sensibilizando sobre la eficiencia en el uso del recurso hídrico con su impacto positivo en la mitigación y adaptación al CC.



4.2 Adaptación del cambio climático

Categoría
2.1. Gestión y Suministro de Agua
Subcategoría
2.1.1. Medidas que contribuyen al acceso y uso eficiente del agua, en un contexto de déficit hídrico por sequías
Actividad elegible
2.1.1.3. Nuevas conexiones de agua potable y/o rehabilitación de las redes de distribución de agua, para mejorar la gestión de los recursos hídricos y adaptarse a la escasez de agua causada por el CC.
Evaluación
La operación involucra para la distribución de agua hacia las parcelas de los productores agrícolas tomas laterales en canales secundarios, con salidas transversales al eje del canal principal para facilitar el riego presurizado a nivel parcelario. 


Categoría
2.1. Gestión y Suministro de Agua
Subcategoría
2.1.1. Medidas que contribuyen al acceso y uso eficiente del agua, en un contexto de déficit hídrico por sequías
Actividad elegible
2.1.1.4. Acciones desde la demanda que ayuden a mejorar el consumo (eficiencia), reducir la huella hídrica y/o promover la economía circular, siempre y cuando sea en un contexto de impacto del cambio climático en la oferta y/o suministro de agua.
Evaluación
El proyecto contempla acciones no-estructurales complementarias a la inversión en infraestructura, como la capacitación a los agricultores para lograr un uso racional de las aguas y un buen manejo (operación y mantenimiento) de las obras a construir, sensibilizando sobre la eficiencia en el uso del recurso hídrico con su impacto positivo en la mitigación y adaptación al CC.


Categoría
2.2. Agricultura
Subcategoría
2.2.1. Medidas que contribuyen al mantenimiento o mejora en la productividad de los sistemas agropecuarios, pesqueros o acuícolas, en un contexto de cambio climático 
Actividad elegible
2.2.1.3. Infraestructura de irrigación nueva y/o rehabilitada en áreas rurales y vulnerables ante las sequías que contribuyan con el rendimiento de la agricultura.
Evaluación
La operación involucra para la distribución de agua hacia las parcelas de los productores agrícolas tomas laterales en canales secundarios, con salidas transversales al eje del canal principal para facilitar el riego presurizado a nivel parcelario que permitirá a la mejora en el rendimiento de la agricultura.



4.3 Conservación y/o uso sostenible del medio ambiente y la biodiversidad 
De manera preliminar no se identifican componentes de la operación que puedan ayudar a atribuir % de FV. Durante la evaluación se deberá confirmar/actualizar esta conclusión.


Elementos recomendados para aumentar el financiamiento verde de la operación a ser considerados durante la etapa de evaluación
Los elementos incluidos en esta sección son recomendaciones que deberán ser analizadas en la etapa de evaluación y para ser contabilizadas en el % de financiamiento verde, deberán estar debidamente integradas a la operación, y así reflejadas en los componentes, actividades y/o presupuesto.
5.1 Mitigación del cambio climático

Categoría
1.4. Agricultura, silvicultura y uso del suelo
Subcategoría
1.4.1. Agricultura
Actividad elegible
1.4.1.2. Proyectos agrícolas que mejoran los reservorios de carbono existentes (reducción en el uso de fertilizantes gestión de los pastizales, recolección y uso de bagazo, cáscara de arroz, u otros residuos agrícolas, técnicas de labranza reducida que aum
Evaluación
Se recomienda evaluar la oportunidad de una CT/componente de la operación para evaluar la factibilidad de implementar proyectos de agricultura sostenible en las parcelas beneficiarias de la operación.



5.2 Adaptación del cambio climático

Categoría
2.2. Agricultura
Subcategoría
2.2.1. Medidas que contribuyen al mantenimiento o mejora en la productividad de los sistemas agropecuarios, pesqueros o acuícolas, en un contexto de cambio climático 
Actividad elegible
2.2.1.2. Agricultura de conservación como el suministro de información sobre opciones de diversificación de cultivos (para adaptarse a una mayor vulnerabilidad en la productividad de los cultivos).
Evaluación
Se recomienda evaluar la oportunidad de una CT/componente de la operación para evaluar la viabilidad técnica y financiera de la diversificación de cultivos para aumentar la productividad y reducir la vulnerabilidad al CC.



5.3 Conservación y/o uso sostenible del medio ambiente y la biodiversidad 

Categoría
3.1. Puesta en valor del capital natural y sostenibilidad de actividades productivas
Subcategoría
3.1.1. Agua
Actividad elegible
3.1.1.1. Restauración y gestión integral de cuencas y subcuencas hidrográficas.
Evaluación
A pesar de que la operación también comprende las acciones de conservación de cuencas, remediación ambiental, y acciones de saneamiento y de gestión de residuos sólidos (apoyo al programa “basura cero” y otras acciones), se recomienda considerar un enfoque de gestión integral de la cuenca intervenida, adicionando los elementos de gestión faltantes para aumentar el % de FV. 


Categoría
3.1. Puesta en valor del capital natural y sostenibilidad de actividades productivas
Subcategoría
3.1.1. Agua
Actividad elegible
3.1.1.5. Dotación, distribución de agua o expansión de la red que demuestren el análisis de capacidad de la fuente y, de ser necesario, considere medidas compensatorias para la recarga de acuíferos.
Evaluación
Se recomienda incluir en los estudios de ingeniería para la infraestructura complementaria los análisis de capacidad de la cuenca/subcuenca; así como aquellas medidas que permitan recargar los acuiferos intervenidos para incrementar el % de FV.


Categoría
3.1. Puesta en valor del capital natural y sostenibilidad de actividades productivas
Subcategoría
3.1.1. Agua
Actividad elegible
3.1.1.7. Modernización y eficiencia en sistemas de irrigación y drenaje agrícola.
Evaluación
Se recomienda incluir en la operación una CT para estudios de factibilidad y/o recursos para la implementación de tecnologías de riego más eficientes (i.e programación de riego, irrigación por goteo, riego hidropónico, IA para pronósticos agroclimáticos, drenaje natural etc.) en las parcelas beneficiarias.



5.4 Recomendaciones estratégicas para enverdecimiento.
Los componentes actuales de la operación; así como aquellos propuestos para su enveerdecimiento se alinean con los siguientes esfuerzos realizados por República Dominicana para la gestión integral del agua, la agricultura sostenible y la mitigación y adaptación al CC:
Acuerdo para la mejora del uso del Agua en la Agricultura FAO-RD
Programa múlti-sectorial de apoyo al manejo de aguas y suelos en la república dominicana IICA-RD
Pacto Nacional del Agua en RD
Proyecto Agricultura Resiliente y Gestión Integrada de Recursos Hídricos (PARGIRH)
Propuesta institucional: Reforma del sector agua
Programa FAO-RD: Manejo Integrado de cuencas, uso eficiente de los recursos naturales y gestión de riesgos y cambio climático.
5.5 Otras recomendaciones de actividades de enverdecimiento:
NA


Análisis cualitativo exploratorio sobre riesgo de “impactos significativos”
A continuación, se presenta un análisis preliminar exploratorio de posibles impactos significativos en las tres categorías generales de la Guía.  Dicho análisis deberá ser profundizado en la etapa de evaluación.
De manera preliminar no se identifica que algun componente de la operación aportando al FV en mitigación, adaptación o medio ambiente y biodiversidad tenga potencialidad de causar daño significativo a otro de los componentes. Lo anterior se deberá confirmar durante la evaluación de la operación.


Recomendación de estudios adicionales a ser realizados para justificar el financiamiento verde en la etapa de evaluación o posterior
Se recomienda (en caso de no haber sido considerado aún) la realización de los siguientes estudios:
Inclusión de la variable de cambio climático (riesgo y vulnerabilidad) en los diseños a ser implementados tanto en las obras del canal principal, como de las tomas laterales hacia las parcelas.
Identificación y factibilidad técnica y económica de la implementación de prácticas de economía circular durante las etapas de construcción y opración de la infraestructura propuesta.
Identificación y factibilidad técnica y económica de la implementación de prácticas menos intensas en emisiones de GEI durante las fases de consrucción y operación de la infraestructura.
Cuantificación de la huella de carbono de la operación durante las fases de construcción y operación; así como la identificación de medidas para su compensación.

 """},
        {"role": "user", "content": f"{data}"}
    ]

    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=chat_prompt,
        temperature=0.5,
    )

    logging.info("OpenAI completado")

    answer = completion.choices[0].message.content
    logging.info(answer)
    return answer


def getResumenBiodiversidad(data):

    chat_prompt = [
        {"role": "system", "content": """Eres un asistente experto en el análisis y aprobación de financiamiento en proyectos ecológicos, se requiere que generes un json diccionario del contexto de biodiversidad del proyecto  con la información que te proporcione el usuario, retorna el resultado unicamente como el siguiente json con la misma estructura unicamnete cambiando el campo content:
             [
  {
  "title": "Marco de políticas y objetivos de política (Biodiversidad y clima)",
  "content": " Mediante la Ley 202 del 2004, la República Dominicana crea el Sistema Nacional de Áreas Protegidas, que es un conjunto de zonas naturales, coordinadas dentro de sus propias categorías de manejo, y que poseen características, objetivos y manejos muy precisos y especializados. Dentro de los límites territoriales de la provincia Valverde inciden siete (7) áreas protegidas, agrupadas en tres (3) categorías de manejo: Paisaje Protegido, Parque Nacional y Reserva Natural, Ocupando una superficie de 128.45 km², equivalente al 16% de la superficie total que están dentro del Sistema Nacional de Áreas Protegidas. Las principales áreas protegidas del Municipio de Mao son: - Área Protegida Furnia de Gurabo -Vía Panorámica Entrada de Mao -Reserva Forestal del río Cana -Parque Ecológico Cincuentenario -Parque Nacional Amina -Parque Nacional Piki Lora -Corredor ecológico de la Autopista Duarte -El principal río que pasa por el Municipio de Mao y toda la Provincia es el Yaque del Norte (35 km en la provincia), el cual atraviesa la provincia de sureste a noroeste. El otro río importante es el Mao (15 km en la provincia), tributario del Yaque del Norte. Otros ríos, de menor caudal y también tributario del Yaque del Norte, son el Ámina (11 km en la provincia) y el Gurabo (16 km), que constituye parcialmente el límite con la provincia Santiago Rodríguez. Otras cuencas hidrográficas que aportan agua a la provincia Valverde, son la cuenca del río Mao, que recarga la presa de Monción, que retroalimenta el Contraembalse y sus afluentes. -Los factores determinantes del clima de la Línea Noroeste son los mismos que afectan a toda la República Dominicana, con modificaciones que introducen la Cordillera Central y la Cordillera Septentrional. El régimen de lluvias está determinado en forma regular por el desplazamiento del frente intertropical que genera los vientos alisios, y por la influencia irregular de los dos sistemas anticiclónicos del Atlántico Norte y del continente norteamericano.  La temperatura muestra variaciones moderadas a lo largo del año. Las temperaturas extremas en general tampoco son exageradas, salvo en áreas bajas (Mao-Montecristi), que en ocasiones suelen sufrir los efectos del calentamiento adiabático de los vientos que proceden del otro lado de la Cordillera (efecto foehn)"
},
{
  "title": "Perfil de emisión de GEI de país / región",
  "content": "República Dominicana tiene una emisión neta anual de 35,50 MtCO2e para el año 2020, reportada en el último inventario GEI oficial; según la Secretaría de la CMNUCC, estas emisiones representan un 0,075% de las emisiones globales y un 0,91% de las emisiones de los países CAF, siendo el 11° país de CAF en orden de emisiones; las emisiones per cápita son 2,1 tCO2e/persona, ubicándose como el 15° país de CAF en orden de emisiones per cápita; las emisiones por PBI son 450,21 tCO2e/MUSD, siendo el 16° país de CAF en este indicador; el perfil sectorial de emisiones es: 60,42% Energía, 26,31% Agricultura, -4,39% AFOLU, 9,89% Procesos Industriales y Uso de Productos, y 7,75% Residuos; el perfil de la matriz energética es 11,8% limpia y 88,2% fósil; y el perfil de la matriz eléctrica es 17,3% limpia y 82,7% fósil."
},
{
  "title": "Perfil de vulnerabilidades, riesgos e impactos de otros desastres del país / región",
  "content": "La República Dominicana es uno de los países más vulnerables al cambio climático. Recientemente ha soportado severas temporadas de sequía y lluvias y el cambio climático está aumentando la probabilidad de desastres naturales como huracanes y otros peligros hidrometeorológicos. También corre el riesgo de sufrir peligros geológicos como terremotos. Es fundamental que el país fortalezca su capacidad de respuesta y recuperación de este tipo de situaciones. El bajo puntaje de vulnerabilidad y el bajo puntaje de preparación de República Dominicana lo ubican en el cuadrante inferior izquierdo de la Matriz ND-GAIN. En comparación con otros países, sus vulnerabilidades actuales son manejables, pero las mejoras en su preparación le ayudarán a adaptarse mejor a los desafíos futuros. La República Dominicana es el 93º país más vulnerable y el 115º país más preparado."
},
{
  "title": "Perfil de conservación de biodiversidad, ecosistema vulnerables y áreas protegidas",
  "content": "Ubicada en el punto de acceso de las islas del Caribe, la República Dominicana comparte la isla Hispaniola con Haití. Junto con Cuba, estas 2 islas son las que más contribuyen a la biodiversidad del Caribe. La República Dominicana se caracteriza por un alto nivel de endemismo, particularmente en lo que respecta a especies de reptiles (hay 5 especies por cada 2.000 km2), plantas vasculares y especies de aves. El país también cuenta con ecosistemas exclusivos del Caribe, como el lago Enriquillo (el lago más grande y de menor elevación de la región) y el pico montañoso más alto de la región, que se eleva a 2.000 metros sobre el nivel del mar. Los ecosistemas del país se están perdiendo debido a la destrucción del hábitat en su mayor parte, influenciada por la expansión de la producción agrícola y ganadera, el desarrollo del turismo (principalmente de playa) y la minería. Entre 1993 y 1997, los sectores agrícola y turístico crecieron a una tasa anual promedio del 5% y el 15%, respectivamente. Estas tasas han seguido aumentando. Los ecosistemas marinos costeros, especialmente las playas del país, son de gran importancia económica. El turismo representa una importante fuente de divisas, estimada en 4.400 millones de dólares estadounidenses al año y contribuye con más del 8,4% del PIB."
}

]
 """},
        {"role": "user", "content": f"{data}"}
    ]

    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=chat_prompt,
        temperature=0.5,
    )

    logging.info("OpenAI completado")

    answer = completion.choices[0].message.content
    logging.info(answer)
    return answer
