import os
from azure.cosmos import CosmosClient, exceptions
import logging
import re
from collections import Counter
import unicodedata
from datetime import datetime

# Configuración de conexión
endpoint = os.environ["COSMOS_DB_URI"]
key = os.environ["COSMOS_DB_KEY"]
database_name = os.environ["COSMOS_DB_NAME"]
container_name = os.environ["COSMOS_DB_CONTAINER_NAME"]

logging.basicConfig(level=logging.DEBUG)

client = CosmosClient(endpoint, key)
try:
    # Probar la conexión a la base de datos

    database = client.get_database_client(database_name)
    logging.info('Conexión exitosa')
except exceptions.CosmosHttpResponseError as e:
    logging.error(f'Error al conectar: {e.message}')
except Exception as e:
    logging.error(f'Error al conectar: {str(e)}')


def normalize_text(text):
    # Convertimos el texto a su forma normalizada sin acentos
    return ''.join(
        c for c in unicodedata.normalize('NFD', text)
        if unicodedata.category(c) != 'Mn'
    ).lower()


def item_cosmos(nombreProyecto=None, pais=None, proyecto=None, tipoProyecto=None, fechaInicio=None, criterio=None, operacion=None, financiado=None, fechaFin=None, actividad=None, categoriaP=None, categoria=None, subCategoria=None, fase=None, indicadores=None, subIndicadores=None, criterios=None):
    try:
        container = client.get_database_client(database_name).get_container_client(container_name)
        excluded_fields = ["_rid", "_self", "_etag", "_attachments", "_ts"]
        data = container.read_all_items()
        items = []
        for item in data:
            filtered_item = {key: value for key, value in item.items() if key not in excluded_fields}

            if "evaluacion" in filtered_item:
                project = filtered_item["evaluacion"]
            elif "originacion" in filtered_item:
                project = filtered_item["originacion"]
            else:
                continue

            if fase and fase not in project.get("FASE", ""):
                continue

            # Filtro por país
            if pais and project.get("PAIS") != pais:
                continue

            # Filtro por id del proyecto (proyecto)
            if proyecto and proyecto.lower() not in project.get("PROYECTO", "").lower():
                continue

            # Filtro por tipo de proyecto
            if tipoProyecto and tipoProyecto not in project.get("TIPO DE PROYECTO", ""):
                continue

            # Filtro por nombre del proyecto
            if nombreProyecto and nombreProyecto.lower() not in project.get("NOMBRE DEL PROYECTO", "").lower():
                continue

            # Para las fechas sin hora (fechaInicio, fechaFin)
            fecha_format = "%d/%m/%Y"
            # Para la fecha con hora (fecha_creacion)
            fecha_hora_format = "%d/%m/%Y %H:%M"
            logging.info("Convertir Fechas a datetime")

            # Convertir las fechas de inicio y fin
            if fechaInicio:
                fechaInicio2 = datetime.strptime(fechaInicio, fecha_format)
            if fechaFin:
                fechaFin2 = datetime.strptime(fechaFin, fecha_format)

            # Si fechaInicio y fechaFin son iguales, ajusta para incluir todo el día
            if fechaInicio and fechaFin and fechaInicio == fechaFin:
                fechaInicio2 = fechaInicio2.replace(hour=0, minute=0)
                fechaFin2 = fechaFin2.replace(hour=23, minute=59)

            logging.info(fechaInicio)

            # Convertir la fecha de creación (que incluye hora)
            fecha_creacion = project.get("FECHA CREACION", "01/01/1900 00:00")

            if fecha_creacion != "01/01/1900 00:00" and (fechaInicio or fechaFin):
                fecha_creacion = datetime.strptime(fecha_creacion, fecha_hora_format)

                # Filtros
                if fechaInicio and fechaFin:
                    logging.info("Fecha inicio y fin")
                    logging.info(fechaFin2)
                    logging.info(fechaInicio2)
                    if not (fechaInicio2 <= fecha_creacion <= fechaFin2):
                        continue

                if fechaInicio and fecha_creacion < fechaInicio2:
                    logging.info("Fecha creacion menor a fecha inicio")
                    continue

            # Filtro por elegibilidad
            if criterio and criterio not in project.get("ELEGIBLE / NO ELEGIBLE", ""):
                continue

            # Filtro por operación soberana
            if operacion and operacion not in project.get("ES SOBERANO / NO SOBERANO", ""):
                continue

            # Filtro por financiación
            if financiado and financiado not in project.get("FINANCIADO POR?", ""):
                continue

            logging.info(f'Actividades: {actividad}')
           # Filtro por actividad específica
            if actividad:
                actividades_proyecto = project.get("ACTIVIDADES ELEGIBLES QUE APLICAN AL PROYECTO", "")
                actividades_list = [item.strip() for item in actividades_proyecto.split('|')]
                logging.info(f'Actividades: {actividades_list}')
                # Verifica si la actividad.strip() está en la lista de actividades
                if actividad.strip().lower() not in [act.lower() for act in actividades_list]:
                    continue

            if categoriaP:
                categoriaP_proyecto = project.get("CATEGORÍAS PRINCIPALES DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA", "")
                categoriaP_list = [item.strip() for item in categoriaP_proyecto.split(',')]
                logging.info(f'categoria: {categoriaP_list}')
                # Verifica si la actividad.strip() está en la lista de actividades
                if categoriaP.strip().lower() not in [act.lower() for act in categoriaP_list]:
                    continue

            if categoria:
                categoria_proyecto = project.get("CATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA", "")
                categoria_list = [item.strip() for item in categoria_proyecto.split('|')]
                logging.info(f'categoria: {categoria_list}')
                # Verifica si la actividad.strip() está en la lista de actividades
                if categoria.strip().lower() not in [act.lower() for act in categoria_list]:
                    continue

            if subCategoria:
                subCategoria_proyecto = project.get("SUBCATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA", "")
                subCategoria_list = [item.strip() for item in subCategoria_proyecto.split('|')]
                logging.info(f'subCat: {subCategoria_list}')
                # Verifica si la actividad.strip() está en la lista de actividades
                if subCategoria.strip().lower() not in [act.lower() for act in subCategoria_list]:
                    continue

            if indicadores:
                indicadores_proyecto = project.get("INDICADORES", "")
                indicadores_list = [item.strip() for item in indicadores_proyecto.split('|')]
                logging.info(f'subCat: {indicadores_list}')
                # Verifica si la actividad.strip() está en la lista de actividades
                if indicadores.strip().lower() not in [act.lower() for act in indicadores_list]:
                    continue

            if subIndicadores:
                subindicadores_proyecto = project.get("SUBINDICADORES", "")
                subindicadores_list = [item.strip() for item in subindicadores_proyecto.split('|')]
                logging.info(f'subCat: {subindicadores_list}')
                # Verifica si la actividad.strip() está en la lista de actividades
                if subIndicadores.strip().lower() not in [act.lower() for act in subindicadores_list]:
                    continue

            if criterios:
                criterios_proyecto = project.get("CRITERIOS DE ELEGIBILIDAD", "")
                criterios_list = [item.strip() for item in criterios_proyecto.split('|')]
                logging.info(f'subCat: {criterios_list}')
                # Verifica si la actividad.strip() está en la lista de actividades
                if criterios.strip().lower() not in [act.lower() for act in criterios_list]:
                    continue

            items.append(project)
            logging.info(f'Proyectos filtrados por actividad "{actividad}": {len(items)}')

        return items
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f'[item_cosmos] - Error al conectar: {e.message}')
        return f'Error al conectar: {e.message}'
    except Exception as e:
        logging.error(f'[item_cosmos] - Error: {str(e)}')
        return f'Error al conectar: {str(e)}'


def convertir_a_float(valor):
    try:
        # Eliminar texto adicional y reemplazar coma con punto decimal
        valor = valor.replace("USD", "").replace("MM", "").replace(",", "").strip()
        return float(valor)
    except ValueError:
        return 0


def obtener_monto_real(monto):
    # Convertimos el valor de `monto` a una cadena
    monto_str = str(monto)
    match = re.search(r'([\d,.]+)\s*MM', monto_str)
    if match:
        # Convertimos el número encontrado a float
        return float(match.group(1).replace(",", ""))
    else:
        return 0.0


def extract_kpis(data):
    kpi = {
        "proyectosPorPais": {},
        "monto_total_prestamos": 0,
        "monto_financiamiento_caf": 0,
        "monto_financiamiento_verde": 0,
        "monto_total_proyecto": 0,
        "proyectos_verdes": 0,
        "proyectos_aprovados": 0,
        "total_proyectos": len(data),
        "porcentaje_financiamiento_verde": 0,
        "porcentajeIncrementoVerde": 0,
        "financiados_internacionales": {"Sí": 0, "No": 0},
        "tipos_de_proyecto": {},
        "soberanos": {"Soberano": 0, "No soberano": 0},
        "incremento_verde": 0,
        "elegibles": {"Elegible": 0, "No elegible": 0},
        "gerencia": Counter(),
        "fuentes": Counter(),
        "enverdecimiento": Counter({"Sí": 0, "No": 0}),
        "indicadores_y_subindicadores": Counter(),
        "SUBINDICADORES": Counter(),
        "criterios_de_elegibilidad": Counter(),
        "CategoriasPrincipales": Counter(),
        "SubCategorias": Counter(),
        "proyectosPorCategoria": Counter(),
        "ACTIVIDADES_PROYECTO": Counter(),

    }

    # Contadores para proyectos por país y criterios de financiamiento verde
    pais_contador = Counter()
    proyectos_verdes = 0
    proyectos_aprovados = 0
    proyectos_socios = 0
    monto_total_prestamos = 0
    monto_financiamiento_caf = 0
    monto_financiamiento_verde = 0
    monto_total_proyecto = 0
    total_porcentaje_financiamiento_verde = 0
    total_incremento_verde = 0
    total_proyectos_con_incremento = 0

    def parse_to_float(value):
        """Convierte un valor a float, manejando valores no numéricos como 'N/A'."""
        try:
            if value in ['N/A', None, '']:
                return 0.0  # Devuelve 0 si es 'N/A' o un valor vacío
            return float(value)
        except ValueError:
            return 0.0

    # Recorrer los datos para calcular los KPIs
    for project in data:
        # KPI 1: Conteo de proyectos por país
        pais = project.get("PAIS")
        estado_elegibilidad = project.get("ELEGIBLE / NO ELEGIBLE")
        incremento_verde_str = project.get("PORCENTAJE FINANCIAMIENTO VERDE", "0%")
        tipo_proyecto = project.get("TIPO DE PROYECTO")
        proyectosFinanciadosInternacionales = project.get("FINANCIADOS CON FONDOS INTERNACIONALES", "No").strip().lower()
        indicadoresCore = project.get("INDICADORES", "sin indicadores")
        subIndicadoresCore = project.get("SUBINDICADORES", "sin indicadores")
        criteriosEle = project.get("CRITERIOS DE ELEGIBILIDAD", "sin criterios")
        categoria = project.get("CATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA", "Sin categoría")
        actividades = project.get("ACTIVIDADES ELEGIBLES QUE APLICAN AL PROYECTO", "")
        categoriasPrincipales = project.get("CATEGORÍAS PRINCIPALES DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA", "")
        subCategorias = project.get("SUBCATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA", "")

        if proyectosFinanciadosInternacionales in ["financiado con fondos internacionales", "si", "sí"]:
            kpi["financiados_internacionales"]["Sí"] += 1
        elif proyectosFinanciadosInternacionales in ["no", "no especificado"]:
            kpi["financiados_internacionales"]["No"] += 1

        if estado_elegibilidad == "Elegible":
            kpi["elegibles"]["Elegible"] += 1
        elif estado_elegibilidad == "No elegible":
            kpi["elegibles"]["No elegible"] += 1

        gerencia = project.get("GERENCIA")
        if gerencia:
            kpi["gerencia"][gerencia] += 1

        # Inicializar el KPI de fuentes si no existe
        if "fuentes" not in kpi:
            kpi["fuentes"] = Counter()

        # Iterar sobre las claves del proyecto para encontrar las fuentes
        for key, value in project.items():
            if key.startswith("MONTO FUENTE"):  # Verifica que la clave sea relevante
                # Extraer el nombre de la fuente a partir de la clave
                nombre = key.replace("MONTO FUENTE ", "").replace("(USD)", "").strip()
                valor = value

                # Sumar el valor al KPI
                kpi["fuentes"][nombre] += valor

        # Contar ENVERDECIMIENTO (Sí/No)
        enverdecimiento = project.get("ENVERDECIMIENTO")
        if enverdecimiento:
            # Para asegurar la consistencia
            enverdecimiento = enverdecimiento.strip().capitalize()
            if enverdecimiento in kpi["enverdecimiento"]:
                kpi["enverdecimiento"][enverdecimiento] += 1

        for ind in indicadoresCore.split("|"):
            sub_indicadores = ind.split("|")
            for sub_ind in sub_indicadores:
                sub_ind = sub_ind.strip()
                if sub_ind:
                    if sub_ind in kpi["indicadores_y_subindicadores"]:
                        kpi["indicadores_y_subindicadores"][sub_ind] += 1
                    else:
                        kpi["indicadores_y_subindicadores"][sub_ind] = 1

        for ind in subIndicadoresCore.split("|"):
            sub_indicadores = ind.split("|")
            for sub_ind in sub_indicadores:
                sub_ind = sub_ind.strip()
                if sub_ind:
                    if sub_ind in kpi["SUBINDICADORES"]:
                        kpi["SUBINDICADORES"][sub_ind] += 1
                    else:
                        kpi["SUBINDICADORES"][sub_ind] = 1

        for crit in criteriosEle.split("|"):
            crite = crit.split("|")
            for criteros in crite:
                criteros = criteros.strip()
                if criteros:
                    if criteros in kpi["criterios_de_elegibilidad"]:
                        kpi["criterios_de_elegibilidad"][criteros] += 1
                    else:
                        kpi["criterios_de_elegibilidad"][criteros] = 1

        for catP in categoriasPrincipales.split("|"):
            CatPrincipal = catP.split("|")
            for cat_prin in CatPrincipal:
                cat_prin = cat_prin.strip()
                if cat_prin:
                    if cat_prin in kpi["CategoriasPrincipales"]:
                        kpi["CategoriasPrincipales"][cat_prin] += 1
                    else:
                        kpi["CategoriasPrincipales"][cat_prin] = 1

        for subCat in subCategorias.split("|"):
            subCatP = subCat.split("|")
            for sub_cat_prin in subCatP:
                sub_cat_prin = sub_cat_prin.strip()
                if sub_cat_prin:
                    if sub_cat_prin in kpi["SubCategorias"]:
                        kpi["SubCategorias"][sub_cat_prin] += 1
                    else:
                        kpi["SubCategorias"][sub_cat_prin] = 1

        for cat in categoria.split("|"):
            cat = cat.strip()
            if cat:
                if cat in kpi["proyectosPorCategoria"]:
                    kpi["proyectosPorCategoria"][cat] += 1
                else:
                    kpi["proyectosPorCategoria"][cat] = 1

        for act in actividades.split("|"):
            act = act.strip()
            if act:
                if act in kpi["ACTIVIDADES_PROYECTO"]:
                    kpi["ACTIVIDADES_PROYECTO"][act] += 1
                else:
                    kpi["ACTIVIDADES_PROYECTO"][act] = 1

        if pais:
            pais_contador[pais] += 1

        try:
            monto_total_prestamos += parse_to_float(project.get("MONTO PRÉSTAMO (USD)", 0))
            monto_financiamiento_caf += parse_to_float(project.get("MONTO FINANCIAMIENTO CAF (USD)", 0))
            monto_financiamiento_verde += parse_to_float(project.get("MONTO FINANCIAMIENTO VERDE (USD)") or project.get("FINANCIAMIENTO VERDE"))
            monto_total_proyecto += parse_to_float(project.get("MONTO TOTAL DEL PROYECTO (USD)", 0))
        except ValueError:
            pass
        if project.get("¿CUMPLE LOS CRITERIOS DE FINANCIAMIENTO VERDE?") == "Sí":
            proyectos_verdes += 1

        if "DEC" in project.get("TIPO DOCUMENTO", []):
            proyectos_aprovados += 1

        if project.get("PROYECTOS VERDES FINANCIADOS POR SOCIOS") == "Sí":
            proyectos_socios += 1

        try:
            porcentaje_financiamiento = float(project.get("PORCENTAJE FINANCIAMIENTO VERDE", 0))
            total_porcentaje_financiamiento_verde += porcentaje_financiamiento
        except ValueError:
            pass

        if tipo_proyecto:
            if tipo_proyecto not in kpi["tipos_de_proyecto"]:
                kpi["tipos_de_proyecto"][tipo_proyecto] = 1
            else:
                kpi["tipos_de_proyecto"][tipo_proyecto] += 1

        soberano_status = project.get("ES SOBERANO / NO SOBERANO")
        if soberano_status == "Soberano":
            kpi["soberanos"]["Soberano"] += 1
        elif soberano_status == "No soberano":
            kpi["soberanos"]["No soberano"] += 1

        if incremento_verde_str == "No especificado en el documento":
            incremento_verde = 0
        else:
            try:
                if isinstance(incremento_verde_str, str):
                    incremento_verde = float(incremento_verde_str.replace('%', '').strip())
                else:
                    incremento_verde = incremento_verde_str
                total_incremento_verde += incremento_verde
                total_proyectos_con_incremento += 1
            except ValueError:
                logging.warning(f'Error al procesar el porcentaje: {incremento_verde_str}')

    if kpi["total_proyectos"] > 0:
        kpi["porcentaje_financiamiento_verde"] = total_porcentaje_financiamiento_verde / kpi["total_proyectos"]

    if total_proyectos_con_incremento > 0:
        kpi["porcentajeIncrementoVerde"] = total_incremento_verde / total_proyectos_con_incremento
    else:
        kpi["porcentajeIncrementoVerde"] = 0

    kpi["proyectosPorPais"] = dict(pais_contador)
    kpi["monto_total_prestamos"] = monto_total_prestamos
    kpi["monto_financiamiento_caf"] = monto_financiamiento_caf
    kpi["monto_financiamiento_verde"] = monto_financiamiento_verde
    kpi["monto_total_proyecto"] = monto_total_proyecto
    kpi["proyectos_verdes"] = proyectos_verdes
    kpi["proyectos_aprovados"] = proyectos_aprovados
    kpi["proyectos_socios"] = proyectos_socios

    return kpi


def filters():
    try:
        container = client.get_database_client(database_name).get_container_client(container_name)
        paises = set()
        proyectos_por_tipo = set()
        financiamiento = set()
        actividad = set()
        ids = set()
        categoriasPrincipales = set()
        categorias = set()
        subCategorias = set()
        indicadoresGef = set()
        subIndicadores = set()
        criterios = set()

        for item in container.read_all_items():
            ids.add(item.get("id"))
            # Obtener países únicos
            if "evaluacion" in item and "PAÍS" in item["evaluacion"]:
                paises.add(item["evaluacion"]["PAÍS"])
            elif "originacion" in item and "PAÍS" in item["originacion"]:
                paises.add(item["originacion"]["PAÍS"])

        for item in container.read_all_items():
            if "evaluacion" in item:
                proyectos_por_tipo.add(item["evaluacion"].get("TIPO DE PROYECTO", item["evaluacion"].get("CATEGORÍA DE FINANCIAMIENTO VERDE", "N/A")))
                financiamiento.add(item["evaluacion"].get("FINANCIADO POR?"))
            elif "originacion" in item:
                proyectos_por_tipo.add(item["originacion"].get("TIPO DE PROYECTO", item["originacion"].get("CATEGORÍA DE FINANCIAMIENTO VERDE", "N/A")))
                financiamiento.add(item["originacion"].get("FINANCIADO POR?"))

        for item in container.read_all_items():
            if "evaluacion" in item and "ACTIVIDADES ELEGIBLES QUE APLICAN AL PROYECTO" in item["evaluacion"]:
                actividades = item["evaluacion"]["ACTIVIDADES ELEGIBLES QUE APLICAN AL PROYECTO"]
                for actividad_item in actividades.split('|'):
                    actividad.add(actividad_item.strip())
            elif "originacion" in item and "ACTIVIDADES ELEGIBLES QUE APLICAN AL PROYECTO" in item["originacion"]:
                actividades = item["originacion"]["ACTIVIDADES ELEGIBLES QUE APLICAN AL PROYECTO"]
                for actividad_item in actividades.split('|'):
                    actividad.add(actividad_item.strip())

        for item in container.read_all_items():
            if "evaluacion" in item and "CATEGORÍAS PRINCIPALES DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA" in item["evaluacion"]:
                catP = item["evaluacion"]["CATEGORÍAS PRINCIPALES DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA"]
                for catP_item in catP.split('|'):
                    categoriasPrincipales.add(catP_item.strip())
            elif "originacion" in item and "CATEGORÍAS PRINCIPALES DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA" in item["originacion"]:
                catP = item["originacion"]["CATEGORÍAS PRINCIPALES DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA"]
                for categoriasPrincipales_item in catP.split('|'):
                    categoriasPrincipales.add(categoriasPrincipales_item.strip())

        for item in container.read_all_items():
            if "evaluacion" in item and "CATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA" in item["evaluacion"]:
                cat = item["evaluacion"]["CATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA"]
                for categorias_item in cat.split('|'):
                    categorias.add(categorias_item.strip())
            elif "originacion" in item and "CATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA" in item["originacion"]:
                actividades = item["originacion"]["CATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA"]
                for categorias_item in actividades.split('|'):
                    categorias.add(categorias_item.strip())

        for item in container.read_all_items():
            if "evaluacion" in item and "SUBCATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA" in item["evaluacion"]:
                catSub = item["evaluacion"]["SUBCATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA"]
                for subCategorias_item in catSub.split('|'):
                    subCategorias.add(subCategorias_item.strip())
            elif "originacion" in item and "SUBCATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA" in item["originacion"]:
                catSub = item["originacion"]["SUBCATEGORÍAS DE FINANCIAMIENTO VERDE EN LAS QUE CLASIFICA"]
                for subCategorias_item in catSub.split('|'):
                    subCategorias.add(subCategorias_item.strip())

        for item in container.read_all_items():
            if "evaluacion" in item and "INDICADORES" in item["evaluacion"]:
                indicadoresG = item["evaluacion"]["INDICADORES"]
                for indicadoresG_item in indicadoresG.split('|'):
                    indicadoresGef.add(indicadoresG_item.strip())
            elif "originacion" in item and "INDICADORES" in item["originacion"]:
                indicadoresG = item["originacion"]["INDICADORES"]
                for indicadoresG_item in indicadoresG.split('|'):
                    indicadoresGef.add(indicadoresG_item.strip())

        for item in container.read_all_items():
            if "evaluacion" in item and "SUBINDICADORES" in item["evaluacion"]:
                subIndicadoresG = item["evaluacion"]["SUBINDICADORES"]
                for subIndicadoresG_item in subIndicadoresG.split('|'):
                    subIndicadores.add(subIndicadoresG_item.strip())
            elif "originacion" in item and "SUBINDICADORES" in item["originacion"]:
                subIndicadoresG = item["originacion"]["SUBINDICADORES"]
                for subIndicadoresG_item in subIndicadoresG.split('|'):
                    subIndicadores.add(subIndicadoresG_item.strip())

        for item in container.read_all_items():
            if "evaluacion" in item and "CRITERIOS DE ELEGIBILIDAD" in item["evaluacion"]:
                crite = item["evaluacion"]["CRITERIOS DE ELEGIBILIDAD"]
                for crite_item in crite.split('|'):
                    criterios.add(crite_item.strip())
            elif "originacion" in item and "CRITERIOS DE ELEGIBILIDAD" in item["originacion"]:
                crite = item["originacion"]["CRITERIOS DE ELEGIBILIDAD"]
                for crite_item in crite.split('|'):
                    criterios.add(crite_item.strip())

        # Ordenar alfabéticamente con 'N/A' al final
        def sort_with_alpha_numeric_na_last(data):
            return sorted(data, key=lambda x: ( x == "N/A", not x, x.lower() if isinstance(x, str) else x))

        return {
            "ids": sort_with_alpha_numeric_na_last(ids),
            "paises": sort_with_alpha_numeric_na_last(paises),
            "proyectosPorTipo": sort_with_alpha_numeric_na_last(proyectos_por_tipo),
            "financiamiento": sort_with_alpha_numeric_na_last(financiamiento),
            "actividad": sort_with_alpha_numeric_na_last(actividad),
            "categoriasPrincipales": sort_with_alpha_numeric_na_last(categoriasPrincipales),
            "categorias": sort_with_alpha_numeric_na_last(categorias),
            "subCategorias": sort_with_alpha_numeric_na_last(subCategorias),
            "indicadoresGef": sort_with_alpha_numeric_na_last(indicadoresGef),
            "subIndicadores": sort_with_alpha_numeric_na_last(subIndicadores),
            "criterios": sort_with_alpha_numeric_na_last(criterios),
        }
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f'Error al conectar: {e.message}')
        return {"error": f'Error al conectar: {e.message}'}
    except Exception as e:
        logging.error(f'Error al conectar: {str(e)}')
        return {"error": f'Error al conectar: {str(e)}'}
