JSON_EXAMPLE_RESPONSE = """[
            {
                "projects": [
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "dfd6a35b-74a2-424c-8af1-89658d68e33b",
                "dfd6a35b-74a2-424c-8af1-89658d68e33b"
                ],
                "reason": "Estos proyectos se pueden agrupar debido a ......"
            },
             {
                "projects": [
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "CAX000",
                "dfd6a35b-74a2-424c-8af1-89658d68e33b",
                "dfd6a35b-74a2-424c-8af1-89658d68e33b"
                ],
                "reason": "Estos proyectos se pueden agrupar debido a ......"
            }
        ]
        """

def get_group_by_projects_prompt(data):
    PROMPT_GROUP_BY_PROJECTS = [
                {"role": "system", "content": "Eres un asistente especializado en el analisis y comparacion de proyectos de impacto ambiental brindando los resultados en formato JSON"},
                {"role": "user", "content": f"""Realiza un análisis de los proyectos {data}. Debes agrupar los proyectos siguiendo estos criterios:

            Agrupa aquellos proyectos que tengan el mismo PAÍS o ORGANIZMO EJECUTOR o PRESTATARIOS.
            Solo debes agrupar los proyectos si se tienen dos o mas similares
            Considera el campo RESUMEN para identificar las características relevantes de cada proyecto.
            En el campo reason de la respuesta debes incluir un analisis descripotivo de por que realizaste la agrupacion de los proyectos y tambien si estos proyectos tienen el mismo impacto ambiental o el proyecto tiene aspectos ambientales similares
            Utiliza únicamente la información proporcionada, sin agregar datos externos. La respuesta debe estar en español y en formato JSON, siguiendo exactamente la siguiente estructura: {JSON_EXAMPLE_RESPONSE}. Asegúrate de que el JSON sea claro, conciso, utilice siempre comillas dobles y esté correctamente formateado. Responde solo con el JSON, sin saltos de línea ni texto adicional, y sin incluir la palabra "JSON" al inicio o al final."""}
            ]
    return PROMPT_GROUP_BY_PROJECTS
