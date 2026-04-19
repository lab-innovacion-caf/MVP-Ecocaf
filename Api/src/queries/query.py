GET_ALL_PROJECTS = """
    SELECT 
        c.id, 
        IIF(IS_DEFINED(c.originacion) AND IS_DEFINED(c.originacion["PRESTATARIOS"]), c.originacion["PRESTATARIOS"], 
            IIF(IS_DEFINED(c.evaluacion) AND IS_DEFINED(c.evaluacion["PRESTATARIOS"]), c.evaluacion["PRESTATARIOS"], "No disponible")) AS PRESTATARIOS,
        IIF(IS_DEFINED(c.originacion) AND IS_DEFINED(c.originacion["ORGANISMO EJECUTOR / CLIENTE"]), c.originacion["ORGANISMO EJECUTOR / CLIENTE"], 
            IIF(IS_DEFINED(c.evaluacion) AND IS_DEFINED(c.evaluacion["ORGANISMO EJECUTOR / CLIENTE"]), c.evaluacion["ORGANISMO EJECUTOR / CLIENTE"], "No disponible")) AS ORGANISMO_EJECUTOR,
        IIF(IS_DEFINED(c.originacion) AND IS_DEFINED(c.originacion["PAÍS"]), c.originacion["PAÍS"], 
            IIF(IS_DEFINED(c.evaluacion) AND IS_DEFINED(c.evaluacion["PAÍS"]), c.evaluacion["PAÍS"], "No disponible")) AS PAIS,
        IIF(IS_DEFINED(c.originacion) AND IS_DEFINED(c.originacion["RESUMEN"]), c.originacion["RESUMEN"], 
            IIF(IS_DEFINED(c.evaluacion) AND IS_DEFINED(c.evaluacion["RESUMEN"]), c.evaluacion["RESUMEN"], "No disponible")) AS RESUMEN
    FROM c
    """

GET_ONE_PROJECT_BY_ID = "SELECT * FROM c WHERE c.id = @project_id"