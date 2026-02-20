from openai import OpenAI
import time

client = OpenAI(
    base_url="https://api.cohere.ai/compatibility/v1",
    api_key="diAgzvkZeyjsyEYIZWV1irhDRNSPCoRqB8MRQ4Wy", # desarrollo
)

calls_count = 0

def parser_sql_dev(oracle_sql: str) -> str:
    """
    Converts an Oracle SQL query into a PostgreSQL command with an AI model.
    """
    #return "SELECT NOW() desarrollo;"

    global calls_count

    # Antes de invocar la API
    if calls_count >= 9:
        # Esperamos un minuto
        time.sleep(60)
        # Reseteamos el contador
        calls_count = 0

    prompt = (
        "You are a SQL conversion agent. Your task is to convert Oracle SQL coming from Oracle Data Integrator version 11g to PostgreSQL version 16.8. "
        "You will always receive an instruction followed by Oracle SQL code. "
        "Convert the provided Oracle SQL code into an equivalent PostgreSQL command. "
        "Ignore any XML-like syntax or dynamic references containing '=snpRef'. "
        "However, if the XML provides relevant constraints such as SQL commands, integrate them appropriately. " 
        "If certain information is missing or ambiguous, omit any assumptions and exclude the XML entirely. "
        "Your response must be a single string containing only the valid PostgreSQL command, with no additional text, comments, or formatting. "
        "Next to the commands 'CREATE TABLE', 'INSERT INTO', 'TRUNCATE TABLE', and 'FROM', you will find the syntax: <?=snpRef.getObjectName('L', 'name1', 'name2', '', 'D')?>, where name1 corresponds to the table name and name2 corresponds to the schema name. (Remember to ignore the syntax <?=snpRef.getObjectName, I only need the table name and the schema name.)"
        "Only in the cases of 'CREATE TABLE', add 'IF NOT EXISTS' (Dont add it in cases of 'TRUNCATE TABLE')."
        "For queries involving JOINs, use INNER JOIN or LEFT OUTER JOIN or RIGHT OUTER JOIN or FULL OUTER JOIN as appropriate."
        "For statements with a 'FROM' clause, also include 'AS <table_name>' to alias the table accordingly."
        "Remember not to return any additional comments, even if you cant find a Postgres translation; in those cases, return only a single character such as ';'. "
        "If the query includes commented code like /* */, dont return that commented portion."
        "If you find Java code, return it exactly as is."
        "Oracle SQL code: " + oracle_sql
    )

    while True:
        try:
            completion = client.chat.completions.create(
                model="command-r-plus-08-2024",
                messages=[
                    {"role": "user", "content": prompt},
                ],
            )
            calls_count += 1
            return completion.choices[0].message.content
        except Exception as e:
            # Manejo específico para el error 429 (Límite de la Trial Key)
            if "429" in str(e):
                print(f"Límite de API Trial alcanzado (429). Esperando 60s antes de reintentar...")
                time.sleep(60)
                continue
            
            print(f"Error procesando query: {oracle_sql[:100]}...")
            print(f"Excepcion: {e}")
            # Retornamos un punto y coma como fallback si falla la conversion por IA
            return ";"

# Ejemplo de uso:
if __name__ == "__main__":
    oracle_query = "SELECT SYSDATE FROM DUAL;" # Select now();
    postgres_command = parser_sql_dev(oracle_query)
    print(postgres_command)
