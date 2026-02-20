from openai import OpenAI

client = OpenAI(
    base_url="https://api.cohere.ai/compatibility/v1",
    api_key="s1ZD88kkRccIf4snL1AlZz3H3GBkbSYBKC31n4VC", # produccion
)

def parser_sql(oracle_sql: str) -> str:
    """
    Converts an Oracle SQL query into a PostgreSQL command with an AI model.
    """
    #return "SELECT NOW() produccion;"

    prompt = (
        "You are a SQL conversion agent. Your task is to convert Oracle SQL coming from Oracle Data Integrator version 11g to PostgreSQL. "
        "You will always receive an instruction followed by Oracle SQL code. "
        "Convert the provided Oracle SQL code into an equivalent PostgreSQL command. "
        "Ignore any XML-like syntax or dynamic references containing '=snpRef'. "
        "However, if the XML provides relevant constraints such as SQL commands, integrate them appropriately. " 
        "If certain information is missing or ambiguous, omit any assumptions and exclude the XML entirely. "
        "Your response must be a single string containing only the valid PostgreSQL command, with no additional text, comments, or formatting. "
        "Next to the commands 'CREATE TABLE', 'INSERT INTO', 'TRUNCATE TABLE', and 'FROM', you will find the syntax: <?=snpRef.getObjectName('L', 'name1', 'name2', '', 'D')?>, where name1 corresponds to the table name and name2 corresponds to the schema name. (Remember to ignore the syntax <?=snpRef.getObjectName, I only need the table name and the schema name.)"
        "Only in the cases of 'CREATE TABLE', add 'IF NOT EXISTS' (Dont add it in cases of 'TRUNCATE TABLE' or 'DROP TABLE')."
        "For queries involving JOINs, use INNER JOIN or LEFT OUTER JOIN or RIGHT OUTER JOIN or FULL OUTER JOIN as appropriate."
        "For statements with a 'FROM' clause, also include 'AS <table_name>' to alias the table accordingly."
        "Remember not to return any additional comments, even if you cant find a Postgres translation; in those cases, return only a single character such as ';'. "
        "If the query includes commented code like /* */, dont return that commented portion."
        "Oracle SQL code: " + oracle_sql
    )

    completion = client.chat.completions.create(
        model="command-a-03-2025",
        messages=[
            {"role": "user", "content": prompt},
        ],
    )
    
    return completion.choices[0].message.content


def parser_sql_sybase(sybase_sql: str) -> str:

    prompt = (
        "You will always receive an instruction followed by Sybase SQL code. "
        "Ignore any XML-like syntax or dynamic references containing '=snpRef'. "
        "However, if the XML provides relevant constraints such as SQL commands, integrate them appropriately. " 
        "If certain information is missing or ambiguous, omit any assumptions and exclude the XML entirely. "
        "Your response must be a single string containing only the valid Sybase command, with no additional text, comments, or formatting. "
        "Next to the commands 'CREATE TABLE', 'INSERT INTO', 'TRUNCATE TABLE', and 'FROM', you will find the syntax: <?=snpRef.getObjectName('L', 'name1', 'name2', '', 'D')?>, where name1 corresponds to the table name and name2 corresponds to the schema name. (Remember to ignore the syntax <?=snpRef.getObjectName, I only need the table name and the schema name.)"
        "For queries involving JOINs, use INNER JOIN or LEFT OUTER JOIN or RIGHT OUTER JOIN or FULL OUTER JOIN as appropriate."
        "For statements with a 'FROM' clause, also include 'AS <table_name>' to alias the table accordingly."
        "Sybase SQL code: " + sybase_sql
    )

    completion = client.chat.completions.create(
        model="command-a-03-2025",
        messages=[
            {"role": "user", "content": prompt},
        ],
    )
    
    return completion.choices[0].message.content

# Ejemplo de uso:
if __name__ == "__main__":
    oracle_query = "SELECT SYSDATE FROM DUAL;" # Select now();
    postgres_command = parser_sql(oracle_query)
    print(postgres_command)
