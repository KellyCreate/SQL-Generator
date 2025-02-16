from databricks import sql
from dotenv import load_dotenv
from langchain.chains.llm import LLMChain
from langchain.output_parsers import ResponseSchema, StructuredOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from yaml.loader import SafeLoader
import numpy as np
import os, sys
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import streamlit.components.v1 as components
import sqlparse
import yaml

load_dotenv() # Get the environment variables. 

@st.experimental_fragment
@st.cache_data
def load_sample_from_databricks(query):
    """Get a sample from databricks"""
    con = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))
    if "LIMIT" not in query.upper():
        query = query.replace(";","")
        query += f" LIMIT 100;"
    df = pd.read_sql(sql=query,con=con)
    return df

@st.cache_data
def user_query_history(user_name):
    """Load the user history table"""
    con = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))
    query = f"SELECT * FROM dev_tools.sqlgenpro_user_history WHERE user_name = {user_name} AND timestamp > current_date - 20"
    df = pd.read_sql(sql=query,con=con)
    return df

@st.cache_data
def catalog_schema_tables_tabletype():
    """List all the catalog, schema and tables present in the database"""
    with sql.connect(server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                      http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                      access_token=os.getenv("DATABRICKS_ACCESS_TOKEN")) as con:
        with con.cursor() as cursor:
            cursor.tables()
            result_tables = cursor.fetchall()
            df_catalog_schema_tables = pd.DataFrame(result_tables)
            df_catalog_schema_tables = df_catalog_schema_tables.iloc[:,:4]
            df_catalog_schema_tables.columns = ['catalog', 'schema', 'table', 'table_type']

            return df_catalog_schema_tables

@st.cache_data
def database_context_for_llm(catalog,schema,tables_list):
    """Create datbase schema details for the prompt"""
    table_schema = ""

    for table in tables_list:
        con = sql.connect(server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                      http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                      access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"))
       
        # Get the Schema for the table
        query = f"SHOW CREATE TABLE `{catalog}`.{schema}.{table}"
        df = pd.read_sql(sql=query,con=con)
        stmt = df['createtab_stmt'][0]
        stmt = stmt.split("USING")[0]

        # Get the string columns from the table to identify categorical columns
        query = f"DESCRIBE TABLE `{catalog}`.{schema}.{table}"
        df = pd.read_sql(sql=query,con=con)
        string_cols = df[df['data_type']=='string']['col_name'].values.tolist()

        # Get the distinct values for each column as a row
        sql_distinct = ""
        for col in string_cols[:-1]:
            sql_distinct += f"SELECT '{col}' AS column_name, COUNT(DISTINCT {col}) AS cnt, ARRAY_AGG(DISTINCT {col}) AS values FROM `{catalog}`.{schema}.{table} UNION ALL "
        sql_distinct += f"SELECT '{string_cols[-1]}' AS column_name, COUNT(DISTINCT {string_cols[-1]}) AS cnt, ARRAY_AGG(DISTINCT {string_cols[-1]}) AS values FROM `{catalog}`.{schema}.{table}"

        df_categorical = pd.read_sql(sql=sql_distinct,con=con)
        df_categorical = df_categorical[df_categorical['cnt'] <= 20]
        df_categorical.drop(columns='cnt',inplace=True)

        if df_categorical.empty: df_categorical_fields = "No Categorical Fields Found"
        else: df_categorical_fields = df_categorical.to_string(index=False)

        # Get sample rows from the table
        query = f"SELECT * FROM `{catalog}`.{schema}.{table} LIMIT 3"
        df = pd.read_sql(sql=query,con=con)
        samplle_rows = df.to_string(index=False)

        if not table_schema: table_schema = stmt + "\n" + samplle_rows + "\n\nCategorical Fields:\n" + df_categorical_fields + "\n"
        else: table_schema += "\n" + stmt + "\n" + samplle_rows + "\n\nCategorical Fields:\n" + df_categorical_fields + "\n"

    return table_schema

def process_llm_to_mermaid(response: str) -> str:
    """"Function to render the mermaid diagram. Extract the mermaid block from the response"""
    start = response.find("```mermaid")+len('```mermaid')
    end = response.find('```',start)
    return response[start:end].strip()

def process_llm_to_sql(response: str) -> str:
    """Function to render the mermaid diagram. Extract the sql code block from the response"""
    start = response.find('```sql')+len('```sql')
    end = response.find('```',start)
    return response[start:end].strip()

def mermaid(code: str) -> None:
    # Escaping backslashes for special characters in the code
    code_escaped = code.replace("\\", "\\\\").replace("`", "\\`")
    components.html(
        f"""
        <div id="mermaid-container" style="width: 100%; height: 800px; overflow: auto;">
            <pre class="mermaid">
                {code_escaped}
            </pre>
        </div>

        <script type="module">
            import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
            mermaid.initialize({{ startOnLoad: true }});
        </script>
        """,
        height=800  # Can be adjusted
    )


@st.experimental_fragment
@st.cache_data
def create_er_diagram(catalog,schema,tables_list):
    """Create the entity relationship diagram for the selected schenma and tables"""
    table_schema = {}
    con = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                        http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                        access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))
    #  Get the list of columns for each table
    for table in tables_list:
        query = f"DESCRIBE TABLE `{catalog}`.{schema}.{table}"
        df = pd.read_sql(sql=query,con=con)
        columns = df['col_name'].tolist()
        columns_types = df['data_type'].tolist()
        columns_list = [f"{column}:{column_type}" for column, column_type in zip(columns,columns_types)]
        table_schema[table] = columns_list

    #  Prompt Template
    template_string = """
    You are an expert in creating Entity Relationship Diagrams for databases. 
    You need to generate the Mermaid code for the complete ERD diagram.
    You are asked to create an ERD diagram that contains the tables and the columns present in the tables for the selected tables in the database.
    Make sure the ERD is clear and easy to understand with proper relationships details.

    The selected tables in the database are given below (delimited //) in the dictionary format:
    Keys being the table names and values being the list of columns and their datatype in the table. 

    //
    {table_schema} 
    //

    Valdate the mermaid code and verify that it is clear and correct.
    Return the final mermaid code for the ERD diagram after proper analysis
    """

    prompt_template = PromptTemplate.from_template(template_string)

    llm_chain = LLMChain(llm=ChatOpenAI(model="gpt-4o-mini",temperature=0),prompt=prompt_template)

    response = llm_chain.invoke({'table_schema':table_schema})
    output = response['text']
    return output


@st.experimental_fragment
@st.cache_data
def generate_questions(table_schema):
    """Generate questions based on the given schema and tables"""
    output_schema = ResponseSchema(name="generated_questions",description="Generated questions for the given tables list")
    output_parser = StructuredOutputParser.from_response_schemas([output_schema])
    format_instructions = output_parser.get_format_instructions()

    # Prompt Template
    template_string = """
    Using the given SCHEMA (delimited by //), generate the top 5 "quick analysis" questions based on the relationships between the tables 
    which can be answered by creating a Databricks SQL code.
    These questions should be insightful and practical, targeting the kind of business inquiries a product manager or analyst would typically investigate daily.

    SCHEMA:
    //
    {table_schema}
    //

    The output should be in a nested JSON format with the following structure: 
    {format_instructions}
    """

    prompt_template = PromptTemplate.from_template(template_string)

    llm_chain = LLMChain(llm=ChatOpenAI(model='gpt-4o-mini',temperature=0),prompt=prompt_template,output_parser=output_parser)

    response = llm_chain.invoke({'table_schema':table_schema,'format_instructions':format_instructions})

    return response

@st.experimental_fragment
@st.cache_data
def create_sql(question,table_schema):
    """Create SQL code for the selected question and return the data from the database"""
    # Prompt Template
    template_string = """
    (delimited by //)
    Your are an expert data engineer working with a Databricks environment. You are asked to generate a working SQL query in Databricks SQL.
    During join if column name are same please use alias ex schema.id in select statement. 
    It is also important to respect the type of columns: If a column i string, the value should be enclosed in quotes. 
    If you are writing CTEs then include all the required columns. While concatenating a non string column, make sure cast the column to string.
    For date columns comparing to string, please cast the string input.
    For string columns, check if it is a categorical column and only use the appropriate values provided in the schema.

    SCHEMA:
    // {table_schema} //

    QUESTION:
    //
    {question}
    //

    //
    IMPORTANT: Make sure to return only the SQL code and nothing else. Ensure the appropriate CATALOG is used in the query and SCHEMA is specified when reading the tables.
    //

    OUTPUT:
    """

    prompt_template = PromptTemplate.from_template(template_string)

    llm_chain = LLMChain(llm=ChatOpenAI(model='gpt-4o-mini',temperature=0),prompt=prompt_template)

    response = llm_chain.invoke({'question':question,'table_schema':table_schema})
    output = response['text']

    return output

@st.experimental_fragment
@st.cache_data
def create_advanced_sql(question,sql_code,table_schema):
    """Create SQL code for the selected question and return the data from the database"""
    template_string = """
    You are an expert data engineer working with a Databricks environment. Your are asked to generate a working Databricks SQL query.
    Enclose the the comple SQL_CODE in a with clause and name it MASTER. Do not alter the given SQL_Code.
    Only if additional information is needed to answer the question, then use the SCHEMA to join the details to get the final answer. 

    INPUT:
    SQL_CODE:
    //
    {sql_code}
    //

    SCHEMA:
    // {table_schema}//

    QUESTION:
    //
    {question}
    //

    IMPORTANT: Only return the SQL code and nothing else

    OUTPUT:
    """

    prompt_template = PromptTemplate.from_template(template_string)
    llm_chain = LLMChain(llm=ChatOpenAI(model='gpt-4o-mini',temperature=0),prompt=prompt_template)

    response = llm_chain.invoke({'sql_code':sql_code,'question':question,'table_schema':table_schema})
    output = response['text']

    return output

@st.experimental_fragment
def error_check(query):
    """Validate if sellf-correction is needed for the generated SQL query"""
    try:
        df = load_sample_from_databricks(query)
        error_msg = "Successful"
    except Exception as e:
        error_msg = str(e)

    return error_msg

@st.experimental_fragment
def correct_sql(question,sql_code,table_schema,error_msg):
    """Validate and self-correct generated SQL query"""
    # Promp Template
    template_string = """
    You are an expert data engineer working with a Databricks environement. You are asked to modiify the SQL_CODE using Databricks SQL based on the 
    QUESTION, SCHEMA and the ERROR_MESSAGE. If ERROR_MESSAGE is provided, then make sure to correct the SQL query according to that. 

    SCHEMA:
    // {table_schema} //

    ERROR_MESSAGE:
    // {error_msg} //

    SQL_CODE:
    // {sql_code} //

    QUESTION:
    // {question} //

    //
    IMPORTANT: Only return the SQL code and nothing else. Ensure the appropriate CATALOG is used in the query and SCHEMA is specified when reading the tables.
    //
    
    OUTPUT:
    """

    prompt_template = PromptTemplate.from_template(template_string)

    llm_chain = LLMChain(llm=ChatOpenAI(model='gpt-4o-mini',temperature=0),prompt=prompt_template)

    response = llm_chain.invoke({'question':question,'sql_code':sql_code,'table_schema':table_schema,'error_msg':error_msg})
    output = response['text']

    return output

def validate_and_correct_sql(question,query,table_schema):
    """Validate and self-correct"""
    error_msg = error_check(query)

    if error_msg == "Successful": 
        return "Successful",query
    else:
        modified_query = correct_sql(question,query,table_schema,error_msg)
        return "Incorrect", modified_query

@st.experimental_fragment
def add_to_user_history(user_name,question,query,favourite_ind):
    """Add the selected question to the user history"""
    con = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))
    
    user_history_table = "hive_metastore.dev_tools.user_query_history"

    query = f"""INSERT INTO {user_history_table} VALUES ('{user_name}',current_timestamp(),'{question}',"{query}",{favourite_ind})"""
    df = pd.read_sql(sql=query,con=con)

def get_user_history_questions(user_name):
    """"Get user's favourite questions"""
    con = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))
    
    user_history_table = "hive_metastore.dev_tools.user_query_history"

    query = f"""SELECT question FROM {user_history_table} WHERE user_name='{user_name}';"""
    df = pd.read_sql(sql=query, con = con)
    return df

def delete_question_from_user_history(user_name, question_to_delete):
    """Delete the question from the user's favourites"""
    con = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                    http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                    access_token    = os.getenv("DATABRICKS_ACCESS_TOKEN"))
    
    user_history_table = "hive_metastore.dev_tools.user_query_history"

    query = f"""DELETE FROM {user_history_table} WHERE question = '{question_to_delete}' and user_name = '{user_name}'; """
    df = pd.read_sql(sql=query, con=con)

    