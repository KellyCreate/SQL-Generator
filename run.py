############################################################################################################# IMPORT LIBRARIES AND LOAD ENV VARIABLES ##########################################################################################################################################
import numpy as np
import pandas as pd 
import os, sys
import streamlit as st
import streamlit_authenticator as stauth
import sqlparse
import yaml
from collections import OrderedDict, Counter
from databricks import sql
from dotenv import load_dotenv
from yaml.loader import SafeLoader
from add_logo import *
from engine import *
load_dotenv() 
#################################################################################################################################################################################################################################################################################################

###################################################################################################################### PAGE CONFIGURATION #######################################################################################################################################################
st.set_page_config(page_title="Text to SQL Generator", page_icon="🤖", layout="centered",initial_sidebar_state="expanded")

st.markdown("<h1 style='text-align:center; color:purple;'> Text to SQL Generator </h1>", unsafe_allow_html=True)

st.markdown("<h6 style='text-align:center; color:white;'> A productivity enhancement tool designed for product managers, business stakeholders, and intermediate coders, aimed at simplifying tasks involving stored data in conventional SQL databases. </h6>", unsafe_allow_html=True)
#################################################################################################################################################################################################################################################################################################

###################################################################################################################### AUTHENTIFICATION #########################################################################################################################################################
with open('authenticator.yml') as f:
    config = yaml.load(f, Loader=SafeLoader)

authenticator = stauth.Authenticate(config['credentials'],
                                    config['cookie']['name'],
                                    config['cookie']['key'],
                                    config['cookie']['expiry_days'],
                                    config['preauthorized'])

name, authentication_status, user_name = authenticator.login()
#################################################################################################################################################################################################################################################################################################

########################################################################################################################## MAIN #################################################################################################################################################################
if authentication_status:
    authenticator.logout('Logout',location='main')
    st.write(f"Welcome {name}!")

    # Slecting the Catalog, Schema, Table and Table Type in the Target Database
    st.sidebar.image('Databricks_Logo_2.png')
    df_catalog_schema_tables = catalog_schema_tables_tabletype()

    # Getting catalog to schema mapping for dynamically selecting only relevant schema for a given catalog
    df_catalog_schema_mapping = df_catalog_schema_tables.groupby(['catalog']).agg({'schema':lambda x: list(np.unique(x))}).reset_index()

    # Getting schema to table mapping for dynamically selecting only relevant tables for a given catalog and schema
    df_schema_table_mapping = df_catalog_schema_tables.groupby(['schema']).agg({'table':lambda x: list(np.unique(x))}).reset_index()

    # Selecting the catalog
    catalog = st.sidebar.selectbox('Select the catalog', options=df_catalog_schema_tables['catalog'].unique().tolist())

    # Selecting the schema
    schema_for_selected_catalog = df_catalog_schema_mapping[df_catalog_schema_mapping['catalog']==catalog]["schema"].values[0] 
    schema_for_selected_catalog = [val for val in schema_for_selected_catalog if val != "dev_tools"] # We do not want to display dev_tools in the options
    schema = st.sidebar.selectbox("Select the schema", options=schema_for_selected_catalog)

    # Selecting the Tables
    table_for_selected_schema = df_schema_table_mapping[df_schema_table_mapping["schema"]==schema]["table"].values[0]
    table_list = st.sidebar.multiselect("Select the table", options=["All"]+table_for_selected_schema)

    if "All" in table_list: table_list = table_for_selected_schema

    if st.sidebar.checkbox(":purple[Proceed]"):
        with st.expander(":purple[View the ERD Diagram]"):
            response = create_er_diagram(catalog,schema,table_list)
            if st.button("Regenerate the entity relationship diagram"):
                # Creating the ERD Diagram
                create_er_diagram.clear()
                response = create_er_diagram(catalog,schema,table_list)
                mermaid_code = process_llm_to_mermaid(response)
                mermaid(mermaid_code)
            else:
                mermaid_code = process_llm_to_mermaid(response)
                mermaid(mermaid_code)
            
            # Getting the table schema. Very important to reduce hallucinaton
            table_schema = database_context_for_llm(catalog,schema,table_list)


        # Suggested Analysis
        st.markdown("<h2 style='text-align:left; color:purple;'> Suggested Analysis </h2>", unsafe_allow_html=True)
        with st.expander(":purple[View the Section]"):
            generated_questions = generate_questions(table_schema)
            if st.button("Suggetions ?"):
                generate_questions.clear()
                generated_questions = generate_questions(table_schema)
                questions = generated_questions['text']['generated_questions']
                selected_question = st.selectbox('Select a queston', options=questions)
                if st.checkbox('Analyze'):
                    st.write(f'#### {selected_question}')
                    suggested_analysis_response_sql = create_sql(selected_question,table_schema)
                    suggested_analysis_response_sql = process_llm_to_sql(suggested_analysis_response_sql)

                    # Self-correction loop
                    flag, suggested_analysis_response_sql = validate_and_correct_sql(selected_question,suggested_analysis_response_sql,table_schema)
                    while flag != 'Successful':
                        flag, suggested_analysis_response_sql = validate_and_correct_sql(selected_question,suggested_analysis_response_sql,table_schema)

                    st.code(suggested_analysis_response_sql)
                    column1, column2 = st.columns(2)

                    if column1.button("Query Sample Data 1"):
                        df_sample_data = load_sample_from_databricks(suggested_analysis_response_sql)
                        column1.write(df_sample_data)

                    # Saving the favorites. Adding session_state for favorite button
                    if 'fav_ind_qa' not in st.session_state: st.session_state.fav_ind_qa = False

                    fav_ind_qa = column2.button("Save the query", key="sugg analysis - 2")
                    if fav_ind_qa:
                        st.sessiion_state.fav_ind_qa = True
                        add_to_user_history(user_name,selected_question,suggested_analysis_response_sql,True)
                        column2.write("Added to favourite")
            else: 
                questions = generated_questions['text']['generated_questions']
                selected_question = st.selectbox("Select a question", options=questions)
                if st.checkbox("Analyze"):
                    st.write(f"#### {selected_question}")
                    suggested_analysis_response_sql = create_sql(selected_question,table_schema)
                    suggested_analysis_response_sql = process_llm_to_sql(suggested_analysis_response_sql)

                    # Self-correction loop 
                    flag, suggested_analysis_response_sql = validate_and_correct_sql(selected_question,suggested_analysis_response_sql, table_schema)
                    while flag !='Successful':
                        flag, suggested_analysis_response_sql = validate_and_correct_sql(selected_question,suggested_analysis_response_sql, table_schema)

                    st.code(suggested_analysis_response_sql)
                    column1, column2 = st.columns(2)
                    if column1.button('Query Sample Data 2'):
                        df_sample_data = load_sample_from_databricks(suggested_analysis_response_sql)
                        column1.write(df_sample_data)

                    # Saving the favourites. Adding session_state for favourite button
                    if 'fav_ind_qa_2' not in st.session_state:
                        st.session_state.fav_ind_qa_2 = False
                    
                    fav_ind_qa_2 = column2.button("Save the query", key='sugg analysis - 3')
                    if fav_ind_qa_2:
                        st.session_state.fav_ind_qa_2 = True
                        add_to_user_history(user_name,selected_question,suggested_analysis_response_sql,True)
                        column2.write('Added to favourites!')

        # Your Favourites 
        st.markdown("<h2 style='text-align:left; color:purple;'> Your Favourites </h2", unsafe_allow_html=True)
        with st.expander(":purple[View the Section]"):
            df_favourites_questions = get_user_history_questions(user_name)
            selected_favourite = st.selectbox(label="Select a question", options=df_favourites_questions['question'].unique().tolist())

            if st.checkbox("Analyse"):
                st.write(f"#### {selected_favourite}")
                favourite_analysis_response_sql = create_sql(selected_favourite, table_schema)
                favourite_analysis_response_sql = process_llm_to_sql(favourite_analysis_response_sql)

                # Self-correction loop
                flag, favourite_analysis_response_sql = validate_and_correct_sql(selected_favourite, favourite_analysis_response_sql, table_schema)
                while flag != 'Successful':
                    flag, favourite_analysis_response_sql = validate_and_correct_sql(selected_favourite, favourite_analysis_response_sql,table_schema)

                st.code(favourite_analysis_response_sql)
                column1, column2 = st.columns(2)
                if column1.button('Query Sample Data 3',key='Favourites - 1'):
                    df_sample_data = load_sample_from_databricks(favourite_analysis_response_sql)
                    column1.write(df_sample_data)
            if st.checkbox(":purple[Delete question from favourites]"):
                delete_question_from_user_history(user_name,selected_favourite)
                st.write("Deleted from favourites !")


        # Deep-Dive Analysis
        st.markdown("<h2 style='text-align:left;color:purple;'> Deep-Dive Analysis </h2>", unsafe_allow_html=True)
        with st.expander(":purple[View the Section]"):
            deep_dive_question = st.text_area("Enter your deep dive question here: ", key="deep dive - 1")

            # We need this checkbox to tell the code when to start generating SQL. Otherwise it will try to 
            # start generating while the user is typing the question
            if st.checkbox("Generate SQL" ,key="deep dive - 2"):
                response_sql_1 = create_sql(deep_dive_question,table_schema)
                response_sql_1 = process_llm_to_sql(response_sql_1)

                # Self-correction loop
                flag, response_sql_1 = validate_and_correct_sql(deep_dive_question, response_sql_1, table_schema)
                while flag != 'Successful':
                    flag, response_sql_1 = validate_and_correct_sql(deep_dive_question, response_sql_1,table_schema)

                st.code(response_sql_1)

                column1, column2 = st.columns(2)

                query_sample_data_1 = column1.checkbox("Query Sample Data",key="deep dive - 3")
                if query_sample_data_1:
                    df_query_1 = load_sample_from_databricks(response_sql_1)
                    column1.write(df_query_1)

                # Saving the favorites. Adding session_state for favorite button
                if 'fav_ind_1' not in st.session_state:
                    st.session_state.fav_ind_1 = False
                
                if column2.button("Save the query", key="deep dive 4"):
                    st.session_state.fav_ind_1 = True
                    add_to_user_history(user_name,deep_dive_question,response_sql_1,True)
                    column2.write('Added to favorites!')

                if column1.checkbox("Conduct additional analysis?",key="deep dive 5"):
                    deep_dive_question_2 = st.text_area("Enter your question here:",key="deep dive 6")
                    generate_sql_2 = st.checkbox("Generate SQL",key="deep dive 7")
                    if generate_sql_2:
                        response_sql_2 = create_advanced_sql(deep_dive_question_2,response_sql_1,table_schema)
                        response_sql_2 = process_llm_to_sql(response_sql_2)

                        # Self-correction loop
                        flag, response_sql_2 = validate_and_correct_sql(deep_dive_question_2,response_sql_2,table_schema)
                        while flag != 'Successful':
                            flag, response_sql_2 = validate_and_correct_sql(deep_dive_question_2, response_sql_2, table_schema)

                        st.code(response_sql_2)

                        column1, column2 = st.columns(2)
                        query_sample_data_2 = column1.checkbox("Query Sample Data", key="deep dive 8")
                        if query_sample_data_2:
                            df_query_2 = load_sample_from_databricks(response_sql_2)
                            column1.write(df_query_2)

                        # Saving the Favourites. Adding session_state for favorite button
                        if 'fav_ind_2' not in st.session_state:
                            st.session_state.fav_ind_2 = False
                        
                        if column2.button("Save the query", key="deep dive 9"):
                            st.session_state.fav_ind_2 = True
                            add_to_user_history(user_name,deep_dive_question_2,response_sql_2,True)
                            column2.write("Added to favorites!")