import streamlit as st
import pandas as pd
from bauplan.pandas_utils import query_to_pandas
from bauplan.catalog import get_branches, get_table, get_branch
from bauplan.run import run
import grpc
from code_editor import code_editor
import os
import yaml
import shutil


@st.cache_data
def query_as_dataframe(sql, branch):
    try:
        return query_to_pandas(sql, branch=branch)
    except ValueError:
        return None, 'Sorry, something dark happened. \nThe table is probably too big to be displayed. \nPlease, try use the query worksheet'
    except grpc._channel._InactiveRpcError:
        return None, 'Are you sure the table exists in the branch?'


@st.cache_data
def query_and_display(sql, branch):
    result = query_as_dataframe(sql, branch)
    # check if we got a dataframe back
    if isinstance(result, pd.DataFrame):
        st.dataframe(result, width=100000)
        return result
    else:
        st.markdown(result[1])
    return None


def get_user_and_branches():
    branches = [branch.name for branch in get_branches()]
    users = {branch.split('.')[0] for branch in branches if branch.split('.')[0] not in ['bauplan-e2e-check']}
    user_branches = []
    for user in users:
        if user == 'main':
            active_branches = {user: ['main']}
        else:
            active_branches = {user: [branch.split('.')[1] for branch in branches if branch.split('.')[0] == user]}
        user_branches.append(active_branches)
    return user_branches


def get_table_names(branch):
    tables = [t.name for t in get_branch(branch)]
    return tables


def table_preview(branch, table):
    # metadata preview
    metadata_schema = get_table(branch, table)
    columns = [column.name for column in metadata_schema]
    types = [column.type for column in metadata_schema]
    schema = {'columns': columns, 'types': types}
    df = pd.DataFrame(schema)
    return df


@st.cache_data
def data_preview(sql, branch):
    try:
        result = query_to_pandas(sql, branch=branch, args={'preview', 'true'})
        if isinstance(result, pd.DataFrame):
            st.dataframe(result, width=100000)
            return result
        else:
            st.markdown(result[1])
        return None
    except grpc._channel._InactiveRpcError:
        return None, 'Are you sure the table exists in the branch?'

def run_dag(path, branch):
    run(path, materialize=branch)
    st.write('DAG complete')
    return


### STREAMLIT APP BEGINS HERE
st.sidebar.image('bpln_logo_colored_black.svg', width=150)
st.markdown("# Bauplan Explorer")
#data_catalog, bauplan_worksheet = st.tabs(["Data Catalog", "Bauplan worksheet"])


# SIDEBAR DATA BRANCH PICKER
drop_down = get_user_and_branches()

# Extract keys from dictionaries
users = sorted([key for d in drop_down for key in d.keys()])
# Create a dropdown menu for selecting a key
st.sidebar.markdown('# Data branches')
selected_user = st.sidebar.selectbox("Select a user", ['None'] + users, key=1)

table_names = None
selected_branch = None

if selected_user == 'None':
    st.markdown("👋 Hi there! \n Please, select a user on the left to begin")
    st.stop()
if selected_user == 'main':
    selected_branch = 'main'
    table_names = get_table_names('main')
else:
    # Get the corresponding values for the selected key
    branches = None
    for d in drop_down:
        if selected_user in d:
            branches = d[selected_user]
    if branches:
        # Create a dropdown menu for selecting a value
        selected_branch = st.sidebar.selectbox("Select a branch", ['None'] + branches, key=2)
        if selected_branch == 'None':
            st.markdown("Please choose a branch")
        else:
            selected_branch = f"{selected_user}.{selected_branch}"
            table_names = get_table_names(selected_branch)
            #st.dataframe(table_names)

if table_names is None:
    st.stop()
else:
    for table in table_names:
        if st.sidebar.button('🗂️ ' + table):
            # metadata preview
            st.markdown(f"## Table schema of: {table}")
            df = table_preview(selected_branch, table)
            st.dataframe(df, width=100000, height=250)
            st.divider()

    # draw the editor
    st.markdown('### Bauplan Worksheet')

    on = st.toggle('SQL')
    user_language = 'sql' if on else 'python'
    response_dict = None

    if user_language == 'python':
        # define the drag and drop options
        materialize = st.checkbox('materialize')
        if materialize:
            value = 'True'
        else:
            value = 'False'
        bauplan_model = f"""@bauplan.model(columns=['*'], materialize={value})"""
        st.code(bauplan_model, language='python')
        # define drag and drop options for the python decorator
        col1, col2 = st.columns(2)
        with col1:
            dependencies = st.text_input('package', )
        with col2:
            version = st.text_input('version', )

        bauplan_python = f"""@bauplan.python('3.11', pip={{'{dependencies}': '{version}'}})"""
        st.code(bauplan_python, language='python')

        #finally the editor
        # add a button with text: 'Copy'
        st.markdown('Write your function here')

        custom_btns = [
            {
                "name": "Run",
                "feather": "Play",
                "primary": True,
                "hasText": True,
                "showWithIcon": True,
                "commands": ["submit"],
                "style": {"bottom": "0.44rem", "right": "0.4rem"}
            }
        ]

        response_dict = code_editor('', lang=user_language, height=[10, 20], buttons=custom_btns)

        # check that there is some code to run otherwise print back a message for the user
        if len(response_dict['id']) != 0 and response_dict['type'] == "submit":
            user_code = response_dict['text'].strip()
            if user_code == '':
                st.markdown('Nothing to run. Please write a query and press Run.')
                st.stop()
            user_code = f"""import bauplan\n{bauplan_model}\n{bauplan_python}\n{user_code}"""

            # Create a temporary directory in which we will run a bauplan run
            temp_dir = 'temp_dir'
            # delete the folder if it already exists
            if os.path.exists(temp_dir):
                # If the folder exists, delete it
                shutil.rmtree(temp_dir)

            os.makedirs(temp_dir)
            # Define the content of the yaml file and write it in the tempo folder
            bauplan_config = {
                'project': {
                    'id': '40d21649-a47h-437b-09hn-plm75edc1bn',
                    'name': 'temp_project'
                },
                'defaults': {
                    'python_version': '3.11'
                }
            }
            with open(os.path.join(temp_dir, 'bauplan_project.yml'), 'w') as f:
                yaml.dump(bauplan_config, f)

            # Define the content of the Python file and write it in the temp folder
            models = user_code
            with open(os.path.join(temp_dir, 'models.py'), 'w') as file:
                file.write(models)
            # run a bauplan run
            run_dag(temp_dir, selected_branch)
            #run(temp_dir, materialize=selected_branch)

    if user_language == 'sql':
        col1, col2 = st.columns(2)
        with col1:
            model_name = st.text_input('model name', )
        with col2:
            materialize = st.checkbox('materialize')
        st.markdown('Write your query here')


        if materialize:
            value = 'True'
        else:
            value = 'False'
        materialize_flag = f"""-- bauplan: materialize={value}"""

        custom_btns = [
            {
                "name": "Run",
                "feather": "Play",
                "primary": True,
                "hasText": True,
                "showWithIcon": True,
                "commands": ["submit"],
                "style": {"bottom": "0.44rem", "right": "0.4rem"}
            }
        ]

        response_dict = code_editor('', lang=user_language, height=[10, 20], buttons=custom_btns)

        if len(response_dict['id']) != 0 and response_dict['type'] == "submit":
                query = response_dict['text'].strip()
                if query == '':
                    st.markdown('No query to run. Please write a query and press Run.')
                    st.stop()
                q_string_max = 30
                q_string = '{} ...'.format(query[:q_string_max]) if len(query) > q_string_max else query
                query = f"""{materialize_flag}\n{query}"""
                # Create a temporary directory in which we will run a bauplan run
                temp_dir = 'temp_dir'
                # delete the folder if it already exists
                if os.path.exists(temp_dir):
                    # If the folder exists, delete it
                    shutil.rmtree(temp_dir)

                os.makedirs(temp_dir)
                # Define the content of the yaml file and write it in the tempo folder
                bauplan_config = {
                    'project': {
                        'id': '40d21649-a47h-437b-09hn-plm75edc1bn',
                        'name': 'temp_project'
                    },
                    'defaults': {
                        'python_version': '3.11'
                    }
                }
                with open(os.path.join(temp_dir, 'bauplan_project.yml'), 'w') as f:
                    yaml.dump(bauplan_config, f)

                # Define the content of the Python file and write it in the temp folder
                models = query

                with open(os.path.join(temp_dir, f"""{model_name}.sql"""
                ), 'w') as file:
                    file.write(models)
                # run a bauplan run
                run_dag(temp_dir, selected_branch)







                # st.write('Running "{}" on branch "{}"'.format(q_string, selected_branch))
                # # we get the DF back in case we want to export it
                # results = query_and_display(query, selected_branch)
                # if results is None:
                #     st.stop()
                # export = results.to_csv()
                # st.download_button(label="Download Results", data=export, file_name="query_results.csv")
