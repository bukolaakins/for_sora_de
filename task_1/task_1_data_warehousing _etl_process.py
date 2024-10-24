"""
Author : Olubukunola Akinsola <oo.akinsola@gmail.com>
Date   : 2024-10-24
Purpose: Sora Union DE Assessment - Task 1
"""

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import logging
import json

# logging to track erros
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#open file with dabase configuration


with open('db_params.json') as config_file:
    db_params = json.load(config_file)

    
# Function to connect to the dw
def connect_to_dw():
    try:
        engine = create_engine(f"postgresql://{db_params['username']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}")
        connection = engine.connect()
        logging.info("Database connection established.")
        return connection, engine
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise        
             
        
# Function to create dimension tables
def create_dimension_tables(engine):
    with engine.begin() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS dw.dim_client (
            client_id SERIAL PRIMARY KEY,
            client_name VARCHAR(255) UNIQUE
        );
        
        CREATE TABLE IF NOT EXISTS dw.dim_project (
            project_id SERIAL PRIMARY KEY,
            project_name VARCHAR(255) UNIQUE
        );
        
        CREATE TABLE IF NOT EXISTS dw.dim_employee (
            employee_id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            role VARCHAR(255)
        );
        
        CREATE TABLE IF NOT EXISTS dw.dim_task (
            task_id SERIAL PRIMARY KEY,
            task_name VARCHAR(255) UNIQUE
        );
        """)
        logging.info("Dimension tables successfully created.")

# Function to create fact tables
def create_fact_tables(engine):
    with engine.begin() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS dw.fact_task_log (
            task_log_id SERIAL PRIMARY KEY,
            client_id INT REFERENCES dw.dim_client(client_id),
            project_id INT REFERENCES dw.dim_project(project_id),
            employee_id INT REFERENCES dw.dim_employee(employee_id),
            task_id INT REFERENCES dw.dim_task(task_id),
            date DATE,
            hours DECIMAL(5, 2) CHECK (hours >= 0),
            note VARCHAR(255),
            is_billable BOOLEAN
        );
        
        CREATE TABLE IF NOT EXISTS dw.fact_project_allocation (
            project_allocation_id SERIAL PRIMARY KEY,
            client_id INT REFERENCES dw.dim_client(client_id),
            project_id INT REFERENCES dw.dim_project(project_id),
            employee_id INT REFERENCES dw.dim_employee(employee_id),
            task_id INT REFERENCES dw.dim_task(task_id),
            start_date DATE,
            end_date DATE,
            estimated_hours DECIMAL(5, 2) CHECK (estimated_hours >= 0)
        );
        """)
        logging.info("Fact tables successfully created.")

        
# function to create staging tables and load raw data into them
def load_data_to_staging(engine, csv_path_1, csv_path_2):
    try:
        # raw data are in csv format, load the csv files
        clickup_data = pd.read_csv(csv_path_1)
        allocation_data = pd.read_csv(csv_path_2)

        # Checking for empty dataframes
        if clickup_data.empty or allocation_data.empty:
            logging.warning("One or more input CSV files are empty. ETL process may not proceed.")
            return

        # Renaming the raw data columns to match the staging table schema
        clickup_data.columns = ['client', 'project', 'name', 'task', 'date', 'hours', 'note', 'is_billable']
        allocation_data.columns = ['client', 'project', 'role', 'name', 'task', 'start_date', 'end_date', 'estimated_hours']

        # Data validation: Ensure no null values are in the key columns
        if clickup_data[['client', 'project', 'name', 'task', 'date']].isnull().any().any():
            logging.warning("Null values found in key columns of ClickUp data. Cleaning the data before proceeding.")
            clickup_data.dropna(subset=['client', 'project', 'name', 'task', 'date'], inplace=True)

        if allocation_data[['client', 'project', 'name', 'task', 'start_date']].isnull().any().any():
            logging.warning("Null values found in key columns of Allocations data. Cleaning the data before proceeding.")
            allocation_data.dropna(subset=['client', 'project', 'name', 'task', 'start_date'], inplace=True)

        with engine.begin() as conn:
            # data load into staging table            
            clickup_data.to_sql('stg_task_log', conn, schema='stg', if_exists='replace', index=False)
            allocation_data.to_sql('stg_project_allocation', conn, schema='stg', if_exists='replace', index=False)


        logging.info("Data loaded into staging tables successfully.")

    except Exception as e:
        logging.error(f"Error loading data to staging: {e}")
        raise


# function to insert data into dimension tables, checking for existence before inserting
def insert_into_dimension_tables(engine):
    with engine.begin() as conn:
        # Insert unique clients into dim_client
        clients = pd.read_sql('SELECT DISTINCT client FROM stg.stg_project_allocation', conn)
        clients.columns = ['client_name']
        clients.drop_duplicates(subset=['client_name'], inplace=True)  

        # Check for existence in dim_client before inserting
        for client_name in clients['client_name']:
            existing_client = conn.execute("""
                SELECT client_id FROM dw.dim_client WHERE client_name = %s
            """, (client_name,)).fetchone()
            
            if not existing_client:
                conn.execute("""
                    INSERT INTO dw.dim_client (client_name)
                    VALUES (%s)
                """, (client_name,))
            else:
                logging.info(f"Client '{client_name}' already exists.")

        # Insert unique projects into dim_project
        projects = pd.read_sql('SELECT DISTINCT project FROM stg.stg_project_allocation', conn)
        projects.columns = ['project_name']
        projects.drop_duplicates(subset=['project_name'], inplace=True)  

        # Check for existence in dim_project before inserting
        for project_name in projects['project_name']:
            existing_project = conn.execute("""
                SELECT project_id FROM dw.dim_project WHERE project_name = %s
            """, (project_name,)).fetchone()
            
            if not existing_project:
                conn.execute("""
                    INSERT INTO dw.dim_project (project_name)
                    VALUES (%s)
                """, (project_name,))
            else:
                logging.info(f"Project '{project_name}' already exists.")
        
        # Insert unique employees into dim_employee
        employees = pd.read_sql('SELECT DISTINCT name, role FROM stg.stg_project_allocation', conn)
        employees.columns = ['name', 'role']
        employees.drop_duplicates(subset=['name'], inplace=True)  
        
        for index, row in employees.iterrows():
            name = row['name']
            role = row['role'] 

            existing_employee = conn.execute("""
                SELECT employee_id, name FROM dw.dim_employee WHERE name = %s
            """, (name,)).fetchone()

            if not existing_employee:
                conn.execute("""
                    INSERT INTO dw.dim_employee (name, role)
                    VALUES (%s, %s)
                """, (name, role))  # Insert both name and role
            else:
                logging.info(f"Employee '{name}' already exists.")
                

        # Insert unique tasks into dim_task
        tasks = pd.read_sql('SELECT DISTINCT task FROM stg.stg_project_allocation', conn)
        tasks.columns = ['task_name']
        tasks.drop_duplicates(subset=['task_name'], inplace=True)  # Ensure uniqueness
        
        # Check for existence in dim_task before inserting
        for task_name in tasks['task_name']:
            existing_task = conn.execute("""
                SELECT task_id FROM dw.dim_task WHERE task_name = %s
            """, (task_name,)).fetchone()
            
            if not existing_task:
                conn.execute("""
                    INSERT INTO dw.dim_task (task_name)
                    VALUES (%s)
                """, (task_name,))
            else:
                logging.info(f"Task '{task_name}' already exists.")



# function to insert data into fact tables
def insert_into_fact_table(engine, stg_table_name):
    with engine.begin() as conn:
        # Fetch relevant IDs from dimension tables
        client_ids = pd.read_sql('SELECT * FROM dw.dim_client', conn)
        project_ids = pd.read_sql('SELECT * FROM dw.dim_project', conn)
        employee_ids = pd.read_sql('SELECT * FROM dw.dim_employee', conn)
        task_ids = pd.read_sql('SELECT * FROM dw.dim_task', conn)

        # staging data loaded for transformation
        staging_data = pd.read_sql(f'SELECT * FROM {stg_table_name}', conn)

        # Merge staging data with dimension IDs, handling missing references
        try:
            staging_data = staging_data.merge(client_ids, left_on='client', right_on='client_name', how='left')
            staging_data = staging_data.merge(project_ids, left_on='project', right_on='project_name', how='left')
            staging_data = staging_data.merge(employee_ids, left_on='name', right_on='name', how='left')
            staging_data = staging_data.merge(task_ids, left_on='task', right_on='task_name', how='left')
        
            # Select relevant fields for fact table
            if stg_table_name == 'stg.stg_task_log':
                fact_data = staging_data[['client_id', 'project_id', 'employee_id', 'task_id', 'date', 'hours', 'note', 'is_billable']]
                
                #checking for where the hours column is <0 to set it to equal 0
                if (fact_data['hours'] < 0).any():
                    logging.warning("Negative values found in hours or estimated hours. They will be set to 0.")
                    fact_data.loc[fact_data['hours'] < 0, 'hours'] = 0
                    fact_data.to_sql('fact_task_log', conn, schema='dw', if_exists='append', index=False)
                
            elif stg_table_name == 'stg.stg_project_allocation':
                fact_data = staging_data[['client_id', 'project_id', 'employee_id', 'task_id', 'start_date', 'end_date', 'estimated_hours']]
                
                #checking for where the estimated_hours column is <0 to set it to equal 0
                if (fact_data['estimated_hours'] < 0).any():  
                    logging.warning("Negative values found in hours or estimated hours. They will be set to 0.")
                    fact_data.loc[fact_data['estimated_hours'] < 0, 'estimated_hours'] = 0
                    fact_data.to_sql('fact_project_allocation', conn, schema='dw', if_exists='append', index=False)
                    

        except Exception as e:
            logging.error(f"Error during data loading: {e}")
            raise
            

# Main function to perform ETL
def main():
    
    clickup = "C:/Users/PC/Downloads/clickup.csv"
    allocations = "C:/Users/PC/Downloads/allocations.csv"
    
    # Connect to the database
    connection, engine = connect_to_dw()

    # Create dimension and fact tables
    create_dimension_tables(engine)
    create_fact_tables(engine)

    # Load data into staging tables
    load_data_to_staging(engine, clickup, allocations)
    
    # Insert data into dimension tables
    insert_into_dimension_tables(engine)
    
    # Insert data into fact tables

    allocation_stg_table = 'stg.stg_project_allocation'
    insert_into_fact_table(engine, allocation_stg_table)
    
    task_stg_table = 'stg.stg_task_log'
    insert_into_fact_table(engine, task_stg_table)


    connection.close()
    logging.info("ETL process completed successfully.")

if __name__ == '__main__':
    main()
