import pandas as pd
import psycopg2
from datetime import datetime

data = '/Users/chiranuvathn/Desktop/Development/etl-csv-titanic/train.csv'
conn = psycopg2.connect("dbname=titanic user=postgres")
cur = conn.cursor()
log_file = '/Users/chiranuvathn/Desktop/Development/etl-csv-titanic/etl_titanic_log.txt'

def extract(data):
    return pd.read_csv(data)

def transform(extracted_data):
    titles = []
    first_names = []
    last_names = []

    for name in extracted_data['Name']:
        last_name, rest = name.split(',',1)
        title, first_name = rest.split('.',1)

        title_split = title.strip()
        first_name_split = first_name.strip()
        last_name_split = last_name.strip()

        titles.append(title_split)
        first_names.append(first_name_split)
        last_names.append(last_name_split)

    extracted_data['Title'] = titles
    extracted_data['FirstName'] = first_names
    extracted_data['LastName'] = last_names

    return extracted_data

def create_staging_tb():
    create_query = """
        CREATE TABLE IF NOT EXISTS staging_titanic (
            passenger_id INTEGER NOT NULL,
            is_survived INTEGER,
            ticket_class INTEGER,
            name VARCHAR(150),
            gender VARCHAR(10),
            age INTEGER,
            no_of_sibling_or_spouse INTEGER,
            no_of_parent_or_child INTEGER,
            ticket_detail VARCHAR(50),
            fare NUMERIC,
            cabin_number VARCHAR(50),
            port_of_embarked VARCHAR(10),
            name_title VARCHAR(20),
            name_first_name VARCHAR(60),
            name_last_name VARCHAR(60),
            PRIMARY KEY (passenger_id)
        );
    """

    cur.execute(create_query)
    conn.commit()

def insert_staging_tb(transformed_data):
    for i, row in transformed_data.iterrows():
        insert_query = f"""
            INSERT INTO staging_titanic (passenger_id, is_survived, ticket_class, name, gender, age, no_of_sibling_or_spouse, 
                no_of_parent_or_child, ticket_detail, fare, cabin_number, port_of_embarked, name_title, name_first_name, name_last_name)
            VALUES ({row.PassengerId}, {row.Survived}, {row.Pclass}, '{row.Name.replace("'", "''")}', '{row.Sex}',
                {row.Age if not pd.isnull(row.Age) else 'NULL'}, {row.SibSp}, {row.Parch},'{row.Ticket.replace("'", "''")}', 
                {row.Fare}, '{row.Cabin}', '{row.Embarked}', '{row.Title}', '{row.FirstName.replace("'", "''")}', '{row.LastName.replace("'", "''")}')
            ON CONFLICT (passenger_id) DO NOTHING;
        """
    
        cur.execute(insert_query)
    conn.commit()

def create_dim_passenger():
    create_query = """
        CREATE TABLE IF NOT EXISTS dim_passenger (
            passenger_id INTEGER NOT NULL,
            is_survived INTEGER,
            full_name VARCHAR(150),
            title VARCHAR(20),
            first_name VARCHAR(60),
            last_name VARCHAR(60),
            gender VARCHAR(10),
            age INTEGER,
            no_of_sibling_or_spouse INTEGER,
            no_of_parent_or_child INTEGER,
            PRIMARY KEY (passenger_id)
        );
    """

    cur.execute(create_query)
    conn.commit()

def insert_dim_passenger():
    insert_query = """
        INSERT INTO dim_passenger (passenger_id, is_survived, full_name, title, first_name, last_name, 
            gender, age, no_of_sibling_or_spouse, no_of_parent_or_child)
        SELECT passenger_id,
               is_survived,
               name,
               name_title,
               name_first_name,
               name_last_name,
               gender,
               age,
               no_of_sibling_or_spouse,
               no_of_parent_or_child
        FROM staging_titanic
        ON CONFLICT (passenger_id) DO NOTHING;
    """

    cur.execute(insert_query)
    conn.commit()

def create_fact_passenger():
    create_query = """
        CREATE TABLE IF NOT EXISTS fact_passenger (
            passenger_id INTEGER NOT NULL,
            ticket_class INTEGER,
            ticket_detail VARCHAR(50),
            fare NUMERIC,
            cabin_number VARCHAR(50),
            port_of_embarked VARCHAR(10),
            passenger_dim_id INTEGER REFERENCES dim_passenger(passenger_id),
            PRIMARY KEY (passenger_id)
        );
    """
    cur.execute(create_query)
    conn.commit()

def insert_fact_passenger():
    insert_query = """
        INSERT INTO fact_passenger (passenger_id, ticket_class, ticket_detail, fare, cabin_number, port_of_embarked, 
            passenger_dim_id)
        SELECT passenger_id,
               ticket_class,
               ticket_detail,
               fare,
               cabin_number,
               port_of_embarked,
               passenger_id
        FROM staging_titanic
        ON CONFLICT (passenger_id) DO NOTHING;
    """

    cur.execute(insert_query)
    conn.commit()

def logging(message):
    time_format = '%Y-%m-%d %H:%M:%S'
    time_now = datetime.now()
    current_time = time_now.strftime(time_format)
    with open(log_file, 'a') as f:
        f.write(current_time + ': ' + message + '.' + '\n')

logging('ETL Process Started')

logging('Extraction Started')
etl_extracted_data = extract(data)

logging('Extraction Completed')

logging('Transformation Started')
etl_transformed_data = transform(etl_extracted_data)

logging('Transformation Completed')

logging('Loading - PostgreSQL "staging_titanic" Started')
create_staging_tb()
insert_staging_tb(etl_transformed_data)

logging('Loading - PostgreSQL "staging_titanic" Completed')

logging('Loading - PostgreSQL "dim_passenger" Table Started')
create_dim_passenger()
insert_dim_passenger()

logging('Loading - PostgreSQL "dim_passenger" Table Completed')

logging('Loading - PostgreSQL "fact_passenger" Table Started')
create_fact_passenger()
insert_fact_passenger()

logging('Loading - PostgreSQL "fact_passenger" Table Completed')

logging('ETL Process Completed')
