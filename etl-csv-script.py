import pandas as pd
import psycopg2 

data = '/Users/chiranuvathn/Desktop/Development/etl-csv-titanic/train.csv'
conn = psycopg2.connect("dbname=titanic user=postgres")
cur = conn.cursor()

def extract(data):
    return pd.read_csv(data)

def transform(extracted_data):
    name_parts = extracted_data['Name'].str.split()

    titles = []
    first_names = []
    last_names = []

    for name in name_parts:
        title = name[1].strip('.')
        first_name = name[2]
        last_name = name[0].strip(',')
        
        titles.append(title)
        first_names.append(first_name)
        last_names.append(last_name)

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
            name_title VARCHAR(10),
            name_first_name VARCHAR(50),
            name_last_name VARCHAR(50),
            PRIMARY KEY (passenger_id)
        );
    """

    cur.execute(create_query)
    conn.commit()

def insert_staging_tb(extracted_data):
    for i, row in extracted_data.iterrows():
        insert_query = f"""
            INSERT INTO staging_titanic (passenger_id, is_survived, ticket_class, name, gender, age, no_of_sibling_or_spouse, 
                no_of_parent_or_child, ticket_detail, fare, cabin_number, port_of_embarked, name_title, name_first_name, name_last_name)
            VALUES ({row.passenger_id}, {row.is_survived}, {row.ticket_class}, '{row.name.replace("'", "''")}', '{row.gender}',
                {row.age if not pd.isnull(row.age) else 'NULL'}, {row.no_of_sibling_or_spouse}, {row.no_of_parent_or_child},
                '{row.ticket_detail.replace("'", "''")}', {row.fare}, '{row.cabin_number.replace("'", "''")}', 
                '{row.point_of_embarked}', '{row.name_title}', '{row.name_first_name}', '{row.name_last_name}')
            ON CONFLICT (passenger_id) DO NOTHING;
        """
    
        cur.execute(insert_query)
    conn.commit()

def create_dim_passenger():
    create_query = """
        CREATE TABLE IF NOT EXISTS dim_passenger (
            passenger_id INTEGER NOT NULL,
            is_survived INTEGER,
            name VARCHAR(150),
            name_title VARCHAR(10),
            name_first_name VARCHAR(50),
            name_last_name VARCHAR(50),
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
        INSERT INTO dim_passenger (passenger_id, is_survived, name, name_title, name_first_name, name_last_name, 
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
