import pandas as pd

data = '/Users/chiranuvathn/Desktop/Development/etl-csv-titanic/train.csv'

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