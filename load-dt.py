import boto3
import xml.etree.ElementTree as et
import pandas as pd
import numpy as np
import mysql.connector
from sqlalchemy import create_engine


# Establish a connection

host='personal-demo-db.cnw1czgvgwru.us-east-1.rds.amazonaws.com',
user='admin',
password='6416711506',
database='reed'

engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')


s3_client= boto3.client('s3')

bucket_name='reading-loading-xml'
key_name='reed.xml'

reponse=s3_client.get_object(Bucket=bucket_name, Key=key_name)

data= reponse['Body'].read().decode('utf-8')


tree = et.ElementTree(et.fromstring(data))
root = tree.getroot()

course_data = []
time_data = []
place_data = []

for element in root:
    reg_num = element.find('reg_num').text
    subj = element.find('subj').text
    crse = element.find('crse').text
    sect = element.find('sect').text
    title = element.find('title').text
    units = element.find('units').text
    instructor = element.find('instructor').text
    days = element.find('days').text
    
    course_data.append((reg_num, subj, crse, sect, title, units, instructor, days))

    for time in element.iter('time'):
        start_time = time.find('start_time').text
        end_time = time.find('end_time').text
        time_data.append((start_time, end_time))

    for place in element.iter('place'):
        building = place.find('building').text
        room = place.find('room').text
        place_data.append((building, room))

# Create Pandas DataFrames
course_df = pd.DataFrame(course_data, columns=['reg_num', 'subj', 'crse', 'sect', 'title', 'units', 'instructor', 'days'])
time_df = pd.DataFrame(time_data, columns=['start_time', 'end_time'])
place_df = pd.DataFrame(place_data, columns=['building', 'room'])

course_df.to_sql(name='courses', con=engine, if_exists='append', index=False)

time_df.to_sql(name='times', con=engine, if_exists='append', index=False)

place_df.to_sql(name='place', con=engine, if_exists='append', index=False)

engine.dispose()

