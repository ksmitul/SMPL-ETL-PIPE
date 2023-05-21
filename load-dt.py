import boto3
import xml.etree.ElementTree as et
import pandas as pd
from sqlalchemy import create_engine
import configparser

config=configparser.ConfigParser()

config.read('config.ini')

host = config.get('host', 'host')
user = config.get('user', 'user')
password = config.get('password', 'password')
database = config.get('database', 'database')


engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

# Define the S3 bucket and key
bucket_name = 'reading-loading-xml'
key_name = 'reed.xml'

# Retrieve the XML data from S3
s3_client = boto3.client('s3')
response = s3_client.get_object(Bucket=bucket_name, Key=key_name)
data = response['Body'].read().decode('utf-8')

# Parse the XML data
tree = et.ElementTree(et.fromstring(data))
root = tree.getroot()

# Prepare lists to store data
course_data = []
time_data = []
place_data = []

# Extract course, time, and place data from XML
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

# Define table names
course_table_name = 'courses'
time_table_name = 'times'
place_table_name = 'place'

# Insert DataFrames into MySQL tables
course_df.to_sql(name=course_table_name, con=engine, if_exists='replace', index=False)
time_df.to_sql(name=time_table_name, con=engine, if_exists='append', index=False)
place_df.to_sql(name=place_table_name, con=engine, if_exists='append', index=False)

# Dispose the engine
engine.dispose()



