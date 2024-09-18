import datetime
import random
import time
import cassandra
import pandas as pd
from util.dbFactory import DatabaseFactory 

factory = DatabaseFactory()

def get_data_from_mysql(query):
    """Retrieve data from MySQL database."""
    cnx = factory.create_connect('mysql')
    data = pd.read_sql(query, cnx)
    cnx.close()
    return data

def get_job_data():
    """Get job data from MySQL."""
    query = "SELECT id AS job_id, campaign_id, group_id, company_id FROM job"
    return get_data_from_mysql(query)

def get_publisher_data():
    """Get publisher data from MySQL."""
    query = "SELECT DISTINCT id AS publisher_id FROM master_publisher"
    return get_data_from_mysql(query)


def generate_random_record(job_list, publisher_list, group_list, campaign_list):
    """Generate a single random record."""
    create_time = cassandra.util.uuid_from_time(datetime.datetime.now())
    bid = random.randint(0, 1)
    interact = ['click', 'conversion', 'qualified', 'unqualified']
    custom_track = random.choices(interact, weights=(70, 10, 10, 10))[0]
    job_id = random.choice(job_list)
    publisher_id = random.choice(publisher_list)
    group_id = random.choice(group_list) if group_list else None
    campaign_id = int(random.choice(campaign_list))
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts)

def insert_record(session, record):
    """Insert a single record into Cassandra."""
    sql = """
        INSERT INTO tracking (create_time, bid, campaign_id, custom_track, group_id, job_id, publisher_id, ts)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(sql, record)

def generate_dummy_data(n_records):
    """Generate and insert dummy data into Cassandra."""
    # Retrieve data
    publishers = get_publisher_data()['publisher_id'].to_list()
    jobs = get_job_data()
    
    job_list = jobs['job_id'].to_list()
    campaign_list = jobs['campaign_id'].to_list()
    group_list = jobs[jobs['group_id'].notnull()]['group_id'].astype(int).to_list()

    session = factory.create_connect('cassandra')

    # Generate and insert data
    for _ in range(n_records):
        record = generate_random_record(job_list, publishers, group_list, campaign_list)
        insert_record(session, record)

    print("Data Generated Successfully")

status = "ON"
while status == "ON":
    generate_dummy_data(random.randint(1,20))
    time.sleep(20)

