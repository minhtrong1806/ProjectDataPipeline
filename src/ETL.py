import time
import datetime
from uuid import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from uuid import * 
from util.util import *

spark = create_spark_session()
mysql_config = get_config('mysql')

def calculating_clicks(df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({'bid':0})
    clicks_data = clicks_data.na.fill({'job_id':0})
    clicks_data = clicks_data.na.fill({'publisher_id':0})
    clicks_data = clicks_data.na.fill({'group_id':0})
    clicks_data = clicks_data.na.fill({'campaign_id':0})
    clicks_data.registerTempTable('clicks')
    clicks_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , avg(bid) as bid_set, count(*) as clicks , sum(bid) as spend_hour from clicks
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return clicks_output 
    
def calculating_conversion(df):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.registerTempTable('conversion')
    conversion_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as conversions  from conversion
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return conversion_output 
    
def calculating_qualified(df):    
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.registerTempTable('qualified')
    qualified_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as qualified  from qualified
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return qualified_output
    
def calculating_unqualified(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.registerTempTable('unqualified')
    unqualified_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as unqualified  from unqualified
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return unqualified_output

def get_updated_time(df):
    updated_time_data = df.filter(df.custom_track == 'unqualified')
    updated_time_data = updated_time_data.na.fill({'job_id':0})
    updated_time_data = updated_time_data.na.fill({'publisher_id':0})
    updated_time_data = updated_time_data.na.fill({'group_id':0})
    updated_time_data = updated_time_data.na.fill({'campaign_id':0})
    updated_time_data.registerTempTable('updated_time')
    updated_time_output = spark.sql("""select job_id , publisher_id , campaign_id , group_id , max(ts) as updated_at  from updated_time
    group by job_id , publisher_id , campaign_id , group_id """)
    return updated_time_output

def process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output, updated_time_output):
    final_data = clicks_output.join(conversion_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full') \
        .join(qualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full') \
        .join(unqualified_output,['job_id','date','hour','publisher_id','campaign_id','group_id'],'full') \
        .join(updated_time_output,['job_id','publisher_id','campaign_id','group_id'],'full')
    return final_data 
    
def process_cassandra_data(df):
    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)
    updated_time_output = get_updated_time(df)
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output,updated_time_output)
    return final_data
    
def retrieve_company_data():
    sql = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company = spark.read.format('jdbc').options(url=mysql_config['url'], \
                                                driver=mysql_config['driver'], \
                                                dbtable=sql, \
                                                user=mysql_config['username'], \
                                                password=mysql_config['password']).load()
    return company 
    
def import_to_mysql(output):
    final_output = output.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id','unqualified','qualified','conversions','clicks','bid_set','spend_hour','updated_at')
    final_output = final_output.withColumnRenamed('date','dates') \
                                .withColumnRenamed('hour','hours') \
                                .withColumnRenamed('qualified','qualified_application') \
                                .withColumnRenamed('unqualified','disqualified_application') \
                                .withColumnRenamed('conversions','conversion')
    
    final_output = final_output.withColumn('sources',lit('Cassandra'))
    final_output.printSchema()
    final_output.write.format("jdbc") \
                        .option("driver",mysql_config['driver']) \
                        .option("url", mysql_config['url']) \
                        .option("dbtable", "events") \
                        .mode("append") \
                        .option("user", mysql_config['username']) \
                        .option("password", mysql_config['password']) \
                        .save()
    return print('Data imported successfully')



def main_task(mysql_time):

    print('Retrieving data from Cassandra')
    print('...')
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking",keyspace="logs").load().where(col('ts')>= mysql_time)

    print('Selecting data from Cassandra')
    print('...')
    df = df.select('ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    df = df.filter(df.job_id.isNotNull())
    df.printSchema()

    print('Processing Cassandra Output')
    print('...')
    cassandra_output = process_cassandra_data(df)

    print('Merge Company Data')
    print('...')
    company = retrieve_company_data()

    print('Finalizing Output')
    print('...')
    final_output = cassandra_output.join(company,'job_id','left').drop(company.group_id).drop(company.campaign_id)


    print('Import Output to MySQL')
    print('...')
    import_to_mysql(final_output)
    return print('Task Finished')
    
    

def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'logs').load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_mysql_latest_time():    
    sql = """(select max(updated_at) from events) data"""
    mysql_time = spark.read.format('jdbc').options(url=mysql_config['url'], \
                                                    driver=mysql_config['driver'], \
                                                    dbtable=sql, \
                                                    user=mysql_config['username'], \
                                                    password=mysql_config['password']).load()
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest 
    

while True :
    start_time = datetime.datetime.now()
    cassandra_time = get_latest_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_time))
    mysql_time = get_mysql_latest_time()
    print('MySQL latest time is {}'.format(mysql_time))
    if cassandra_time > mysql_time : 
        main_task(mysql_time)
    else :
        print("No new data found")
    end_time = datetime.datetime.now()
    execution_time = (end_time - start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))
    time.sleep(10)
    






 
