from util.util import *

spark = create_spark_session()
mysql_config = get_config('mysql')

def get_mysql_latest_time():    
    sql = """(select * from job) data"""
    mysql_time = spark.read.format('jdbc').options(url=mysql_config['url'], \
                                                    driver=mysql_config['driver'], \
                                                    dbtable=sql, \
                                                    user=mysql_config['username'], \
                                                    password=mysql_config['password']).load()
    # mysql_time = mysql_time.take(1)[0][0]
    # if mysql_time is None:
    #     mysql_latest = '1998-01-01 23:59:59'
    # else :
    #     mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_time 


mysql_time = get_mysql_latest_time()
print(mysql_time)