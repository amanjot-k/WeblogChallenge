__author__ = 'amanjotkaur'

# imports
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import ujson
import ast
import csv

def sparkSettings():
    '''
    Spark configuration
    '''
    conf = SparkConf().set('spark.shuffle.manager', 'SORT')
    conf.set('spark.akka.frameSize', 40)
    conf.set('spark.cores.max', 40)
    conf.set('spark.executor.memory', '1g')
    return conf

def date_to_millis(str_dt):
    '''
    Convert ISO 8601 format datetime to milliseconds for easier processing
    '''
    import datetime
    import dateutil.parser
    dt = dateutil.parser.parse(str_dt)
    return int(datetime.datetime.strftime(dt, '%s'))

def process_raw_data(filename):
    '''
    Some initial processing to read in the space delimited file and save it as
    a csv. This was done since just doing a split on space in spark led to the URL 
    and user agent being split into many different elements. A csv reader seemed 
    like a better way to handle this
    '''
    reader = csv.reader(open(filename), delimiter=' ')
    temp_file = open('processed_2015_07_22_mktplace_shop_web_log_sample.csv', 'w')
    for line in reader:
        temp_file.write(', '.join(line))
        temp_file.write('\n')
    temp_file.close()

def initialize_rdd(sc, filename):
    '''
    Function to do the initial processing of RDD required in all tasks. This reads 
    in a comma-separated line, splits it into a list and keep only the elements of 
    interest namely, timestamp(element 0), user IP and port (element 2), 
    request URL(element 11)
    '''
    rdd = sc.textFile(filename)\
        .map(lambda x: x.split(', '))\
        .map(lambda x: (x[0], x[2], x[11]))
    return rdd

process_raw_data('data/2015_07_22_mktplace_shop_web_log_sample.log')
processed_filename = 'processed_2015_07_22_mktplace_shop_web_log_sample.csv'
conf = sparkSettings()

#########################################
# Task 1: Sessionize the data
#########################################
sc = SparkContext(conf=conf, appName="weblogChallenge: Task 1")
session_rdd = initialize_rdd(sc, processed_filename)

session_rdd = session_rdd.map(lambda (ts, ip_port, url): (ip_port.split(':')[0], (url, ts,date_to_millis(ts))))\
    .groupByKey()\
    .map(lambda (x,y): (x, sorted(list(y), key=lambda x: x[-1])))

sessions = session_rdd.collect()
sessions_file = open('sessionize.json', 'w')
for item in sessions:
    (ip, ts_url_list) = item
    session_dict = {}
    session_dict['ip'] = str(ip)
    session_dict['ts_url_list'] = []
    for (ts, url, int_ts) in ts_url_list:
        session_dict['ts_url_list'].append({"ts": ts, "url": url})
    sessions_file.write(ujson.encode(session_dict))
    sessions_file.write('\n')
sessions_file.close()
sc.stop()

############################################
# Task 2: get the average session time
############################################
sc = SparkContext(conf=conf, appName="weblogChallenge: Task 2")
avg_session = initialize_rdd(sc, processed_filename)

avg_session = avg_session\
    .map(lambda (ts, ip_port, url): (int(date_to_millis(ts)), ip_port.split(':')[0], url))\
    .map(lambda (ts, ip, url): (ip, (ts)))\
    .groupByKey()\
    .map(lambda (ip, tss): ('', (max(list(tss)) - min(list(tss)), 1)))\
    .filter(lambda (_, (session_len, cnt)): session_len > 0)\
    .reduceByKey(lambda x,y: [a+b for (a, b) in zip(x, y)])\
    .map(lambda (_, (session_len, total_cnt)): session_len*1.0/total_cnt)
avg_session_len = avg_session.collect()[0]
avg_session_file = open('avg_session_time.csv', 'w')
avg_session_file.write(str(avg_session_len) + ' ms')
avg_session_file.close()
sc.stop()

# #Average session time:  3405.29847648 ms

#############################################
# Task 3: get unique URL visits per session
#############################################
sc = SparkContext(conf=conf, appName="weblogChallenge: Task 3")
url_visits = initialize_rdd(sc, processed_filename)
url_visits = url_visits\
    .map(lambda (ts, ip_port, url): (int(date_to_millis(ts)), ip_port.split(':')[0], url))\
    .map(lambda (ts, ip, url): (ip, [url]))\
    .reduceByKey(lambda x,y: x+y)\
    .map(lambda (ip, url_list): (ip, (len(set(url_list)))))

unique_url_hits = url_visits.collect()

unique_url_file = open('unique_url_hits.csv', 'w')
unique_url_file.write('IP, num_unique_urls\n')
for item in unique_url_hits:
    (ip, url_cnt) = item
    unique_url_file.write(str(ip) + ',' + str(url_cnt))
    unique_url_file.write('\n')
unique_url_file.close()
sc.stop()

#############################################
# Task 4: get top engaged IPs
#############################################
sc = SparkContext(conf=conf, appName="weblogChallenge: Task 4")
top_ips_rdd = initialize_rdd(sc, processed_filename)
top_ips_rdd = top_ips_rdd\
    .map(lambda (ts, ip, url): (ip.split(':')[0], int(date_to_millis(ts))))\
    .groupByKey()\
    .map(lambda (ip, tss): (ip, (max(list(tss)) - min(list(tss)))))\
    .filter(lambda (ip, session_len): session_len >= avg_session_len)\
    .map(lambda (ip, session_len): (session_len, ip))\
    .sortByKey(False)\
    .map(lambda (session_len, ip): (ip, session_len))
    
top_ips = top_ips_rdd.collect()
ips_data_file = open('top_ips.csv', 'w')
ips_data_file.write('ip, session_length(ms) \n')
for item in top_ips:
    (ip, session_len) = item
    ips_data_file.write(str(ip) + ', '+ str(session_len))
    ips_data_file.write('\n')
ips_data_file.close()
sc.stop()





