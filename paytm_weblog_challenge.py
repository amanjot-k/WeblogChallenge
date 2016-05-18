__author__ = 'amanjotkaur'

#######################################################################
# The following file is a python script that handles all tasks for the
# weblog challenge. It assumes the unzipped data is available in the 
# data/ directory
# TO RUN: spark-submit paytm_weblog_challenge.py
########################################################################

# imports
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import ujson
import csv

# global variables
SESSION_DEF = 900000 #15 minutes
TOP_IPS = 100 #Number of top IPs to extract

# helper functions
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


def sessionize(ts_url_list):
    '''
    This function takes in a list of (timestamp, url) tuples and breaks them into 
    a list of sessions where a session is all activity within a certain period of
    time defined by SESSION_DEF variable.
    '''
    beginning_ts = ts_url_list[0][-1]
    cutoff_ts = beginning_ts + SESSION_DEF
    sessions_list = []
    current_session = []
    for (url, ts, milli_ts) in ts_url_list:
        if milli_ts <= cutoff_ts:
            current_session.append((url, ts, milli_ts))
        else:
            sessions.append(current_session)
            current_session = []
            beginning_ts = milli_ts
            cutoff_ts = beginning_ts + SESSION_DEF
            current_session.append((url, ts, milli_ts))
    if len(sessions_list) == 0:
        sessions_list.append(current_session)
    return sessions_list



if __name__ == '__main__':
    process_raw_data('data/2015_07_22_mktplace_shop_web_log_sample.log')
    processed_filename = 'processed_2015_07_22_mktplace_shop_web_log_sample.csv'
    conf = sparkSettings()

    ##############################################################
    # Task 1: Sessionize the data
    # This snippet writes the session information to a JSON file
    # A session is of the format {'ip': ip, [list of ts_url dicts]}
    # The list of ts_url dicts contains dictionaries of the form
    # {'ts': timestamp, 'url': url} as its elements. The list is 
    # ordered by earliest timestamp to last to map out the path of 
    # user through the website in a session
    ###############################################################
    sc = SparkContext(conf=conf)

    # Initial preprocessing to extract metrics of interest namely 
    # timestamp, IP and request URL

    session_rdd = sc.textFile(processed_filename)\
        .map(lambda x: x.split(', '))\
        .map(lambda x: (x[0], x[2], x[11]))

    session_rdd = session_rdd.map(lambda (ts, ip_port, url): 
            (ip_port.split(':')[0], (url, ts, date_to_millis(ts))))\
        .groupByKey()\
        .map(lambda (x,y): (x, sorted(list(y), key=lambda x: x[-1])))\
        .flatMap(lambda (ip, url_ts_list): ((ip, session) for session in sessionize(url_ts_list)))
    sessionized_rdd = session_rdd    

    sessions = session_rdd.collect()
    sessions_file = open('sessionize.json', 'w')
    for item in sessions:
        (ip, url_ts_list) = item
        session_dict = {}
        session_dict['ip'] = str(ip)
        session_dict['ts_url_list'] = []
        for (url, ts, int_ts) in url_ts_list:
            session_dict['ts_url_list'].append({"ts": ts, "url": url})
        sessions_file.write(ujson.encode(session_dict))
        sessions_file.write('\n')
    sessions_file.close()

    #cleanup
    session_rdd.unpersist()
    del sessions

    #################################################################
    # Task 2: get the average session time
    # This snippet writes the average session time in milliseconds 
    # to a file. Sessions with only one URL hit are excluded in 
    # calculation of average session time so that the results are not 
    # skewed by a large number of zero length sessions.
    # #Average session time:   2663.60503180774 ms
    #################################################################

    avg_session_rdd = sessionized_rdd
    avg_session_rdd = avg_session_rdd\
        .map(lambda (ip, url_ts_list): (ip, [item[-1] for item in url_ts_list]))\
        .map(lambda (ip, session_tss): ('', (max(session_tss) - min(session_tss), 1)))\
        .filter(lambda (ip, session_len): session_len > 0)\
        .reduceByKey(lambda x,y : [a+b for (a,b) in zip(x, y)])\
        .map(lambda (_, (total_session_length, total_cnt)): ((total_session_length*1.0)/total_cnt))

    avg_session_len = avg_session_rdd.collect()[0]
    print avg_session_len
    avg_session_file = open('avg_session_time.csv', 'w')
    avg_session_file.write(str(avg_session_len) + ' ms')
    avg_session_file.close()

    #cleanup
    avg_session_rdd.unpersist()
    del avg_session_len

    ################################################################
    # Task 3: get unique URL visits per session
    # This snippet writes the IP for a session and the corresponding 
    # session start timestamp and unique URL hits to a comma separated 
    # file.
    #################################################################

    url_visits_rdd = sessionized_rdd
    url_visits_rdd = url_visits_rdd\
        .map(lambda (ip, url_ts_list): ((ip, url_ts_list[0][1]), len(set([item[0] for item in url_ts_list]))))

    unique_url_hits = url_visits_rdd.collect()
    unique_url_file = open('unique_url_hits.csv', 'w')
    unique_url_file.write('IP, session_start_ts, num_unique_urls\n')
    for item in unique_url_hits:
        ((ip, ts), url_cnt) = item
        unique_url_file.write(str(ip) + ',' + str(ts) + ',' + str(url_cnt))
        unique_url_file.write('\n')
    unique_url_file.close()
    url_visits_rdd.unpersist()
    del unique_url_hits

    ################################################################
    # Task 4: get top engaged IPs
    # This snippet writes the Top N IPs and their session lengths in 
    # milliseconds to a csv file. 
    #################################################################

    top_ips_rdd = sessionized_rdd
    top_ips_rdd = top_ips_rdd\
        .map(lambda (ip, url_ts_list): (ip, [item[-1] for item in url_ts_list]))\
        .map(lambda (ip, ts_list): (ip, (max(ts_list) - min(ts_list))))\
        .filter(lambda (ip, session_len): session_len >= 0)\
        .map(lambda (ip, session_len): (session_len, ip))\
        .sortByKey(False)\
        .map(lambda (session_len, ip): (ip, session_len))
        
    top_ips = top_ips_rdd.take(TOP_IPS)
    ips_data_file = open('top_ips.csv', 'w')
    ips_data_file.write('ip, session_length(ms) \n')
    for item in top_ips:
        (ip, session_len) = item
        ips_data_file.write(str(ip) + ', '+ str(session_len))
        ips_data_file.write('\n')
    ips_data_file.close()
    top_ips_rdd.unpersist()
    del top_ips





