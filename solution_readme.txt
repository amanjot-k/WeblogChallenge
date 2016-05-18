The script is contained in the file paytm_weblog_challenge.py
To run the script:
1. Make sure there is unzipped version of data available in the data/ directory
2. Run 'spark-submit paytm_weblog_challenge.py

Output files created:
1. 'sessionize_json' : This file contains a snippet of the format of sesssionized 
    information since it was too large to upload to git. A re-run of the script 
    will create the entire file.
2. 'average_session_time.csv': This file just contains one value: the average
    session time in milliseconds
3. 'unique_url_hits.csv': This file contains count of unique URLs hit per session.
4. 'top_ips.csv': This file contains top 100 IPs with their corresponding session
    times in milliseconds.


