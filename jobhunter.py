# Josh Brown
# CNE 340 Christine Sutton
# 2/21/23 Job Hunter Project

import mysql.connector
import time
import json
import requests
from datetime import date
import html2text

def connect_to_sql():
    conn = mysql.connector.connect(user='root', password='root',
                                   host='127.0.0.1', database='cne340', port='8889')
    return conn

def create_tables(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS jobs (num INT PRIMARY KEY auto_increment, id varchar(50), 
    company_name LONGBLOB, publication_date DATE, url varchar(3000), title LONGBLOB, description LONGBLOB)''')

def query_sql(cursor, query):
    cursor.execute(query)
    return cursor

with open('config.json') as config_file: # (SOURCE: https://www.loekvandenouweland.com/content/using-json-config-files-in-python.html)
    data = json.load(config_file)

width = data['width']
height = data['height']

def add_new_job(jobdetails):
    conn = mysql.connector.connect(user='root', password='root', # (SOURCE: https://www.youtube.com/watch?v=ITmOVaZ0-ko)
                                    host='127.0.0.1', database='cne340', port='8889')
    cursor = conn.cursor()
    description = html2text.html2text(jobdetails['description'])
    date = jobdetails['publication_date'][0:10]
    job_id = jobdetails['id']
    company = jobdetails['company_name']
    url = jobdetails['url']
    title = jobdetails['title']
    query = cursor.execute('INSERT INTO jobs(description, publication_date, id, company_name, url, title)'
                        'VALUES(%s,%s,%s,%s,%s,%s)', (description, date, job_id, company, url, title))
    conn.commit()
    cursor.close()
    conn.close()
    return query_sql(cursor, query)

def check_if_job_exists(cursor, jobdetails):
    title = jobdetails['title']
    query = "SELECT * FROM jobs WHERE title = %s"
    cursor.execute(query, (title,))
    return cursor

def delete_job(cursor):
    query = "DELETE FROM jobs WHERE publication_date < CURRENT_DATE() - INTERVAL 14 DAY" # (SOURCE: https://stackoverflow.com/questions/4364913/delete-rows-with-date-older-than-30-days-with-sql-server-query)
    cursor.execute(query)
    return query_sql(cursor, query)

def fetch_new_jobs():
    query = requests.get("https://remotive.com/api/remote-jobs")
    datas = json.loads(query.text)
    return datas

def jobhunt(cursor):
    jobpage = fetch_new_jobs()
    # print(jobpage)
    add_or_delete_job(jobpage, cursor)

def add_or_delete_job(jobpage, cursor):
    for jobdetails in jobpage['jobs']:  # https://careerkarma.com/blog/python-typeerror-int-object-is-not-iterable/
        check_if_job_exists(cursor, jobdetails)
        is_job_found = len(cursor.fetchall()) > 0  # https://stackoverflow.com/questions/2511679/python-number-of-rows-affected-by-cursor-executeselect
        if is_job_found:
            delete_job(cursor)
        else:
            print("New job is found: " + jobdetails["title"] + " from " + jobdetails["company_name"])
            add_new_job(jobdetails)
            print(jobdetails["tags"])

def main():
    conn = connect_to_sql()
    cursor = conn.cursor()
    create_tables(cursor)

    while (1):
        jobhunt(cursor)
        time.sleep(14400)

if __name__ == '__main__':
    main()
