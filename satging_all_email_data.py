# The DAG object; we'll need this to instantiate a DAG Yeah...
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

import json
from optparse import OptionParser
import smtplib
import sys
import urllib.request
import datetime
import os.path
import numpy as np
import requests
import json
#import lxml
import site
import importlib
import pkg_resources
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
import base64
from bs4 import BeautifulSoup
import pandas as pd
import boto3
import csv
from datetime import timedelta as ti
from datetime import datetime as ts
import pymysql
import time
# These args will be pass to use in the dag 

""" Default Arguments are defined Below """

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['olasehindeomolayo9@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': ti(minutes=5),
}

def google_cred(**kwargs):
    task_instance = kwargs['ti']
    
    GOOGLE_ACCOUNTS_BASE_URL = 'https://accounts.google.com'
    client_secret = "HxRQOY2fhaHmsAtOifH8QOkf"
    client_id = '474590554509-3gb10k3dosghohd26ueuukqg7imdsf04.apps.googleusercontent.com'
    refresh_token = '1//03pZdWxvzyWcbCgYIARAAGAMSNwF-L9IrWX6osPWfmkLtsDgo4MKJ1s9uiVC0ovWk1th9_Krbw3rIQMXHo1oe68n4iKm3pb_yc8M'
    
    
    # Hardcoded dummy redirect URI for non-web apps.
    REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
    scope = 'https://mail.google.com/'
    
    task_instance.xcom_push('t_scope', scope)
    task_instance.xcom_push('t_GOOGLE_ACCOUNTS_BASE_URL', GOOGLE_ACCOUNTS_BASE_URL)
    task_instance.xcom_push('t_client_secret', client_secret)
    task_instance.xcom_push('t_client_id', client_id)
    task_instance.xcom_push('t_refresh_token', refresh_token)
    task_instance.xcom_push('t_REDIRECT_URI', REDIRECT_URI)

def AccountsUrl(**kwargs):
    """Generates the Google Accounts URL.
    Args:
    command: The command to execute.
    Returns:
    A URL for the given command.
    """
    task_instance = kwargs['ti']
    command = 'o/oauth2/auth'
    GOOGLE_ACCOUNTS_BASE_URL = task_instance.xcom_pull(task_ids='google_cred', key='t_GOOGLE_ACCOUNTS_BASE_URL')
    accounturl = '%s/%s' % (GOOGLE_ACCOUNTS_BASE_URL, command)

    task_instance.xcom_push('t_accounturlpush', accounturl)


def RefreshToken(**kwargs):
    """Obtains a new token given a refresh token.
    See https://developers.google.com/accounts/docs/OAuth2InstalledApp#refresh
    Args:
    client_id: Client ID obtained by registering your app.
    client_secret: Client secret obtained by registering your app.
    refresh_token: A previously-obtained refresh token.
    Returns:
    The decoded response from the Google Accounts server, as a dict. Expected
    fields include 'access_token', 'expires_in', and 'refresh_token'.
    """

    task_instance = kwargs['ti']
    client_id = task_instance.xcom_pull(task_ids='google_cred', key='t_client_id')
    client_secret = task_instance.xcom_pull(task_ids='google_cred', key='t_client_secret')
    refresh_token = task_instance.xcom_pull(task_ids='google_cred', key='t_refresh_token')
    the_url = 'https://accounts.google.com/o/oauth2/token'
  

    headers = {'Content-Type': 'application/x-www-form-urlencoded' }
    data={"refresh_token":refresh_token,"client_id":client_id,"client_secret":client_secret,"grant_type":"refresh_token"}
    response=requests.post(the_url, headers=headers,data=data)
    values=response.json()
    json_response= values['access_token']
    expiry = str(datetime.datetime.now() + datetime.timedelta(days=1)).replace(' ', 'T') + 'Z'
    print(json_response)
    print(expiry)
    #access_token = auth_data['access_token']
    task_instance.xcom_push('t_expiry', expiry)
    task_instance.xcom_push('t_access_token', json_response)


def api_access(**kwargs):
    task_instance = kwargs['ti']
    
    
    Access_token = task_instance.xcom_pull(task_ids='RefreshToken', key='t_access_token')
    refresh_token = task_instance.xcom_pull(task_ids='google_cred', key='t_refresh_token')
    client_id = task_instance.xcom_pull(task_ids='google_cred', key='t_client_id')
    client_secret = task_instance.xcom_pull(task_ids='google_cred', key='t_client_secret')
    expiry = task_instance.xcom_pull(task_ids='RefreshToken', key='t_expiry')
    
    

    new_auth_file = {"token":Access_token, "refresh_token":refresh_token,
                    "token_uri": "https://oauth2.googleapis.com/token", "client_id":client_id,
                    "client_secret":client_secret, "scopes": ["https://mail.google.com/"],
                    "expiry":expiry
                    }
    # Serializing json 
    json_object = json.dumps(new_auth_file)
    
    # Writing to sample.json
    with open("token.json", "w") as outfile:
        outfile.write(json_object)
    
    
def emailTread_to_dataframe(**kwargs):
    task_instance = kwargs['ti']
    SCOPES = ['https://mail.google.com/']
    
    
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    service = build('gmail', 'v1', credentials=creds)
    # try:
    results = service.users().messages().list(userId='me').execute()
    messages = results['messages']
    # We are getting there!!!
    
    while 'nextPageToken' in results:
        page_token = results['nextPageToken']
        results = service.users().messages().list(userId='me', pageToken=page_token).execute()
        messages.extend(results['messages'])
    # except:
    #     print('An error occurred, most likely an error 401')
    
    
    text_label = []
    headers_inspect = []
    all_data_frames = []
    
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    date_str = yesterday.strftime("%d/%m/%Y")
    yesterday_was = date_str.split('/')[0]
    month_is = date_str.split('/')[1]
    year_is = date_str.split('/')[2]
    
    count = 0
    
    for msg in messages:
        count = count + 1
        print('HI, I am at email COUNT: ',count)
        # Get the message from its id
        txt = service.users().messages().get(userId='me', id=msg['id']).execute()
        #txt2 = service.users().messages().modify(userId='me', id=msg['id'], body={'removeLabelIds': ['UNREAD']}).execute()
        text_label.append(txt)
        #print(txt)
    
        #try:
        payload = txt['payload']
        headers = payload['headers']
        
        x = []
        Subject = []
        Email_from = []
        Customer_name = []
        Local_date = []
        Email_to = []
        Email_Cc = []
        Email_Bcc = []
        Message_reply_to = []
        Email_text = []
        
        
        Email_flag = []
        Email_header = []
        Local_message_date = []
        
        
        headers_inspect.append(headers)
        for d in headers:
            if d['name'] == 'Subject':
                subject = d['value']
                Subject.append(subject)
            if d['name'] == 'From':
                sender = d['value'].split('<')[-1].split('>')[0]
                Email_from.append(sender)
                k = str(sender)
                customer = k.split('<')[0]
                # print(customer)
                Customer_name.append(customer)
            if d['name'] == 'Date':
                Date = d['value']
                Local_date.append(Date)
            if d['name'] == 'To':
                eemail_To = d['value']
                # print('THIS IS EMAIL TO',eemail_To)
                Email_to.append(eemail_To) 
                
            if d['name'] == 'Cc':
                eemail_cc = d['value']
                Email_Cc.append(eemail_cc) 
            
            if  d['name'] == 'Bcc' :
                eemail_bc = d['value']
                Email_Bcc.append(eemail_bc)
                
                        
            if d['name'] == 'Reply-To':
                Reply_To = d['value']
                # print('THIS IS REPLY TO',Reply_To)
                Message_reply_to.append(Reply_To)
    
        if len(Message_reply_to)==0:
            Message_reply_to.append('null')
        if len(Subject) ==0:
            Subject.append('null')
        if len(Email_from) ==0:
            Email_from.append('null')
        if len(Customer_name) ==0:
            Customer_name.append('null')
        if len(Local_date) ==0:
            Local_date.append('null')
        if len(Email_Cc) ==0:
            Email_Cc.append('null')
        if len(Email_Bcc) ==0:
            Email_Bcc.append('null')
        if len(Email_to) ==0:
            Email_to.append('null')
            
        try:
            parts = payload.get('parts')[0]
            #print(parts) #hjhjhsa
            data = parts['body']['data']
            data = data.replace("-","+").replace("_","/")
            
            decoded_data = base64.b64decode(data)
            soup = BeautifulSoup(decoded_data , "lxml")
            body = soup.body()

            emailtext = str(body[0].get_text())
            newtext = emailtext.replace('\r\n', ' ')
            newtext = newtext.replace('\xa0', ' ').replace('\r', ' ').replace('-', " ")
            newtext = newtext.replace(';', ' ').replace('\t\t', ' ').replace('>>', ' ').replace('>', ' ')
            newtext = newtext.replace(',', ' ').replace('\n', ' ').replace('|'," ").replace('________________________________', ' ')
            Email_text.append(str(newtext))
        except:
            body = 'null'
            Email_text.append(body)
            pass
        
        if txt['labelIds'] == list(['IMPORTANT', 'CATEGORY_PERSONAL', 'INBOX']) and txt['labelIds'][-2] == 'CATEGORY_PERSONAL':
            Email_flag.append("('SEEN' 'ANSWERED')")
        elif txt['labelIds'] == list(['SENT']):
            Email_flag.append("('SEEN')")
        elif txt['labelIds'] == list(['CATEGORY_UPDATES', 'INBOX']):
            Email_flag.append("('UPDATES')")
        elif txt['labelIds'] == list(['IMPORTANT', 'CATEGORY_UPDATES', 'INBOX']) and txt['labelIds'][-2] == 'CATEGORY_UPDATES':
            Email_flag.append("('UPDATES')")
        elif txt['labelIds'] == list(['CATEGORY_PROMOTIONS', 'INBOX']):
            Email_flag.append("('PROMOTIONS')")
        elif txt['labelIds'] == list(['INBOX']):
            Email_flag.append("('UPDATES')")
        else:
            Email_flag.append("('UPDATES')") 
        
        email_data_df = {'Email_from': Email_from, 'Email_to': Email_to,'Email_Cc':Email_Cc,'Email_Bcc':Email_Bcc, 'Customer_name': Customer_name, 'Subject': Subject, 'Local_date': Local_date, 'Message_reply_to':Message_reply_to,'Email_flag': Email_flag, 'Email_text':Email_text}
        #  return email_data_df #Testing
        email_dataframe = pd.DataFrame(data=email_data_df)
        #print(email_dataframe)
        task_instance.xcom_push('t_all_Archive', email_data_df)
        all_data_frames.append(email_dataframe)
        
    
    all_email_dataframe = pd.concat(all_data_frames,ignore_index=True,axis=0)

    connection = pymysql.connect(host='cndro.cjoujctokgd7.us-west-1.rds.amazonaws.com', user='admin', password='Olasehinde', db='cndro_staging')
    cursor = connection.cursor()
    sql = "INSERT INTO `staging_email_airflow_test` (`Email_from`, `Email_to`, `Email_Cc`, `Email_Bcc`,`Customer_name`, `Subject`, `Local_date`,`Message_reply_to`,`Email_flag`, `Email_text`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    database_count = 0               
    for i in range(0, len(all_email_dataframe)):
        database_count = database_count + 1
        cursor.execute(sql, (all_email_dataframe.iloc[i]['Email_from'], all_email_dataframe.iloc[i]['Email_to'],all_email_dataframe.iloc[i]['Email_Cc'], all_email_dataframe.iloc[i]['Email_Bcc'], all_email_dataframe.iloc[i]['Customer_name'], all_email_dataframe.iloc[i]['Subject'], all_email_dataframe.iloc[i]['Local_date'], all_email_dataframe.iloc[i]['Message_reply_to'], all_email_dataframe.iloc[i]['Email_flag'], all_email_dataframe.iloc[i]['Email_text'] ))
        time.sleep(3)
        connection.commit()
    
        print("Row number : ", database_count ," Is now in the Database")
                   
def Send_data_TO_S3(**kwargs):
    task_instance = kwargs['ti']
    bucket_name = "airflowbuckettest"
    if os.path.exists('all_data.csv'):
        client=boto3.client('s3', aws_access_key_id='AKIA3SCCI2XCBCCHAFFZ', aws_secret_access_key='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')
        client.upload_file('all_data.csv', 'airflowbuckettest','Archive_Historical/staging_Archive_dataframe.csv')
        print('I HAVE SEND YOUR DATA TO S3, Enjoy!!!')

with DAG('staging_all_email_data_airflow',default_args=default_args,description='Archive ETL Process',schedule_interval='0 * * * *',start_date=ts(2021,9,23),tags=['wix'],catchup=False) as dag:
    
    google_cred_task = PythonOperator(task_id='google_cred', python_callable=google_cred, email_on_failure=True,)
    account_url_task = PythonOperator(task_id='AccountsUrl', python_callable=AccountsUrl, email_on_failure=True,)
    Refresh_task = PythonOperator(task_id='RefreshToken', python_callable=RefreshToken,email_on_failure=True,)
    api_access_task = PythonOperator(task_id='api_access', python_callable=api_access,email_on_failure=True,)
    emailTread_To_dataframe_task = PythonOperator(task_id='emailTread_to_dataframe', python_callable=emailTread_to_dataframe,email_on_failure=True,)
    Send_data_To_S3_task = PythonOperator(task_id='Send_data_To_S3', python_callable=Send_data_TO_S3,email_on_failure=True,)

    google_cred_task >> account_url_task >> Refresh_task >> api_access_task >> emailTread_To_dataframe_task >> Send_data_To_S3_task
   