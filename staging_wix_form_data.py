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
    
    
    tex_label = []
    all_data_frames = []
    all_form_data = []
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    date_str = yesterday.strftime("%d/%m/%Y")
    yesterday_was = date_str.split('/')[0]
    month_is = date_str.split('/')[1]
    year_is = date_str.split('/')[2]
    
    count = 0
    for msg in messages:
        # Get the message from its id
        txt = service.users().messages().get(userId='me', id=msg['id']).execute()
        #txt2 = service.users().messages().modify(userId='me', id=msg['id'], body={'removeLabelIds': ['UNREAD']}).execute()
        tex_label.append(txt)
        payload = txt['payload']
        headers = payload['headers']
        
        for d in headers:
            if d['name'] == 'From' and d['value'] == 'no-reply@crm.wix.com':
                print(count)
                for d in headers:
                    if d['name'] == 'Date' and pd.to_datetime(d['value']).to_pydatetime().day == int(yesterday_was) and pd.to_datetime(d['value']).to_pydatetime().month == int(month_is) and pd.to_datetime(d['value']).to_pydatetime().year == int(year_is):

                        Date_time = d['value']
                        print(Date_time)
                        soup = BeautifulSoup(base64.b64decode(txt['payload']['body']['data'].replace("-","+").replace("_","/")) , "lxml")
            
                        data_full = []
                        Local_date = []
                        Contact_Last_Name = []
                        Contact_First_Name = []
                        form_name = []
                        First_Name = []
                        Last_Name = []
                        Email = []
                        Phone = []
                        Address = []
                        Address_2 = []
                        Zip_Code = []
                        Dropdown_Field = []
                        Home_will_be = []
                        Timeline = []
                        Square_Feet = []
                        ID = []
                        Rooms_To_Stage = []
                        Additional_Information = []
                        Source = []
            
                        body = soup.body()
                        for span in body:
                            try:
                                #Fields.append(span.select_one('span[class="text light"]').text)
                                Full_data =  span.select_one('span[class="text"]').text + '##@##' + span.select_one('span[class="text light"]').text
                                #print(Full_data)
                                data_full.append(Full_data)
                                #values.append(span.select_one('span[class="text"]').text)
                            except:
            
                                pass
            
            
                        data_full = pd.unique(data_full).tolist()
            
                        for data in data_full:
                            if data.split(':##@##')[0] == 'Contact Last Name':
                                Contact_Last_Name.append(data.split(':##@##')[1])
                                Date_time = str(pd.to_datetime(Date_time).to_pydatetime().year) + '-' + str(pd.to_datetime(Date_time).to_pydatetime().month) + '-' + str(pd.to_datetime(Date_time).to_pydatetime().day) + 'T' + str(pd.to_datetime(Date_time).to_pydatetime().hour) + ':' + str(pd.to_datetime(Date_time).to_pydatetime().minute) + ':' + str(pd.to_datetime(Date_time).to_pydatetime().second) + 'Z'
                                print(Date_time)
                                Local_date.append(Date_time)
                                ID.append(str(uuid.uuid1()))
                            elif data.split(':##@##')[0] == 'Contact First Name':
                                Contact_First_Name.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'form-name':
                                form_name.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'First Name':
                                First_Name.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'Last Name':
                                Last_Name.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'Email':
                                Email.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'Phone':
                                Phone.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'Address':
                                Address.append(data.split(':##@##')[1].replace("\r", " ").replace("-", " ").replace(";", " ").replace("\t\t", " ").replace(",","***").replace("\n", " "))
                            elif data.split(':##@##')[0] == 'Address 2':
                                Address_2.append(data.split(':##@##')[1].replace("\r", " ").replace("-", " ").replace(";", " ").replace("\t\t", " ").replace(",","***").replace("\n", " "))
                            elif data.split(':##@##')[0] == 'Zip Code':
                                Zip_Code.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'Dropdown Field':
                                Dropdown_Field.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'Home will be':
                                Home_will_be.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'Timeline':
                                Timeline.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'Square Feet':
                                Square_Feet.append(data.split(':##@##')[1])
                            elif data.split(':##@##')[0] == 'Rooms To Stage':
                                Rooms_To_Stage.append(data.split(':##@##')[1].replace("\r", " ").replace("-", " ").replace(";", " ").replace("\t\t", " ").replace(",","***").replace("\n", " "))
                            elif data.split(':##@##')[0] == 'Additional Information':
                                Additional_Information.append(data.split(':##@##')[1].replace("\r", " ").replace("-", " ").replace(";", " ").replace("\t\t", " ").replace(",","***").replace("\n", " "))
                                    
                            elif data.split(':##@##')[0] == 'Source':
                                Source.append(data.split(':##@##')[1])
            
            
                        if len(Additional_Information) == 0:
                            Additional_Information.append('null')
                        if len(Source)==0:
                            Source.append('null')
                        if len(Rooms_To_Stage)==0:
                            Rooms_To_Stage.append('null')
                        if len(Square_Feet)==0:
                            Square_Feet.append('null')
                        if  len(Contact_Last_Name) == 0:
                            Contact_Last_Name.append('null')
                        if len(Contact_First_Name)==0:
                            Contact_First_Name.append('null')
            
                        if len(form_name)==0:
                            form_name.append('null')
                        if len(First_Name) == 0:
                            First_Name.append('null')
            
                        if len(Last_Name) == 0:
                            Last_Name.append('null')
                        if len(Email) == 0:
                            Email.append('null')
            
                        if len(Phone)==0:
                            Phone.append('null')
            
                        if len(Address) == 0:
                            Address.append('null')
            
                        if len(Address_2) == 0:
                            Address_2.append('null')
                        if len(Zip_Code)==0:
                            Zip_Code.append('null')
            
                        if len(Dropdown_Field)==0:
                            Dropdown_Field.append('null')
            
                        if len(Home_will_be)==0:
                            Home_will_be.append('null')
                        if len(Timeline)==0:
                            Timeline.append('null')
            
            
            
                        form_data = pd.DataFrame({'Date_Time':Local_date, 'Contact_First_Name': Contact_First_Name, 'Contact_Last_Name': Contact_Last_Name, 'Email':Email, 'Phone': Phone, 'Address':Address,
                                                 'Address_2':Address_2,'Zip_Code': Zip_Code, 'Dropdown_Field':Dropdown_Field, 'Home_will_be': Home_will_be, 
                                                 'Timeline':Timeline, 'Square_Feet':Square_Feet, 'Rooms_To_Stage':Rooms_To_Stage, 
                                                 'Additional_Information':Additional_Information, 'Source':Source, 'ID': ID})
            
            
                        all_form_data.append(form_data)
    
    
    all_email_dataframe = pd.concat(all_form_data,ignore_index=True,axis=0)

    connection = pymysql.connect(host='cndro.cjoujctokgd7.us-west-1.rds.amazonaws.com', user='admin', password='Olasehinde', db='cndro_staging')
    cursor = connection.cursor()
    sql = "INSERT INTO `wix_form_table` (`Date_Time`, `Contact_First_Name`, `Contact_Last_Name`, `Email`, `Phone`, `Address`,`Address_2`,`Zip_Code`, `Dropdown_Field`, `Home_will_be`,  `Timeline`, `Square_Feet`, `Rooms_To_Stage`,`Additional_Information`, `Source`, `ID`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    database_count = 0               
    for i in range(0, len(all_email_dataframe)):
        database_count = database_count + 1
        cursor.execute(sql, (all_email_dataframe.iloc[i]['Date_Time'], all_email_dataframe.iloc[i]['Contact_First_Name'], all_email_dataframe.iloc[i]['Contact_Last_Name'], all_email_dataframe.iloc[i]['Email'], all_email_dataframe.iloc[i]['Phone'], all_email_dataframe.iloc[i]['Address'],all_email_dataframe.iloc[i]['Address_2'],all_email_dataframe.iloc[i]['Zip_Code'], all_email_dataframe.iloc[i]['Dropdown_Field'], all_email_dataframe.iloc[i]['Home_will_be'],  all_email_dataframe.iloc[i]['Timeline'], all_email_dataframe.iloc[i]['Square_Feet'], all_email_dataframe.iloc[i]['Rooms_To_Stage'],all_email_dataframe.iloc[i]['Additional_Information'], all_email_dataframe.iloc[i]['Source'], all_email_dataframe.iloc[i]['ID']))
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

with DAG('staging_wix_form_data',default_args=default_args,description='Archive ETL Process',schedule_interval='0 * * * *',start_date=ts(2021,9,23),tags=['wix'],catchup=False) as dag:
    
    google_cred_task = PythonOperator(task_id='google_cred', python_callable=google_cred, email_on_failure=True,)
    account_url_task = PythonOperator(task_id='AccountsUrl', python_callable=AccountsUrl, email_on_failure=True,)
    Refresh_task = PythonOperator(task_id='RefreshToken', python_callable=RefreshToken,email_on_failure=True,)
    api_access_task = PythonOperator(task_id='api_access', python_callable=api_access,email_on_failure=True,)
    emailTread_To_dataframe_task = PythonOperator(task_id='emailTread_to_dataframe', python_callable=emailTread_to_dataframe,email_on_failure=True,)
    Send_data_To_S3_task = PythonOperator(task_id='Send_data_To_S3', python_callable=Send_data_TO_S3,email_on_failure=True,)

    google_cred_task >> account_url_task >> Refresh_task >> api_access_task >> emailTread_To_dataframe_task >> Send_data_To_S3_task
   