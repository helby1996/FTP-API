#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  2 14:23:24 2021

@author: admin
"""

import pandas as pd
import numpy as np
import datetime
import imaplib
import imap_tools
import requests
import json
import os
from io import StringIO
import ssl
import io
import sys
import pysftp 
import paramiko

def from_sftp():
            sftp= pysftp.Connection('', username='',
                        password='')
            sftp.cwd('/input/')    
            listoffiles = [
                attr.filename
                for attr in sftp.listdir_attr()
                if ".csv" in attr.filename
                and 'CstPln' in attr.filename #включить при работе
                           ]
                       #if datetime.datetime.fromtimestamp(attr.st_mtime).date() == datetime.datetime.now().date()]
            data = pd.read_csv(listoffiles[5], dtype=str, sep=";")
            data =pd.concat(
                list(map(lambda x:pd.read_csv(listoffiles[x],
                 dtype=str, sep=","),
                 range(0,len(listoffiles))))
                )
            list(map(lambda x: sftp.rename('/var/sftp/paz/input/',
                                           '/var/sftp/paz/input/OK'+x),
                     listoffiles))
            sftp.rename('/input/'+listoffiles[5], '/input/OK/'+listoffiles)
                        '/var/sftp/paz/input/OK/'+listoffiles[0])
            return data
            
data = from_sftp()
def data_processing1(data):
    data['dateTime'] = pd.to_datetime(data['date']
                                      + " " + data['time'][:-2]).astype(str)
    data['dateTime'] = data['dateTime'].str.replace("-","/").astype(str).str[:-3]
    data['energy'] = np.where(data['energy'].str.contains("-"),
                              data['energy'].str.replace("-",""), "-"+data['energy'])
    return data[['dateTime','deviceId', 'energy']]
def data_processing_cstprd(data):
    data = pd.read_csv('/Users/admin/Downloads/CustomerPrediction20210824120000.csv', 
                       dtype=str, names=['Pl_Type', 'dateTime', 'Hour', 'energy',
                                        'deviceId'],
                       skiprows=1)
    data['dateTime'] =  data['dateTime']+ " "+data['Hour']
    #data['energy'] = int(data['energy'])*(-1)
    data = data[['dateTime', 'deviceId', 'energy']]
    return data
    
filename=data_processing_cstprd(data)
def to_ftp(filename):
        sftp= pysftp.Connection('', username='',
                        password='')
        sftp.cwd('/output/')
        name = f"Cst_Pln_{datetime.datetime.now()}.csv"
        filename.to_csv(name, index=False)
        sftp.put(name, f"/output/{name}")                
to_ftp(filename)
def run_job():
    response=requests.post(url='',
                           headers={'Content-Type':'application/json'},
                           data='{"login": "","password": ""}')
    
    token = response.text.encode('utf8')
    toke = json.loads(str(token)[2:len(token)+2])
    response = requests.get(
        url='',
        headers ={'Authorization':'Bearer '+toke['id_token'],
        'accept': '*/*'}
        )
    return response.status_code
def email_sender(sender_email_id,sender_password,
                 email_message,recipients,host,subject):
    body = email_message
    msg = MIMEMultipart()
    msg['Subject'] = host + " - " + subject
    msg['From'] = sender_email_id
    msg['To'] = (', ').join(recipients.split(','))
    msg.attach(MIMEText(body,'plain'))
    email = smtplib.SMTP('smtp.gmail.com', 587) 
    email.starttls() 
    email.login(sender_email_id, sender_password) 
    email.send_message(msg)
    email.quit()