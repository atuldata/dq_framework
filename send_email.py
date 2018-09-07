"""
TODO: What am I?
"""
from __future__ import print_function
from builtins import str
from builtins import range
from builtins import object

import yaml
import sys
import os
import pid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import json

from ox_dw_db import OXDB
from ox_dw_db.settings import ENV
from ox_dw_logger import get_etl_logger

APP_NAME = 'dq_check'
APP_ROOT = \
    os.environ['APP_ROOT'] if 'APP_ROOT' in os.environ else os.environ['PWD']
LOCK_ROOT = os.path.join(APP_ROOT, 'locks')
OUTPUT_DIR = os.path.join(APP_ROOT, 'output')
JOB_ENV_FILE = os.path.join(APP_ROOT, 'conf', 'env.yaml')
JOB_CONFIG_FILE = os.path.join(APP_ROOT, 'jobs', APP_NAME, APP_NAME+'.yaml')

class SendEmail(object):
    """
    TODO
    """
    def __init__(self):
        self.lock = pid.PidFile(pidname="%s.LOCK" % APP_NAME,
                               piddir=LOCK_ROOT, enforce_dotpid_postfix=False)
        self.logger = get_etl_logger(APP_NAME)
        self.env = yaml.load(open(JOB_ENV_FILE))
        self.config = yaml.load(open(JOB_CONFIG_FILE))

        self.msg = MIMEMultipart('alternative')
        if 'prod' in ENV['DW_DSN']:
            self.msg['Subject'] = "Snowflake Prod DQ Framework Checks"
        else:
            self.msg['Subject'] = "Snowflake QA DQ Framework Checks"
        self.msg['From'] = self.config["MAIL_FROM"]
        self.msg['To'] = self.config["MAIL_TO"]
        self.mail_result = '<html><head><style> body {background-color: powderblue;} h1   {color: blue;}p    {color: red;} td, th {border: 1px solid #999;padding: 0.5rem;}</style></head><body>'

    def send_email(self, send_from,send_to,content):
        """
        Get check list from yaml files or dq_check list.
        """
        mail_result = self.mail_result + content + "</body></html>"
        separate_msg = MIMEMultipart('alternative')
        part1 = MIMEText(mail_result, 'html')
        separate_msg['From'] = send_from
        separate_msg['Subject'] = ENV['DW_DSN'] + " Alerts"
        separate_msg.attach(part1)
        new = smtplib.SMTP('localhost')
        if len(content) > 0:
            new.sendmail(send_from, send_to,
                         separate_msg.as_string())
        new.close()
