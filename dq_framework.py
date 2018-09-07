"""
TODO: What am I?
"""
import os
import pid
import traceback
import json
import yaml
import argparse
import time
from datetime import datetime, timedelta
import subprocess

from ox_dw_logger import get_etl_logger
from ox_dw_load_state import LoadState
from ox_dw_db import  OXDB
from send_email import SendEmail
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

APP_NAME = 'dq_framework'
APP_ROOT = \
    os.environ['APP_ROOT'] if 'APP_ROOT' in os.environ else os.environ['PWD']
LOCK_ROOT = os.path.join(APP_ROOT, 'locks')
OUTPUT_DIR = os.path.join(APP_ROOT, 'output')
JOB_ENV_FILE = os.path.join(APP_ROOT, 'conf', 'env.yaml')
JOB_CONFIG_FILE = os.path.join(APP_ROOT, 'jobs', APP_NAME, APP_NAME+'.yaml')

class DQCheck(object):
    """
    TODO
    """
    def __init__(self):

        self.lock = pid.PidFile(pidname="%s.LOCK" % APP_NAME,
                                piddir=LOCK_ROOT, enforce_dotpid_postfix=False)
        self.logger = get_etl_logger(APP_NAME)
        self.env = yaml.load(open(JOB_ENV_FILE))
        self.config = yaml.load(open(JOB_CONFIG_FILE))
        self.db = OXDB('SNOWFLAKE')
        self.logger.info("Snowflake connection established")


    def get_check_list(self):
        """
        Get check list from yaml files or dq_check list.
        """
        checks = self.config['CHECK_LIST']
        checks_tuple = []
        for check in checks:
            check = check['check']
            if 'send_to' in check:
                send_to = check['send_to']
            else:
                send_to = None
            checks_tuple.append((check['check_id'], check['source_dsn'],
                                 check['check_type'], check['description'],
                                 check['check_result'], check['on_change'],
                                 check['priority'], send_to))

        return checks_tuple

    def get_color(self,priority):
        if priority == 'high':
            color_code = 'red'
        elif priority == 'medium':
            color_code = 'orange'
        else:
            color_code = 'blue'
        return color_code

    def update_check_history(self,check_id,rows):

        metadb = OXDB('SNOWFLAKE')
        if len(rows) > 0:
            metadb.execute(self.config['CHECK_RUN_HISTORY'], check_id,
                           rows[0][0])
        else:
            metadb.execute(self.config['CHECK_RUN_HISTORY'], check_id, 0)
        metadb.commit()
        metadb.close()

    def create_fancy_grid(self,check_id,des,column_list):
        fancy_grid = "new FancyGrid({\n"
        fancy_grid += "title: " + "'check_" + str(
            check_id) + " " + str(des).replace("'", "") + "',\n"
        fancy_grid += "renderTo: " + "'check_" + str(
            check_id) + "',\n"
        fancy_grid += "width: 'fit',\n"
        fancy_grid += "height: 'fit',\n"
        fancy_grid += "theme: 'blue',\n"
        fancy_grid += "selModel: 'row',\n"
        fancy_grid += "trackOver: true,\n"
        fancy_grid += "data: " + "check_" + str(check_id) + ",\n"
        fancy_grid += "clicksToEdit: 1,\n"
        fancy_grid += "defaults: {\n"
        fancy_grid += "type: 'string',\n"
        fancy_grid += "editable: true,\n"
        fancy_grid += "sortable: true,\n"
        fancy_grid += "filter: {\n"
        fancy_grid += "header: true\n"
        fancy_grid += "}\n"
        fancy_grid += "},\n"
        fancy_grid += "paging: true,\n"
        fancy_grid += "columns: "

        fancy_grid += json.dumps(column_list).replace("},", "},\n").replace(
            '"title"', 'title').replace('"index"', 'index').replace(
                '"flex"', 'flex') + "\n"


        fancy_grid += "});\n"

        return fancy_grid

    def processor(self):

        checks = self.get_check_list()
        content = ""
        file = open('/var/www/html/data.js', 'w')
        index_html = open('/var/www/html/index.html', 'w')
        
    
        index_html.writelines(self.config['INDEX_HEADER'])
        index_html.writelines('<div style="width:100%; height: 200px; ">')

        file.write("$(function() {\n")

        for check in checks:
            # -----------------------
            dict_data = []
            check_id = check[0]
            check_type = check[2]
            des = check[3]
            command = check[4]
            priority = check[6]
            table_names = command

            # -----------------------
            color_code = self.get_color(priority)

            # -----------------------
            if check[1] == 'PYTHON':
                proc = subprocess.Popen(
                    [command], stdout=subprocess.PIPE, shell=True)
                (rows, err) = proc.communicate()
                fields = ['log_name', 'log_error']
            else:
                self.oxdb = OXDB(check[1])
                self.logger.info(command)
                rows = self.oxdb.get_executed_cursor(command).fetchall()
                fields = self.oxdb.get_executed_cursor(command).description
                self.oxdb.close()

            #-----------------------
            #self.update_check_history(check_id, rows)

            # -----------------------
            column_list = []
            row_data = ""
            # -----------------------

            if check[1] != 'PYTHON' and len(rows) > 0:
                if len(rows[0]) > 0:
                    # Compose the header of the table
                    row_data = "<p><b><font color='" + color_code + "'> check_id - " + str(
                        check_id) + " | " + des + "</font></b></p>"
                    row_data += "<table><tr>"

                    index_html.writelines('<div class="cellContainer">')
                    index_html.writelines('<div id="check_' + str(check_id) +
                                        '" style="margin: 5px; "></div>')

                    index_html.writelines('</div>')

                    if len(fields) == 1:
                        row_data = row_data + "<td><b>" + "check_id" + "</b></td>"
                        row_data = row_data + "<td><b>" + "check_type" + "</b></td>"
                        row_data = row_data + "<td><b>" + "table_involved" + "</b></td>"
                        row_data = row_data + "<td><b>" + "bad_data_count" + "</b></td>"
                    else:
                        for f in fields:
                            row_data = row_data + "<td><b>" + f[0] + "</b></td>"
                            column_list.append({
                                'title': f[0],
                                'index': f[0],
                                'flex': 1
                            })

                    row_data += "</tr>"

                    # Fill the table with data
                    for r in rows:
                        row_data += "<tr>"
                        if len(fields) == 1:
                            row_data = row_data + "<td>" + str(
                                check_id) + "</td>"
                            row_data = row_data + "<td>" + str(
                                check_type) + "</td>"
                            row_data = row_data + "<td>" + str(
                                table_names) + "</td>"
                            row_data = row_data + "<td>" + str(r[0]) + "</td>"

                            dict_data.append({
                                'check_id': str(check_id),
                                'check_type': str(check_type),
                                'table_names': str(table_names),
                                'bad_data_count': str(r[0])
                            })

                            column_list.append({
                                'title': 'check_id',
                                'index': 'check_id',
                                'flex': 1
                            })
                            column_list.append({
                                'title': 'check_type',
                                'index': 'check_type',
                                'flex': 1
                            })
                            column_list.append({
                                'title': 'table_names',
                                'index': 'table_names',
                                'flex': 1
                            })
                            column_list.append({
                                'title': 'bad_data_count',
                                'index': 'bad_data_count',
                                'flex': 1
                            })

                        else:
                            item = {}
                            for f in fields:
                                item[f[0]] = ''
                            for i in range(len(r)):
                                row_data = row_data + "<td>" + str(
                                    r[i]) + "</td>"
                                item[fields[i][0]] = str(r[i])
                            dict_data.append(item)

                        row_data += "</tr>"

                    row_data += "</table>"

                    # -----------------------

                    file.write("var check_" + str(check_id) + " = ")
                    file.write(json.dumps(dict_data).replace("},", "},\n"))
                    file.write(";\n")
                    file.write("\n")

                    fancy_grid = self.create_fancy_grid(check_id,des,column_list)
                    file.write(fancy_grid)

                    file.write("\n")
                # -----------------------
            content += row_data
            # -----------------------
            if check[1] == 'PYTHON':
                row_data = "<p><b><font color='" + color_code + "'> check_id - " + str(
                    check_id) + " | " + des + "</font></b></p>"
                row_data += "<table><tr>"
                for f in fields:
                    row_data = row_data + "<td><b>" + f + "</b></td>"
                    print(row_data)
                for r in rows:
                    for i in range(len(fields)):
                        row_data = row_data + "<td>" + str(r[i]) + "</td>"
                        print(row_data)

            if check[7]:
                SendEmail.send_email(self.config['MAIL_FROM'], check[7], row_data)

        # -----------------------
        file.write("});\n")
        file.close()
        index_html.writelines('</div></body></html>')
        index_html.close()
        #SendEmail.send_email(self.config['MAIL_FROM'], self.config['MAIL_TO'], content)

if __name__ == "__main__":
    checker = DQCheck()
    checker.lock.create()
    checker.processor()
