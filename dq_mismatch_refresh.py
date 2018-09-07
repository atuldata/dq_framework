# ETL Name      : sqoop_pyhive.py
# Purpose       : to interact with hive to convert the data format and drop the older data
import os
import sys
import yaml
import time
import datetime
import textwrap
import sys
import re

"""
TODO: What am I?
"""
import os
import pid
import yaml
import time
import argparse

from ox_dw_logger import get_etl_logger
from ox_dw_load_state import LoadState
from ox_dw_db import OXDB

APP_NAME = 'data_compare'
APP_ROOT = \
    os.environ['APP_ROOT'] if 'APP_ROOT' in os.environ else os.environ['PWD']
LOCK_ROOT = os.path.join(APP_ROOT, 'locks')
OUTPUT_DIR = os.path.join(APP_ROOT, 'output')
JOB_ENV_FILE = os.path.join(APP_ROOT, 'conf', 'env.yaml')
JOB_CONFIG_FILE = os.path.join(APP_ROOT, 'jobs', 'dq_framework', APP_NAME + '.yaml')


class DataCompare:
    def __init__(self, yaml_file, data_source_dsn, data_type, table_name, partition_value):

        if table_name:
            self.lock = pid.PidFile(
                pidname="%s.LOCK" % APP_NAME + "_" + data_source_dsn + "_" + data_type + "_" + table_name,
                piddir=LOCK_ROOT, enforce_dotpid_postfix=False)
            self.logger = get_etl_logger(APP_NAME + "_" + data_source_dsn + "_" + data_type + "_" + table_name)
        else:
            self.lock = pid.PidFile(pidname="%s.LOCK" % APP_NAME + "_" + data_source_dsn + "_" + data_type,
                                    piddir=LOCK_ROOT, enforce_dotpid_postfix=False)
            self.logger = get_etl_logger(APP_NAME + "_" + data_source_dsn + "_" + data_type)
        self.env = yaml.load(open(JOB_ENV_FILE))
        self.config = yaml.load(open(JOB_CONFIG_FILE))

        self.logger.info("Snowflake connection established")

        self.data_source_dsn = data_source_dsn
        self.data_type = data_type
        self.table_schema = self.config['SCHEMA']
        self.table_name = table_name
        self.partition_value = partition_value

    def get_partition_column(self, table_name):
        if 'IMPALA' in self.data_source_dsn:
            impala_db = OXDB(self.data_source_dsn, transactions_support=False)
            sql_impala = 'SHOW TABLE STATS ' + self.table_schema + "." + table_name
            print (sql_impala)
            cursor = impala_db.get_executed_cursor(sql_impala)
            column_list = [column[0] for column in cursor.description]
            impala_db.close()
            partition_key = []

            if 'network_daily_fact' in table_name:
                for column_name in column_list:
                    if 'date_part_sid' in column_name:
                        partition_key = column_name
            elif 'daily_fact' in table_name:
                for column_name in column_list:
                    if 'date_sid' in column_name:
                        partition_key = column_name
            elif 'monthly_fact' in table_name:
                for column_name in column_list:
                    if 'month_sid' in column_name:
                        partition_key = column_name
            elif 'hourly_fact' in table_name:
                if 'utc_datehour_sid' in column_list:
                    partition_key = 'utc_datehour_sid'
                elif 'utc_date_sid' in column_list:
                    partition_key = 'utc_date_sid'
                else:
                    for column_name in column_list:
                        if 'date_sid' in column_name:
                            partition_key = column_name

        elif 'SNOWFLAKE' in self.data_source_dsn:
            column_list = self.get_column_list(table_name)
            print (column_list)
            partition_key = []
            if 'network_daily_fact' in table_name:
                for column_name in column_list:
                    if 'date_part_sid' in column_name:
                        partition_key = column_name
            elif 'daily_fact' in table_name:
                for column_name in column_list:
                    if 'date_sid' in column_name:
                        partition_key = column_name
            elif 'monthly_fact' in table_name:
                for column_name in column_list:
                    if 'month_sid' in column_name:
                        partition_key = column_name
            elif 'hourly_fact' in table_name:
                if 'utc_datehour_sid' in column_list:
                    partition_key = 'utc_datehour_sid'
                elif 'utc_date_sid' in column_list:
                    partition_key = 'utc_date_sid'
                else:
                    for column_name in column_list:
                        if 'date_sid' in column_name:
                            partition_key = column_name
                print (partition_key, 'key')
            else:
                if '_fact' in table_name:
                    if 'utc_datehour_sid' in column_list:
                        partition_key = 'utc_datehour_sid'
                    else:
                        for column_name in column_list:
                            if 'date_sid' in column_name:
                                partition_key = column_name
                else:
                    partition_key = None

        else:
            src_db = OXDB(self.data_source_dsn, transactions_support=False)
            partition_key = src_db.get_executed_cursor(self.config['GET_PARTITION'],
                                                       (self.table_schema,
                                                        table_name)).fetchall()
            src_db.close()
            if partition_key:
                partition_key = partition_key[0][0].replace(table_name + '.', '')

        return partition_key

    def get_table_list(self):
        if 'IMPALA' in self.data_source_dsn:
            print (self.data_source_dsn)
            impala_db = OXDB('IMPALA_DSN', transactions_support=False)
            table_list = impala_db.get_executed_cursor("show tables").fetchall()
            impala_db.close()
        elif 'SNOWFLAKE' in self.data_source_dsn:
            with OXDB('SNOWFLAKE') as oxdb:
                table_list = oxdb.get_executed_cursor("show tables").fetchall()
                table_list = [(table_name[1].lower(), table_name[2].lower()) for table_name in table_list]
                oxdb.commit()

        else:
            src_db = OXDB(self.data_source_dsn)
            set_schema_sql = 'set search_path=' + self.table_schema
            src_db.execute(set_schema_sql)
            table_list = src_db.get_executed_cursor(self.config['GET_TABLE'], self.table_schema).fetchall()
            src_db.close()
        exclude_table_list = self.config['EXCLUDE_TABLE_NAMES']
        result_list = []

        for name in table_list:
            flag = [1 for str in exclude_table_list if str in name[0]]
            if '_dim' in name:
                if len(flag) == 0 and bool(re.search(r'\d', name[0])) == False and name[0] in self.config['DIM_TABLES']:
                    if len(name[0]) == name[0].find(self.data_type) + len(self.data_type):
                        result_list.append(name[0])
            else:
                if len(flag) == 0 and bool(re.search(r'\d', name[0])) == False:
                    if len(name[0]) == name[0].find(self.data_type) + len(self.data_type):
                        result_list.append(name[0])
                    elif ('in_utc' in name[0] or 'networktz' in name[0] or 'instancetz') and 'fact' in name[0]:
                        result_list.append(name[0])

        return result_list

    def get_column_list(self, table_name):
        if 'IMPALA' in self.data_source_dsn:
            impala_db = OXDB(self.data_source_dsn, transactions_support=False)
            column_list = impala_db.get_executed_cursor(
                "describe " + self.table_schema + "." + table_name).fetchall()
            impala_db.close()
        elif 'SNOWFLAKE' in self.data_source_dsn:
            with OXDB('SNOWFLAKE') as oxdb:
                column_list = oxdb.get_executed_cursor(
                    "describe " + table_name).fetchall()
        else:
            src_db = OXDB(self.data_source_dsn)
            column_list = src_db.get_executed_cursor(self.config['GET_COLUMN'], self.table_schema,
                                                     table_name).fetchall()
            src_db.close()

        result_column_list = []
        for name in column_list:
            result_column_list.append(name[0].lower())
        return result_column_list

    def get_required_fact_columns(self, column_list, table_name):
        columns = []
        for column_name in column_list:
            if 'tot_' in column_name:
                columns.append(column_name)
        columns.append('rowcount')
        return columns

    def get_required_dim_columns(self, column_list):
        exclude_columns = ['external_id',
                           'timezone',
                           'secondary',
                           'parent',
                           '_uol',
                           'name']
        result_column_list = []

        for name in column_list:
            flag = [1 for str in exclude_columns if str in name]
            if len(flag) == 0:
                if len(name) == (name.find("_nk") + len("_nk")) and len(result_column_list) < 3:
                    result_column_list.append(name)
                elif len(name) == (name.find("_id") + len("_id")) and len(result_column_list) < 3:
                    result_column_list.append(name)
                elif len(name) == (name.find("_sid") + len("_sid")) and len(result_column_list) < 3:
                    result_column_list.append(name)
        if len(result_column_list) == 0:
            result_column_list.append(column_list[0])
        result_column_list.append('rowcount')
        return result_column_list

    def repupulate_update_records(self, source, table_name):

        with OXDB('SNOWFLAKE') as oxdb:
            oxdb.execute(self.config['REPOPULATE_TABLE'], (source, table_name))
            oxdb.commit()

    def generate_sql_for_fact(self, table_name, column_list, partition_key):

        matrix_col = ""
        matrix = ""

        # with OXDB('SNOWFLAKE') as oxdb:
        #    self.logger.info(self.data_source_dsn+','+table_name+','+self.table_schema+','+partition_key)
        #    existing_value = oxdb.get_executed_cursor(self.config['GET_PARTITION_KEY'], (self.data_source_dsn,table_name, self.table_schema, partition_key)).fetchall()


        # existing_value = [value[0] for value in existing_value]
        # no_row=len(existing_value)
        # existing_value = ','.join(existing_value)

        with OXDB('SNOWFLAKE') as oxdb:
            self.logger.info(self.data_source_dsn + ',' + table_name + ',' + self.table_schema + ',' + partition_key)
            max_value = oxdb.get_executed_cursor(self.config['GET_MAX_VALUE'], (
            self.data_source_dsn, table_name, self.table_schema, partition_key)).fetchall()

        max_value = max_value[0][0]

        for column_name in column_list:
            if 'tot_' in column_name:
                if 'IMPALA' in self.data_source_dsn:
                    matrix = matrix + "sum(" + column_name + "),"
                elif 'SNOWFLAKE' in self.data_source_dsn:
                    matrix = matrix + "sum(" + column_name + "),"
                else:
                    matrix = matrix + "sum_float(" + column_name + "),"
                matrix_col = matrix_col + column_name + ','

        matrix_col = matrix_col[:-1]
        where_clause = "  "
        join_clause = ""
        key_index = ""
        key_list = 'platform_id'
        if partition_key == 'instance_datehour_sid' and 'hourly_fact' in table_name:
            if 'instance_timestamp' in column_list:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(instance_timestamp),'yyyyMMddHH') as INT)"
                    return
                else:
                    key_index = "CAST(TO_CHAR(instance_timestamp,'YYYYMMDDHH') as INT)"
                    return
            else:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(from_utc_timestamp(utc_timestamp, coalesce(pd.timezone, 'UTC'))),'yyyyMMddHH') as INT)"
                    return
                    # where_clause= key_index+" in (select "+key_index+" from "+self.table_schema + '.' + table_name +" where "+key_index+ " not in ("+existing_value+")"+ "group by 1 limit 10")
                if 'VERTICA' in self.data_source_dsn:
                    key_index = "CAST(TO_CHAR(NEW_TIME(utc_timestamp, 'UTC',coalesce(pd.timezone, 'UTC')),'YYYYMMDDHH') as INT)"
                    return
                if 'SNOWFLAKE' in self.data_source_dsn:
                    key_index = "CAST(TO_CHAR(convert_timezone('UTC',coalesce(pd.timezone, 'UTC',utc_timestamp)),'YYYYMMDDHH') as INT)"
                    return
                if 'x_platform_id' in column_list:
                    key_list = 'x_platform_id'
                elif 'platform_id' in column_list:
                    key_list = 'platform_id'
                elif 'p_platform_id' in column_list:
                    key_list = 'p_platform_id'
                elif 'a_platform_id' in column_list:
                    key_list = 'a_platform_id'
                join_clause = ' ox inner join ' + self.table_schema + '.' + 'platform_dim pd ON ( ox.' + key_list + ' = pd.platform_id )'

        elif partition_key == 'advt_datehour_sid' and 'hourly_fact' in table_name:
            if 'advt_timestamp' in column_list:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(advt_timestamp),'yyyyMMddHH') as INT)"
                    return
                else:
                    key_index = "CAST(TO_CHAR(advt_timestamp,'YYYYMMDDHH') as INT)"
                    return
            if 'advt_date_sid' in column_list and 'advt_hour_sid' in column_list:
                key_index = "advt_date_sid*100+advt_hour_sid"
            else:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(from_utc_timestamp(utc_timestamp, coalesce(ad.advertiser_account_timezone, 'UTC'))),'yyyyMMddHH') as INT)"
                    return
                if 'VERTICA' in self.data_source_dsn:
                    key_index = "CAST(TO_CHAR(NEW_TIME(utc_timestamp, 'UTC',coalesce(ad.advertiser_account_timezone, 'UTC')),'YYYYMMDDHH') as INT)"
                    return
                if 'SNOWFLAKE' in self.data_source_dsn:
                    key_index = "CAST(TO_CHAR(convert_timezone('UTC',coalesce(ad.advertiser_account_timezone, 'UTC',utc_timestamp)),'YYYYMMDDHH') as INT)"
                    return
                if 'a_platform_id' in column_list:
                    key_list = 'a_platform_id'
                elif 'platform_id' in column_list:
                    key_list = 'platform_id'
                elif 'p_platform_id' in column_list:
                    key_list = 'p_platform_id'
                join_clause = ' ox inner join ' + self.table_schema + '.' + 'advertiser_dim ad ON ( ox.' + key_list + ' = ad.platform_id )'

        elif partition_key == 'utc_datehour_sid' and 'hourly_fact' in table_name:
            if 'utc_hour_sid' in column_list:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "utc_date_sid*100+utc_hour_sid"
                else:
                    key_index = "utc_date_sid*100+utc_hour_sid"
            else:
                if 'IMPALA' in self.data_source_dsn:
                    key_index = "CAST(from_unixtime(unix_timestamp(utc_timestamp),'yyyyMMddHH') as INT)"
                    return
                else:
                    key_index = "CAST(TO_CHAR(utc_timestamp,'YYYYMMDDHH') as INT)"
                    return
        else:
            key_index = partition_key

        self.delete_older_records(table_name, partition_key, key_index)

        if self.partition_value:
            if partition_key == 'utc_datehour_sid' and 'hourly_fact' in table_name:
                where_clause = where_clause + " where  " + 'utc_date_sid' + "=cast(" + self.partition_value + "/100 as int) and " + key_index + "=" + self.partition_value
            else:
                where_clause = where_clause + " where  " + key_index + "=" + self.partition_value + ""

        elif max_value:
            max_value = str(max_value)
            if partition_key == 'utc_datehour_sid' and 'hourly_fact' in table_name:
                where_clause = where_clause + " where  " + 'utc_date_sid' + ">=cast(" + max_value + "/100 as int)"
            else:
                where_clause = where_clause + " where  " + key_index + ">" + max_value + ""
        else:
            with OXDB('SNOWFLAKE') as oxdb:
                min_value = oxdb.get_executed_cursor(self.config['GET_MIN_VALUE'], ('IMPALA_DSN',
                                                                                    table_name, self.table_schema,
                                                                                    partition_key)).fetchall()

            min_value = min_value[0][0]

            if min_value is not None:
                min_value = str(min_value)
            else:
                with OXDB('SNOWFLAKE') as oxdb:
                    min_value = oxdb.get_executed_cursor(self.config['GET_MIN_VALUE'], ('VERTICA_PRIMARY',
                                                                                        table_name, self.table_schema,
                                                                                        partition_key)).fetchall()
                min_value = min_value[0][0]
                if min_value is not None:
                    min_value = str(min_value)

            if min_value:
                if partition_key == 'utc_datehour_sid' and 'hourly_fact' in table_name:
                    where_clause = where_clause + " where  " + 'utc_date_sid' + ">=cast(" + min_value + "/100 as int)"
                else:
                    where_clause = where_clause + " where  " + key_index + ">=" + min_value + ""
            else:
                where_clause = ""

        sql = 'SELECT ' + key_index + ' as ' + partition_key + ',' + matrix + ' count(1) rowcount' \
              + ' FROM ' + self.table_schema + '.' + table_name \
              + join_clause \
              + where_clause \
              + ' group by 1'

        return sql

    def delete_older_records(self, table_name, partition_key, key_index):
        sql = "SELECT MIN(" + key_index + ") FROM " + self.table_schema + "." + table_name
        if 'IMPALA' in self.data_source_dsn:
            impala_db = OXDB('IMPALA_DSN', transactions_support=False)
            delete_value = impala_db.get_executed_cursor(sql).fetchall()
            impala_db.close()
        elif 'SNOWFLAKE' in self.data_source_dsn:
            with OXDB('SNOWFLAKE') as oxdb:
                delete_value = oxdb.get_executed_cursor(sql).fetchall()
        else:
            oxdb = OXDB(self.data_source_dsn)
            delete_value = oxdb.get_executed_cursor(sql).fetchall()

        if delete_value:
            delete_value = delete_value[0][0]
            if delete_value is not None:
                print(delete_value)
                print (self.data_source_dsn,
                       table_name,
                       self.table_schema,
                       partition_key,
                       delete_value)
                with OXDB('SNOWFLAKE') as oxdb:
                    oxdb.execute(self.config['DELETE_OLDER_PARTITION_INFO'], (self.data_source_dsn,
                                                                              table_name,
                                                                              self.table_schema,
                                                                              partition_key,
                                                                              delete_value))
                    oxdb.commit()

    def generate_sql_for_dim(self, table_name, column_list):
        keys = ""
        if len(column_list[:-1]) > 0:
            keys = ','.join(column_list[:-1])
            sql = "select " + keys + ", count(1) rowcount " + " from " + \
                  self.table_schema + "." + table_name + " group by " + keys
        else:
            sql = "select  count(1) rowcount " + " from " + self.table_schema + "." + table_name
        return sql

    def insert_dim_data(self, instance_id, table_name, columns, sql):

        source_db = OXDB('VERTICA_PRIMARY')
        master_data = source_db.get_executed_cursor(sql).fetchall()
        self.logger.info(sql)
        source_db.close()

        if 'IMPALA' in self.data_source_dsn or 'SPARK' in self.data_source_dsn:
            source_db = OXDB('IMPALA_DSN', transactions_support=False)
            self.logger.info(sql)
            source_data = source_db.get_executed_cursor(sql).fetchall()
            source_db.close()
        if 'SNOWFLAKE' in self.data_source_dsn:
            with OXDB('SNOWFLAKE') as oxdb:
                self.logger.info(sql)
                source_data = oxdb.get_executed_cursor(sql).fetchall()

        self.logger.info("table_name %s", table_name)
        self.logger.info("Column List %s", str(columns))

        master_data_list = [tuple(data) for data in master_data]
        source_data_list = [tuple(data) for data in source_data]
        master_data = list(set(master_data_list) - set(source_data_list))

        master_tuple = []
        for data in master_data:
            partition_key = columns[0]
            partition_value = data[0]
            for index in range(1, len(columns)):
                if partition_value is None:
                    partition_value = '-1'
                master_tuple.append((instance_id, 'mstr_datamart',
                                     table_name,
                                     columns[index],
                                     partition_key,
                                     partition_value,
                                     data[index],
                                     'VERTICA_PRIMARY'))
        if len(master_tuple) > 0 and len(master_tuple) < 1000:
            with OXDB('SNOWFLAKE') as oxdb:
                oxdb.executemany(self.config['INSERT_DIM_COMPARE_FACT'], master_tuple)
                oxdb.commit()

    def insert_fact_data(self, instance_id, table_name, partition_key, columns, sql):
        if 'SNOWFLAKE' in self.data_source_dsn:
            with OXDB('SNOWFLAKE') as oxdb:
                source_data = oxdb.get_executed_cursor(sql).fetchall()

        else:
            source_db = OXDB(self.data_source_dsn, transactions_support=False)
            print (sql)
            source_data = source_db.get_executed_cursor(sql).fetchall()
            source_db.close()

        source_data_tuple = []
        for data in source_data:
            partition_value = data[0]
            for index in range(len(columns)):
                source_data_tuple.append((
                    'mstr_datamart',
                    table_name,
                    columns[index],
                    partition_key,
                    partition_value,
                    data[index],
                    self.data_source_dsn))

        if len(source_data_tuple) > 0:
            with OXDB('SNOWFLAKE') as oxdb:
                self.logger.info("Inserting the dataset")
                oxdb.executemany(self.config['INSERT_FACT_COMPARE_FACT'], source_data_tuple)
                oxdb.commit()

    def update_instance(self):
        if 'dim' in self.data_type:
            with OXDB('SNOWFLAKE') as oxdb:
                oxdb.execute(self.config['DELETE_INSTANCE1'].format(self.data_source_dsn))
                oxdb.execute(self.config['UPDATE_INSTANCE2'].format(self.data_source_dsn))
                oxdb.execute(self.config['UPDATE_INSTANCE3'].format(self.data_source_dsn))
                oxdb.execute(self.config['UPDATE_INSTANCE4'].format(self.data_source_dsn))
                oxdb.commit()

    def start_compare(self, table_name):
        instance_id = 4
        column_list = self.get_column_list(table_name)
        self.logger.info(table_name)
        daily_partition_key_list = []

        columns = self.get_required_fact_columns(column_list, table_name)
        print (table_name)
        print(columns)
        if 'fact' in table_name:

            if '_hourly_fact' in table_name:
                table_list = self.get_table_list()
                base_name = table_name.replace('_hourly_fact', '')

                daily_table = [t for t in table_list if base_name + '_daily_fact' in t]

                for daily_table_name in daily_table:
                    daily_partition_key = self.get_partition_column(daily_table_name)
                    daily_partition_key_list.append(daily_partition_key)

                if 'instance_date_sid' in daily_partition_key_list:
                    partition_key = 'instance_datehour_sid'
                    sql = self.generate_sql_for_fact(table_name, column_list, partition_key)
                    if sql:
                        self.logger.info("SQL QUERY: %s", sql)
                        columns_with_partition_key = [partition_key] + columns
                        self.insert_fact_data(instance_id, table_name, partition_key, columns_with_partition_key, sql)
                if 'advt_date_sid' in daily_partition_key_list:
                    partition_key = 'advt_datehour_sid'
                    sql = self.generate_sql_for_fact(table_name, column_list, partition_key)
                    if sql:
                        self.logger.info("SQL QUERY: %s", sql)
                        columns_with_partition_key = [partition_key] + columns
                        self.insert_fact_data(instance_id, table_name, partition_key, columns_with_partition_key, sql)

            partition_key = self.get_partition_column(table_name)
            print(partition_key)
            if partition_key == 'utc_date_sid' and '_hourly_fact' in table_name:
                partition_key = 'utc_datehour_sid'
            if partition_key == 'advt_date_sid' and '_hourly_fact' in table_name:
                partition_key = 'advt_datehour_sid'
                return
            if partition_key == 'instance_date_sid' and '_hourly_fact' in table_name:
                partition_key = 'instance_datehour_sid'
                return
            if partition_key == '' or partition_key == None or len(partition_key) == 0:
                self.logger.info("No partition_key for fact table")
                return

            sql = self.generate_sql_for_fact(table_name, column_list, partition_key)
            self.logger.info("SQL QUERY: %s", sql)
            columns_with_partition_key = [partition_key] + columns
            self.insert_fact_data(instance_id, table_name, partition_key, columns_with_partition_key, sql)

        elif 'dim' in table_name:
            columns = self.get_required_dim_columns(column_list)
            sql = self.generate_sql_for_dim(table_name, columns)
            self.logger.info("SQL QUERY: %s", sql)
            self.insert_dim_data(instance_id, table_name, columns, sql)
        else:
            columns = self.get_required_dim_columns(column_list)
            sql = self.generate_sql_for_dim(table_name, columns)
            self.logger.info("SQL QUERY: %s", sql)
            self.insert_dim_data(instance_id, table_name, columns, sql)

    def start_job(self, table_name):
        with OXDB('SNOWFLAKE') as oxdb:
            table_list = oxdb.get_executed_cursor(self.config['MISMATCHED_VERTICA_TABLE']).fetchall()

        partition_values=[]
        if table_name:
            with OXDB('SNOWFLAKE') as oxdb:
                table_value = oxdb.get_executed_cursor(self.config['REFRESH_DATA_VERTICA'],(table_name,table_name,)).fetchall()

            for (table_schema,table_name,partition_key,partition_value) in table_value:
                partition_values.append(partition_value)

        else:
            for (table_schema,table_name) in table_list:

            populate_table_final=populate_table_list

        table_index = 0
        table_len = len(table_list)

        print(table_list)

        if table_name:
            with OXDB('SNOWFLAKE') as oxdb:
                if self.partition_value:
                    oxdb.execute(self.config['DELETE_EXISTING_TABLE_PARTITION'],
                                 (self.data_source_dsn, table_name, self.partition_value))

                oxdb.commit()

        try:
            while table_index < table_len:
                self.start_compare(table_list[table_index])
                table_index = table_index + 1

            self.update_instance()

            with OXDB('SNOWFLAKE') as oxdb:
                LoadState(
                    oxdb.connection, variable_name=APP_NAME + "_" + self.data_source_dsn + "_" + self.data_type
                ).upsert(commit=True)

        except (pid.PidFileAlreadyLockedError, pid.PidFileAlreadyRunningError):
            self.logger.warning("Unable to get lock.Exiting...")
        except Exception:
            self.logger.exception("Unhandled exception!")
            raise

if __name__ == "__main__":

    yaml_file = sys.argv[0].replace(".py", ".yaml")
    startTime = time.time()
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter,
        description=textwrap.dedent('''compute stats for table which has missing stats'''))

    parser.add_argument('--source', help='data source')
    parser.add_argument('--table_type', help='Type of the table')
    parser.add_argument('--table_name', help='Name of the table')
    parser.add_argument('--partition_value', help='Name of the table')

    options = parser.parse_args()
    roll = DataCompare(yaml_file, options.source, options.table_type, options.table_name, options.partition_value)
    try:
        start_time = time.time()
        roll.lock.create()

        roll.logger.info("Start running data compare %s", datetime.datetime.now())
        roll.start_job(options.table_name)
        roll.logger.info("Finished running data compare script %s", datetime.datetime.now())

        end_time = time.time()
        roll.logger.info("Total elapsed time = %s seconds", (end_time - start_time))

    except (pid.PidFileAlreadyLockedError, pid.PidFileAlreadyRunningError):
        roll.logger.warning("Unable to get lock.Exiting...")
    except Exception as error:
        roll.logger.error("Daily Rollups Job FAILED. Error: %s", error)
        raise Exception(error)
    finally:
        pass