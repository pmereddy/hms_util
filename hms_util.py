################################################################################
"""
Script Name:
Description: hive metastore utility for schema backup, reports, compare features
Version: 2
Author: Pramodh Mereddy

Disclaimer: This script is fairly new and has not been tested very well in the field.
            Please use it cautiously
"""
################################################################################

import os
import sys
import json
import time
import base64
import argparse
import traceback
import pprint
import logging
from logging_setup import setup_logging
from iniReader import iniReader
from postgresqlDatabase import PostgreSQLDatabase
from mysqlDatabase import MySQLDatabase
from reportWriter import ReportWriter
from commands.icebergMigration import IcebergMigration
from commands.databaseSummary import DatabaseSummary
from commands.databaseReports import DatabaseReports
from commands.databaseBackup import DatabaseBackup
from commands.databaseCompare import DatabaseCompare
from commands.hiveSchemaComparator import HiveSchemaComparator

logger = logging.getLogger("hms_util")

def read_query_file(filename):
    try:
        with open(filename, 'r') as file:
            queries = json.load(file)
        return queries
    except Exception as e:
        logger.error(f"read_query_file: {e}")
        sys.exit(1)


def get_dbobject(db_type, config, db_ufn="source"):
    host = config.get_property(db_ufn, 'host', 'localhost')
    port = config.get_property(db_ufn, 'port', 5432)
    database = config.get_property(db_ufn, 'database', 'unknowndb')
    user = config.get_property(db_ufn, 'user', 'unknownuser')
    password = config.get_property(db_ufn, 'password', 'unknown')

    if db_type == 'postgresql':
        dbo = PostgreSQLDatabase()
        dbo.connect(host=host, port=port, database=database, user=user, password=password)
    elif db_type == 'mysql':
        dbo = MySQLDatabase()
        dbo.connect(host=host, port=port, database=database, user=user, password=password)
    else:
        dbo = None
    return dbo


def main():
    parser = argparse.ArgumentParser(description="Arguments to hms_util script.")
    parser.add_argument("--log_level", type=str, help="log level", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument("--config", type=str, help="path to config.ini file", default='config.ini')

    signature = int(time.time())
    args = parser.parse_args()
    setup_logging(args.log_level)
    config = iniReader(args.config)
    config.validate()

    password = config.get_property('source', 'password', '')
    if password.isspace() or len(password) == 0:
        password = getpass.getpass(prompt="Enter password for the source database: ")
        config.set('source', 'password', password)

    # check/create results_dir
    results_dir = config.get_property('global', 'results_dir', 'results')
    try:
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
            logger.info(f"created result directory: {results_dir}")
    except Exception as e:
        logger.error(f"creating results directory: {e}")


    command = config.get_property('global', 'command', 'summary')
    db_type = config.get_property('global', 'database_type', 'postgresql')
    hms_db = config.get_property('source', 'database', 'hive'),
    dbo = get_dbobject(db_type, config, "source")

    db = config.get_property('global', 'catalog', 'default')
    if db == 'ALL':
        try:
            if db_type == 'postgresql':
                db_list_query = "select \"NAME\" from \"DBS\" where \"NAME\" not in ('sys', 'information_schema');"
            elif db_type == 'mysql':
                db_list_query = "select NAME from DBS where NAME not in ('sys', 'information_schema');"

            tdbs,_ = dbo.query(db_list_query)
            dbs = [db[0] for db in tdbs]
        except Exception as e:
            logger.error("Connecting to source: {e}")
            sys.exit(1)
    else:
        dbs=[db]

    if command == 'summary':
        try:
            queries = read_query_file(os.path.join(
                config.get_property('global', 'queries_dir', 'queries'),
                config.get_property('global', 'database_type', 'postgresql'),
                config.get_property('summary', 'query_file', 'summary.queries'))
            )
            value = config.get_property('summary', 'report_format', 'csv, md')
            report_formats = [item.strip() for item in value.split(',')]
            rw = ReportWriter()
            ds = DatabaseSummary()
            for db in dbs:
                summary_info = ds.get_summary(dbo, db_type, hms_db, db, queries)
                filebase = db+"_summary_"+str(signature)
                if 'csv' in report_formats:
                    output_file = os.path.join(results_dir,filebase+".csv")
                    rw.write_csv_file(summary_info, output_file)

                if 'md' in report_formats:
                    output_file = os.path.join(results_dir,filebase+".md")
                    rw.write_md_file(summary_info, output_file)
        except Exception as e:
            traceback.print_exc()
            logger.error(f"Command summary failed: {e}")

    elif command == 'reports':
        try:
            queries = read_query_file(os.path.join(
                config.get_property('global', 'queries_dir', 'queries'),
                config.get_property('global', 'database_type', 'postgresql'),
                config.get_property('reports', 'query_file', 'reports.queries')))
            value = config.get_property('reports', 'report_format', 'html, md')
            report_formats = [item.strip() for item in value.split(',')]
            dr = DatabaseReports()
            for db in dbs:
                results = dr.gather_database_info(dbo, 'hive', db, queries, results_dir)
                if results:
                    dr.create_database_reports(results, results_dir, f"{db}_reports_{signature}", report_formats)
        except Exception as e:
            traceback.print_exc()
            logger.error(f"command reports failed: {e}")

    elif command == 'compare':
        dbcompare = DatabaseCompare(config)
        password = config.get_property('target', 'password', '')
        if password.isspace() or len(password) == 0:
            password = getpass.getpass(prompt="Enter password for the target database: ")
            config.set('target', 'password', password)
        source_hive_catalog = config.get_property('compare', 'source_hive_catalog', 'default')
        target_hive_catalog = config.get_property('compare', 'target_hive_catalog', 'default')
        dbo_tgt = get_dbobject(db_type, config, "target")
        schema_backup_queries = read_query_file(os.path.join(
                          config.get_property('global', 'queries_dir', 'queries'),
                          config.get_property('global', 'database_type', 'postgresql'),
                          config.get_property('schema_backup', 'query_file', 'backup_ddl.queries')))
        src_db_schema = dbcompare.get_database_schema(dbo, db_type, hms_db, source_hive_catalog, schema_backup_queries)
        tgt_db_schema = dbcompare.get_database_schema(dbo_tgt, db_type, hms_db, target_hive_catalog, schema_backup_queries)
        comparator = HiveSchemaComparator(config)
        # Compare the two schemas
        diffs = comparator.compare_schemas(src_db_schema, tgt_db_schema)
        # Print the differences
        comparator.print_diffs_hierarchical(diffs)
        comparator.generate_html_report(diffs, results_dir, f"schema_differences_report_{signature}.html")

        print("\nSQL Statements to reconcile the differences")
        statements=comparator.sql_statements
        for statement in statements:
            print(statement)

    elif command == 'schema_backup':
        dbbackup = DatabaseBackup(config)
        try:
            schema_backup_queries = read_query_file(os.path.join(
                              config.get_property('global', 'queries_dir', 'queries'),
                              config.get_property('global', 'database_type', 'postgresql'),
                              config.get_property('schema_backup', 'query_file', 'backup_ddl.queries')))
            for db in dbs:
                filebase = f"{db}_backup_{signature}.ddl"
                results_file = os.path.join(results_dir, filebase)
                db_schema = dbbackup.database_schema_backup(dbo, db_type, hms_db, db, schema_backup_queries, results_file)
                logger.info(f"{command} saved to {results_file}")
        except Exception as e:
            logger.error(f"Getting schema_backup: {str(e)}")

    elif command == 'iceberg_migration':
        try:
            iceberg_version = config.get_property('iceberg_migration', 'iceberg_version', '2')
            approach = config.get_property('iceberg_migration', 'migration_approach', 'inplace')
            table_properties = config.get_property('iceberg_migration', 'table_properties', '')
            ib = IcebergMigration(version=iceberg_version, approach=approach, table_properties=table_properties, results_dir=results_dir)
            for db in dbs:
                filebase = f"{db}_iceberg_migration_{signature}"
                ib.create_iceberg_migration_statements(dbo, db_type, db, filebase)
        except Exception as e:
            traceback.print_exc()
            logger.error(f"command reports failed: {e}")
    else:
        logger.error(f"Unsupported command specified: {config['global']['command']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
