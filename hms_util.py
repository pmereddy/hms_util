################################################################################
"""
Script Name: 
Description: A utility script for hive metastore
Version: 1
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
import gzip
import csv
import configparser
import logging
import argparse
import traceback
import psycopg2
from psycopg2 import pool


""" Helper function """
def get_property(config, section, property_name, default_value):
    if section in config and property_name in config[section]:
        return config[section][property_name]
    else:
        return default_value

""" Helper function """
def read_config_file(filename):
    try:
        config = configparser.ConfigParser()
        config.read(filename)
        return config
    except Exception as e:
        logging.error(f"read_config_file: {e}")
        sys.exit(1)

""" Helper function """
def write_csv_file(data, filename):
    with open(filename, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        #column_names = list(data.keys())
        #writer.writerow(column_names)
        writer.writerow(['Metric', 'Value'])
        for key, value in data.items():
            writer.writerow([key, value])


""" Helper function """
def write_md_file(data, filename):
    keys = list(data.keys())
    values = list(data.values())

    max_key_length = max(len(str(key)) for key in keys)
    max_value_length = max(len(str(value)) for value in values)

    # Create the Markdown table header
    table = "| METRIC | VALUE |\n"
    table += "|---" + "-" * max_key_length + "|---" + "-" * max_value_length + "|\n"

    # Add each key-value pair to the table
    for key, value in data.items():
        table += f"| {str(key):<{max_key_length}} | {str(value):<{max_value_length}} |\n"

    with open(filename, 'w', newline='') as md_file:
        md_file.write(table)


def tuples_to_html_table(query_name, columns, records):
    if not records:
        return f"\n<h2> {query_name} </h2>\n<p><strong>Results empty</strong></p>"

    table_header=f"\n<h2> {query_name} </h2>\n"
    table_footer=f"<br><br>"
    # Build the header row
    header_row = "<thead><tr>" + "".join(f"<th>{column.upper()}</th>" for column in columns) + "</tr></thead>"

    # Build the rows with values
    rows = []
    for row in records:
        values = [str(value) for value in row]
        row_html = "<tr>" + "".join(f"<td>{value}</td>" for value in values) + "</tr>"
        rows.append(row_html)

    # Combine header and rows
    html_table = f"""<table id="{query_name}", class="display">
{header_row}
<tbody>
{''.join(rows)}
</tbody>
</table>"""
    return "\n".join([table_header, html_table, table_footer])


""" Helper function """
def tuples_to_markdown_table(query_name, columns, records):
    if not records:
        return "## {query_name}\n> ** Results empty**"

    table_header=f"## {query_name}\n"
    table_footer=f"\n\n"

    # Build the header row
    columns = [ column.upper() for column in columns]
    header_row = " | ".join(columns) if columns else ""
    header_separator = " | ".join([":---" for _ in columns]) if columns else ""

    # Build the rows with values
    rows = []
    for row in records:
        values = [str(value) for value in row]
        rows.append(" | ".join(values))
    
    # Combine header, header separator, and rows
    markdown_table = "\n".join([table_header, header_row, header_separator] + rows + [table_footer])
    return markdown_table


""" Helper function """
def read_query_file(filename):
    try:
        with open(filename, 'r') as file:
            queries = json.load(file)
        return queries
    except Exception as e:
        logging.error(f"read_query_file: {e}")
        sys.exit(1)


""" Function to get hive database summary """
def get_summary(database, catalog, queries, **kwargs):
    summary={}
    logging.info(f"Get summary for database: {catalog}")
    try:
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        table=kwargs.get('table', 'ALL')
        table_filter=""
        if table != 'ALL':
            table_filter=f"\"TBL_NAME\"='{table}' and"
        for query_name, query_template in queries.items():
            # catalog is always hive
            formatted_query = query_template.format(database='hive', catalog=catalog, past_days=kwargs.get('past_days', 1), table=table, table_filter=table_filter)
            logging.debug(f"Executing query {query_name} : {formatted_query}")
            cursor.execute(formatted_query)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            results = [dict(zip(cols, row)) for row in rows]
            logging.debug(f"Results for {query_name} : {results}")
            for entry in results:
                summary[entry['key']] = entry['value']
    except Exception as e:
        traceback.print_exc()
        logging.error(f"An error occurred in get_summary: {e}")
    connection_pool.putconn(connection)
    return summary


def write_section1(hfd, title):
    with open('./templates/html_section1.template', 'r') as section_file:
        section1_template = section_file.read()
    section1 = section1_template.replace("[[title]]", title)
    hfd.write(section1)

def write_section2(hfd):
    with open('./templates/html_section2.template', 'r') as section_file:
        section2_template = section_file.read()
    section2 = section2_template
    hfd.write(section2)


""" Function to generate and save hive database reports """
def create_database_reports(database, catalog, queries, results_dir):
    logging.info(f"Generate reports for database: {catalog}")
    md_tables=[]
    html_tables=[]
    try:
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        md_results_file=os.path.join(results_dir, f"{catalog}_reports_{signature}.md")
        html_results_file=os.path.join(results_dir, f"{catalog}_reports_{signature}.html")

        for query_name, query_template in queries.items():
            formatted_query = query_template.format(database=database, catalog=catalog)
            logging.debug(f"Executing query {query_name} : {formatted_query}")
            cursor.execute(formatted_query)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]

            # md results
            temp={}
            temp[query_name] = tuples_to_markdown_table(query_name, cols, rows)
            md_tables.append(temp)

            # html results
            temp={}
            temp[query_name] = tuples_to_html_table(query_name, cols, rows)
            html_tables.append(temp)
    except Exception as e:
        traceback.print_exc()
        logging.error(f"An error occurred in create_database_reports: {e}")

    try:
        with open(md_results_file, 'w') as rfd:
            for table in md_tables:
                for k,v in table.items():
                    rfd.write(v)
        logging.info(f"Database {config['global']['command']} saved to {md_results_file}")

        title="Database reports"
        with open(html_results_file, 'w') as hfd:
            write_section1(hfd, title)        
            for table in html_tables:
                for k,v in table.items():
                    hfd.write(v)
            write_section2(hfd)        
        logging.info(f"Database {config['global']['command']} saved to {html_results_file}")

    except Exception as e:
        traceback.print_exc()
        logging.error(f"error writing to report files in create_database_reports: {e}")
    connection_pool.putconn(connection)


""" 
Function to extract and save hive table DDL
"""
def backup_table_ddl(database, catalog, table, table_ddl_queries, ofd):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    create_statement=""
    try:
        table_id=0
        table_type='TABLE'
        serde_id=0
        sd_id=0
        column_string=""
        tbl_properties=""
        serde_properties=""
        stored_by=""
        db_location_uri=""
        db_managed_uri=""
        location_string=""
        row_format=""
        format_string=""
        partition_key_string=""
        table_comment=""
        sorted_by_string=""
        alter_statement_string=""
        for query_name, query_template in table_ddl_queries.items():
            formatted_query = query_template.format(database=database, catalog=catalog,
                table=table, table_id=table_id, serde_id=serde_id, sd_id=sd_id,
                db_location_uri=db_location_uri, db_managed_uri=db_managed_uri)
            logging.debug(f"Executing query {query_name} : {formatted_query}")
            cursor.execute(formatted_query)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            results = [dict(zip(cols, row)) for row in rows]
            logging.debug(f"Results for {query_name} : {results}")
            if query_name == 'Q1':
                if len(results) > 0:
                    table_name = results[0].get('TBL_NAME')
                    ttype = results[0].get('TBL_TYPE')
                    table_id = results[0].get('TBL_ID')
                    if ttype.strip()=='MANAGED_TABLE':
                        table_type = 'TABLE'
                    elif ttype.strip()=='EXTERNAL_TABLE':
                        table_type = 'EXTERNAL TABLE'
                    else:
                        table_type = ttype
                else:
                    logging.warning(f"Table: {table} not found in the metastore")
                    return
            if query_name == 'Q2':
                if len(results) > 0:
                    sd_id = results[0].get('SD_ID')
                    location = results[0].get('LOCATION')
                    db_location_uri = results[0].get('DB_LOCATION_URI')
                    db_managed_uri = results[0].get('DB_MANAGED_LOCATION_URI')
                    num_buckets = results[0].get('NUM_BUCKETS')
                    if num_buckets > 0:
                        bucket_string=f"\nINTO {num_buckets} BUCKETS"
                    else:
                        bucket_string=f""
                    if db_location_uri and not db_managed_uri:
                        db_managed_uri = db_location_uri.replace('external', 'managed')
                    if db_location_uri and location:
                        if not db_location_uri in location:
                            location_string=f"\nLOCATION\n  '{location}'"
                    ipf = results[0].get('INPUT_FORMAT')
                    opf = results[0].get('OUTPUT_FORMAT')
                    if ipf is not None or opf is not None:
                        if ipf is not None:
                            format_string=f"\nSTORED AS INPUTFORMAT\n  '{ipf}'"
                        if opf is not None:
                            format_string += f"\nOUTPUTFORMAT\n  '{opf}'"
            if query_name == 'Q3':
                arr = []
                for entry in results:
                    if entry['PARAM_KEY'] == 'COLUMN_STATS_ACCURATE' or entry['PARAM_KEY'] == 'EXTERNAL':
                        continue
                    if entry['PARAM_KEY'] == 'storage_handler':
                        stored_by=f"\nSTORED BY\n  '{entry['PARAM_VALUE']}'"
                        continue
                    if entry['PARAM_KEY'] == 'comment':
                        table_comment=f"\nCOMMENT '{entry['PARAM_VALUE']}'"
                        continue
                    if entry['PARAM_KEY'] == 'numFiles' or entry['PARAM_KEY'] == 'numFilesErasureCoded' or entry['PARAM_KEY'] == 'totalSize':
                        continue
                    arr.append(f"'{entry['PARAM_KEY']}'='{entry['PARAM_VALUE']}'")
                if len(arr) > 0:
                    a=',\n  '.join(arr)
                    tbl_properties=f"\nTBLPROPERTIES(\n  {a})"
                else:
                    tbl_properties=""
            if query_name == 'Q6':
                arr = []
                for entry in results:
                    arr.append(f"{entry['COLUMN_NAME']} {entry['ORDER']}")
                if len(arr) > 0:
                    a=', '.join(arr)
                    sorted_by_string=f" SORTED BY ({a})"
            if query_name == 'Q9':
                arr = []
                for entry in results:
                    if entry['COMMENT'] is None:
                        arr.append(f"`{entry['COLUMN_NAME']}` {entry['TYPE_NAME']}")
                    else:
                        arr.append(f"`{entry['COLUMN_NAME']}` {entry['TYPE_NAME']} COMMENT '{entry['COMMENT']}'")
                if len(arr) > 0:
                    column_string=',\n  '.join(arr)

            if query_name == 'Q10':
                if len(results) > 0:
                    deser = results[0].get('DESERIALIZER_CLASS')
                    ser = results[0].get('SERIALIZER_CLASS')
                    slib = results[0].get('SLIB')
                    serde_id = results[0].get('SERDE_ID')
                    row_format=f"\nROW FORMAT SERDE\n  '{slib}'"

            if query_name == 'Q11':
                arr = []
                for entry in results:
                    arr.append(f"'{entry['PARAM_KEY']}'='{entry['PARAM_VALUE']}'")
                if len(arr) > 0:
                    a=',\n '.join(arr)
                    serde_properties=f"\nWITH SERDEPROPERTIES (\n  {a}\n)"

            if query_name == 'Q12':
                arr = []
                for entry in results:
                    arr.append(f"'{entry['SKEWED_COL_NAME']}'")
                if len(arr) > 0:
                    a=',\n '.join(arr)
                    skewed_cols=f"\nSKEWED BY ({a})"

            if query_name == 'Q16':
                arr = []
                for entry in results:
                    arr.append(f"`{entry['PKEY_NAME']}` {entry['PKEY_TYPE']}")
                if len(arr) > 0:
                    partition_key_string="\nPARTITIONED BY (\n  "+', '.join(arr)+")"

            if query_name == 'Q17':
                clustered_by_string=""
                if len(results) > 0:
                    bucket_cols = results[0].get('PARAM_VALUE')
                    if bucket_cols is not None:
                        clustered_by_string=f"\nCLUSTERED BY ({bucket_cols}) {sorted_by_string} {bucket_string}"

            if query_name == 'Q18':
                alter_statements = []
                for entry in results:
                    temp=f"ALTER TABLE {catalog}.{table_name} ADD PARTITION ({entry['PART_NAME']}) LOCATION '{entry['LOCATION']}';\n"
                    alter_statements.append(temp)

        create_statement = f"CREATE {table_type} `{catalog}`.`{table_name}`(\n  {column_string}) {table_comment} {partition_key_string} {clustered_by_string} {row_format} {format_string} {location_string} {stored_by} {serde_properties} {tbl_properties};\n\n"
        ofd.write(create_statement)
        for alter_statement in alter_statements:
            logging.debug(f"{alter_statement}")
            ofd.write(alter_statement)

        connection.commit()
    except Exception as e:
        connection.rollback()
        traceback.print_exc()
        print("Error:", e)
    finally:
        cursor.close()
        connection_pool.putconn(connection)

"""
Function to generate DDL to convert eligible hive tables to iceberg
"""
def iceberg_migration_ddl(database, catalog, results_file, output_file):

    iceberg_version = get_property(config, 'iceberg_migration', 'iceberg_version', '2')
    approach = get_property(config, 'iceberg_migration', 'migration_approach', 'inplace')
    table_properties = get_property(config, 'iceberg_migration', 'table_properties', '')

    valid_formats=["org.apache.hadoop.hive.ql.io.orc.OrcSerde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "org.apache.hadoop.hive.serde2.avro.AvroSerDe"]

    try:
        logging.info(f"Running iceberg migration for database: {catalog}")
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        list_tables = f'SELECT a."TBL_ID", a."TBL_NAME", a."TBL_TYPE", b."IS_COMPRESSED", b."IS_STOREDASSUBDIRECTORIES", b."INPUT_FORMAT", b."OUTPUT_FORMAT",c."SLIB" FROM "TBLS" a inner join "SDS" b on a."SD_ID"=b."SD_ID" INNER JOIN "SERDES" c on b."SERDE_ID"=c."SERDE_ID" where a."DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\');'
        cursor.execute(list_tables)
        results = cursor.fetchall()

        with open(results_file, "w") as res, open(output_file, "w") as out:
            for entry in results:
                props=[]
                props.append(f"'storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'")
                props.append(f"'format-version'='{iceberg_version}'")
                logging.debug(f"{entry[0]}, {entry[1]}, {entry[2]}, {entry[7]}")
                table_id=entry[0]
                if entry[2] != 'EXTERNAL_TABLE':
                    out.write(f"Table : {entry[1]} is not an EXTERNAL table\n")
                    continue
                if entry[7] == 'org.apache.iceberg.mr.hive.HiveIcebergSerDe':
                    out.write(f"Table : {entry[1]} is already an Iceberg table")
                    continue
                if not entry[7] in valid_formats:
                    out.write(f"Table : {entry[1]} is not eligible to convert to Iceberg\n")
                    continue
                table_props_query = f'select tp."PARAM_KEY", tp."PARAM_VALUE" from "TABLE_PARAMS" tp where tp."TBL_ID"={table_id}'
                cursor.execute(table_props_query)
                prop_results = cursor.fetchall()
                handler=""
                transactional=""
                external_purge='false'
                for prop in prop_results:
                    logging.debug(f"{prop[0]}, {prop[1]}")
                    if prop[0] == 'storage_handler':
                        handler=prop[1]
                    if prop[0] == 'transactional':
                        transactional=prop[1]
                    if prop[0] == 'external.table.purge' and prop[1] == 'true':
                        external_purge='true'

                if handler == 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler':
                    out.write(f"Table : {entry[1]} is already an Iceberg table")
                    continue
                if transactional == 'true':
                    out.write(f"Table : {entry[1]} is a transactional table")
                    continue

                tbl_properties=','.join(props)
                alter_statement = f"ALTER TABLE {entry[1]} \n SET TBLPROPERTIES ({tbl_properties});\n"
                res.write(alter_statement)

    except Exception as e:
        connection.rollback()
        print("Error:", e)
        traceback.print_exc()
    finally:
        cursor.close()
        connection_pool.putconn(connection)


"""
Function to extract and save hive object DDL
"""
def backup_database_ddl(database, catalog, output_file, queries):
    try:
        status_counter=0
        logging.info(f"Extracting DDL for database: {catalog}")
        connection = connection_pool.getconn()
        cursor = connection.cursor()

        with open(output_file, 'w') as fd:
            header = f"-- Beginning of backup\n"
            fd.write(header)

        # Create database statement
        if get_property(config, 'schema_backup', 'create_db_statement', 'true') == 'true':
            create_db = f'select "DESC", "DB_LOCATION_URI" from "DBS" where "NAME"=\'{catalog}\''
            cursor.execute(create_db)
            results = cursor.fetchone()
            if len(results) > 0:
                with open(output_file, 'a') as fd:
                    header = f"-- Create Database\n"
                    fd.write(header)
                    create_db_statement=f"CREATE DATABASE IF NOT EXISTS {catalog} \n COMMENT \"{results[0]}\" \n LOCATION \"{results[1]}\";\n"
                    fd.write(create_db_statement)
            else:
                logging.warn(f"database: {catalog} not found")
                return

        # Get tables
        table_list_cmd = f'select "TBL_NAME" from "TBLS" where "TBL_TYPE" not in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "TBL_ID"'
        cursor.execute(table_list_cmd)
        rows = cursor.fetchall()
        total_tables=len(rows)
        logging.info(f"Found {total_tables} tables in {catalog}")
        with open(output_file, 'a') as fd:
            for row in rows:
                backup_table_ddl(database, catalog, row[0], queries, fd)
                if status_counter > 0 and total_tables > 10 and status_counter%(int(total_tables/10)) == 0:
                    logging.info(f"Processed {status_counter} tables")
                status_counter = status_counter + 1
        logging.info(f"Table DDL saved to {output_file} successfully.")

        # Get views and materialized views and append to the results file at the end.
        # The views and materialized views are ordered to resolve the dependencies
        if get_property(config, 'schema_backup', 'include_views', 'true') == 'true':
            view_list_cmd = f'select "TBL_ID" from "TBLS" where "TBL_TYPE" in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "TBL_ID"'
            cursor.execute(view_list_cmd)
            rows = cursor.fetchall()
            logging.info(f"Found {len(rows)} views in {catalog}")
            with open(output_file, 'a') as fd:
                for row in rows:
                    formatted_query=f'select "TBL_TYPE", "VIEW_EXPANDED_TEXT", "TBL_NAME" from "TBLS" where "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') and "TBL_ID"={row[0]}'
                    cursor.execute(formatted_query)
                    results = cursor.fetchone()
                    if len(results) > 0:
                        view_text = results[1]
                        logging.debug(f"View DDL {results[0]}, {view_text}")
                        if results[0] == 'VIRTUAL_VIEW':
                            create_view_statement = f"CREATE VIEW {results[2]} as ({view_text});\n"
                        if results[0] == 'MATERIALIZED_VIEW':
                            create_view_statement = f"CREATE MATERIALIZED VIEW {results[2]} as ({view_text});\n"
                        fd.write(create_view_statement)

        # Get functions DDL and add to the end of the results file
        if get_property(config, 'schema_backup', 'include_functions', 'true') == 'true':
            func_list_cmd = f'select "CLASS_NAME", "FUNC_NAME","FUNC_TYPE", "OWNER_NAME" from "FUNCS" where "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "FUNC_ID"'
            cursor.execute(func_list_cmd)
            rows = cursor.fetchall()
            with open(output_file, 'a') as fd:
                for row in rows:
                    create_func_statement = f"CREATE {row[2]} FUNCTION {row[1]} AS {row[0]};\n"
                    fd.write(create_func_statement)

    except Exception as e:
        logging.error(f"An error occurred in backup_database_ddl: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection_pool.putconn(connection)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Arguments to the backup script.")
    parser.add_argument("--log_level", type=str, help="log level", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument("--config", type=str, help="path to config.ini file", default='config.ini')

    args = parser.parse_args()
    config_filename = args.config
    log_level = args.log_level
    config = read_config_file(config_filename)

    signature = int(time.time())

    # Set log level
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()

    # Check config file : 1
    if 'global' in config:
        global_section = config['global']
        logger.log(logging.DEBUG, "[global] section:")
        for option in global_section:
            logger.log(logging.DEBUG, f"{option} = {global_section[option]}")
        logger.log(logging.DEBUG, f"\n")
    else:
        logging.error("Invalid config file: missing global section")
        sys.exit(1)

    # Check config file : 2
    if 'source' in config:
        source_section = config['source']
        logger.log(logging.DEBUG, "[source] section:")
        for option in source_section:
            logger.log(logging.DEBUG, f"{option} = {source_section[option]}")
        logger.log(logging.DEBUG, f"\n")
    else:
        logging.error("Invalid config file: missing source section")
        sys.exit(1)

    # Check config file : command is schema_backup. Check if schema_backup section is missing
    if config['global']['command'] == 'schema_backup':
        if 'schema_backup' in config:
            schema_backup_section = config['schema_backup']
            logger.log(logging.DEBUG, "[schema_backup] section:")
            for option in schema_backup_section:
                logger.log(logging.DEBUG, f"{option} = {schema_backup_section[option]}")
            logger.log(logging.DEBUG, f"\n")
        else:
            logging.error("Invalid config file: missing schema_backup section")
            sys.exit(1)

    # Check config file : command is to schema_backup. But, schema_backup section is missing
    if config['global']['command'] == 'compare':
        if 'source' in config and 'target' in config:
            logging.debug("source and target sections present")
        else:
            logging.error("Invalid config file: missing source or target section")
            sys.exit(1)

    logging.info(f"COMMAND: {config['global']['command']}")


    # check/create results_dir
    results_dir = get_property(config, 'global', 'results_dir', 'results')
    try:
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
            logging.info(f"created result directory: {results_dir}")
    except Exception as e:
        logging.error(f"creating results directory: {e}")

    # Connect to source hive metastore
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=5,
                        dbname=get_property(config, 'source', 'database', 'hive'),
                        user=get_property(config, 'source', 'user', 'hive'),
                        password=get_property(config, 'source', 'password', 'xxxx'),
                        host=get_property(config, 'source', 'host', 'localhost'),
                        port=get_property(config, 'source', 'port', 5432))
    except Exception as e:
        logging.error("Connecting to source: {e}")
        sys.exit(1)

    # If hive catalog is set to ALL, find all hive catalogs and run the command on each
    db = get_property(config, 'global', 'catalog', 'default')
    if db == 'ALL':
        try:
            get_db_list="select \"NAME\" from \"DBS\" where \"NAME\" not in ('sys', 'information_schema');"
            connection = connection_pool.getconn()
            cursor = connection.cursor()
            cursor.execute(get_db_list)
            tdbs = cursor.fetchall()
            dbs = [db[0] for db in tdbs]
            connection_pool.putconn(connection)
        except Exception as e:
            logging.error("Connecting to source: {e}")
            sys.exit(1)
    else:
        dbs=[db]

    for db in dbs:
        # command: Summary
        if config['global']['command'] == 'summary':
            try:
                queries = read_query_file(os.path.join(
                                  get_property(config, 'global', 'queries_dir', 'queries'),
                                  get_property(config, 'global', 'database_type', 'postgresql'),
                                  get_property(config, 'summary', 'query_file', 'summary.queries'))
                )
                summary_info = get_summary(
                    get_property(config, 'source', 'database', 'hive'),
                    db,
                    queries
                )
                filebase = db+"_summary_"+str(signature)
                output_file = os.path.join(results_dir,filebase+".csv")
                write_csv_file(summary_info, output_file)
                logging.info(f"{config['global']['command']} for database: {db} saved to {output_file}")

                output_file = os.path.join(results_dir,filebase+".md")
                write_md_file(summary_info, output_file)
                logging.info(f"{config['global']['command']} saved to {output_file}")
            except Exception as e:
                logging.error(f"Getting summary: {e}")

        # command: reports
        elif config['global']['command'] == 'reports':
            try:
                report_queries = read_query_file(os.path.join(
                                  get_property(config, 'global', 'queries_dir', 'queries'),
                                  get_property(config, 'global', 'database_type', 'postgresql'),
                                  get_property(config, 'reports', 'query_file', 'reports.queries')))

                create_database_reports(
                    get_property(config, 'source', 'database', 'hive'),
                    db,
                    report_queries,
                    results_dir 
                )
            except Exception as e:
                logging.error(f"Database reports: {e}")

        # command: backup ddl
        elif config['global']['command'] == 'schema_backup':
            try:
                schema_backup_queries = read_query_file(os.path.join(
                                  get_property(config, 'global', 'queries_dir', 'queries'),
                                  get_property(config, 'global', 'database_type', 'postgresql'),
                                  get_property(config, 'schema_backup', 'query_file', 'backup_ddl.queries')))
                filebase = f"{db}_backup_{signature}.ddl"
                results_file = os.path.join(results_dir, filebase)
                backup_database_ddl(
                    get_property(config, 'source', 'database', 'hive'),
                    db,
                    results_file,
                    schema_backup_queries)
                logging.info(f"{config['global']['command']} saved to {results_file}")
            except Exception as e:
                logging.error(f"Getting schema_backup: {str(e)}")

        # command: migration to iceberg
        elif config['global']['command'] == 'iceberg_migration':
            filebase = f"{db}_iceberg_migration_{signature}"
            output_file = os.path.join(results_dir, filebase+".log")
            results_file = os.path.join(results_dir, filebase+".ddl")
            iceberg_migration_ddl(
                get_property(config, 'source', 'database', 'hive'),
                db,
                results_file,
                output_file)
            logging.info(f"saved ddl to {results_file}")
            logging.info(f"saved logs to {output_file}")

        else:
            logging.error(f"Unsupported command specified: {config['global']['command']}")
            sys.exit(1)
