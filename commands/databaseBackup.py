import os
import sys
import logging
import traceback

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
from reportWriter import ReportWriter

logger = logging.getLogger(__name__)

class DatabaseBackup:
    def __init__(self, config):
        self.logger = logger
        self.config = config

    def database_schema_backup(self, dbo, db_type, hms_db, catalog, queries, output_file):
        db_schema={}
        DEFAULT_SCHEMA="default"
        try:
            status_counter=0
            self.logger.info(f"Extracting DDL for database: {catalog}")

            with open(output_file, 'w') as fd:
                header = f"-- Beginning of backup\n"
                fd.write(header)

            # Create "create database" statement
            if self.config.get_property('schema_backup', 'create_db_statement', 'true') == 'true':
                if db_type == 'postgresql':
                    create_db = f'select "DESC", "DB_LOCATION_URI" from "DBS" where "NAME"=\'{catalog}\''
                elif db_type == 'mysql':
                    create_db = f'select `DESC`, DB_LOCATION_URI from DBS where NAME=\'{catalog}\''

                results = dbo.query(create_db)
                if len(results) > 0:
                    with open(output_file, 'a') as fd:
                        header = f"-- Create Database\n"
                        fd.write(header)
                        create_db_statement=f"CREATE DATABASE IF NOT EXISTS {catalog} \n COMMENT \"{results[0]}\" \n LOCATION \"{results[1]}\";\n"
                        fd.write(create_db_statement)
                else:
                    logger.warn(f"database: {catalog} not found")
                    return

            db_schema['schemas']={}
            db_schema['schemas'][DEFAULT_SCHEMA]={}
            db_schema['schemas'][DEFAULT_SCHEMA]['tables']={}

            # Get tables
            if db_type == 'postgresql':
                table_list_cmd = f'select "TBL_NAME" from "TBLS" where "TBL_TYPE" not in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "TBL_ID"'
            elif db_type == 'mysql':
                table_list_cmd = f'select TBL_NAME from TBLS where TBL_TYPE not in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and DB_ID=(select DB_ID from DBS where NAME=\'{catalog}\') order by TBL_ID'

            rows, cols = dbo.query(table_list_cmd)
            total_tables = len(rows)
            logger.info(f"Found {total_tables} tables in {catalog}")
            with open(output_file, 'a') as fd:
                for row in rows:
                    table_dict = self.backup_table_ddl(dbo, 'hive', catalog, row[0], queries, fd)
                    if status_counter > 0 and total_tables > 10 and status_counter%(int(total_tables/10)) == 0:
                        logger.info(f"Processed {status_counter} tables")
                    status_counter = status_counter + 1
                    db_schema['schemas'][DEFAULT_SCHEMA]['tables'][row[0]] = table_dict
            logger.info(f"Table DDL saved to {output_file} successfully.")

            # Get views and materialized views and append to the results file at the end.
            # The views and materialized views are ordered to resolve the dependencies
            if self.config.get_property('schema_backup', 'include_views', 'true') == 'true':
                if db_type == 'postgresql':
                    view_list_cmd = f'select "TBL_ID" from "TBLS" where "TBL_TYPE" in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "TBL_ID"'
                elif db_type == 'mysql':
                    view_list_cmd = f'select TBL_ID from TBLS where TBL_TYPE in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and DB_ID=(select DB_ID from DBS where NAME=\'{catalog}\') order by TBL_ID'

                rows, _ = dbo.query(view_list_cmd)
                logger.info(f"Found {len(rows)} views in {catalog}")
                with open(output_file, 'a') as fd:
                    for row in rows:
                        if db_type == 'postgresql':
                            formatted_query=f'select "TBL_TYPE", "VIEW_EXPANDED_TEXT", "TBL_NAME" from "TBLS" where "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') and "TBL_ID"={row[0]}'
                        elif db_type == 'mysql':
                            formatted_query=f'select TBL_TYPE, VIEW_EXPANDED_TEXT, TBL_NAME from TBLS where DB_ID=(select DB_ID from DBS where NAME=\'{catalog}\') and TBL_ID={row[0]}'
                        results, _ = dbo.query(formatted_query)
                        if len(results) > 0:
                            create_view_statement='' 
                            view_text = results[0][1]
                            logger.debug(f"View DDL {results[0]}, {view_text}")
                            if results[0] == 'VIRTUAL_VIEW':
                                create_view_statement = f"CREATE VIEW {results[0][2]} as ({view_text});\n"
                            if results[0] == 'MATERIALIZED_VIEW':
                                create_view_statement = f"CREATE MATERIALIZED VIEW {results[0][2]} as ({view_text});\n"
                            fd.write(create_view_statement)

            # Get functions DDL and add to the end of the results file
            if self.config.get_property('schema_backup', 'include_functions', 'true') == 'true':
                if db_type == 'postgresql':
                    func_list_cmd = f'select "CLASS_NAME", "FUNC_NAME","FUNC_TYPE", "OWNER_NAME" from "FUNCS" where "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "FUNC_ID"'
                elif db_type == 'mysql':
                    func_list_cmd = f'select CLASS_NAME, FUNC_NAME,FUNC_TYPE, OWNER_NAME from FUNCS where DB_ID=(select DB_ID from DBS where NAME=\'{catalog}\') order by FUNC_ID'
                
                rows, cols = dbo.query(func_list_cmd)
                with open(output_file, 'a') as fd:
                    for row in rows:
                        create_func_statement = f"CREATE {row[2]} FUNCTION {row[1]} AS {row[0]};\n"
                        fd.write(create_func_statement)

        except Exception as e:
            logger.error(f"An error occurred in backup_database_ddl: {e}")
            traceback.print_exc()
        return db_schema


    def backup_table_ddl(self, dbo, database, catalog, table, table_ddl_queries, ofd):
        create_statement=""
        table_dict={}
        try:
            table_id=0
            table_type='TABLE'
            serde_id=0
            sd_id=0
            cd_id=0
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
                    table=table, table_id=table_id, serde_id=serde_id, sd_id=sd_id, cd_id=cd_id,
                    db_location_uri=db_location_uri, db_managed_uri=db_managed_uri)
                logger.debug(f"Executing query {query_name} : {formatted_query}")
                rows, cols = dbo.query(formatted_query)
                logger.debug(f"Rows: {len(rows)}, cols: {len(cols)}")
                results = [dict(zip(cols, row)) for row in rows]
                logger.debug(f"Results for {query_name} : {results}")
                if query_name == 'Q1':
                    if len(results) > 0:
                        table_name = results[0].get('TBL_NAME')
                        ttype = results[0].get('TBL_TYPE')
                        table_id = results[0].get('TBL_ID')
                        table_dict['type'] = ttype.strip()
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
                        cd_id = results[0].get('CD_ID')
                        location = results[0].get('LOCATION')
                        db_location_uri = results[0].get('DB_LOCATION_URI')
                        db_managed_uri = results[0].get('DB_MANAGED_LOCATION_URI')
                        num_buckets = results[0].get('NUM_BUCKETS')
                        if num_buckets > 0:
                            bucket_string=f"\nINTO {num_buckets} BUCKETS"
                            table_dict['bucket']={}
                            table_dict['bucket']['bucketed'] = True
                            table_dict['bucket']['num_buckets'] = num_buckets
                        else:
                            bucket_string=f""
                        if db_location_uri and not db_managed_uri:
                            db_managed_uri = db_location_uri.replace('external', 'managed')
                        if db_location_uri and location:
                            if not db_location_uri in location:
                                location_string=f"\nLOCATION\n  '{location}'"
                                table_dict['location'] = location

                        ipf = results[0].get('INPUT_FORMAT')
                        opf = results[0].get('OUTPUT_FORMAT')
                        if ipf is not None or opf is not None:
                            if ipf is not None:
                                format_string=f"\nSTORED AS INPUTFORMAT\n  '{ipf}'"
                            if opf is not None:
                                format_string += f"\nOUTPUTFORMAT\n  '{opf}'"
                if query_name == 'Q3':
                    arr = []
                    table_dict['properties']={}
                    for entry in results:
                        if entry['PARAM_KEY'] == 'COLUMN_STATS_ACCURATE' or entry['PARAM_KEY'] == 'EXTERNAL':
                            continue
                        if entry['PARAM_KEY'] == 'storage_handler':
                            stored_by=f"\nSTORED BY\n  '{entry['PARAM_VALUE']}'"
                            table_dict['stored_by'] = entry['PARAM_VALUE']
                            continue
                        if entry['PARAM_KEY'] == 'comment':
                            table_comment=f"\nCOMMENT '{entry['PARAM_VALUE']}'"
                            table_dict['comment'] = entry['PARAM_VALUE']
                            continue
                        if entry['PARAM_KEY'] == 'numFiles' or entry['PARAM_KEY'] == 'numFilesErasureCoded' or entry['PARAM_KEY'] == 'totalSize':
                            table_dict['properties'][entry['PARAM_KEY']] = entry['PARAM_VALUE']
                            continue
                        table_dict['properties'][entry['PARAM_KEY']] = entry['PARAM_VALUE']
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
                    table_dict['columns']={}
                    for entry in results:
                        if entry['COMMENT'] is None:
                            arr.append(f"`{entry['COLUMN_NAME']}` {entry['TYPE_NAME']}")
                        else:
                            arr.append(f"`{entry['COLUMN_NAME']}` {entry['TYPE_NAME']} COMMENT '{entry['COMMENT']}'")
                        table_dict['columns'][entry['COLUMN_NAME']]={}
                        table_dict['columns'][entry['COLUMN_NAME']]['type']=entry['TYPE_NAME']
                        table_dict['columns'][entry['COLUMN_NAME']]['comment']=''
                    if len(arr) > 0:
                        column_string=',\n  '.join(arr)

                if query_name == 'Q10':
                    if len(results) > 0:
                        deser = results[0].get('DESERIALIZER_CLASS')
                        ser = results[0].get('SERIALIZER_CLASS')
                        slib = results[0].get('SLIB')
                        serde_id = results[0].get('SERDE_ID')
                        row_format=f"\nROW FORMAT SERDE\n  '{slib}'"
                        table_dict['serde']=slib

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
                        part_key=entry['PART_NAME']
                        key, value = part_key.split('=')
                        try:
                            int(value)
                            is_numeric = True
                        except ValueError:
                            is_numeric = False

                        if not is_numeric:
                            part_key=f"{key}='{value}'"

                        temp=f"ALTER TABLE {catalog}.{table_name} ADD PARTITION ({part_key}) LOCATION '{entry['LOCATION']}';\n"
                        alter_statements.append(temp)

            if table_type == 'TABLE':
                create_statement = f"CREATE {table_type} `{catalog}`.`{table_name}`(\n  {column_string}) {table_comment} {partition_key_string} {clustered_by_string} {row_format} {serde_properties} {format_string} {stored_by} {tbl_properties};\n\n"
            else:
                create_statement = f"CREATE {table_type} `{catalog}`.`{table_name}`(\n  {column_string}) {table_comment} {partition_key_string} {clustered_by_string} {row_format} {serde_properties} {format_string} {location_string} {stored_by} {tbl_properties};\n\n"

            if self.config.get_property('schema_backup', 'single_line_statement', 'true') == 'true':
                new_create_statement = ''.join([char for char in create_statement if char not in ['\n', '\r']])
                ofd.write(f"{new_create_statement}\n")
            else:
                ofd.write(create_statement)

            for alter_statement in alter_statements:
                logging.debug(f"{alter_statement}")
                ofd.write(alter_statement)

            return table_dict
        except Exception as e:
            traceback.print_exc()
            print("Error:", e)
