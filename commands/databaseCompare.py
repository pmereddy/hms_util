import os
import sys
import logging
import traceback
logger = logging.getLogger(__name__)

class DatabaseCompare:
    def __init__(self, config):
        self.logger = logger
        self.config = config

    def get_database_schema(self, dbo, db_type, hms_db, catalog, queries):
        db_schema={}
        DEFAULT_SCHEMA="default"
        try:
            if self.config.get_property('compare', 'compare_database_properties', 'true') == 'true':
                db_schema['properties']={}
                if db_type == 'postgresql':
                    create_db = f'select "DESC", "DB_LOCATION_URI" from "DBS" where "NAME"=\'{catalog}\''
                elif db_type == 'mysql':
                    create_db = f'select `DESC`, DB_LOCATION_URI from DBS where NAME=\'{catalog}\''

                results = dbo.query(create_db)
                if len(results) > 0:
                    db_schema['properties']['comment']=results[0]
                    db_schema['properties']['location']=results[1]
                else:
                    logger.warn(f"database: {catalog} not found")
                    return db_schema

            db_schema['schemas']={}
            db_schema['schemas'][DEFAULT_SCHEMA]={}
            db_schema['schemas'][DEFAULT_SCHEMA]['tables']={}

            # Get tables
            if self.config.get_property('compare', 'compare_tables', 'true') == 'true':
                if db_type == 'postgresql':
                    table_list_cmd = f'select "TBL_NAME" from "TBLS" where "TBL_TYPE" not in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "TBL_ID"'
                elif db_type == 'mysql':
                    table_list_cmd = f'select TBL_NAME from TBLS where TBL_TYPE not in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and DB_ID=(select DB_ID from DBS where NAME=\'{catalog}\') order by TBL_ID'

                rows, cols = dbo.query(table_list_cmd)
                for row in rows:
                    table_dict = self.get_table_schema(dbo, 'hive', catalog, row[0], queries)
                    db_schema['schemas'][DEFAULT_SCHEMA]['tables'][row[0]] = table_dict


            # Get views
            if self.config.get_property('compare', 'compare_views', 'true') == 'true':
                db_schema['schemas'][DEFAULT_SCHEMA]['views']={}
                if db_type == 'postgresql':
                    view_list_cmd = f'select "TBL_ID" from "TBLS" where "TBL_TYPE" in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "TBL_ID"'
                elif db_type == 'mysql':
                    view_list_cmd = f'select TBL_ID from TBLS where TBL_TYPE in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and DB_ID=(select DB_ID from DBS where NAME=\'{catalog}\') order by TBL_ID'

                rows, _ = dbo.query(view_list_cmd)
                for row in rows:
                    if db_type == 'postgresql':
                        formatted_query=f'select "TBL_TYPE", "VIEW_EXPANDED_TEXT", "TBL_NAME" from "TBLS" where "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') and "TBL_ID"={row[0]}'
                    elif db_type == 'mysql':
                        formatted_query=f'select TBL_TYPE, VIEW_EXPANDED_TEXT, TBL_NAME from TBLS where DB_ID=(select DB_ID from DBS where NAME=\'{catalog}\') and TBL_ID={row[0]}'
                    results, _ = dbo.query(formatted_query)
                    if len(results) > 0:
                        view_text = results[0][1]
                        logger.debug(f"View DDL {results[0]}, {view_text}")
                        db_schema['schemas'][DEFAULT_SCHEMA]['views'][results[0][2]] = {}
                        db_schema['schemas'][DEFAULT_SCHEMA]['views'][results[0][2]]['definition'] = view_text
                        if results[0][0] == 'VIRTUAL_VIEW':
                            db_schema['schemas'][DEFAULT_SCHEMA]['views'][results[0][2]]['type']='VIRTUAL_VIEW'
                        if results[0][0] == 'MATERIALIZED_VIEW':
                            db_schema['schemas'][DEFAULT_SCHEMA]['views'][results[0][2]]['type']='MATERIALIZED_VIEW'

            # Get functions DDL and add to the end of the results file
            if self.config.get_property('compare', 'compare_functions', 'true') == 'true':
                if db_type == 'postgresql':
                    func_list_cmd = f'select "CLASS_NAME", "FUNC_NAME","FUNC_TYPE", "OWNER_NAME" from "FUNCS" where "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "FUNC_ID"'
                elif db_type == 'mysql':
                    func_list_cmd = f'select CLASS_NAME, FUNC_NAME,FUNC_TYPE, OWNER_NAME from FUNCS where DB_ID=(select DB_ID from DBS where NAME=\'{catalog}\') order by FUNC_ID'
                
                rows, cols = dbo.query(func_list_cmd)
                for row in rows:
                    db_schema['schemas'][DEFAULT_SCHEMA]['functions'][row[1]] = {}
                    db_schema['schemas'][DEFAULT_SCHEMA]['functions'][row[1]]['class'] = row[0]
                    db_schema['schemas'][DEFAULT_SCHEMA]['functions'][row[1]]['type'] = row[2]
                    db_schema['schemas'][DEFAULT_SCHEMA]['functions'][row[1]]['owner'] = row[3]
        except Exception as e:
            logger.error(f"An error occurred in get_database_schema: {e}")
            traceback.print_exc()
        return db_schema


    def get_table_schema(self, dbo, database, catalog, table, table_ddl_queries):
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
                        return None
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
                                table_dict['location'] = location

                        ipf = results[0].get('INPUT_FORMAT')
                        opf = results[0].get('OUTPUT_FORMAT')
                        if ipf is not None or opf is not None:
                            if ipf is not None:
                                table_dict['input_format']=ipf
                            if opf is not None:
                                table_dict['output_format']=opf
                if query_name == 'Q3':
                    arr = []
                    table_dict['properties']={}
                    for entry in results:
                        if entry['PARAM_KEY'] == 'COLUMN_STATS_ACCURATE' or entry['PARAM_KEY'] == 'EXTERNAL':
                            continue
                        if entry['PARAM_KEY'] == 'storage_handler':
                            table_dict['stored_by'] = entry['PARAM_VALUE']
                            continue
                        if entry['PARAM_KEY'] == 'comment':
                            table_dict['comment'] = entry['PARAM_VALUE']
                            continue
                        if entry['PARAM_KEY'] == 'numFiles' or entry['PARAM_KEY'] == 'numFilesErasureCoded' or entry['PARAM_KEY'] == 'totalSize':
                            table_dict['properties'][entry['PARAM_KEY']] = entry['PARAM_VALUE']
                            continue
                        table_dict['properties'][entry['PARAM_KEY']] = entry['PARAM_VALUE']
                if query_name == 'Q6':
                    arr = []
                    for entry in results:
                        arr.append(f"{entry['COLUMN_NAME']} {entry['ORDER']}")
                    if len(arr) > 0:
                        a=', '.join(arr)
                        table_dict['sorted_by'] = a
                if query_name == 'Q9':
                    arr = []
                    table_dict['columns']={}
                    table_dict['data_types']=[]
                    table_dict['column_list']=[]
                    for entry in results:
                        table_dict['columns'][entry['COLUMN_NAME']]={}
                        table_dict['columns'][entry['COLUMN_NAME']]['type']=entry['TYPE_NAME']
                        table_dict['columns'][entry['COLUMN_NAME']]['comment']=entry.get('COMMENT', '')
                        table_dict['data_types'].extend([entry['TYPE_NAME']])
                        table_dict['column_list'].extend([entry['COLUMN_NAME']])

                if query_name == 'Q10':
                    if len(results) > 0:
                        deser = results[0].get('DESERIALIZER_CLASS')
                        ser = results[0].get('SERIALIZER_CLASS')
                        slib = results[0].get('SLIB')
                        table_dict['serde']=slib
                        table_dict['serializer']=ser
                        table_dict['deserializer']=deser

                if query_name == 'Q11':
                    arr = []
                    table_dict['serde_properties'] = {}
                    for entry in results:
                        table_dict['serde_properties'][entry['PARAM_KEY']] = entry['PARAM_VALUE']

                if query_name == 'Q12':
                    arr = []
                    for entry in results:
                        arr.append(f"'{entry['SKEWED_COL_NAME']}'")
                    if len(arr) > 0:
                        table_dict['skewed_columns'] = arr

                if query_name == 'Q16':
                    arr = []
                    for entry in results:
                        arr.append(f"`{entry['PKEY_NAME']}` {entry['PKEY_TYPE']}")
                    if len(arr) > 0:
                        table_dict['partitioned_by'] = arr

                if query_name == 'Q17':
                    if len(results) > 0:
                        bucket_cols = results[0].get('PARAM_VALUE')
                        if bucket_cols is not None:
                            table_dict['clustered_by'] = bucket_cols

                if query_name == 'Q18':
                    for entry in results:
                        part_key=entry['PART_NAME']
                        key, value = part_key.split('=')
                        table_dict['custom_partitions'] = {}
                        table_dict['custom_partitions'][key] = {}
                        table_dict['custom_partitions'][key]['value'] = value
                        table_dict['custom_partitions'][key]['value'] = entry['LOCATION']

            return table_dict
        except Exception as e:
            traceback.print_exc()
