import configparser
import os
import sys
import logging
import traceback

logger = logging.getLogger(__name__)

valid_formats=["org.apache.hadoop.hive.ql.io.orc.OrcSerde", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", "org.apache.hadoop.hive.serde2.avro.AvroSerDe"]

class IcebergMigration:
    def __init__(self, version='2', approach='inplace', table_properties='', results_dir="results"):
        self.logger = logger
        self.results_dir = results_dir
        self.version = version
        self.table_properties = table_properties

    def create_iceberg_migration_statements(self, dbo, db_type, catalog, filebase):
        try:
            logger.info(f"Running iceberg migration for database: {catalog}")
            list_tables = f'SELECT a."TBL_ID", a."TBL_NAME", a."TBL_TYPE", b."IS_COMPRESSED", b."IS_STOREDASSUBDIRECTORIES", b."INPUT_FORMAT", b."OUTPUT_FORMAT",c."SLIB" FROM "TBLS" a inner join "SDS" b on a."SD_ID"=b."SD_ID" INNER JOIN "SERDES" c on b."SERDE_ID"=c."SERDE_ID" where a."DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\');'
            rows, cols = dbo.query(list_tables)
            results_file=os.path.join(self.results_dir, f"{filebase}.sql")
            output_file=os.path.join(self.results_dir, f"{filebase}.log")
            with open(results_file, "w") as res, open(output_file, "w") as out:
                for entry in rows:
                    props=[]
                    props.append(f"'storage_handler'='org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'")
                    props.append(f"'format-version'='{self.version}'")
                    logger.debug(f"{entry[0]}, {entry[1]}, {entry[2]}, {entry[7]}")
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
                    prop_results,_ = dbo.query(table_props_query)
                    handler=""
                    transactional=""
                    external_purge='false'
                    for prop in prop_results:
                        logger.debug(f"{prop[0]}, {prop[1]}")
                        if prop[0] == 'storage_handler':
                            handler = prop[1]
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
                logger.info(f"saved ddl to {results_file}")
                logger.info(f"saved logs to {output_file}")
        except Exception as e:
            print("Error in create_iceberg_migration_statements:", e)
            traceback.print_exc()
