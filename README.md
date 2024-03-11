# hms_util
**hms_util** project provides DDL backup and utility functions based on hive metastore. 

It is written in python, distributed under the [APLv2](./LICENSE) license, and currently supports PostgreSQL database backend for hive metastore. 
Support for additional databases will be added in the future.
 
hms_util's DDL backup is **100x-500x** faster than taking the DDL backup using _show create table_ with _beeline_ or _describe table_ with _impala-shell_

In addition to hive database DDL backup, **hms_util** supports commands like **database summarization**, **database reports**, **change_history**, **iceberg_migration** and **compare two hive metastores** (in the future)

The main script **hms_util.py** takes two optional arguments. 
- log_level: --log_level [INFO|WARN|DEBUG]
- config: --config path_to_config file

**NOTE**: hms_util does not make any changes to the source or target hive metastore(s). 

# Installation

Clone this repo

```git clone https://github.com/pmereddy/hms_util.git```

Install the depdendencies

```pip3 install psycopg2-binary```

# Configuration
The **config** file option (defaults to config.ini in the project directory) is the only way to specify the command and parameters to run this tool. 

Below is the description of various sections in config.ini

**[global]**
- `command`: Specifies the command to run.
    - Valid values are `summary`, `backup_ddl`, `change_history`, `iceberg_migration`, and `compare`.
- `database_type`: Specifies the type of database.
    - Valid values are `postgresql`.
- `catalog`: Specifies the Hive catalog. Default value is `default`.
- `results_dir`: Specifies the directory to store results. Default value is `results`.
- `queries_dir`: Specifies the directory containing query files. Default value is `queries`. It will be concatinated with `database_type`

**[summary]**

`Provides high-level Hive database summary`

- `output_file`: Specifies the file to store the results.
- `query_file`: Specifies the query file to run against the metastore.

**[backup_ddl]**

`Takes a schema backup of a Hive database`

- `output_file`: Specifies the file to store the results.
- `create_db_statement`: Indicates whether to include CREATE DATABASE statements. Defaults to `true`
- `include_views`: Indicates whether to include views. Defaults to `true`
- `include_functions`: Indicates whether to include user-defined functions. Defaults to `true`

**[change_history]**

`Summarizes the changes occurred during the specified past days on the specified database/table combination.`

- `past_days`: Specifies the number of days worth of history.
- `table`: Specifies the table to analyze changes. Use "ALL" to include all tables.
- `summary`: Indicates whether to provide a summary of changes.
- `change_summary_query_file`: Specifies the query file for change summary.
- `details`: Indicates whether to provide change details.

**[iceberg_migration]**

`Creates DDL to convert Hive tables to Iceberg tables.`

- `iceberg_version`: Specifies the Iceberg version to use (1 or 2). Defaults to 2.
- `migration_approach`: Specifies the migration approach. Valid values are `inplace` and `ctas`.

**[compare]**

`Compares two Hive metastores and identifies differences`

- [source]: Specifies the source Hive metastore database.
  - `host`: Hostname of the source database.
  - `port`: Port number of the source database.
  - `user`: Username for the source database.
  - `database`: Database name of the source database.
  - `password`: Password for the source database.

- [target]: Specifies the target Hive metastore database (future enhancement).
  - `host`: Hostname of the target database.
  - `port`: Port number of the target database.
  - `user`: Username for the target database.
  - `database`: Database name of the target database.
  - `password`: Password for the target database.

# Where to run `hms_util`
hms_util can be run from any linux host that can access hive metastore database backend.

# Commands
The following commands are supported by the tool. The **command** can be specified in the **[global]** section of config.ini

## Database schema backup

To take the schema backup of a hive database, fill out/review the following sections 
- **[global]**
    - set **command** to **backup_ddl**
    - set the **catalog** property to the name of the hive database to backup
- **[source]**
    - set/review the properties to connect to source hive metastore database
- **[backup_ddl]**
    - Change properties if you want to want to customize the output
      
Here is the output from a sample run
```
2024-03-04 02:33:55,026 - INFO - command: backup_ddl
2024-03-04 02:33:55,054 - INFO - Extracting DDL for database: ooxpdev
2024-03-04 02:33:55,088 - INFO - Found 13941 tables in ooxpdev
2024-03-04 02:34:13,024 - INFO - Processed 1394 tables
2024-03-04 02:34:33,443 - INFO - Processed 2788 tables
2024-03-04 02:34:54,089 - INFO - Processed 4182 tables
2024-03-04 02:35:13,054 - INFO - Processed 5576 tables
2024-03-04 02:35:32,245 - INFO - Processed 6970 tables
2024-03-04 02:35:51,411 - INFO - Processed 8364 tables
2024-03-04 02:36:10,660 - INFO - Processed 9758 tables
2024-03-04 02:36:30,431 - INFO - Processed 11152 tables
2024-03-04 02:36:50,289 - INFO - Processed 12546 tables
2024-03-04 02:37:09,650 - INFO - Processed 13940 tables
2024-03-04 02:37:09,650 - INFO - Table DDL saved to results/ddl_backup.hql successfully.
2024-03-04 02:37:09,656 - INFO - Found 40 views in ooxpdev
2024-03-04 02:37:09,681 - INFO - backup_ddl saved to results/ddl_backup.hql

```

## Database Summary
To get high level summary of a hive database, fill out/review the following sections 
- **[global]**
    - set **command** to **summary**
    - set the **catalog** property to the name of the hive database to summarize
- **[source]**
    - set/review the properties to connect to source hive metastore database    
- **[summary]**
    - Change properties if you want to want to customize the output

Here is the output from a sample run
```
2024-03-04 02:55:39,400 - INFO - command: summary
2024-03-04 02:55:39,420 - INFO - Get summary for database: ooxpdev
2024-03-04 02:55:41,861 - INFO - summary saved to results/summary.csv
2024-03-04 02:55:41,862 - INFO - summary saved to results/summary.md
```

## Database Change history

This command can be used to get information about changes that occurred at a table or database level.
Change information is captured at two levels. A high level summary and also details. 

To run this command, fill out/review the following sections 
- **[global]**
    - set **command** to **change_history**
    - set the **catalog** property to the name of the hive database to check for changes
- **[source]**
    - set/review the properties to connect to source hive metastore database    
- **[change_history]**
    - set the **past_days** to the number of days of history to check for changes
    - set **table** to ALL to get details of the entire database or a specific table name for change details on just one table
  
Here is the output from a sample run
```
2024-03-04 15:23:30,932 - INFO - command: change_history
2024-03-04 15:23:30,952 - INFO - Get summary for database: ooxpdev
2024-03-04 15:23:30,959 - INFO - change_history saved to results/change_summary.csv
2024-03-04 15:23:30,959 - INFO - change_history saved to results/change_summary.md
2024-03-04 15:23:30,960 - INFO - Running get change details for database: ooxpdev, table: ALL, duration: 60
2024-03-04 15:23:30,979 - INFO - Found 32 events
2024-03-04 15:23:30,989 - INFO - change_history saved to results/change_details.hql
```

## Iceberg Migration

This command generates DDL commands to migrate eligible tables in a hive database to Iceberg table format. 
External tables that are using the supported formats like **AVRO**, **PARQUET**, and **ORC**, and not already converted to Iceberg are considered eligible for conversion.

To run this command, fill out/review the following sections 
- **[global]**
    - set **command** to **iceberg_migration**
    - set the **catalog** property to the name of the hive database to convert to Iceberg table format 
- **[source]**
    - set/review the properties to connect to source hive metastore database
- **[iceberg_migration]**
    - review/set the **iceberg_version**. Valid values are **1** and **2**. Defaults to **2**
    - review/set the **migration_approach**. Valid values are **inplace** and **ctas**. Defaults to **inplace**
    - review/set **create_drop_statements_for_external_no_purge** to create drop statements for hive tables that do not purge data files on deletion

Here is the output from a sample run
```
2024-03-04 15:04:24,575 - INFO - command: iceberg_migration
2024-03-04 15:04:24,598 - INFO - Running iceberg migration for database: ooxpdev
2024-03-04 15:04:32,151 - INFO - saved ddl to results/iceberg_migration.ddl
2024-03-04 15:04:32,151 - INFO - saved logs to results/iceberg_migration.log
```
