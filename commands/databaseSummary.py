import os
import sys
import logging
import traceback

logger = logging.getLogger(__name__)

class DatabaseSummary:
    def __init__(self):
        self.logger = logger

    def get_summary(self, dbo, db_type, hms_db, catalog, queries, **kwargs):
        summary={}
        logger.info(f"Get summary for database: {catalog}")
        try:
            table=kwargs.get('table', 'ALL')
            table_filter=""
            if table != 'ALL':
                if db_type == 'postgresql':
                    table_filter=f"\"TBL_NAME\"='{table}' and"
                elif db_type == 'mysql':
                    table_filter=f"TBL_NAME='{table}' and"
                else:
                    table_filter=f"\"TBL_NAME\"='{table}' and"
            for query_name, query_template in queries.items():
                formatted_query = query_template.format(database='hive', catalog=catalog, past_days=kwargs.get('past_days', 1), table=table, table_filter=table_filter)
                logger.debug(f"Executing query {query_name} : {formatted_query}")
                rows, cols = dbo.query(formatted_query)
                if not rows or not cols:
                    logger.error(f"Failed to run query. Ignoring")
                else:
                    results = [dict(zip(cols, row)) for row in rows]
                    logger.debug(f"Results for {query_name} : {results}")
                    for entry in results:
                        summary[entry['key']] = entry['value']
        except Exception as e:
            print("Error in get_summary:", e)
            traceback.print_exc()
            return None
        return summary
