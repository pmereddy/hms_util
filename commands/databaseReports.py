import os
import sys
import logging
import traceback

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
from reportWriter import ReportWriter

logger = logging.getLogger(__name__)

class DatabaseReports:
    def __init__(self):
        self.logger = logger

    def gather_database_info(self, dbo, database, catalog, queries, results_dir):
        logger.info(f"Gather database database: {catalog}")
        results = {}
        try:
            for query_name, query_template in queries.items():
                formatted_query = query_template.format(database=database, catalog=catalog)
                logger.debug(f"Executing query {query_name} : {formatted_query}")
                rows, cols = dbo.query(formatted_query)
                results[query_name]={}
                results[query_name]['rows']=rows
                results[query_name]['cols']=cols
        except Exception as e:
            traceback.print_exc()
            logger.error(f"An error occurred in gather_database_info: {e}")
            return None
        return results

    def create_database_reports(self, results, results_dir, filebase, report_formats):
        md_tables = []
        html_tables = []
        rw = ReportWriter()
        for query_name, value in results.items():
            rows=value['rows']
            cols=value['cols']

            # md results
            if 'md' in report_formats:
                temp={}
                temp[query_name] = rw.tuples_to_markdown_table(query_name, cols, rows)
                md_tables.append(temp)

            # html results
            if 'html' in report_formats:
                temp={}
                temp[query_name] = rw.tuples_to_html_table(query_name, cols, rows)
                html_tables.append(temp)

        try:
            if 'md' in report_formats:
                md_results_file = os.path.join(results_dir, f"{filebase}.md")
                with open(md_results_file, 'w') as rfd:
                    for table in md_tables:
                        for k,v in table.items():
                            rfd.write(v)
                logger.info(f"Report saved to {md_results_file}")

            if 'html' in report_formats:
                html_results_file = os.path.join(results_dir, f"{filebase}.html")
                title="Database reports"
                with open(html_results_file, 'w') as hfd:
                    rw.write_section1(hfd, title)
                    for table in html_tables:
                        for k,v in table.items():
                            hfd.write(v)
                    rw.write_section2(hfd)
                logger.info(f"Report saved to {html_results_file}")

        except Exception as e:
            traceback.print_exc()
            logger.error(f"error writing to report files in create_database_reports: {e}")
