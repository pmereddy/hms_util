import csv
import logging
import os
logger = logging.getLogger(__name__)

class ReportWriter:
    def __init__(self):
        self.logger = logger

    def write_csv_file(self, data, filename):
        try:
            with open(filename, 'w', newline='') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(['Metric', 'Value'])
                for key, value in data.items():
                    writer.writerow([key, value])
            self.logger.info(f"CSV file written successfully to {filename}")
        except IOError as e:
            self.logger.error(f"Error writing CSV file {filename}: {e}")
            raise

    def write_md_file(self, data, filename):
        try:
            keys = list(data.keys())
            values = list(data.values())

            max_key_length = max(len(str(key)) for key in keys)
            max_value_length = max(len(str(value)) for value in values)

            table = "| METRIC | VALUE |\n"
            table += "|---" + "-" * max_key_length + "|---" + "-" * max_value_length + "|\n"

            for key, value in data.items():
                table += f"| {str(key):<{max_key_length}} | {str(value):<{max_value_length}} |\n"

            with open(filename, 'w') as md_file:
                md_file.write(table)
            self.logger.info(f"Markdown file written successfully to {filename}")
        except IOError as e:
            self.logger.error(f"Error writing Markdown file {filename}: {e}")
            raise

    def tuples_to_html_table(self, query_name, columns, records):
        if not records:
            return f"\n<h2> {query_name} </h2>\n<p><strong>Results empty</strong></p>"

        table_header = f"\n<h2> {query_name} </h2>\n"
        table_footer = "<br><br>"
        header_row = "<thead><tr>" + "".join(f"<th>{column.upper()}</th>" for column in columns) + "</tr></thead>"

        rows = []
        for row in records:
            values = [str(value) for value in row]
            row_html = "<tr>" + "".join(f"<td>{value}</td>" for value in values) + "</tr>"
            rows.append(row_html)

        html_table = f"""<table id="{query_name}" class="display">
{header_row}
<tbody>
{''.join(rows)}
</tbody>
</table>"""
        self.logger.info(f"HTML table generated for query: {query_name}")
        return "\n".join([table_header, html_table, table_footer])

    def tuples_to_markdown_table(self, query_name, columns, records):
        if not records:
            return f"## {query_name}\n> ** Results empty**"

        table_header = f"## {query_name}\n"
        table_footer = "\n\n"
        columns = [column.upper() for column in columns]
        header_row = " | ".join(columns) if columns else ""
        header_separator = " | ".join([":---" for _ in columns]) if columns else ""

        rows = []
        for row in records:
            values = [str(value) for value in row]
            rows.append(" | ".join(values))

        markdown_table = "\n".join([table_header, header_row, header_separator] + rows + [table_footer])
        self.logger.info(f"Markdown table generated for query: {query_name}")
        return markdown_table

    def write_section1(self, hfd, title):
        try:
            with open('./templates/html_section1.template', 'r') as section_file:
                section1_template = section_file.read()
            section1 = section1_template.replace("[[title]]", title)
            hfd.write(section1)
            self.logger.debug("Section 1 written to file")
        except IOError as e:
            self.logger.error(f"Error reading or writing Section 1: {e}")
            raise

    def write_section2(self, hfd):
        try:
            with open('./templates/html_section2.template', 'r') as section_file:
                section2_template = section_file.read()
            section2 = section2_template
            hfd.write(section2)
            self.logger.debug("Section 2 written to file")
        except IOError as e:
            self.logger.error(f"Error reading or writing Section 2: {e}")
            raise

# Example usage:
if __name__ == "__main__":
    writer = reportWriter()

    # Example data
    data = {'Metric1': 'Value1', 'Metric2': 'Value2'}
    writer.write_csv_file(data, 'example.csv')
    writer.write_md_file(data, 'example.md')

    columns = ['column1', 'column2']
    records = [('value1', 'value2'), ('value3', 'value4')]
    html_table = writer.tuples_to_html_table('Query Name', columns, records)
    md_table = writer.tuples_to_markdown_table('Query Name', columns, records)

    print(html_table)
    print(md_table)

    # Example of writing sections
    with open('example.html', 'w') as hfd:
        writer.write_section1(hfd, 'Section Title')
        writer.write_section2(hfd)
