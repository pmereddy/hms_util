import os

class HiveSchemaComparator:

    def __init__(self, config):
        self.config = config 
        self.sql_statements = []

    def compare_schemas(self, schema1, schema2):
        differences = {}

        schema1_names = set(schema1['schemas'].keys())
        schema2_names = set(schema2['schemas'].keys())

        if self.config.get_property('compare', 'compare_schemas', True):
            schema_diff = self._compare_sets(schema1_names, schema2_names, 'schemas')
            if schema_diff:
                differences['schemas'] = schema_diff

        for schema_name in schema1_names.intersection(schema2_names):
            schema_diff = self._compare_schema_objects(schema1['schemas'][schema_name], schema2['schemas'][schema_name], schema_name)
            if schema_diff:
                differences[schema_name] = schema_diff

        return differences

    def _compare_schema_objects(self, schema1, schema2, schema_name):
        differences = {}

        if self.config.get_property('compare', 'compare_tables', True):
            tables_diff = self._compare_objects(schema1.get('tables', {}), schema2.get('tables', {}), 'tables', schema_name)
            if tables_diff:
                differences['tables'] = tables_diff

        if self.config.get_property('compare', 'compare_views', True):
            views_diff = self._compare_objects(schema1.get('views', {}), schema2.get('views', {}), 'views', schema_name)
            if views_diff:
                differences['views'] = views_diff

        if self.config.get_property('compare', 'compare_udfs', True):
            udfs_diff = self._compare_objects(schema1.get('udfs', {}), schema2.get('udfs', {}), 'udfs', schema_name)
            if udfs_diff:
                differences['udfs'] = udfs_diff

        return differences

    def _compare_objects(self, obj1, obj2, object_type, schema_name):
        differences = {
            'added_objects': [],
            'dropped_objects': [],
            'modified_objects': {}
        }

        obj1_names = set(obj1.keys())
        obj2_names = set(obj2.keys())

        # Handle objects present in schema2 but not in schema1 (add new objects)
        for obj_name in obj2_names - obj1_names:
            if object_type == 'tables':
                differences['added_objects'].append(obj_name)
                self.sql_statements.append(self._generate_create_table(obj_name, obj2[obj_name], schema_name))
            elif object_type == 'views':
                differences['added_objects'].append(obj_name)
                self.sql_statements.append(self._generate_create_view(obj_name, obj2[obj_name], schema_name))
            elif object_type == 'udfs':
                differences['added_objects'].append(obj_name)
                self.sql_statements.append(self._generate_create_udf(obj_name, obj2[obj_name], schema_name))

        # Handle objects present in schema1 but not in schema2 (drop old objects)
        for obj_name in obj1_names - obj2_names:
            if object_type == 'tables':
                differences['dropped_objects'].append(obj_name)
                self.sql_statements.append(f"DROP TABLE {schema_name}.{obj_name};")
            elif object_type == 'views':
                differences['dropped_objects'].append(obj_name)
                self.sql_statements.append(f"DROP VIEW {schema_name}.{obj_name};")
            elif object_type == 'udfs':
                differences['dropped_objects'].append(obj_name)
                self.sql_statements.append(f"DROP FUNCTION {schema_name}.{obj_name};")

        # Compare object names (only if object names differ, indicating added or dropped objects)
        obj_diff = self._compare_sets(obj1_names, obj2_names, object_type)
        if obj_diff:
            differences['name_differences'] = obj_diff

        # Compare objects present in both schemas (check for attribute differences)
        for obj_name in obj1_names.intersection(obj2_names):
            attributes_diff = self._compare_attributes(obj1[obj_name], obj2[obj_name], obj_name, object_type, schema_name)
            if attributes_diff:
                differences['modified_objects'][obj_name] = attributes_diff

        # Only return differences if there are actual changes
        if differences['added_objects'] or differences['dropped_objects'] or differences['modified_objects']:
            return differences
        else:
            return None


    def _compare_attributes(self, obj1, obj2, obj_name, object_type, schema_name):
        differences = {}

        # Handle changes in table attributes (e.g., columns, SERDE, primary key)
        if object_type == 'tables':
            # Compare columns if the option is enabled
            if self.config.get_property('compare', 'compare_columns', True):
                if 'columns' in obj1 and 'columns' in obj2:
                    column_diff = self._compare_columns(
                        obj1['columns'], obj2['columns'],
                        obj1.get('data_types', []), obj2.get('data_types', []),
                        obj_name, schema_name
                    )
                    if column_diff:
                        differences['columns'] = column_diff

            # Compare SERDE if the option is enabled
            if self.config.get_property('compare', 'compare_serdes', True):
                if obj1.get('serdes') != obj2.get('serdes'):
                    differences['serdes'] = {
                        'schema1': obj1.get('serdes', []),
                        'schema2': obj2.get('serdes', [])
                    }
                    self.sql_statements.append(f"ALTER TABLE {schema_name}.{obj_name} SET SERDE '{obj2['serdes'][0]}';")

            # Compare location if the option is enabled
            if self.config.get_property('compare', 'compare_location', True):
                if obj1.get('location') != obj2.get('location'):
                    differences['location'] = {
                        'schema1': obj1.get('location', ''),
                        'schema2': obj2.get('location', '')
                    }
                    self.sql_statements.append(f"ALTER TABLE {schema_name}.{obj_name} SET LOCATION '{obj2['location']}';")

            # Compare primary key if the option is enabled
            if self.config.get_property('compare', 'compare_primary_key', True):
                if obj1.get('primary_key') != obj2.get('primary_key'):
                    differences['primary_key'] = {
                        'schema1': obj1.get('primary_key', []),
                        'schema2': obj2.get('primary_key', [])
                    }
                    self.sql_statements.append(f"ALTER TABLE {schema_name}.{obj_name} DROP PRIMARY KEY;")
                    self.sql_statements.append(f"ALTER TABLE {schema_name}.{obj_name} ADD PRIMARY KEY ({', '.join(obj2['primary_key'])});")

            # Compare bucket information if the option is enabled
            if self.config.get_property('compare', 'compare_bucket', True):
                if obj1.get('bucket') != obj2.get('bucket'):
                    differences['bucket'] = {
                        'schema1': obj1.get('bucket', {}),
                        'schema2': obj2.get('bucket', {})
                    }
                    self.sql_statements.append(f"ALTER TABLE {schema_name}.{obj_name} CLUSTERED BY ({', '.join(obj2['bucket']['bucket_columns'])}) INTO {obj2['bucket']['num_buckets']} BUCKETS;")

        # Handle changes in views
        if object_type == 'views':
            # Compare view query if the option is enabled
            if obj1.get('query') != obj2.get('query'):
                differences['query'] = {
                    'schema1': obj1.get('query', ''),
                    'schema2': obj2.get('query', '')
                }
                self.sql_statements.append(f"CREATE OR REPLACE VIEW {schema_name}.{obj_name} AS {obj2['query']};")

        return differences


    def _compare_columns(self, cols1, cols2, types1, types2, table_name, schema_name):
        differences = {
            'new_columns': [],
            'dropped_columns': []
        }

        # Add missing columns (present in cols2 but not in cols1)
        for i, col in enumerate(cols2):
            if col not in cols1:
                new_column = {
                    'column': col,
                    'data_type': types2[i]
                }
                differences['new_columns'].append(new_column)
                self.sql_statements.append(f"ALTER TABLE {schema_name}.{table_name} ADD COLUMNS ({col} {types2[i]});")

        # Drop old columns (present in cols1 but not in cols2)
        for i, col in enumerate(cols1):
            if col not in cols2:
                dropped_column = {
                    'column': col,
                    'data_type': types1[i]
                }
                differences['dropped_columns'].append(dropped_column)
                self.sql_statements.append(f"ALTER TABLE {schema_name}.{table_name} DROP COLUMN {col};")

        # Only return differences if there are actual changes
        if differences['new_columns'] or differences['dropped_columns']:
            return differences
        else:
            return None


    def _compare_sets(self, set1, set2, name):
        differences = {}
        only_in_1 = set1 - set2
        only_in_2 = set2 - set1

        if only_in_1:
            differences[f"only_in_1_{name}"] = list(only_in_1)
        if only_in_2:
            differences[f"only_in_2_{name}"] = list(only_in_2)

        return differences if differences else None

    # SQL Generation Helpers
    def _generate_create_table(self, table_name, table_obj, schema_name):
        columns = ', '.join([f"{col} {dtype}" for col, dtype in zip(table_obj['column_list'], table_obj['data_types'])])
        if 'primary_key' in table_obj:
            primary_key = f"PRIMARY KEY ({', '.join(table_obj['primary_key'])})"
        else:
            primary_key = ''
        if 'bucket' in table_obj:
            bucket = f"CLUSTERED BY ({', '.join(table_obj['bucket']['bucket_columns'])}) INTO {table_obj['bucket']['num_buckets']} BUCKETS"
        else:
            bucket = ''
        if 'location' in table_obj:
            location = f"LOCATION '{table_obj['location']}'"
        else:
            location = ''
        if 'serdes' in table_obj:
            serdes = f"SERDE '{table_obj['serdes'][0]}'"
        else:
            serdes = ''
        return f"CREATE TABLE {schema_name}.{table_name} ({columns}, {primary_key}) {bucket} {serdes} {location};"

    def _generate_create_view(self, view_name, view_obj, schema_name):
        return f"CREATE VIEW {schema_name}.{view_name} AS {view_obj['definition']};"

    def _generate_create_udf(self, udf_name, udf_obj, schema_name):
        return f"CREATE FUNCTION {schema_name}.{udf_name} AS '{udf_obj['return_type']}' RETURNS {udf_obj['arguments'][0]};"


    def print_diffs_detailed(self, diffs):
        for key, value in diffs.items():
            if isinstance(value, dict):
                print(f"\nDifferences in {key}:")
                self.print_diffs_detailed(value)
            else:
                if isinstance(value, tuple):
                    print(f"  - Difference in {key}:")
                    print(f"    - Schema1: {value[0]}")
                    print(f"    - Schema2: {value[1]}")
                else:
                    print(f"  - {key}: {value}")


    def print_diffs_hierarchical(self, diffs, indent=2):
        for key, value in diffs.items():
            if isinstance(value, dict):
                print('    ' * indent + f"{key}:")
                self.print_diffs_hierarchical(value, indent + 1)
            else:
                print('    ' * indent + f"{key}: {value}")



    def generate_html_report(self, differences, results_dir="results", output_file="report.html"):
        # Start the HTML structure
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Schema Differences Report</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; }
                h1 { color: #2C3E50; }
                h2 { color: #34495E; }
                h3 { color: #5D6D7E; }
                table { width: 100%; border-collapse: collapse; margin: 20px 0; }
                th, td { border: 1px solid #ddd; padding: 8px; }
                th { background-color: #f2f2f2; }
                tr:nth-child(even) { background-color: #f9f9f9; }
                .added { background-color: #d4edda; }
                .dropped { background-color: #f8d7da; }
                .modified { background-color: #fff3cd; }
            </style>
        </head>
        <body>
            <h1>Schema Differences Report</h1>
        """

        def create_table_row(attribute, schema1_value, schema2_value, css_class=""):
            return f"""
            <tr class="{css_class}">
                <td>{attribute}</td>
                <td>{schema1_value}</td>
                <td>{schema2_value}</td>
            </tr>
            """

        def process_attribute(attribute_name, attribute_diff):
            """Helper function to handle differences in attributes."""
            rows = ""
            if isinstance(attribute_diff, dict):
                schema1_val = attribute_diff.get('schema1', '')
                schema2_val = attribute_diff.get('schema2', '')
            elif isinstance(attribute_diff, tuple):
                schema1_val, schema2_val = attribute_diff
            else:
                schema1_val = attribute_diff
                schema2_val = ""

            rows += create_table_row(attribute_name, schema1_val, schema2_val, "modified")
            return rows

        # Recursive function to process differences
        def process_differences(differences, depth=0):
            nonlocal html_content

            indent = " " * (depth * 4)
            if isinstance(differences, dict):
                for key, value in differences.items():
                    if key in ['added_objects', 'dropped_objects']:
                        html_content += f"<h3>{key.replace('_', ' ').capitalize()}</h3>"
                        html_content += "<ul>"
                        for obj in value:
                            html_content += f"<li>{obj}</li>"
                        html_content += "</ul>"

                    elif key == 'modified_objects':
                        for obj_name, attributes in value.items():
                            html_content += f"<h3>Modified Object: {obj_name}</h3>"
                            html_content += "<table>"
                            html_content += """
                            <tr>
                                <th>Attribute</th>
                                <th>Schema 1 Value</th>
                                <th>Schema 2 Value</th>
                            </tr>
                            """
                            for attribute_name, attribute_diff in attributes.items():
                                if isinstance(attribute_diff, dict):
                                    for sub_attr, sub_diff in attribute_diff.items():
                                        html_content += process_attribute(f"{attribute_name} ({sub_attr})", sub_diff)
                                else:
                                    html_content += process_attribute(attribute_name, attribute_diff)
                            html_content += "</table>"

                    elif key == 'name_differences':
                        html_content += "<h3>Object Name Differences</h3>"
                        for sub_key, sub_value in value.items():
                            html_content += f"<h4>{sub_key.replace('_', ' ').capitalize()}</h4>"
                            html_content += "<ul>"
                            for obj in sub_value:
                                html_content += f"<li>{obj}</li>"
                            html_content += "</ul>"
                    else:
                        html_content += f"<h2>{key.capitalize()}</h2>"
                        process_differences(value, depth + 1)

        process_differences(differences)

        # Close the HTML structure
        html_content += """
        </body>
        </html>
        """

        # Write the HTML content to the file
        html_results_file = os.path.join(results_dir, output_file)
        with open(html_results_file, 'w') as file:
            file.write(html_content)

        print(f"HTML report generated: {os.path.abspath(html_results_file)}")
