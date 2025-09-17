"""Auto-generated from Create Power BI Project From TWB File.ipynb"""

from python_scripts.config_loader import load_config

def main():

    import os
    import xml.etree.ElementTree as ET
    import csv
    import re
    import shutil
    import fnmatch
    import uuid
    import pandas as pd
    import time 
    import json
    import stat
    import logging
    from datetime import datetime
    from typing import Optional
    from pathlib import Path
    from io import StringIO
    

    from azure.storage.blob import BlobServiceClient, ContentSettings
    from azure.identity import DefaultAzureCredential
    from azure.ai.projects import AIProjectClient
    from azure.ai.agents.models import MessageRole

    # Load configuration from config.yaml
    config = load_config()
    today_str = datetime.now().strftime('%Y-%m-%d')

    # Use config values instead of hardcoded paths/names
    INPUT_PATH = config["paths"]["input_path"]
    TEMPLATE_PATH = config["paths"]["template_path"]
    METADATA_PATH = f'{config["paths"]["metadata_path"]}/{config["report"]["type"]}/{config["report"]["name"]}'
    LOG_PATH = f'{config["paths"]["log_path"]}/{config["report"]["type"]}/{today_str}/{config["report"]["name"]}'
    OUTPUT_PATH = f'{config["paths"]["output_path"]}/{config["report"]["type"]}/{config["report"]["name"]}'
    REPORT_TYPE = config["report"]["type"]
    REPORT_NAME = config["report"]["name"]
    REPORT_NAME_WITH_EXT = REPORT_NAME + ".twb" if REPORT_TYPE == "Tableau" else ".qvf"
    REPORT_FILE_PATH = f"{INPUT_PATH}/{REPORT_TYPE}/{REPORT_NAME_WITH_EXT}"
    PROJECT_ENDPOINT = config["ai"]["project_endpoint"]
    AGENT_ID = config["ai"]["agent_id"]

    # Create Metadata folder if it doesn't exist
    Metadata_Folder_Path = Path(METADATA_PATH)
    Metadata_Folder_Path.mkdir(parents=True, exist_ok=True)

    # Create Output folder if it doesn't exist
    Output_Folder_Path = Path(OUTPUT_PATH)
    Output_Folder_Path.mkdir(parents=True, exist_ok=True)

    # --- Logging Setup ---
    log_dir = LOG_PATH
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'migration_{today_str}.log')
    # Remove all handlers before setting up logging
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(
        filename=log_file,
        filemode='w',
        format='%(asctime)s %(levelname)s: %(message)s',
        level=logging.INFO
    )
    


    # --- File paths ---
    summary_log_file = os.path.join(log_dir, f"summary_{today_str}.log")

    # --- Function to create a logger per operation ---
    def get_logger(name, log_file):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)

        # Prevent duplicate handlers if function is called multiple times
        if not logger.handlers:
            handler = logging.FileHandler(log_file)
            formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        return logger

    # --- Create two loggers ---
    summary_logger = get_logger("SummaryLogger", summary_log_file)
    summary_logger.info(f"Migration started...")
    logging.info('Model Migration process started.')

    def extract_connections(twb_file, output_path):
        logging.info(f"Extracting Connection Names from TWB file: {twb_file}")
        try:
            # Parse the TWB XML
            tree = ET.parse(twb_file)
            root = tree.getroot()
            output_csv = os.path.join(output_path, "Connections.csv")

            rows = []

            # Loop through each datasource
            for datasource in root.findall(".//datasource"):
                datasource_caption = datasource.get("caption", "")
                datasource_name = datasource.get("name", "")
                
                # Look for connection -> named-connections -> named-connection
                for connection in datasource.findall(".//connection"):
                    for named_conn in connection.findall(".//named-connection"):
                        named_connection_name = named_conn.get("name", "")
                        
                        # Extract inner connection attributes
                        inner_conn = named_conn.find("connection")
                        if inner_conn is not None:
                            conn_class = inner_conn.get("class", "")
                            conn_dbname = inner_conn.get("dbname", "")
                            conn_one_time_sql = inner_conn.get("one-time-sql", "")
                            conn_server = inner_conn.get("server", "")
                            
                            rows.append([
                                datasource_name,
                                datasource_caption,
                                named_connection_name,
                                conn_class,
                                conn_server,
                                conn_dbname,
                                conn_one_time_sql
                            ])

            # Write to CSV
            with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["DataSourceID", "DataSourceName", "ConnectionID", "ConnectionType", "ServerName", "DatabaseName", "InitialSQL"])
                writer.writerows(rows)
            
            logging.info(f"Extracted {len(rows)} connections. Saved to {output_csv}")
        
        except Exception as e:
            logging.error(f"Error extracting connections: {e}")
        

    # Helper to clean/extract Schema and TableName from object-id
    def extract_tables(twb_file, output_path):
        logging.info(f"Extracting Table Names from TWB file: {twb_file}")
        try:
            # Parse the TWB XML
            tree = ET.parse(twb_file)
            root = tree.getroot()
            output_csv = os.path.join(output_path, "Tables.csv")

            rows = []

            # Loop through each datasource
            for datasource in root.findall(".//datasource"):
                datasource_name = datasource.get("name", "")
                
                # Loop through objects under object-graph
                for obj_graph in datasource.findall(".//object-graph"):
                    for obj in obj_graph.findall(".//object"):
                        object_caption = obj.get("caption", "")
                        object_id = obj.get("id", "")
                        
                        # Only properties with context=''
                        for prop in obj.findall("properties"):
                            if prop.get("context") == "":
                                relation = prop.find("relation")
                                if relation is not None:
                                    relation_connection = relation.get("connection", "")
                                    relation_name = relation.get("name", "")
                                    relation_table = relation.get("table", "")
                                    relation_type = relation.get("type", "")
                                    # Optional: Custom SQL query inside <relation>
                                    custom_sql = relation.text.strip() if relation.text else ""
                                    sql_single_line = custom_sql.replace("\r\n", "#(cr)#(lf)").replace("\n", "#(cr)#(lf)").replace("\r", "#(cr)#(lf)")

                                    # Split [Schema].[Table] into Schema and Table
                                    match = re.match(r"\[(.*?)\]\.\[(.*?)\]", relation_table)
                                    if match:
                                        schema = match.group(1)
                                        table = match.group(2)
                                    else:
                                        schema = ""
                                        table = relation_table  # fallback if pattern not match
                                    
                                    rows.append([
                                        datasource_name,
                                        # object_caption,
                                        object_id,
                                        relation_connection,
                                        object_caption, ##relation_name,
                                        schema,
                                        table,
                                        relation_type,
                                        sql_single_line
                                    ])

            # print("RRR", rows)
            # Write to CSV
            with open(output_csv, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(["DataSourceID", "TableID", "ConnectionID", "PresentationTableName", "SchemaName", "TableName", "TableType", "CustomSQL"])
                writer.writerows(rows)
            row_count = len(rows)
            logging.info(f"Extracted {len(rows)} tables. Saved to {output_csv}")
            return row_count
        except Exception as e:
            logging.error(f"Error extracting tables: {e}")

    def extract_custom_sql(twb_file, output_path):
        """
        Extracts all custom SQL queries from a Tableau .twb file.
        Returns a list of tuples: (datasource_name, custom_sql_query)
        """
        logging.info(f"Parsing XML for custom SQL queries: {twb_file}")
        try:
            tree = ET.parse(twb_file)
            root = tree.getroot()
            output_csv = os.path.join(output_path, "CustomSQL.csv")
            custom_sql_queries = []

            for datasource in root.findall(".//datasource"):
                datasource_name = datasource.get("name")
                for relation in datasource.findall(".//relation"):
                    relation_type = relation.get("type")
                    if relation_type == "text":
                        inline_tag = relation.find("inline")
                        if inline_tag is not None and inline_tag.text:
                            sql = inline_tag.text.strip()
                            # Replace line breaks with #(cr)#(lf)
                            sql_single_line = sql.replace("\r\n", "#(cr)#(lf)").replace("\n", "#(cr)#(lf)").replace("\r", "#(cr)#(lf)")
                            custom_sql_queries.append((datasource_name, sql_single_line))
                        elif relation.text and relation.text.strip():
                            sql = relation.text.strip()
                            # Replace line breaks with #(cr)#(lf)
                            sql_single_line = sql.replace("\r\n", "#(cr)#(lf)").replace("\n", "#(cr)#(lf)").replace("\r", "#(cr)#(lf)")
                            custom_sql_queries.append((datasource_name, sql_single_line))

                unique_records = []
                seen_unique_records = set()

                for sublist in custom_sql_queries:
                    t = tuple(sublist)  # convert inner list to tuple
                    if t not in seen_unique_records:
                        seen_unique_records.add(t)
                        unique_records.append(sublist)

            # Write directly to CSV
            header = ["Datasource", "CustomSQL"]
            with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(header)
                writer.writerows(unique_records)

            logging.info(f"Extracted {len(unique_records)} Custom SQL queries. Saved to {output_csv}")
        except Exception as e:
            logging.error(f"Error extracting custom SQL: {e}")

    def extract_table_columns(twb_file, output_path):
        logging.info(f"Extracting Table Columns from TWB file: {twb_file}")
        try:
            # Parse XML
            tree = ET.parse(twb_file)
            root = tree.getroot()
            records = []
            output_csv = os.path.join(output_path, "TableColumns.csv")
            
            # Namespace handling (if needed)
            ns = {}
            # Find all metadata-records
            column_metadata = root.findall(".//datasources/datasource/connection/metadata-records/metadata-record", ns)
            
            for rec in column_metadata:
                object_id = rec.findtext("object-id", "").strip('[]')
                parent_name = rec.findtext("parent-name", "").strip('[]')
                remote_name = rec.findtext("remote-name", "")
                local_type = rec.findtext("local-type", "")
                aggregation = rec.findtext("aggregation", "")

                # local_name = rec.findtext("local-name", "")
                # remote_type = rec.findtext("remote-type", "")
                # precision = rec.findtext("precision", "")
                # scale = rec.findtext("scale", "")
                # width = rec.findtext("width", "")
                # contains_null = rec.findtext("contains-null", "")
                
                # Collation (optional)
                # collation_elem = rec.find("collation")
                # collation = collation_elem.get("name") if collation_elem is not None else ""
                
                # Attributes (debug info)
                # attributes = []
                # for attr in rec.findall("attributes/attribute"):
                #     name = attr.get("name")
                #     value = attr.text
                #     attributes.append(f"{name}={value}")
                # attributes_str = "; ".join(attributes)
                
                # Object ID

                records.append([
                    object_id,
                    parent_name,
                    remote_name,
                    local_type,
                    aggregation
                ])


            # Write directly to CSV
            header = ["TableID", "TableName", "ColumnName", "DataType", "Aggregation"]
            with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(header)
                writer.writerows(records)
            col_count = len(records)
            logging.info(f"Extracted {len(records)} table columns. Saved to {output_csv}")
            return col_count 
        except Exception as e:
            logging.error(f"Error extracting table columns: {e}")

    def extract_calculated_fields(twb_file, output_path):
        logging.info(f"Extracting Calculated Fields from TWB file: {twb_file}")
        try:
            tree = ET.parse(twb_file)
            root = tree.getroot()
            output_csv = os.path.join(output_path, "CalculatedFields.csv")

            ns = {"user": "http://www.tableausoftware.com/xml/user"}
            name_to_caption = {}
            rows = []

            # --- First pass: build lookup of internal names → captions ---
            for col in root.findall(".//column", ns):
                calc = col.find("calculation", ns)
                if calc is not None:
                    internal_name = col.attrib.get("name")
                    caption = col.attrib.get("caption") or internal_name
                    clean_internal = internal_name.strip("[]")
                    clean_caption = caption.strip("[]")
                    name_to_caption[clean_internal] = clean_caption

            # --- Second pass: extract and replace formulas ---
            for ds in root.findall(".//datasource", ns):
                datasource_caption = ds.attrib.get("caption", ds.attrib.get("name", "Unknown"))

                for col in ds.findall(".//column", ns):
                    calc = col.find("calculation", ns)
                    if calc is not None:
                        internal_name = col.attrib.get("name").strip("[]")
                        caption = col.attrib.get("caption") or internal_name
                        formula = calc.attrib.get("formula")
                        role = col.attrib.get("role")
                        col_type = col.attrib.get("type")

                        fmt = col.find("default-format", ns)
                        default_format = fmt.attrib.get("format") if fmt is not None else None

                        # Replace internal names with captions in formula
                        def replace_match(match):
                            token = match.group(1)
                            return f"[{name_to_caption.get(token, token)}]"

                        formula_readable = (
                            re.sub(r"\[([^\]]+)\]", replace_match, formula) if formula else None
                        )

                        rows.append((
                            # datasource_caption,
                            internal_name,
                            caption,
                            formula_readable,
                            role,
                            col_type
                            # default_format
                        ))

            # --- Remove duplicates ---
            unique_rows = list(set(rows))

            # --- Save to CSV ---
            with open(output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    # "TableName",
                    "CalculationID",
                    "CalculationName",
                    "Expression",
                    "Role",
                    "Type"
                    # "Default Format"
                ])
                writer.writerows(unique_rows)
            cal_count = len(unique_rows)
            logging.info(f"Extracted {len(unique_rows)} calculated fields. Saved to {output_csv}")
            return cal_count
        except Exception as e:
            logging.error(f"Error extracting calculated fields: {e}")

    def clean_bracketed_name(op: Optional[str]) -> Optional[str]:
        """Normalize bracketed token:
        - turn '[AddressID (Address)]' -> '[AddressID]'
        - turn '[SalesOrderHeader].[AccountNumber]' -> '[AccountNumber]'
        - if None -> None
        """
        if not op:
            return None

        op = op.strip()
        # If it contains a dot (table.column), take last segment
        if '.' in op:
            last = op.split('.')[-1].strip()
            m = re.match(r'^\[([^\]]+)\]$', last)
            if m:
                return f'[{m.group(1)}]'
            return last

        # If single bracket with optional parenthetical: capture base name
        m = re.match(r'^\[([^\]]+?)(?:\s*\([^\)]+\))?\]$', op)
        if m:
            return f'[{m.group(1)}]'

        # fallback: return trimmed token
        return op

    def resolve_column(op: Optional[str], map_dict: dict) -> Optional[str]:
        """Resolve an op token to a cleaned bracketed column name, preferring mapped remote names."""
        if not op:
            return None

        # try exact map lookup first
        if op in map_dict:
            remote = map_dict[op]  # e.g. '[SalesOrderHeader].[AccountNumber]' or '[SalesLT].[SalesOrderHeader].[AccountNumber]'
            return clean_bracketed_name(remote)

        # try cleaned key lookup (remove parenthetical)
        cleaned_key = clean_bracketed_name(op)
        if cleaned_key in map_dict:
            remote = map_dict[cleaned_key]
            return clean_bracketed_name(remote)

        # try without brackets (some map keys might be unbracketed) 
        bare = re.sub(r'^\[|\]$', '', op or '')
        if bare in map_dict:
            return clean_bracketed_name(map_dict[bare])

        # fallback: just return cleaned op (removes the ' (Table)' suffix if present)
        return cleaned_key

    def extract_relationships_with_remote_columns(twb_file, output_path):
        logging.info(f"Extracting Relationships from TWB file: {twb_file}")

        try:
            tree = ET.parse(twb_file)
            root = tree.getroot()
            output_csv = os.path.join(output_path, "Relationships.csv")

            rows = []
            # Iterate datasources
            for ds in root.findall(".//datasource"):
                datasource_caption = ds.attrib.get("caption", ds.attrib.get("name", "Unknown"))

                # Build map dict from <cols><map key=.. value=.. />
                map_dict = {}
                cols_tag = ds.find(".//cols")
                if cols_tag is not None:
                    for m in cols_tag.findall("map"):
                        key = m.attrib.get("key")
                        val = m.attrib.get("value")
                        if key and val:
                            map_dict[key] = val
                            # also store cleaned key variant to help lookups
                            map_dict[clean_bracketed_name(key)] = val

                # Build object-id -> full table name lookup (from object's relation under properties)
                object_map = {}
                for obj in ds.findall(".//object-graph/objects/object"):
                    obj_id = obj.attrib.get("id")
                    # prefer the relation in properties context ''
                    rel = obj.find("./properties/relation")
                    if rel is None:
                        # fallback: any relation under object
                        rel = obj.find(".//relation")
                    table_name = rel.attrib.get("table") if rel is not None else obj.attrib.get("caption")
                    object_map[obj_id] = table_name

                # Walk relationships
                for rel in ds.findall(".//object-graph/relationships/relationship"):
                    # endpoints
                    first_ep = rel.find("./first-end-point")
                    second_ep = rel.find("./second-end-point")
                    left_obj = first_ep.get("object-id") if first_ep is not None else None
                    right_obj = second_ep.get("object-id") if second_ep is not None else None
                    left_table = object_map.get(left_obj, left_obj)
                    right_table = object_map.get(right_obj, right_obj)

                    # nested expressions: the actual key expressions sit under expression/expression
                    exprs = rel.findall("./expression/expression")
                    left_raw = exprs[0].attrib.get("op") if len(exprs) > 0 else None
                    right_raw = exprs[1].attrib.get("op") if len(exprs) > 1 else None

                    # Resolve to remote column or clean bracketed name
                    left_col = resolve_column(left_raw, map_dict)
                    right_col = resolve_column(right_raw, map_dict)

                    # Get Table IDs
                    left_table_id = first_ep.attrib.get("object-id")
                    right_table_id = second_ep.attrib.get("object-id")

                    # Determine cardinality
                    left_unique = first_ep is not None and (
                        first_ep.attrib.get("unique-key") == "true" or first_ep.attrib.get("is-db-set-unique-key") == "true"
                    )
                    right_unique = second_ep is not None and (
                        second_ep.attrib.get("unique-key") == "true" or second_ep.attrib.get("is-db-set-unique-key") == "true"
                    )

                    if left_unique and right_unique:
                        cardinality = "one-to-one"
                    elif left_unique and not right_unique:
                        cardinality = "one-to-many"
                    elif not left_unique and right_unique:
                        cardinality = "many-to-one"
                    else:
                        cardinality = "many-to-many"

                    rows.append([
                        datasource_caption,
                        left_table_id,
                        right_table_id,
                        left_table,
                        right_table,
                        left_col,
                        right_col,
                        cardinality
                    ])

            # write CSV
            with open(output_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["DataSourceCaption", "LeftTableID", "RightTableID", "LeftTable", "RightTable", "LeftColumn", "RightColumn", "Cardinality"])
                writer.writerows(rows)
            rel_count = len(rows)
            logging.info(f"Extracted {rel_count} relationships. Saved to {output_csv}")
            return rel_count
        except Exception as e:
            logging.error(f"Error extracting relationships: {e}")

    def extract_parameters(twb_file, output_path):
        logging.info(f"Extracting Parameters from TWB file: {twb_file}")
        try:
            tree = ET.parse(twb_file)
            root = tree.getroot()
            output_csv = os.path.join(output_path, "Parameters.csv")

            tree = ET.parse(twb_file)
            root = tree.getroot()

            parameters_list = []

            # Find the datasource named 'Parameters'
            for datasource in root.findall(".//datasource[@name='Parameters']"):
                for column in datasource.findall('column'):
                    name = column.get('name').strip('[]')
                    caption = column.get('caption').strip('[]')
                    datatype = column.get('datatype')
                    domain_type = column.get('param-domain-type')
                    default_value = column.get('value')

                    # If range exists
                    range_tag = column.find('range')
                    if range_tag is not None:
                        range_min = range_tag.get('min')
                        range_max = range_tag.get('max')
                    else:
                        range_min = ''
                        range_max = ''

                    # If list members exist
                    members_tag = column.find('members')
                    if members_tag is not None:
                        # Join members into a single string separated by semicolon
                        members = '; '.join([m.get('value') for m in members_tag.findall('member')])
                    else:
                        members = ''

                    parameters_list.append([name, caption, datatype, domain_type, default_value, range_min, range_max, members])

            # Write to CSV
            csv_columns = ['ParameterID', 'ParameterName', 'DataType', 'DomainType', 'DefaultValue', 'RangeMin', 'RangeMax', 'Members']

            with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(csv_columns)
                for param in parameters_list:
                    writer.writerow(param)

            logging.info(f"Extracted {len(parameters_list)} parameters. Saved to {output_csv}")
        except Exception as e:
            logging.error(f"Error extracting parameters: {e}")

    def extract_worksheets(twb_file, output_path):
        logging.info(f"Extracting Worksheets from TWB file: {twb_file}")
        try:
            tree = ET.parse(twb_file)
            root = tree.getroot()
            output_csv = os.path.join(output_path, "Worksheets.csv")

            tree = ET.parse(twb_file)
            root = tree.getroot()
            
            worksheets = []

            # Iterate through all worksheet tags
            for worksheet in root.findall(".//worksheet"):
                ws = {}
                simple_id_tag = worksheet.find('simple-id')
                ws['WorksheetID'] = simple_id_tag.get('uuid') if simple_id_tag is not None else ''
                ws['WorksheetName'] = worksheet.get('name')
                worksheets.append(ws)

            # Write to CSV
            csv_columns = ['WorksheetID', 'WorksheetName']
            with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for ws in worksheets:
                    writer.writerow(ws)
            worksheets_count = len(worksheets)
            logging.info(f"Extracted {worksheets_count} worksheets. Saved to {output_csv}")
            return worksheets_count
        except Exception as e:
            logging.error(f"Error extracting worksheets: {e}")

    ############### Main Metadata Extraction Call #################
    def generate_metadata(report_file_path, metadata_path):
        extract_connections(report_file_path, metadata_path)
        row_count = extract_tables(report_file_path, metadata_path)
        col_count = extract_table_columns(report_file_path, metadata_path)
        cal_count = extract_calculated_fields(report_file_path, metadata_path)
        rel_count = extract_relationships_with_remote_columns(report_file_path, metadata_path)
        extract_parameters(report_file_path, metadata_path)
        worksheets_count = extract_worksheets(report_file_path, metadata_path)
        return row_count, col_count, cal_count, rel_count, worksheets_count
    ############### End of Metadata Extraction Call #################

    TYPE_MAPPING = {
        "Text": "string",
        "string": "string",
        "Numeric": "decimal",
        "Integer": "int64",
        "integer": "int64",
        "Date": "dateTime",
        "datetime": "dateTime",
        "Time": "time",
        "Timestamp": "datetime",
        "boolean": "boolean",
        "real": "decimal"
    }

    def remove_readonly(func, path, excinfo):
        # Change the file to writable and then retry
        os.chmod(path, stat.S_IWRITE)
        func(path)
    
    def create_powerbi_project(input_folder, output_folder):
        # Copy template files into new folder and rename all the files and folders
        logging.info(f"Creating Power BI project in: {output_folder}")
        try:
            #This is Temp Code Remove it after debugging
            if os.path.exists(output_folder):
                shutil.rmtree(output_folder, onerror=remove_readonly)

            if os.path.exists(output_folder):
                # print(f"{output_folder} already exists, skipping.")
                exit()
            else:
                shutil.copytree(input_folder, output_folder)
            logging.info(f"Copied template files to {output_folder}")

        except Exception as e:
            logging.error(f"Error copying template files: {e}")

    def create_database_file(report_directory, compatibility_level):
        logging.info(f"Creating database.tmdl in: {report_directory}")
        try:
            tmdl_file_path = f'{report_directory}/TemplateReport.SemanticModel/definition/database.tmdl'

            # Write TMDL file
            with open(tmdl_file_path, 'w') as f:
                # f.write(f"database '{report_name}'\n")
                f.write(f"database TemplateReport\n")
                f.write(f"	compatibilityLevel: {compatibility_level}")
                f.write("\n")

            logging.info(f"Created database.tmdl file: {tmdl_file_path}")
        except Exception as e:
            logging.error(f"Error creating database.tmdl: {e}")

    def create_table_files(metadata_directory, report_directory):
        logging.info(f"Creating table .tmdl files in: {report_directory}/TemplateReport.SemanticModel/tables/")
        
        try:
            tables = {}
            connections_csv_file = metadata_directory + '/Connections.csv'
            table_csv_file = metadata_directory + '/Tables.csv'
            columns_csv_file = metadata_directory + '/TableColumns.csv'

            with open(connections_csv_file, newline='') as f:
                reader_conn = csv.DictReader(f)
                rows_conn = list(reader_conn)

            with open(table_csv_file, newline='') as f:
                reader_table = csv.DictReader(f)
                rows_table = list(reader_table)

            with open(columns_csv_file, newline='') as f:
                reader_columns = csv.DictReader(f)
                rows_columns = list(reader_columns)

            # print("rows_table", rows_table)
            # rows_table [{'DataSourceID': 'federated.1lbhjox1trszhc16vfrnw081qevh', 'TableID': 'Address (SalesLT.Address)_AAFA9EB605554B69B6CEDE84FEA1881C', 'ConnectionID': 'azure_sqldb.01lrtjm1pdl3uy16yoyyt1sf0pa9', 'PresentationTableName': 'Address', 'SchemaName': 'SalesLT', 'TableName': 'Address', 'TableType': 'table', 'CustomSQL': ''}, {'DataSourceID': 'federated.1lbhjox1trszhc16vfrnw081qevh', 'TableID': 'Customer (SalesLT.Customer)_C300D610ABE6454EAEE7553CD01C0282', 'ConnectionID': 'azure_sqldb.01lrtjm1pdl3uy16yoyyt1sf0pa9', 'PresentationTableName': 'Customer', 'SchemaName': 'SalesLT', 'TableName': 'Customer', 'TableType': 'table', 'CustomSQL': ''}, {'DataSourceID': 'federated.1lbhjox1trszhc16vfrnw081qevh', 'TableID': 'CustomerAddress (SalesLT.CustomerAddress)_1FD0F0C15B844CDEA0EE60F8D3984AA7', 'ConnectionID': 'azure_sqldb.01lrtjm1pdl3uy16yoyyt1sf0pa9', 'PresentationTableName': 'CustomerAddress', 'SchemaName': 'SalesLT', 'TableName': 'CustomerAddress', 'TableType': 'table', 'CustomSQL': ''}, {'DataSourceID': 'federated.1lbhjox1trszhc16vfrnw081qevh', 'TableID': 'Product (SalesLT.Product)_A2E0CC90AC4B46A8B80BB995741F1341', 'ConnectionID': 'azure_sqldb.01lrtjm1pdl3uy16yoyyt1sf0pa9', 'PresentationTableName': 'Product', 'SchemaName': 'SalesLT', 'TableName': 'Product', 'TableType': 'table', 'CustomSQL': ''}, {'DataSourceID': 'federated.1lbhjox1trszhc16vfrnw081qevh', 'TableID': 'ProductCategory (SalesLT.ProductCategory)_F0309C97C6A14A34A22FC0E93A963A41', 'ConnectionID': 'azure_sqldb.01lrtjm1pdl3uy16yoyyt1sf0pa9', 'PresentationTableName': 'ProductCategory', 'SchemaName': 'SalesLT', 'TableName': 'ProductCategory', 'TableType': 'table', 'CustomSQL': ''}, {'DataSourceID': 'federated.1lbhjox1trszhc16vfrnw081qevh', 'TableID': 'SalesOrderDetail (SalesLT.SalesOrderDetail)_C590C06B732C46C6A287A620CA51058A', 'ConnectionID': 'azure_sqldb.01lrtjm1pdl3uy16yoyyt1sf0pa9', 'PresentationTableName': 'SalesOrderDetail', 'SchemaName': 'SalesLT', 'TableName': 'SalesOrderDetail', 'TableType': 'table', 'CustomSQL': ''}, {'DataSourceID': 'federated.1lbhjox1trszhc16vfrnw081qevh', 'TableID': 'SalesOrderHeader (SalesLT.SalesOrderHeader)_2B97B89F8A5F46A99C109317B478DF90', 'ConnectionID': 'azure_sqldb.01lrtjm1pdl3uy16yoyyt1sf0pa9', 'PresentationTableName': 'SalesOrderHeader', 'SchemaName': 'SalesLT', 'TableName': 'SalesOrderHeader', 'TableType': 'table', 'CustomSQL': ''}]
            
            for tbl in rows_table:

                server_name = next(iter({item["ServerName"] for item in rows_conn if item["ConnectionID"] == tbl['ConnectionID'] and item["DataSourceID"] == tbl['DataSourceID']}))
                database_name = next(iter({item["DatabaseName"] for item in rows_conn if item["ConnectionID"] == tbl['ConnectionID'] and item["DataSourceID"] == tbl['DataSourceID']}))
                # print("server_name", server_name)
                # print("database_name", database_name)

                schema_name = tbl['SchemaName'].strip()
                table_id = tbl['TableID'].strip()
                table_name = tbl['PresentationTableName'].strip()
                custom_sql = tbl['CustomSQL'].strip()

                # print("tbl", custom_sql)

                for row in rows_columns:
                    if row['TableID'] == table_id:
                        table = table_name
                        column = row['ColumnName'].strip()
                        datatype = TYPE_MAPPING[row['DataType'].strip()] # Add DataType conversion code here, example: Integer -> int64, Text -> string, etc.. Madhu to look later
                        # print("DEBUG", schema, table, column, datatype)

                        if table not in tables:
                            tables[table] = []
                        tables[table].append({'ColumnName': column, 'DataType': datatype})
                        # print('Schema', schema, 'ColumnName', column, 'DataType', datatype)

                tmdl_file_path = f'{report_directory}/TemplateReport.SemanticModel/definition/tables/{table_name}.tmdl'

                # print("tables.items()", tables.items())
                # Write TMDL file
                with open(tmdl_file_path, 'w') as f:
                    for tbls, cols in tables.items():
                        # print("tables-11", schema, table, columns)
                        if tbls == table:
                            f.write(f"table  '{table}'\n")
                            for col in cols:
                                f.write(f"	column '{col['ColumnName']}'\n")
                                f.write(f"		dataType: {col['DataType']}\n")
                                # f.write(f"    formatString: Long Date\n") #To format the column
                                # f.write(f"    isHidden\n") #To hide the column
                                f.write(f"		dataCategory: {col['ColumnName']}\n")
                                f.write(f"		summarizeBy: none\n")
                                f.write(f"		sourceColumn: {col['ColumnName']}\n")
                                f.write("\n")
                                f.write(f"		annotation SummarizationSetBy = Automatic\n")
                                # f.write("\n")
                                # f.write(f'    annotation PBI_ChangedProperties = ["IsHidden"]\n')
                                # f.write("\n")
                                # f.write(f'    annotation UnderlyingDateTimeDataType = Date\n') #Applicable to Date Tables
                                # f.write("\n")
                                # f.write(f'    annotation PBI_FormatHint = {"currencyCulture":"en-GB"}\n') #Applicable to Date Tables
                            f.write("\n")

                            # f.write("hierarchy Geography\n")
                            f.write(f"	partition '{table}' = m\n")
                            f.write("		mode: import\n")
                            f.write("		source =\n")
                            f.write("				let\n")
                            f.write(f'				    CustomSQL = "{custom_sql}",\n')
                            if custom_sql == "":
                                f.write(f'				    Source = Sql.Database("{server_name}", "{database_name}"),\n')
                                f.write(f'				    Data = Source{{[Schema="{schema_name}",Item="{table}"]}}[Data]\n')
                            else:
                                f.write(f'				    Data = Sql.Database("{server_name}", "{database_name}", [Query=CustomSQL])\n')
                            f.write("				in\n")
                            f.write("				    Data\n")
                            f.write("\n")

                            f.write("	annotation PBI_ResultType = Table\n")
            
            logging.info(f"Created {len(tables)} table .tmdl files in: {report_directory}/TemplateReport.SemanticModel/tables/")
        except Exception as e:
            logging.error(f"Error creating table .tmdl files: {e}")

    def create_model_file(metadata_directory, report_directory):
        logging.info(f"Creating model.tmdl in: {report_directory}")
        try:
            table_csv_file = metadata_directory + '/Tables.csv'

            with open(table_csv_file, newline='') as f:
                reader_table = csv.DictReader(f)
                rows_table = list(reader_table)

            # Extract unique table names
            tables = list({item["PresentationTableName"] for item in rows_table})

            # report_directory += '/' + report_name
            tmdl_file_path = f'{report_directory}/TemplateReport.SemanticModel/definition/model.tmdl'
            with open(tmdl_file_path, 'w') as f:
                f.write(f"model Model\n")
                f.write(f"	culture: en-US\n")
                f.write(f"	defaultPowerBIDataSourceVersion: powerBI_V3\n")
                f.write(f"	sourceQueryCulture: en-US\n")
                f.write(f"	dataAccessOptions\n")
                f.write(f"		legacyRedirects\n")
                f.write(f"		returnErrorValuesAsNull\n")
                f.write("\n")

                f.write(f"annotation PBI_QueryOrder = {str(tables).replace("'", '"')}\n")
                f.write("\n")

                f.write(f"annotation __PBI_TimeIntelligenceEnabled = 1\n")
                f.write("\n")

                f.write(f"annotation PBIDesktopVersion = 2.146.705.0 (25.08)+b71502aff0a01340c88f384884ef42771068f115\n")
                f.write("\n")

                f.write(f'annotation PBI_ProTooling = ["DevMode"]\n')
                f.write("\n")

                for tbl in tables:
                    f.write(f"ref table '{tbl}'\n")
                f.write("\n")

                f.write(f"ref cultureInfo en-US\n")
                f.write("\n")
            logging.info(f"Created model.tmdl file: {tmdl_file_path}")
        except Exception as e:
            logging.error(f"Error creating model.tmdl: {e}")

    def write_relationship_tmdl(left_table, left_col, right_table, right_col, cardinality):
        rel_id = str(uuid.uuid4())
        
        tmdl = f"relationship {rel_id}\n"
        
        # Map cardinality to crossFilteringBehavior
        if cardinality in ["one-to-one", "many-to-many"]:
            tmdl += "    crossFilteringBehavior: bothDirections\n"
        # For one-to-many or many-to-one, TMDL default is single direction
        # You can adjust direction by swapping from/to columns
        if cardinality == "one-to-many":
            left_table, right_table = right_table, left_table
            left_col, right_col = right_col, left_col
        
        tmdl += f"    fromColumn: '{left_table}'.{left_col}\n"
        tmdl += f"    toColumn: '{right_table}'.{right_col}\n\n"
        
        return tmdl

    def create_relationships_file(metadata_directory, report_directory):
        logging.info(f"Creating relationships.tmdl in: {report_directory}")
        try:
            csv_file = metadata_directory + '/Relationships.csv'
            table_csv_file = metadata_directory + '/Tables.csv'

            rows = []
            relationships = []

            with open(table_csv_file, newline='') as f:
                reader_table = csv.DictReader(f)
                rows_table = list(reader_table)

            with open(csv_file, newline='') as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            for row in rows:
                from_table = next(iter({item["PresentationTableName"] for item in rows_table if item["TableID"] == row['LeftTableID']}))
                to_table = next(iter({item["PresentationTableName"] for item in rows_table if item["TableID"] == row['RightTableID']}))
                # print("server_name", server_name)
                # print("database_name", database_name)

                # from_table = row['LeftTable'].split(".")[-1].strip('[]')
                from_column = row['LeftColumn'].strip('[]')
                # to_table = row['RightTable'].split(".")[-1].strip('[]')
                to_column = row['RightColumn'].strip('[]')
                cardinality = row['Cardinality'].strip()
                relationships.append({'from_table': from_table, 'from_column': from_column, 'to_table': to_table, 'to_column': to_column, 'cardinality': cardinality})
            
            # print("relationships", relationships)

            tmdl_file_path = f'{report_directory}/TemplateReport.SemanticModel/definition/relationships.tmdl'

            with open(tmdl_file_path, "w") as f:
                for rel in relationships:
                    tmdl_text = write_relationship_tmdl(
                        left_table=rel['from_table'],
                        left_col=rel['from_column'], 
                        right_table=rel['to_table'], 
                        right_col=rel['to_column'], 
                        cardinality=rel['cardinality']
                    )
                    f.write(tmdl_text)
            logging.info(f"Created relationships.tmdl file: {tmdl_file_path}")
        except Exception as e:
            logging.error(f"Error creating relationships.tmdl: {e}")


    ##### Generate Big Bang String for Agentic AI's Calls #####
    def _read_csv_flex(path: Path) -> pd.DataFrame:
        if not path or not Path(path).expanduser().exists():
            raise FileNotFoundError(f"File not found: {path}")

        encodings_to_try = ("utf-8-sig", "utf-8", "cp1252")
        last_err = None
        for enc in encodings_to_try:
            try:
                return pd.read_csv(
                    Path(path).expanduser(),
                    encoding=enc,
                    dtype=str,
                    keep_default_na=False,
                    na_filter=False,
                    low_memory=False
                )
            except Exception as e:
                last_err = e
        raise RuntimeError(f"Failed to read {path} with encodings {encodings_to_try}. "
                           f"Last error: {last_err}")

    def csvs_to_bigbang_string(tables_csv_path: str, calculations_csv_path: str) -> str:
        """
        Read the two CSVs and return one concatenated string for AI Agent prompt.
        Each dataset is tagged with a header for clarity.
        """
        logging.info(f"Reading CSVs for Big Bang string:\n Tables: {tables_csv_path}\n Calculations: {calculations_csv_path}")
        try:
            tables_df = _read_csv_flex(tables_csv_path)
            calcs_df  = _read_csv_flex(calculations_csv_path)

            tables_df.columns = [c.strip() for c in tables_df.columns]
            calcs_df.columns  = [c.strip() for c in calcs_df.columns]

            # Convert to string (pipe-separated for readability)
            tables_str = tables_df.to_csv(index=False, sep="|")
            calcs_str  = calcs_df.to_csv(index=False, sep="|")

            # Big bang concatenated string
            bigbang_prompt = (
                "=== TABLES METADATA ===\n"
                f"{tables_str}\n\n"
                "=== CALCULATIONS METADATA ===\n"
                f"{calcs_str}"
            )
            logging.info("Successfully created Big Bang string for AI Agent.")
            return bigbang_prompt
        except Exception as e:
            logging.error(f"Error creating Big Bang string: {e}")


    # --- Example usage ---
    # bigbang_string = csvs_to_bigbang_string(
    #     r"D:\Projects\...\Tables.csv",
    #     r"D:\Projects\...\Calculations.csv"
    # )
    # print(bigbang_string[:1000])   # Print first 1000 chars for preview


    def send_tables_calcs_to_agent_and_save(
        tables_csv_path: str,
        calculations_csv_path: str,
        output_csv_path: str,
        project_endpoint: str,
        output_dir : str,
        agent_id: str,
        max_tokens: int = 30000
    ):
        logging.info("Sending tables and calculations metadata to AI Agent...")
        try:
            #account_url = f"https://{account_name}.blob.core.windows.net"
            credential = DefaultAzureCredential()
            #blob_service_client = BlobServiceClient(account_url=account_url, credential=account_key)
            #input_container = blob_service_client.get_container_client(container_name)

            project_client = AIProjectClient(credential=credential, endpoint=project_endpoint)
            agent = project_client.agents.get_agent(agent_id)
            thread = project_client.agents.threads.create()
            # print(f"🧵 Created thread: {thread.id}")
            logging.info(f"🧵 Created Thread: {thread.id} Using Agent: {agent.name} (ID: {agent.id})")  

            user_prompt = csvs_to_bigbang_string(tables_csv_path, calculations_csv_path)

            message = project_client.agents.messages.create(
                            thread_id=thread.id,
                            role=MessageRole.USER,
                            content=user_prompt
                        )

                    #run = project_client.agents.runs.create(thread_id=thread.id, agent_id=agent.id)
                    #run = project_client.agents.runs.wait(thread_id=thread.id, run_id=run.id)
                
            run = project_client.agents.runs.create_and_process(
                            thread_id=thread.id,
                            agent_id=agent.id)
                
            if run.status == "failed":
                error_msg = run.last_error or {"message": "Unknown agent failure"}
                print(f"❌ Run failed: {error_msg}")
                logging.info(f"❌ Run failed: {error_msg}")
                
                    
                if "rate_limit" in str(error_msg).lower():
                    print("🕒 Rate limit hit. Waiting 90 seconds before retrying...")
                    logging.info("🕒 Rate limit hit. Waiting 90 seconds before retrying...")
                    time.sleep(90)  # Wait longer than normal
                raise RuntimeError(f"Agent run failed: {error_msg}")

                    # Get response and extract PowerBIVisualTemplateName
            first_response = project_client.agents.messages.get_last_message_text_by_role(thread_id=thread.id, role=MessageRole.AGENT)
            first_text = first_response.text.value if first_response else ""
            print("🔍 First Agent Response Received")
            logging.info("🔍 First Agent Response Received")

            if not output_csv_path:
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                #output_dir = "D:/Projects/Solution Harvesting/Visual Creator Automation/GeneratePBIP_Project/Metadata/TableauSrcPBIP_Files/Reports/Customer Analysis/"  
                #output_csv_path = os.path.join(output_dir, f"agent_response_{stamp}.csv")  
                output_csv_path = os.path.join(output_dir, f"AgentResponseCalculations.csv")  
                #output_csv_path = f"D:/Projects/Solution Harvesting/Visual Creator Automation/GeneratePBIP_Project/Metadata/TableauSrcPBIP_Files/Reports/Customer Analysis/agent_response_{stamp}.csv"

            #pd.DataFrame({first_text}).to_csv(output_csv_path, index=False,header=False)
            with open(output_csv_path, "w", encoding="utf-8") as f:f.write(first_text)

            # Delete the thread after CSV creation  
            project_client.agents.threads.delete(thread.id)  
            print(f"🧹 Deleted thread: {thread.id}") 
            logging.info(f"🧹 Deleted Thread: {thread.id}")
            logging.info(f"Agent response saved to: {output_csv_path}")
            return output_csv_path
        except Exception as e:
            logging.error(f"Error sending metadata to AI Agent: {e}")

    def create_measure_file(metadata_directory, report_directory):
        logging.info(f"Creating Metrics.tmdl in: {report_directory}/TemplateReport.SemanticModel/definition/tables/")

        try:
            csv_file = metadata_directory + '/AgentResponseCalculations.csv'
            # report_directory += '/' + report_name

            filtered_rows = []

            with open(csv_file, encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                # filtered_rows = [row for row in reader if row['ReportName'] == report_name and row['ColumnType'] == "Measures" ]
                filtered_rows = list(reader)
            # `filtered_rows` is now a list of dictionaries
            # print("filtered_rows", filtered_rows)

            tmdl_file_path = f'{report_directory}/TemplateReport.SemanticModel/definition/tables/Metrics.tmdl'

            # Write TMDL file
            with open(tmdl_file_path, 'w') as f:
                f.write(f"table Metrics\n")
                f.write(f"	lineageTag: {str(uuid.uuid4())}\n")
                f.write("\n")

                for row in filtered_rows:
                    # viz_id = row['VisualID'].strip()[:5] #Get first 5 characters
                    # column_index = row['ColumnIndex']
                    title = row['CalculationName']
                    expression = row['Expression']
                    DAX_expression = row['DAX_expression']
                    guid = str(uuid.uuid4()) # generate a random GUID (UUID4)
                
                    #measure_name = viz_id + '_' + column_index + '_' + title #Keeping measure name unique
                    measure_name = title #Keeping measure name unique
                    # measure_name = measure_name.replace("'", "").replace("=", "")
                    measure_name = measure_name.replace("'", "").replace("=", "")
                    # print("DEBUG", table, column, datatype)
                    # 🔑 Remove all newlines / carriage returns / tabs and collapse spaces
                    # DAX_expression = " ".join(DAX_expression)

                    f.write(f"	measure '{measure_name}' = {DAX_expression}\n")
                    # f.write(f"		formatString: \$#,0.###############;(\$#,0.###############);\$#,0.###############\n")
                    f.write(f"		lineageTag: {guid}\n")
                    f.write("\n")

                f.write(f"	column Column\n")
                f.write(f"		isHidden\n")
                f.write(f"		formatString: 0\n")
                f.write(f"		lineageTag: {str(uuid.uuid4())}\n")
                f.write(f"		summarizeBy: sum\n")
                f.write(f"		isNameInferred\n")
                f.write(f"		sourceColumn: [Column]\n")
                f.write("\n")

                f.write(f"		annotation SummarizationSetBy = Automatic\n")
                f.write("\n")

                f.write(f"	partition Metrics = calculated\n")
                f.write(f"		mode: import\n")
                f.write(f'		source = Row("Column", BLANK())\n')
                f.write(f"\n")
                f.write(f"	annotation PBI_Id = 776fbf18a13d472392796cf4c7b18af5\n")
                f.write(f"\n")
                f.write(f'	annotation {str(uuid.uuid4())} = {{"Expression":""}}\n')
                f.write(f"\n")
            logging.info(f"Created Metrics.tmdl file: {tmdl_file_path}")
        except Exception as e:
            logging.error(f"Error creating Metrics.tmdl: {e}")

    ############### Main Power BI Report Generation Call #################
    def generate_report():
        create_powerbi_project(TEMPLATE_PATH, OUTPUT_PATH)
        create_database_file(OUTPUT_PATH, '1600')
        
        create_table_files(METADATA_PATH, OUTPUT_PATH)
        create_model_file(METADATA_PATH, OUTPUT_PATH)
        create_relationships_file(METADATA_PATH, OUTPUT_PATH)

        send_tables_calcs_to_agent_and_save(f"{METADATA_PATH}/TableColumns.csv",f"{METADATA_PATH}/CalculatedFields.csv",None,PROJECT_ENDPOINT,METADATA_PATH,AGENT_ID,)
        create_measure_file(METADATA_PATH, OUTPUT_PATH)


    # create_powerbi_project('D:\TMDL POC\Template File', 'D:\TMDL POC\Output', 'Harm Prevention Performance Metrics')
    summary_logger.info(f"  Semantic Model Migration process started.")
    row_count, col_count, cal_count, rel_count, worksheets_count = generate_metadata(REPORT_FILE_PATH, METADATA_PATH)
    generate_report()
    summary_logger.info(f"      Extracted {row_count} tables. ")
    summary_logger.info(f"      Extracted {col_count} table columns. ")
    summary_logger.info(f"      Extracted {cal_count} unique calculated fields. ")
    summary_logger.info(f"      Extracted {rel_count} unique relationships. ")
    summary_logger.info(f"      Extracted {worksheets_count} unique worksheets. ")
    summary_logger.info(f"  Semantic Model Migration process completed.")
    # Release the summary_logger file handle
    for handler in summary_logger.handlers:
        handler.close()
        summary_logger.removeHandler(handler)

if __name__ == "__main__":
    main()
