"""Auto-generated from Create Power BI Project From TWB File.ipynb"""


import sys
import os
import csv
import logging
import re
import xml.etree.ElementTree as ET
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config_loader import load_config

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
            return len(rows)
        except Exception as e:
            logging.error(f"Error extracting connections: {e}")
            return 0

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
            return len(parameters_list)
        except Exception as e:
            logging.error(f"Error extracting parameters: {e}")
            return 0

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
        conn_count = extract_connections(report_file_path, metadata_path)
        row_count = extract_tables(report_file_path, metadata_path)
        col_count = extract_table_columns(report_file_path, metadata_path)
        cal_count = extract_calculated_fields(report_file_path, metadata_path)
        rel_count = extract_relationships_with_remote_columns(report_file_path, metadata_path)
        param_count = extract_parameters(report_file_path, metadata_path)
        worksheets_count = extract_worksheets(report_file_path, metadata_path)
        return conn_count, row_count, col_count, cal_count, rel_count, param_count, worksheets_count
    ############### End of Metadata Extraction Call #################

    # Create GeneratedMetadata folder under Metadata
    # Place GeneratedMetadata under the main metadata path for the report type
    main_metadata_folder = os.path.dirname(METADATA_PATH)
    consolidated_metadata_path = os.path.join(main_metadata_folder, "GeneratedMetadata")
    os.makedirs(consolidated_metadata_path, exist_ok=True)

    # Get all files in INPUT_PATH/REPORT_TYPE
    input_folder = os.path.join(INPUT_PATH, REPORT_TYPE)
    # Initialize overall counters
    total_conn_count = 0
    total_row_count = 0
    total_col_count = 0
    total_cal_count = 0
    total_rel_count = 0
    total_param_count = 0
    total_worksheets_count = 0

    for file_name in os.listdir(input_folder):
        file_path = os.path.join(input_folder, file_name)
        # Only process .twb files
        if os.path.isfile(file_path) and file_name.lower().endswith('.twb'):
            # Use file name (without extension) as subfolder name
            base_name = os.path.splitext(file_name)[0]
            output_folder = os.path.join(consolidated_metadata_path, base_name)
            os.makedirs(output_folder, exist_ok=True)
            logging.info(f"Processing {file_path} -> {output_folder}")

            conn_count, row_count, col_count, cal_count, rel_count, param_count, worksheets_count = generate_metadata(file_path, output_folder)
            # print(f"Report: {base_name}")
            # print(f"  Connections: {conn_count}")
            # print(f"  Tables: {row_count}")
            # print(f"  Columns: {col_count}")
            # print(f"  Calculated Fields: {cal_count}")
            # print(f"  Relationships: {rel_count}")
            # print(f"  Parameters: {param_count}")
            # print(f"  Worksheets: {worksheets_count}")

            total_conn_count += conn_count if conn_count else 0
            total_row_count += row_count if row_count else 0
            total_col_count += col_count if col_count else 0
            total_cal_count += cal_count if cal_count else 0
            total_rel_count += rel_count if rel_count else 0
            total_param_count += param_count if param_count else 0
            total_worksheets_count += worksheets_count if worksheets_count else 0

    print("\n=== Overall Totals ===")
    print(f"Total Connections: {total_conn_count}")
    print(f"Total Tables: {total_row_count}")
    print(f"Total Columns: {total_col_count}")
    print(f"Total Calculated Fields: {total_cal_count}")
    print(f"Total Relationships: {total_rel_count}")
    print(f"Total Parameters: {total_param_count}")
    print(f"Total Worksheets: {total_worksheets_count}")

    logging.info('Model Migration process completed.')
    summary_logger.info(f"Migration completed.")

if __name__ == "__main__":
    main()



