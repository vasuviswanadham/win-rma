"""Auto-generated from VisualMigrator_LocalPath_ReadXML_GenerateChunks.ipynb"""

from python_scripts.config_loader import load_config
from datetime import datetime
import logging

def main():
    import os
    from pathlib import Path
    import shutil

    config = load_config()
    today_str = datetime.now().strftime('%Y-%m-%d')

    INPUT_PATH = config["paths"]["input_path"]
    OUTPUT_PATH = config["paths"]["output_path"]
    VISUALSOUTPUT_PATH = config["paths"]["visualoutput_path"]
    LOG_PATH = f'{config["paths"]["log_path"]}/{config["report"]["type"]}/{today_str}/{config["report"]["name"]}'
    TEMPLATE_PATH = config["paths"]["template_path"]
    METADATA_PATH = config["paths"]["metadata_path"]
    REPORT_TYPE = config["report"]["type"]
    REPORT_NAME = config["report"]["name"]
    CONVERTED_XML_PATH = f"{INPUT_PATH}/{REPORT_TYPE}/ConvertedXML"
    VISUALS_EXTRACTOR_OUTPUT = f"{VISUALSOUTPUT_PATH}"
    TABLES_METADATA_OUTPUT = f"{VISUALS_EXTRACTOR_OUTPUT}/{REPORT_NAME}/extracted_dashboards/tables_metadata_output.csv"
    JSON_OUTPUT_PATH = f"{VISUALS_EXTRACTOR_OUTPUT}/{REPORT_NAME}/json"

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
    summary_logger.info(f"  Visual Extraction process started...")
    logging.info('Visual Extraction process started.')

    # Step 1: Extract dashboards individually
    def twb_to_xml(twb_path: str, output_dir: str = None) -> str:
        """
        Reads a Tableau .twb file (which is XML) and saves it as an .xml file.
    
        Args:
            twb_path (str): Path to the .twb file
            output_dir (str, optional): Directory to save the .xml file. 
                                        If None, saves next to the .twb file.
    
        Returns:
            str: Path of the saved .xml file
        """
        twb_file = Path(twb_path)
        if not twb_file.exists():
            raise FileNotFoundError(f"TWB file not found: {twb_path}")
    
        # Define output directory
        if output_dir is None:
            output_dir = twb_file.parent
        else:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
    
        xml_path = output_dir / f"{twb_file.stem}.xml"
    
        # Since TWB is already XML, just copy it
        shutil.copyfile(twb_file, xml_path)
    
        return str(xml_path)





    import xml.etree.ElementTree as ET
    from pathlib import Path
    import re, io, os
    from transformers import GPT2TokenizerFast

    # --- Step 1: Extract dashboards separately ---
    def extract_and_save_dashboards(local_xml_path: str, output_dir: str, report_name: str):
        """
        Reads Tableau XML locally, extracts each dashboard (excl. device layouts),
        and saves as individual extracted.xml files under dashboard folders.
        """
        report_dir = Path(output_dir) / report_name / "extracted_dashboards"
        report_dir.mkdir(parents=True, exist_ok=True)

        tree = ET.parse(local_xml_path)
        root = tree.getroot()

        dashboards = root.find('dashboards')
        worksheets = root.find('worksheets')
        windows = root.find('windows')   # ✅ include windows

        saved_files = []

        if dashboards is not None:
            for dashboard in dashboards.findall('dashboard'):
                # remove <devicelayouts>
                for devicelayouts in dashboard.findall('devicelayouts'):
                    dashboard.remove(devicelayouts)

                # dashboard name
                dash_name = dashboard.get("name", "UnnamedDashboard")

                # create folder per dashboard
                dash_dir = report_dir / dash_name
                dash_dir.mkdir(parents=True, exist_ok=True)

                # new root with dashboard + worksheets + windows
                new_root = ET.Element("extracted")
                new_root.append(dashboard)
                if worksheets is not None:
                    new_root.append(worksheets)
                if windows is not None:
                    new_root.append(windows)

                # save extracted.xml
                output_file = dash_dir / "extracted.xml"
                new_tree = ET.ElementTree(new_root)
                new_tree.write(output_file, encoding="utf-8", xml_declaration=True)
                saved_files.append(str(output_file))

                print(f"📂 Extracted dashboard {dash_name} → {output_file}")

        return saved_files


    # --- Step 2: Extract worksheet names ---
    def extract_worksheet_names(xml_text):
        worksheet_names = set()

        # Normal worksheets
        worksheet_names.update(
            re.findall(r"<worksheet[^>]*name=['\"]([^'\"]+)['\"]", xml_text)
        )

        # LOD worksheets inside <window class="worksheet">
        worksheet_names.update(
            re.findall(r"<window[^>]*class=['\"]worksheet['\"][^>]*name=['\"]([^'\"]+)['\"]", xml_text)
        )

        return worksheet_names


    # --- Helper: detect filter zones ---
    def is_filter_zone(zone_xml: str) -> bool:
        """
        Returns True if the <zone> represents a filter.
        Handles both:
          1) type-v2="filter" as an attribute on <zone>
          2) <property name="type-v2" value="filter"/> inside the zone
        """
        return bool(re.search(
            r'(?:\btype-v2\s*=\s*["\']filter["\'])|(?:<property\b[^>]*name\s*=\s*["\']type-v2["\'][^>]*value\s*=\s*["\']filter["\'][^>]*/?>)',
            zone_xml,
            flags=re.IGNORECASE | re.DOTALL
        ))


    # --- Step 3: Extract dashboard zones ---
    def _normalize_name(s: str) -> str:
        return re.sub(r'[^a-z0-9]', '', (s or '').lower())


    # --- Step 3: Extract dashboard zones ---
    def extract_dashboard_zones1(xml_text):
        dashboards = re.findall(r"<dashboard.*?>.*?</dashboard>", xml_text, re.DOTALL)
        worksheet_names = extract_worksheet_names(xml_text)
        norm_list = {_normalize_name(n) for n in worksheet_names}

        dashboard_pairs = {}

        for dashboard in dashboards:
            dash_name_match = re.search(r'name=[\'"]([^\'"]+)[\'"]', dashboard)
            dash_name = dash_name_match.group(1) if dash_name_match else "UnnamedDashboard"
            valid_pairs = []

            # ✅ Match <zone ... />, <zone ...></zone>, and <zone ...>...</zone>
            zones = re.findall(
                r"<zone\b[^>]*/>|<zone\b[^>]*></zone>|<zone\b[^>]*>.*?</zone>",
                dashboard,
                re.DOTALL,
            )

            for zone in zones:
                # ⛔ skip filter zones
                #if is_filter_zone(zone):
                #    continue

                # ✅ Prefer "name=" over "param="
                match = re.search(r'name=["\']([^"\']+)["\']', zone)
                if not match:
                    match = re.search(r'param=["\']([^"\']+)["\']', zone)

                if match:
                    ws_name = match.group(1)
                    norm_ws = _normalize_name(ws_name)

                    if norm_ws in norm_list or "LOD" in ws_name.upper():
                        valid_pairs.append((zone.strip(), ws_name))
                    else:
                        # ⚠️ Fallback: keep zone even if no worksheet match
                        valid_pairs.append((zone.strip(), ws_name + "_NO_WS"))

            dashboard_pairs[dash_name] = valid_pairs
        return dashboard_pairs

    def extract_dashboard_zones(xml_text, include_filters: bool = False):
        """
        Extract dashboard zones.
        If include_filters=False → skips filter zones.
        If include_filters=True → keeps everything (including filters).
        """
        dashboards = re.findall(r"<dashboard.*?>.*?</dashboard>", xml_text, re.DOTALL)
        worksheet_names = extract_worksheet_names(xml_text)
        norm_list = {_normalize_name(n) for n in worksheet_names}

        dashboard_pairs = {}

        for dashboard in dashboards:
            dash_name_match = re.search(r'name=[\'"]([^\'"]+)[\'"]', dashboard)
            dash_name = dash_name_match.group(1) if dash_name_match else "UnnamedDashboard"
            valid_pairs = []

            zones = re.findall(
                r"<zone\b[^>]*/>|<zone\b[^>]*></zone>|<zone\b[^>]*>.*?</zone>",
                dashboard,
                re.DOTALL,
            )

            for zone in zones:
                # 🚫 Skip filter zones unless explicitly allowed
                if not include_filters and is_filter_zone(zone):
                    continue

                # Prefer "name=" over "param="
                match = re.search(r'name=["\']([^"\']+)["\']', zone)
                if not match:
                    match = re.search(r'param=["\']([^"\']+)["\']', zone)

                if match:
                    ws_name = match.group(1)
                    norm_ws = _normalize_name(ws_name)

                    if norm_ws in norm_list or "LOD" in ws_name.upper():
                        valid_pairs.append((zone.strip(), ws_name))
                    else:
                        valid_pairs.append((zone.strip(), ws_name + "_NO_WS"))

            dashboard_pairs[dash_name] = valid_pairs
        return dashboard_pairs



    # --- Step 4: Extract worksheet block ---
    def extract_worksheet_block(xml_text, worksheet_name):
        # First try normal <worksheet>
        pattern_ws = re.compile(
            fr"<worksheet[^>]*name=['\"]{re.escape(worksheet_name)}['\"][^>]*>.*?</worksheet>",
            re.DOTALL,
        )
        match = pattern_ws.search(xml_text)
        if match:
            return match.group(0).strip()

        # Fallback: try <window class="worksheet">
        pattern_win = re.compile(
            fr"<window[^>]*class=['\"]worksheet['\"][^>]*name=['\"]{re.escape(worksheet_name)}['\"][^>]*>.*?</window>",
            re.DOTALL | re.IGNORECASE,
        )
        match = pattern_win.search(xml_text)
        if match:
            return match.group(0).strip()

        return None


    # --- Step 5: Build chunks per dashboard ---
    def build_zone_worksheet_chunks_per_dashboard1(xml_file): #, max_tokens=40000):
        #tokenizer = GPT2TokenizerFast.from_pretrained("gpt2-xl")
        with open(xml_file, "r", encoding="utf-8") as f:
            xml_text = f.read()

        dashboard_pairs = extract_dashboard_zones(xml_text)
        dash_chunks = {}

        for dash_name, pairs in dashboard_pairs.items():
            chunks = []
            for i, (zone_block, ws_name) in enumerate(pairs):
                worksheet_block = extract_worksheet_block(xml_text, ws_name)
                if worksheet_block:
                    chunk = f"<zone-block>\n{zone_block}\n</zone-block>\n<worksheet-block>\n{worksheet_block}\n</worksheet-block>"
                    chunks.append(chunk)
            dash_chunks[dash_name] = chunks
        return dash_chunks

    # --- Step 5: Build chunks per dashboard ---
    import re

    def fix_zone_block(zone_block: str) -> str:
        """
        Fixes unclosed <zone> tags inside a <zone-block>.
        Ensures that every <zone> opened is properly closed before </zone-block>.
        """
        stack = []
        fixed_tokens = []

        tokens = re.split(r'(<[^>]+>)', zone_block)
        for token in tokens:
            if not token.strip():
                continue

            if token.startswith("</zone"):
                if stack:
                    stack.pop()
                fixed_tokens.append(token)

            elif token.startswith("<zone") and not token.endswith("/>"):
                stack.append("zone")
                fixed_tokens.append(token)

            else:
                fixed_tokens.append(token)

        # ✅ Before </zone-block>, close remaining <zone> tags
        fixed_xml = ""
        for token in fixed_tokens:
            if token.startswith("</zone-block"):
                while stack:
                    stack.pop()
                    fixed_xml += "</zone>"
                    print("🔧 Auto-added </zone> before </zone-block>")
            fixed_xml += token

        return fixed_xml


    # --- Step 5: Build chunks per dashboard ---
    def build_zone_worksheet_chunks_per_dashboard(xml_file): #, max_tokens=40000):
        with open(xml_file, "r", encoding="utf-8") as f:
            xml_text = f.read()

        #dashboard_pairs = extract_dashboard_zones(xml_text)
        dashboard_pairs = extract_dashboard_zones(xml_text, include_filters=False)  # ✅ filters excluded

        dash_chunks = {}

        for dash_name, pairs in dashboard_pairs.items():
            chunks = []
            for i, (zone_block, ws_name) in enumerate(pairs):
                worksheet_block = extract_worksheet_block(xml_text, ws_name)

                # ✅ Fix zone-block before combining
                fixed_zone_block = fix_zone_block(f"<zone-block>{zone_block}</zone-block>")

                if zone_block and worksheet_block:
                    chunk = (
                        f"{fixed_zone_block}\n"
                        f"<worksheet-block>\n{worksheet_block}\n</worksheet-block>"
                    )
                    chunks.append(chunk)
                else:
                    print(f"⚠️ Skipping chunk for {ws_name} in {dash_name} "
                          f"(zone-block or worksheet-block missing)")

            dash_chunks[dash_name] = chunks
        return dash_chunks


    # --- Step 6: validate and fix the xml ---

    def validate_and_fix_xml(xml_string: str) -> str:
        """
        Validates XML. If parsing fails due to unclosed <zone> tags,
        auto-fix by balancing <zone> only (ignores worksheet/window/etc).
        """
        try:
            ET.fromstring(xml_string)
            return xml_string
        except ET.ParseError as e:
            print(f"⚠️ XML not well-formed, attempting zone auto-fix: {e}")

            stack = []
            fixed_tokens = []

            tokens = re.split(r'(<[^>]+>)', xml_string)
            for token in tokens:
                if not token.strip():
                    continue

                if token.startswith("</zone"):  # closing zone
                    if stack:
                        stack.pop()  # match one open
                    fixed_tokens.append(token)

                elif token.startswith("<zone") and not token.endswith("/>"):  
                    # opening zone (not self-closing)
                    stack.append("zone")
                    fixed_tokens.append(token)

                else:
                    fixed_tokens.append(token)

            # ✅ close any unclosed <zone> tags
            while stack:
                stack.pop()
                fixed_tokens.append("</zone>")
                print("🔧 Auto-added </zone>")

            fixed_xml = "".join(fixed_tokens)

            try:
                ET.fromstring(fixed_xml)
                return fixed_xml
            except ET.ParseError as e2:
                print(f"❌ Still invalid after zone-fix: {e2}")
                return fixed_xml



    # --- Step 6: Save chunks per dashboard ---
    def upload_chunks_to_local(dash_chunks, output_dir: str, report_name: str):
        saved_files = []
        namespace_declarations = 'xmlns:ns0="http://www.tableausoftware.com/xml/user"'

        for dash_name, chunks in dash_chunks.items():
            chunks_dir = Path(output_dir) / report_name / "extracted_dashboards" / dash_name / "extractedchunks"
            chunks_dir.mkdir(parents=True, exist_ok=True)

            created, skipped = 0, 0

            for i, chunk in enumerate(chunks, 1):
                # Wrap in <visual> root
                full_xml = f"""<?xml version='1.0' encoding='utf-8'?>
    <visual {namespace_declarations}>
    {chunk}
    </visual>"""

                # ✅ validate and auto-fix if needed
                full_xml = validate_and_fix_xml(full_xml)


                # Save valid chunk
                chunk_file = chunks_dir / f"zone_worksheet_chunk_{i}.xml"
                with open(chunk_file, "w", encoding="utf-8") as f:
                    f.write(full_xml)

                saved_files.append(str(chunk_file))
                created += 1
                print(f"✅ Saved {dash_name} chunk {i} → {chunk_file}")

            print(f"📊 Dashboard '{dash_name}': created {created} chunks, skipped {skipped} invalid")
        return saved_files


    import xml.etree.ElementTree as ET
    import pandas as pd
    import csv
    from pathlib import Path

    def extract_metadata_to_csv(xml_file, csv_file):
        """
        Extracts <metadata-record class="column"> fields from Tableau XML 
        and saves them to a CSV file with renamed columns, deduplication,
        and proper quoting for commas.
        """

        # Ensure target directory exists
        csv_path = Path(csv_file)
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        # Original fields from XML
        fields = [
            "remote-name",
            "local-name",
            "parent-name",
            "aggregation"
        ]

        # Mapping for renaming
        rename_map = {
            "remote-name": "source_column_name",
            "local-name": "report_column_name",
            "parent-name": "table_name"
        }

        tree = ET.parse(xml_file)
        root = tree.getroot()

        rows = []
        for record in root.findall(".//metadata-record[@class='column']"):
            row = {}
            for f in fields:
                elem = record.find(f)
                value = elem.text.strip() if elem is not None and elem.text else ""
                # remove square brackets if present
                value = value.strip("[]")
                row[f] = value
            rows.append(row)

        df = pd.DataFrame(rows, columns=fields)

        # Rename selected columns
        df.rename(columns=rename_map, inplace=True)

        # Deduplicate by source_column_name → keep first occurrence
        if "source_column_name" in df.columns:
            df = df.drop_duplicates(subset=["source_column_name"], keep="first")

        # Save to CSV with quoting for commas
        df.to_csv(
            csv_path,
            index=False,
            encoding="utf-8",
            quoting=csv.QUOTE_MINIMAL
        )

        print(f"✅ Extracted {len(df)} unique records and saved to {csv_path}")
        return df


    def extract_zone_only_with_formatted_text(xml_file, existing_chunks, output_dir: Path, report_name: str):
        """
        Reads extracted.xml and saves zone-blocks (without worksheet) that contain <formatted-text>
        into the dashboard's extractedchunks folder.
        """
        with open(xml_file, "r", encoding="utf-8") as f:
            xml_text = f.read()

        dashboard_pairs = extract_dashboard_zones(xml_text)
        saved_files = []

        # collect worksheet names already paired
        paired_ws = {
            (dash, ws_name)
            for dash, pairs in existing_chunks.items()
            for chunk in pairs
            for ws_name in re.findall(r'<worksheet-block.*?name=[\'"]([^\'"]+)[\'"]', chunk)
        }

        namespace_declarations = 'xmlns:ns0="http://www.tableausoftware.com/xml/user"'

        for dash_name, pairs in dashboard_pairs.items():
            # extractedchunks path
            chunks_dir = output_dir / report_name / "extracted_dashboards" / dash_name / "extractedchunks"
            chunks_dir.mkdir(parents=True, exist_ok=True)

            counter = 1
            for zone_block, ws_name in pairs:
                if (dash_name, ws_name) in paired_ws:
                    continue
                if "<formatted-text" in zone_block:
                    fixed_zone_block = fix_zone_block(f"<zone-block>{zone_block}</zone-block>")
                    full_xml = f"""<?xml version='1.0' encoding='utf-8'?>
    <visual {namespace_declarations}>
    {fixed_zone_block}
    </visual>"""
                    full_xml = validate_and_fix_xml(full_xml)

                    file_path = chunks_dir / f"zone_only_text_{counter}.xml"
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(full_xml)
                    saved_files.append(str(file_path))
                    print(f"📝 Saved {dash_name} zone-only-text {counter} → {file_path}")
                    counter += 1

        return saved_files


    def extract_zone_only_filters(xml_file, existing_chunks, output_dir: Path, report_name: str):
        """
        Reads extracted.xml and saves zone-blocks (without worksheet) 
        that are identified as filter zones into the dashboard's extractedchunks folder.
        """
        with open(xml_file, "r", encoding="utf-8") as f:
            xml_text = f.read()

        #dashboard_pairs = extract_dashboard_zones(xml_text)
        dashboard_pairs = extract_dashboard_zones(xml_text, include_filters=True)  # ✅ keep filters here
        saved_files = []

        # collect worksheet names already paired
        paired_ws = {
            (dash, ws_name)
            for dash, pairs in existing_chunks.items()
            for chunk in pairs
            for ws_name in re.findall(r'<worksheet-block.*?name=[\'"]([^\'"]+)[\'"]', chunk)
        }

        namespace_declarations = 'xmlns:ns0="http://www.tableausoftware.com/xml/user"'

        for dash_name, pairs in dashboard_pairs.items():
            # extractedchunks path
            chunks_dir = output_dir / report_name / "extracted_dashboards" / dash_name / "extractedchunks"
            chunks_dir.mkdir(parents=True, exist_ok=True)

            counter = 1
            for zone_block, ws_name in pairs:
                # Skip zones already paired with worksheet chunks
                if (dash_name, ws_name) in paired_ws:
                    continue

                # ✅ Check if this is a filter zone
                if is_filter_zone(zone_block):
                    fixed_zone_block = fix_zone_block(f"<zone-block>{zone_block}</zone-block>")
                    full_xml = f"""<?xml version='1.0' encoding='utf-8'?>
    <visual {namespace_declarations}>
    {fixed_zone_block}
    </visual>"""
                    full_xml = validate_and_fix_xml(full_xml)

                    file_path = chunks_dir / f"zone_only_filter_{counter}.xml"
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(full_xml)

                    saved_files.append(str(file_path))
                    print(f"🔎 Saved {dash_name} zone-only-filter {counter} → {file_path}")
                    counter += 1

        return saved_files

    def count_files_in_dir(directory: str) -> int:
        """Helper to count files in a given directory."""
        if not os.path.exists(directory):
            return 0
        return sum(len(files) for _, _, files in os.walk(directory))

    # Step 1: Extract dashboards individually


    # Example usage:
    logging.info(f"Converting TWB to XML: {REPORT_NAME}")
    try:
        xml_file = twb_to_xml(
            f"{INPUT_PATH}/{REPORT_TYPE}/{REPORT_NAME}.twb",
            CONVERTED_XML_PATH
        )
        print(f"Saved XML file at: {xml_file}")
        logging.info(f"Successfully converted TWB to XML: {xml_file}")
    except Exception as e:
        logging.error(f"Error converting TWB to XML: {e}")

    logging.info(f"Extracting metadata to CSV: {REPORT_NAME}")
    try:
        extract_metadata_to_csv(
            f"{CONVERTED_XML_PATH}/{REPORT_NAME}.xml",
            TABLES_METADATA_OUTPUT
        )
        logging.info(f"Successfully extracted metadata to CSV: {REPORT_NAME}")
    except Exception as e:
        logging.error(f"Error extracting metadata to CSV: {e}")

    logging.info(f"Extracting dashboards from XML: {xml_file}")
    try:
        dash_files = extract_and_save_dashboards(
            local_xml_path = f"{CONVERTED_XML_PATH}/{REPORT_NAME}.xml",
            output_dir = VISUALS_EXTRACTOR_OUTPUT,
            report_name = REPORT_NAME
        )
        dashboard_count = len(dash_files)
        
        summary_logger.info(f"      Total Number of dashboards:{dashboard_count}")
        logging.info(f"Successfully extracted dashboards: {len(dash_files)} found")
    except Exception as e:
        logging.error(f"Error extracting dashboards: {e}")
        

    # Step 2: For each dashboard extracted.xml → build chunks
    for dash_file in dash_files:
                # Split the path and find the folder right after "extracted_dashboards"
        parts = dash_file.split(os.sep)
        if "extracted_dashboards" in parts:
            idx = parts.index("extracted_dashboards")
            dash_name = parts[idx + 1]   # the folder name right after "extracted_dashboards"
        else:
            dash_name = os.path.splitext(os.path.basename(dash_file))[0]  # fallback
        logging.info(f"Building and saving chunks for dashboard: {dash_file}")
        try:
            dash_chunks = build_zone_worksheet_chunks_per_dashboard(dash_file)
            logging.info(f"Successfully built chunks for dashboard: {dash_file}")
        except Exception as e:
            logging.error(f"Error building chunks for {dash_file}: {e}")
        
        logging.info(f"Uploading chunks to local for dashboard: {dash_file}")
        try:
            chunks_dir = upload_chunks_to_local(dash_chunks, VISUALS_EXTRACTOR_OUTPUT, REPORT_NAME)
        except Exception as e:
            logging.error(f"Error uploading chunks to local for {dash_file}: {e}")

        logging.info(f"Extracting zone-only text blocks for dashboard: {dash_file}")
        try:
            zone_only = extract_zone_only_with_formatted_text(dash_file, dash_chunks, output_dir = Path(VISUALS_EXTRACTOR_OUTPUT), report_name= REPORT_NAME)
        except Exception as e:
            logging.error(f"Error extracting zone-only text blocks for {dash_file}: {e}")

        logging.info(f"Extracting zone-only filter blocks for dashboard: {dash_file}")
        try:  
            filter_only = extract_zone_only_filters(dash_file, dash_chunks, output_dir = Path(VISUALS_EXTRACTOR_OUTPUT), report_name= REPORT_NAME)
        except Exception as e:
            logging.error(f"Error extracting zone-only filter blocks for {dash_file}: {e}")
        count_chunks = len(chunks_dir)
        count_zone_only = len(zone_only)
        count_filters = len(filter_only)

        total_files = count_chunks + count_zone_only + count_filters
        
        summary_logger.info(f"      Total Number of visuals extracted in {dash_name} dashboard:{total_files}")
        summary_logger.info(f"      Visual Extractor Agent code is running in background to convert tableau visual type to power bi visual type for {dash_name} dashboard, please wait...")

        print(f"Total visuals extracted from tableau dashboard: {total_files}")

    import xml.etree.ElementTree as ET
    from pathlib import Path

    def read_and_print_extracted_xml(output_dir: str, report_name: str, dashboard_name: str, file_name: str):
        """
        Reads a single XML chunk from:
        <output_dir>/<report_name>/extracted_dashboards/<dashboard_name>/extractedchunks/<file_name>
        and returns the XML content as string.
        """
        chunk_file = Path(output_dir) / report_name / "extracted_dashboards" / dashboard_name / "extractedchunks" / file_name

        if not chunk_file.exists():
            raise FileNotFoundError(f"❌ File not found: {chunk_file}")

        with open(chunk_file, "r", encoding="utf-8") as f:
            xml_data = f.read()

        # Parse XML to validate structure
        root = ET.fromstring(xml_data)

        print(f"✅ Extracted XML from Dashboard: {dashboard_name}, File: {file_name}\n")
        #print(xml_data)

        return xml_data


    import xml.etree.ElementTree as ET
    from pathlib import Path
    import pandas as pd

    def read_and_print_extracted_xml(output_dir: str, report_name: str, dashboard_name: str, file_name: str): #, tables_metadata_csv: str = None
        """
        Reads a single XML chunk from:
        <output_dir>/<report_name>/extracted_dashboards/<dashboard_name>/extractedchunks/<file_name>
        Optionally appends tables metadata CSV content to the returned string.
        """
        # Locate chunk file
        chunk_file = Path(output_dir) / report_name / "extracted_dashboards" / dashboard_name / "extractedchunks" / file_name

        if not chunk_file.exists():
            raise FileNotFoundError(f"❌ File not found: {chunk_file}")

        # Read XML content
        with open(chunk_file, "r", encoding="utf-8") as f:
            xml_data = f.read()

        # Validate XML (will raise if malformed)
        ET.fromstring(xml_data)

        print(f"✅ Extracted XML from Dashboard: {dashboard_name}, File: {file_name}")

        combined_data = xml_data

        # # Append tables metadata CSV if provided
        # if tables_metadata_csv:
        #     csv_path = Path(tables_metadata_csv)
        #     print(f"🔍 Checking CSV path: {csv_path}")
        #     if csv_path.exists():
        #         try:
        #             df = pd.read_csv(csv_path)
        #             csv_text = df.to_csv(index=False)
        #             combined_data += f"\n\n---\nHere is the tables metadata:\n{csv_text}"
        #             print(f"📑 Appended tables metadata from {csv_path}")
        #         except Exception as e:
        #             print(f"⚠️ Failed to read CSV {csv_path}: {e}")
        #     else:
        #         print(f"❌ CSV file does not exist at: {csv_path}")

        return combined_data


    import time
    from pathlib import Path
    from azure.ai.projects import AIProjectClient
    from azure.identity import DefaultAzureCredential
    from azure.ai.agents.models import MessageRole

    def send_xml_to_agent_and_save(blob_data: str, output_dir: str, report_name: str, tables_metadata_csv: str = None, return_thread_id: bool = False):
        """
        Sends XML content + optional tables metadata CSV to AI agent,
        retrieves output, and saves it as a JSON file locally.
        If return_thread_id=True, returns (output_file, thread_id).
        """

        # 1. Init project + agent
        project = AIProjectClient(
            credential=DefaultAzureCredential(),
            endpoint="https://accelatoraifoundry.services.ai.azure.com/api/projects/firstProject"
        )

        agent = project.agents.get_agent("asst_Kkok1qKLj1i5LYB05xJwLAA9")

        # 2. Create thread
        thread = project.agents.threads.create()
        print(f"🧵 Created thread: {thread.id}")

        # 3. Prepare message content
        prompt_content = blob_data

        if tables_metadata_csv and Path(tables_metadata_csv).exists():
            try:
                import pandas as pd
                df = pd.read_csv(tables_metadata_csv)
                csv_text = df.to_csv(index=False)
                prompt_content += f"\n\n---\nHere is the tables metadata:\n{csv_text}"
                print(f"📑 Appended tables metadata from {tables_metadata_csv}")
            except Exception as e:
                print(f"⚠️ Failed to read CSV {tables_metadata_csv}: {e}")

        # 4. Send to agent
        project.agents.messages.create(
            thread_id=thread.id,
            role="user",
            content=prompt_content
        )

        # 5. Run the agent
        run = project.agents.runs.create_and_process(
            thread_id=thread.id,
            agent_id=agent.id
        )

        if run.status == "failed":
            error_msg = run.last_error or {"message": "Unknown agent failure"}
            print(f"❌ Run failed: {error_msg}")
            if "rate_limit" in str(error_msg).lower():
                print("🕒 Rate limit hit. Waiting 90s...")
                time.sleep(90)
            raise RuntimeError(f"Agent run failed: {error_msg}")

        # 6. Get agent response
        messages = project.agents.messages.get_last_message_text_by_role(
            thread_id=thread.id,
            role=MessageRole.AGENT
        )

        output_file = None
        if messages:
            output_text = messages.text.value
            print(f"🤖 Agent Output: {output_text[:200]}...")  # preview

            # Save output
            report_dir = Path(output_dir) / report_name
            report_dir.mkdir(parents=True, exist_ok=True)

            output_file = report_dir / "extractedvisuals.json"
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(output_text)

            print(f"✅ Saved agent output to: {output_file}")

        # 7. Clean up thread normally (unless caller wants ID back)
        if return_thread_id:
            return str(output_file), thread.id

        project.agents.threads.delete(thread.id)
        print(f"🧹 Deleted thread: {thread.id}")

        return str(output_file)



    # blob_data = read_and_print_extracted_xml(
    #     output_dir="D:/Projects/Solution Harvesting/Visual Creator Automation/GeneratePBIP_Project/VisualsExtractorOutput",
    #     report_name="Superstore",
    #     dashboard_name="Simple",
    #     file_name="zone_worksheet_chunk_5.xml"
    # )


    # send_xml_to_agent_and_save(
    #     blob_data=blob_data,
    #     output_dir="D:/Projects/Solution Harvesting/Visual Creator Automation/GeneratePBIP_Project/VisualsExtractorOutput",
    #     report_name="Superstore"
    # )


    import os
    import time
    from pathlib import Path

    def process_all_chunks_local(output_dir: str, report_name: str, max_runtime: int = 60):
        """
        For each dashboard and its XML chunks:
        - Send each chunk to AI agent
        - Store response JSON under <output_dir>/<report_name>/json/<dashboard_name>/
        - Retry up to 3 times on failure or timeout
        - Wait 10 seconds between retries
        """

        dashboards_dir = Path(output_dir) / report_name / "extracted_dashboards"
        json_parent_dir = Path(output_dir) / report_name / "json"
        json_parent_dir.mkdir(parents=True, exist_ok=True)

        # Loop dashboards
        for dashboard_dir in dashboards_dir.iterdir():
            if not dashboard_dir.is_dir():
                continue

            dashboard_name = dashboard_dir.name
            chunks_dir = dashboard_dir / "extractedchunks"
            if not chunks_dir.exists():
                print(f"⚠️ No extractedchunks for dashboard {dashboard_name}, skipping...")
                continue

            json_dir = json_parent_dir / dashboard_name
            json_dir.mkdir(parents=True, exist_ok=True)

            xml_files = sorted(chunks_dir.glob("*.xml"))
            if not xml_files:
                print(f"⚠️ No XML chunks for dashboard {dashboard_name}")
                continue

            for xml_file in xml_files:
                print(f"\n🚀 Processing {dashboard_name} → {xml_file.name}")
                success = False

                for attempt in range(1, 4):  # Retry up to 3 times
                    try:
                        # Read XML content
                        with open(xml_file, "r", encoding="utf-8") as f:
                            xml_text = f.read()

                        # Expected JSON file path
                        output_file = json_dir / (xml_file.stem + ".json")

                        # Track runtime
                        start_time = time.time()
                        result_path, thread_id = send_xml_to_agent_and_save(
                            blob_data=xml_text,
                            output_dir=json_dir,
                            report_name="",  # already writing into json_dir
                            return_thread_id=True  # new flag you add in send_xml_to_agent_and_save
                        )
                        runtime = time.time() - start_time

                        if runtime > max_runtime:
                            print(f"⏱️ Response took {runtime:.1f}s (> {max_runtime}s). Deleting thread {thread_id} and retrying...")
                            # delete thread explicitly
                            try:
                                project = AIProjectClient(
                                    credential=DefaultAzureCredential(),
                                    endpoint="https://accelatoraifoundry.services.ai.azure.com/api/projects/firstProject"
                                )
                                project.agents.threads.delete(thread_id)
                                print(f"🧹 Deleted thread {thread_id}")
                            except Exception as e:
                                print(f"⚠️ Failed to delete thread {thread_id}: {e}")
                            # wait 10s before retry
                            time.sleep(10)
                            continue  # retry loop

                        # If send_xml_to_agent_and_save always saves as extractedvisuals.json → rename
                        if Path(result_path).exists() and Path(result_path) != output_file:
                            os.replace(result_path, output_file)

                        print(f"✅ Success on attempt {attempt} → {output_file}")
                        success = True
                        break

                    except Exception as e:
                        print(f"❌ Attempt {attempt} failed for {xml_file.name}: {str(e)}")
                        if attempt < 3:
                            print("🔁 Retrying in 10s...")
                            time.sleep(10)
                        else:
                            print("⛔ Max retries reached. Skipping this chunk.")

                if not success:
                    print(f"⚠️ Failed to process {xml_file.name} after 3 attempts")

                # Short wait between chunks
                print("⏳ Waiting 10 seconds before next chunk...")
                time.sleep(10)

        print(f"\n🎉 All dashboards processed. JSONs saved under {json_parent_dir}")


    logging.info(f"Processing all chunks locally for report: {REPORT_NAME}")
    try:
        process_all_chunks_local(
            output_dir=VISUALS_EXTRACTOR_OUTPUT,
            report_name=REPORT_NAME
        )
        logging.info(f"Successfully processed all chunks for report: {REPORT_NAME}")
    except Exception as e:
        logging.error(f"Error processing all chunks for report {REPORT_NAME}: {e}")


    import json
    import re
    from pathlib import Path
    import pandas as pd
    from typing import Tuple, List, Union


    # ----------------- helpers -----------------

    def _normalize(s: str) -> str:
        return re.sub(r'[^a-z0-9]', '', (s or '').lower())

    def _extract_from_tokenized(shelf_val: str) -> str:
        """'none:Title:nk' -> 'Title' (middle token if >=3 parts)."""
        if shelf_val is None:
            return ''
        parts = [p.strip() for p in str(shelf_val).split(':')]
        if len(parts) >= 3:
            return parts[1]
        return str(shelf_val).strip()

    def _clean_readable_name(s: str) -> str:
        if s is None:
            return ''
        s = re.sub(r'\([^)]*\)', '', str(s))  # drop "(...)" blobs
        return re.sub(r'\s+', ' ', s).strip()

    def _prepare_metadata(metadata_csv_path: str) -> pd.DataFrame:
        md_df = pd.read_csv(metadata_csv_path, dtype=str).fillna('')
        required = {'source_column_name', 'table_name'}
        if not required.issubset(md_df.columns):
            raise ValueError("Metadata CSV must contain 'source_column_name' and 'table_name' columns.")
        md_df['_norm_src'] = md_df['source_column_name'].map(_normalize)
        return md_df

    def _match_source_and_table(candidate: str, md_df: pd.DataFrame) -> Tuple[str, str]:
        """
        Return (matched_source_column_name, table_name) or ('','') if no match.
        """
        if not candidate:
            return '', ''
        trimmed = candidate.strip()

        # 1) exact (case-insensitive)
        exact = md_df[md_df['source_column_name'].str.casefold() == trimmed.casefold()]
        if not exact.empty:
            row = exact.iloc[0]
            return row['source_column_name'], row['table_name']

        # 2) normalized exact
        norm = _normalize(trimmed)
        exact_norm = md_df[md_df['_norm_src'] == norm]
        if not exact_norm.empty:
            row = exact_norm.iloc[0]
            return row['source_column_name'], row['table_name']

        # 3) like/substring on normalized
        hits = md_df[md_df['_norm_src'].str.contains(re.escape(norm)) | md_df['_norm_src'].map(lambda s: norm in s)]
        if not hits.empty:
            hits = hits.assign(_len=hits['_norm_src'].str.len()).sort_values(['_len'])
            row = hits.iloc[0]
            return row['source_column_name'], row['table_name']

        return '', ''

    def _resolve_shelf(shelf_value: str, md_df: pd.DataFrame) -> Tuple[str, str]:
        """
        Apply rules and return (display_value, table_name).
        """
        if str(shelf_value).strip().casefold() == 'measure names':
            return '', ''
        if not shelf_value:
            return '', ''

        candidate = _clean_readable_name(_extract_from_tokenized(shelf_value))
        matched_src, table_name = _match_source_and_table(candidate, md_df)

        if matched_src:
            return matched_src, table_name
        return str(shelf_value), ''


    # ----------------- JSON auto-fix -----------------
    def _fix_json_quotes(json_str: str) -> str:
        """
        Fix JSON with missing or extra quotes on numbers.
        Ensures numbers remain numbers, and strings are valid.
        """
        # Remove trailing commas
        json_str = re.sub(r",(\s*[}\]])", r"\1", json_str)

        # Fix unbalanced quotes in numeric values, e.g. "Y": 242" → "Y": 242
        json_str = re.sub(r'(":\s*)(\d+)"', r'\1\2', json_str)

        return json_str


    # ----------------- core -----------------
    def update_shelf_tables_in_json(
        json_path: Union[str, Path],
        md_df: pd.DataFrame
    ) -> List[dict]:
        text = Path(json_path).read_text(encoding='utf-8')

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # try auto-fix
            fixed_text = _fix_json_quotes(text)
            try:
                data = json.loads(fixed_text)
            except json.JSONDecodeError as e:
                raise ValueError(f"Unable to fix JSON in file {json_path}: {e}")

        # Detect if this is a zone_only_filter file
        is_zone_only_filter = "zone_only_filter" in str(json_path).lower()

        for rec in data:
            col_val = rec.get('Column Shelf', '')
            row_val = rec.get('Row Shelf', '')
            other_val = rec.get('Other Dimensions Used', '')

            col_disp, col_tbl = _resolve_shelf(col_val, md_df)
            row_disp, row_tbl = _resolve_shelf(row_val, md_df)

            # fallback only for zone_only_filter files
            if is_zone_only_filter and not col_disp and not row_disp and other_val:
                candidates = [c.strip() for c in other_val.split(',') if c.strip()]
                if candidates:
                    fallback_candidate = candidates[0]
                    matched_src, table_name = _match_source_and_table(fallback_candidate, md_df)
                    if matched_src:
                        row_tbl = table_name

            rec['Column Shelf'] = col_disp
            rec['Row Shelf'] = row_disp
            rec['Column Shelf Table'] = col_tbl
            rec['Row Shelf Table'] = row_tbl

        return data



    def process_jsons(
        json_path_or_dir: Union[str, Path],
        metadata_csv_path: str,
        output_root_name: str = 'jsonwithtables'
    ) -> List[Path]:
        """
        - If given a file: process that file.
        - If given a directory: process all *.json (non-recursive).
        Writes to: <report>\<output_root_name>\<subfolder>\<filename>.json
          where:
            report    = the folder above 'json'
            subfolder = the folder name under 'json' (e.g., 'Simple')
        Returns list of written file paths.
        """
        json_path_or_dir = Path(json_path_or_dir)
        md_df = _prepare_metadata(metadata_csv_path)

        # Determine report folder and desired subfolder (e.g., 'Simple')
        if json_path_or_dir.is_file():
            # ...\json\<subfolder>\file.json
            json_folder = json_path_or_dir.parent.parent          # ...\json
            subfolder_name = json_path_or_dir.parent.name         # <subfolder>
            report_folder = json_folder.parent                    # ...\<report>
        elif json_path_or_dir.is_dir():
            # ...\json\<subfolder>
            json_folder = json_path_or_dir.parent                 # ...\json
            subfolder_name = json_path_or_dir.name                # <subfolder>
            report_folder = json_folder.parent                    # ...\<report>
        else:
            raise FileNotFoundError(f"Path not found: {json_path_or_dir}")

        # Fallback: if structure isn't ...\json\<subfolder>, put beside the input
        if json_folder.name.lower() != 'json':
            report_folder = (json_path_or_dir.parent if json_path_or_dir.is_file()
                             else json_path_or_dir)
            subfolder_name = json_path_or_dir.parent.name if json_path_or_dir.is_file() else json_path_or_dir.name

        out_dir = report_folder / output_root_name / subfolder_name
        out_dir.mkdir(parents=True, exist_ok=True)

        # Collect files
        if json_path_or_dir.is_dir():
            files = sorted(p for p in json_path_or_dir.glob('*.json') if p.is_file())
        else:
            files = [json_path_or_dir]

        written = []
        for infile in files:
            updated = update_shelf_tables_in_json(infile, md_df)
            outfile = out_dir / infile.name
            outfile.write_text(json.dumps(updated, indent=2, ensure_ascii=False), encoding='utf-8')
            written.append(outfile)

        return written

    def process_all_dashboards(
        report_json_root: Union[str, Path],
        metadata_csv_path: str,
        output_root_name: str = 'jsonwithtables'
    ) -> List[Path]:
        """
        Process ALL dashboards under a report.
        Input:  report_json_root = ...\<report>\json
        Loops through each dashboard subfolder inside "json" and processes JSONs.
    
        Writes to: <report>\<output_root_name>\<dashboard_name>\*.json
        Returns list of all written file paths.
        """
        report_json_root = Path(report_json_root)
        if not report_json_root.exists():
            raise FileNotFoundError(f"JSON root not found: {report_json_root}")
        if report_json_root.name.lower() != "json":
            raise ValueError("Expected path ending with '\\json' for report root")

        md_df = _prepare_metadata(metadata_csv_path)
        written_all = []

        # Iterate over each dashboard folder inside json
        for dash_folder in sorted(p for p in report_json_root.iterdir() if p.is_dir()):
            out_dir = report_json_root.parent / output_root_name / dash_folder.name
            out_dir.mkdir(parents=True, exist_ok=True)

            # Collect JSON files under this dashboard
            files = sorted(p for p in dash_folder.glob("*.json") if p.is_file())
            for infile in files:
                updated = update_shelf_tables_in_json(infile, md_df)
                outfile = out_dir / infile.name
                outfile.write_text(
                    json.dumps(updated, indent=2, ensure_ascii=False), encoding="utf-8"
                )
                written_all.append(outfile)

        return written_all


    # ----------------- example usage -----------------
    # Example 1: single file
    # outputs to <folder_of_file>/jsonwithtables/<same_name>.json
    # written_paths = process_jsons(
    #     r"D:\...\json\Simple\visuals.json",
    #     r"D:\...\metadata.csv"
    # )
    #
    # Example 2: folder of many JSON files
    # outputs to <that_folder>\jsonwithtables\*.json
    # written_paths = process_jsons(
    #     r"D:\...\json\Simple",
    #     r"D:\...\metadata.csv"
    # )
    # print("Wrote:", *map(str, written_paths), sep="\n- ")


    # ----------------- example usage -----------------
    
    logging.info(f"Processing all dashboards to add table metadata for report: {REPORT_NAME}")
    try:
        written_paths = process_all_dashboards(
            f"{VISUALS_EXTRACTOR_OUTPUT}/{REPORT_NAME}/json",
            TABLES_METADATA_OUTPUT
        )
        for p in written_paths:
            print("-", p)
        logging.info(f"Successfully processed all dashboards for report: {REPORT_NAME}")
    except Exception as e:
        logging.error(f"Error processing all dashboards for report {REPORT_NAME}: {e}")

    summary_logger.info(f"  Visual Extraction process completed.")
if __name__ == "__main__":
    main()
