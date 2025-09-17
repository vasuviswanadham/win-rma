"""Auto-generated from VisualMigrator_pbi_template_files.ipynb"""

from python_scripts.config_loader import load_config
from datetime import datetime
import logging

def main():
    import os
    import re
    import time
    import json
    from pathlib import Path
    from azure.ai.projects import AIProjectClient
    from azure.identity import DefaultAzureCredential
    from azure.ai.agents.models import MessageRole

    config = load_config()
    today_str = datetime.now().strftime('%Y-%m-%d')

    OUTPUT_PATH = config["paths"]["output_path"]
    REPORT_TYPE = config["report"]["type"]
    VISUALSOUTPUT_PATH = config["paths"]["visualoutput_path"]
    REPORT_NAME = config["report"]["name"]
    TEMPLATE_FOLDER = config["paths"]["template_folder"]
    LOG_PATH = f'{config["paths"]["log_path"]}/{config["report"]["type"]}/{today_str}/{config["report"]["name"]}'
    PROJECT_ENDPOINT = config["ai"]["project_endpoint"]
    AGENT_ID_STAGE1 = config["ai"]["agent_id_stage1"]
    AGENT_ID_STAGE2 = config["ai"]["agent_id_stage2"]
    #DESTINATION_PATH = config["paths"]["destination_path"]
    DESTINATION_PATH = f"{OUTPUT_PATH}/{REPORT_TYPE}/{REPORT_NAME}/TemplateReport.Report/definition/pages"
    # DESTINATION_PATH = config["paths"].get("destination_path", f"{OUTPUT_PATH}/{REPORT_TYPE}/{REPORT_NAME}/TemplateReport.Report/definition/pages")

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
    summary_logger.info(f"  Visual Migration process started...")
    logging.info('Visual Migration process started.')

    summary_logger.info(f"      Visual Migrator Agent code is running in background to migrate tableau visuals to power bi visuals & generate PBIP folder for report, please wait...")
    
    def process_all_json_files_with_agent_local(
        output_dir: str,
        report_name: str,
        template_folder: str,
        project_endpoint: str,
        agent_id_stage1: str,  # <-- first agent (template identification)
        agent_id_stage2: str,  # <-- second agent (merge/update with template)
        max_tokens: int = 30000,
        wait_seconds_between_chunks: int = 10
    ):
        """
        Process each dashboard's chunk JSON files with two AI agents using Power BI templates.

        Flow:
          1) First message → agent_id_stage1: Identify the correct template file name.
          2) Second message → agent_id_stage2: Provide the identified template + original chunk JSON
             and return a FULL updated JSON visual.

        Saves final visuals under:
          - <output_dir>/<report_name>/pbi_visuals/<dashboard_name>/<visual_name>.json
          - <output_dir>/<report_name>/pbi_visuals_filesinfolder/<dashboard_name>/<visual_name>/visual.json
        """

        # Init agents client and fetch agents
        credential = DefaultAzureCredential()
        project_client = AIProjectClient(credential=credential, endpoint=project_endpoint)
        agent1 = project_client.agents.get_agent(agent_id_stage1)
        agent2 = project_client.agents.get_agent(agent_id_stage2)

        json_parent_dir = Path(output_dir) / report_name / "jsonwithtables"
        visuals_flat_parent = Path(output_dir) / report_name / "pbi_visuals"
        visuals_folder_parent = Path(output_dir) / report_name / "pbi_visuals_filesinfolder"

        # Loop dashboards
        for dashboard_dir in json_parent_dir.iterdir():
            if not dashboard_dir.is_dir():
                continue

            dashboard_name = dashboard_dir.name
            print(f"\n📊 Processing Dashboard: {dashboard_name}")

            # Prepare output dirs
            flat_dir = visuals_flat_parent / dashboard_name
            flat_dir.mkdir(parents=True, exist_ok=True)

            foldered_dir = visuals_folder_parent / dashboard_name
            foldered_dir.mkdir(parents=True, exist_ok=True)

            json_files = sorted(dashboard_dir.glob("*.json"))
            if not json_files:
                print(f"⚠️ No JSON files for dashboard {dashboard_name}")
                continue
            
            processed_count = 0   # ✅ counter for dashboard

            for json_file in json_files:
                thread1 = None
                thread2 = None
                try:
                    print(f"\n📂 Processing: {json_file.name}")

                    # Load chunk JSON (original dashboard visual description)
                    with open(json_file, "r", encoding="utf-8") as f:
                        json_data_raw = f.read()

                    # Token check (approx.)
                    estimated_tokens = len(json_data_raw) // 4
                    if estimated_tokens > max_tokens:
                        print(f"⚠️ Skipping {json_file.name} (exceeds token limit: {estimated_tokens} > {max_tokens})")
                        continue

                    # --------------------------
                    # Stage 1 → Agent 1 (Template identification)
                    # --------------------------
                    thread1 = project_client.agents.threads.create()
                    print(f"🧵 [Stage1] Created thread: {thread1.id}")

                    # First message → send the raw chunk to agent 1
                    project_client.agents.messages.create(
                        thread_id=thread1.id,
                        role=MessageRole.USER,
                        content=json_data_raw
                    )

                    run1 = project_client.agents.runs.create_and_process(
                        thread_id=thread1.id,
                        agent_id=agent1.id
                    )
                    if run1.status == "failed":
                        error_msg = run1.last_error or {"message": "Unknown agent failure"}
                        raise RuntimeError(f"❌ Stage1 agent run failed: {error_msg}")

                    # Retrieve agent 1 response (template filename)
                    first_response = project_client.agents.messages.get_last_message_text_by_role(
                        thread_id=thread1.id, role=MessageRole.AGENT
                    )
                    first_text = first_response.text.value if first_response else ""
                    print("🔍 [Stage1] Agent 1 response received")

                    # Try to extract a .json template filename from a table/line like: | <template>.json |
                    # Fallback: any token ending with .json
                    match = re.search(r"\|\s*([^\|]+\.json)\s*\|", first_text)
                    print(f"🔍 [Stage1] Agent 1 response {first_text}")
                    if not match:
                        match = re.search(r"([A-Za-z0-9_\-\.\/]+\.json)", first_text)

                    if not match:
                        raise ValueError("❌ Could not find any template filename (e.g., *.json) in agent 1 response")

                    template_filename = match.group(1).strip()
                    print(f"🎯 Identified template: {template_filename}")

                    # Load template JSON locally
                    # template_path = Path(template_folder) / template_filename
                    # if not template_path.exists():
                    #     raise FileNotFoundError(f"❌ Template not found: {template_path}")
                    # Try exact match first
                    template_path = Path(template_folder) / template_filename

                    if not template_path.exists():
                        # 🔎 Fallback: normalize and search all templates in folder
                        template_files = list(Path(template_folder).glob("*.json"))
                        normalized_target = template_filename.replace("_", "").lower()

                        matches = [
                            f for f in template_files
                            if f.name.replace("_", "").lower() == normalized_target
                        ]

                        if matches:
                            template_path = matches[0]
                        else:
                            raise FileNotFoundError(f"❌ Template not found (even after normalization): {template_filename}")

                    with open(template_path, "r", encoding="utf-8") as f:
                        template_content = f.read()

                    # --------------------------
                    # Stage 2 → Agent 2 (Merge/update using template + original chunk)
                    # --------------------------
                    thread2 = project_client.agents.threads.create()
                    print(f"🧵 [Stage2] Created thread: {thread2.id}")

                    # Second message → send instructions + template + original JSON to agent 2
                    stage2_prompt = f"""You are given:
    1) A Power BI visual template JSON (Power BI visual JSON).
    2) The original Tableau-derived chunk JSON (Tableau visual JSON).

    Power BI visual JSON:
    {template_content}

    Tableau visual JSON:
    {json_data_raw}
    """

                    project_client.agents.messages.create(
                        thread_id=thread2.id,
                        role=MessageRole.USER,
                        content=stage2_prompt
                    )

                    run2 = project_client.agents.runs.create_and_process(
                        thread_id=thread2.id,
                        agent_id=agent2.id
                    )
                    if run2.status == "failed":
                        error_msg = run2.last_error or {"message": "Unknown agent failure"}
                        raise RuntimeError(f"❌ Stage2 agent run failed: {error_msg}")

                    final_response = project_client.agents.messages.get_last_message_text_by_role(
                        thread_id=thread2.id, role=MessageRole.AGENT
                    )

                    if not final_response:
                        raise RuntimeError("❌ Stage2 agent did not return a response")

                    output_text = final_response.text.value
                    # Prefer fenced JSON
                    json_matches = re.findall(r"```json\s*(.*?)```", output_text, re.DOTALL | re.IGNORECASE)

                    if json_matches:
                        clean_json = json_matches[0].strip()
                    else:
                        # Fallback → extract by braces
                        json_start = output_text.find("{")
                        json_end = output_text.rfind("}")
                        if json_start != -1 and json_end != -1:
                            clean_json = output_text[json_start:json_end + 1]
                        else:
                            raise ValueError("❌ Could not extract valid JSON from agent 2 response")

                    # Parse + update "name" property to match source filename (without extension)
                    base_name = json_file.stem
                    try:
                        parsed_json = json.loads(clean_json)
                        parsed_json["name"] = base_name
                        clean_json = json.dumps(parsed_json, indent=4)
                    except Exception:
                        # If parsing fails, still save the raw JSON block
                        pass

                    # Save flat version
                    flat_output = flat_dir / json_file.name
                    with open(flat_output, "w", encoding="utf-8") as f:
                        f.write(clean_json)

                    # Save foldered version
                    visual_subfolder = foldered_dir / base_name
                    visual_subfolder.mkdir(parents=True, exist_ok=True)
                    structured_output_file = visual_subfolder / "visual.json"
                    with open(structured_output_file, "w", encoding="utf-8") as f:
                        f.write(clean_json)

                    print(f"✅ Saved flat: {flat_output}")
                    print(f"📂 Saved foldered: {structured_output_file}")

                    processed_count += 1   # ✅ increment count

                except Exception as e:
                    print(f"❌ Error processing {json_file.name}: {str(e)}")

                finally:
                    # Cleanup threads if created
                    try:
                        if thread1 is not None:
                            project_client.agents.threads.delete(thread1.id)
                            print(f"🧹 [Stage1] Deleted thread: {thread1.id}")
                    except Exception as _:
                        pass
                    try:
                        if thread2 is not None:
                            project_client.agents.threads.delete(thread2.id)
                            print(f"🧹 [Stage2] Deleted thread: {thread2.id}")
                    except Exception as _:
                        pass

                    # Wait between chunks
                    if wait_seconds_between_chunks and wait_seconds_between_chunks > 0:
                        print(f"⏳ Waiting {wait_seconds_between_chunks}s before next chunk...")
                        time.sleep(wait_seconds_between_chunks)

        # ✅ After finishing all json files in this dashboard
        print(f"\n📊 Dashboard '{dashboard_name}' processed {processed_count} JSON files\n")

        summary_logger.info(f"      Total Number of JSON files processed in {dashboard_name} dashboard: {processed_count}")

    logging.info("Starting processing of all JSON files with AI agents...")
    try:
        process_all_json_files_with_agent_local(
            output_dir=VISUALSOUTPUT_PATH,
            report_name=REPORT_NAME,
            template_folder=TEMPLATE_FOLDER,
            project_endpoint=PROJECT_ENDPOINT,
            agent_id_stage1=AGENT_ID_STAGE1,
            agent_id_stage2=AGENT_ID_STAGE2
        )
        logging.info("Completed processing of all JSON files successfully.")
    except Exception as e:
        logging.error(f"Error during processing of JSON files: {str(e)}")

    import os
    import shutil
    import uuid
    import json
    from pathlib import Path

    def copy_dashboards_with_unique_ids_and_update_pages(output_dir: str, report_name: str, destination: str):
        """
        Copies all dashboard folders from pbi_visuals_filesinfolder to a destination path.
        Each dashboard folder is renamed with a unique ID.
        Inside each unique ID folder:
          - Create a 'visuals/' subfolder
          - Copy all dashboard visuals there
          - Create a page.json with metadata
        Also appends the unique IDs into pages.json under 'pageOrder'.
        """

        source_parent = Path(output_dir) / report_name / "pbi_visuals_filesinfolder"
        destination_parent = Path(destination)
        destination_parent.mkdir(parents=True, exist_ok=True)

        if not source_parent.exists():
            raise FileNotFoundError(f"❌ Source folder not found: {source_parent}")

        copied_folders = {}
        new_ids = []

        for dashboard_dir in source_parent.iterdir():
            if not dashboard_dir.is_dir():
                continue

            # Generate unique ID
            unique_id = uuid.uuid4().hex[:20]
            new_ids.append(unique_id)

            dest_dir = destination_parent / unique_id
            visuals_dir = dest_dir / "visuals"

            # Create destination structure
            visuals_dir.mkdir(parents=True, exist_ok=True)

            # Copy each visual folder inside "visuals"
            for item in dashboard_dir.iterdir():
                if item.is_dir():
                    shutil.copytree(item, visuals_dir / item.name)

            # Create page.json with schema in <unique_id> folder
            page_json_content = {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/2.0.0/schema.json",
                "name": unique_id,
                "displayName": dashboard_dir.name,  # dashboard name
                "displayOption": "FitToPage",
                "height": 900,
                "width": 1400
            }

            page_json_file = dest_dir / "page.json"
            with open(page_json_file, "w", encoding="utf-8") as f:
                json.dump(page_json_content, f, indent=4)

            copied_folders[dashboard_dir.name] = str(dest_dir)
            print(f"📂 Copied {dashboard_dir.name} → {dest_dir}")
            print(f"📝 Created {page_json_file}")

        # Update pages.json
        pages_file = destination_parent / "pages.json"
        if pages_file.exists():
            with open(pages_file, "r", encoding="utf-8") as f:
                pages_data = json.load(f)

            if "pageOrder" not in pages_data or not isinstance(pages_data["pageOrder"], list):
                pages_data["pageOrder"] = []

            # Append unique IDs (no duplicates)
            for uid in new_ids:
                if uid not in pages_data["pageOrder"]:
                    pages_data["pageOrder"].append(uid)

            with open(pages_file, "w", encoding="utf-8") as f:
                json.dump(pages_data, f, indent=4)

            print(f"✅ Updated pages.json with IDs: {new_ids}")
        else:
            print(f"⚠️ pages.json not found at {pages_file}")

        return copied_folders, new_ids


    logging.info("Starting to copy dashboards with unique IDs and update pages.json...")
    try:
        copied, ids = copy_dashboards_with_unique_ids_and_update_pages(
            output_dir=VISUALSOUTPUT_PATH,
            report_name=REPORT_NAME,
            destination=DESTINATION_PATH
        )
        logging.info("Completed copying dashboards and updating pages.json successfully.")
    except Exception as e:
        logging.error(f"Error during copying dashboards or updating pages.json: {str(e)}")

    print("\n✅ Dashboard copy summary:")
    for dash, dest in copied.items():
        print(f"{dash} → {dest}")

    print("\n📄 IDs added to pages.json:", ids)


    # #  below is the orchestrator code to run all the function


    # import time
    # from pathlib import Path

    # # ---- Import your functions here (from previous scripts) ----
    # # XML extraction
    # from your_module import extract_and_save_dashboards, build_zone_worksheet_chunks_per_dashboard, upload_chunks_to_local
    # # AI agent chunk processing
    # from your_module import process_all_chunks_local
    # # AI agent JSON → Power BI visuals
    # from your_module import process_all_json_files_with_agent_local


    # def run_pipeline(
    #     local_xml_path: str,
    #     output_dir: str,
    #     report_name: str,
    #     template_folder: str,
    #     project_endpoint: str,
    #     agent_id: str
    # ):
    #     """
    #     Full pipeline:
    #     1. Extract dashboards from Tableau XML
    #     2. Build and save zone/worksheet chunks
    #     3. Process chunks with AI agent → JSONs
    #     4. Process JSONs with AI agent + templates → Power BI visuals
    #     """

    #     print("\n===== STEP 1: Extract Dashboards =====")
    #     dash_files = extract_and_save_dashboards(local_xml_path, output_dir, report_name)

    #     print("\n===== STEP 2: Build Chunks =====")
    #     for dash_file in dash_files:
    #         dash_chunks = build_zone_worksheet_chunks_per_dashboard(dash_file)
    #         upload_chunks_to_local(dash_chunks, output_dir, report_name)

    #     print("\n===== STEP 3: Process Chunks → JSON =====")
    #     process_all_chunks_local(output_dir, report_name)

    #     print("\n===== STEP 4: Process JSON → Power BI Visuals =====")
    #     process_all_json_files_with_agent_local(
    #         output_dir=output_dir,
    #         report_name=report_name,
    #         template_folder=template_folder,
    #         project_endpoint=project_endpoint,
    #         agent_id=agent_id
    #     )

    #     print("\n🎉 Pipeline complete! All visuals generated successfully.")


    # if __name__ == "__main__":
    #     # ==== CONFIG ====
    #     local_xml_path = "D:/Projects/.../TableauXMLDownloads/XML/CustomerAnalysis.xml"
    #     output_dir = "D:/Projects/.../VisualsExtractorOutput"
    #     report_name = "CustomerAnalysis"

    #     template_folder = "D:/Projects/.../PBIVisualTemplates"
    #     project_endpoint = "https://accelatoraifoundry.services.ai.azure.com/api/projects/firstProject"
    #     agent_id = "asst_108VUgY6R6PjZ317WINf6GhC"

    #     # ==== RUN PIPELINE ====
    #     run_pipeline(local_xml_path, output_dir, report_name, template_folder, project_endpoint, agent_id)

    summary_logger.info(f"  Visual Migration process completed.")
    summary_logger.info(f"Migration completed.")

    # --- Close and release the Summary log file ---
    for handler in summary_logger.handlers:
        handler.close()
        summary_logger.removeHandler(handler)

    # Remove all handlers before setting up logging
    for handler in logging.root.handlers[:]:
        handler.close()
        logging.root.removeHandler(handler)
    
    time.sleep(5)  # Ensure all logs are flushed before exiting

if __name__ == "__main__":
    main()
