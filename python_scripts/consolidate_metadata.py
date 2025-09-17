import os
import pandas as pd

def download_twb_files():
    # Placeholder for downloading .twb files
    print("Downloading .twb files... (functionality not implemented)")
    ## Place the files in Input folder

## call generate_metadata from modelmigrator
#generate_metadata(REPORT_FILE_PATH, METADATA_PATH)

## Call Consolidate function

def consolidate_csvs(parent_dir):
    consolidated_dir = os.path.join(parent_dir, "Consolidated Metadata")
    os.makedirs(consolidated_dir, exist_ok=True)

    # Dictionary to hold file name → list of DataFrames
    csv_collections = {}

    # Walk through subfolders
    for root, dirs, files in os.walk(parent_dir):
        # Skip the consolidated folder itself
        if os.path.abspath(root) == os.path.abspath(consolidated_dir):
            continue

        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                source_folder = os.path.basename(root)  # subfolder name

                try:
                    df = pd.read_csv(file_path)
                    df["ReportName"] = source_folder  # add source column
                except Exception as e:
                    print(f"⚠️ Skipping {file_path}: {e}")
                    continue

                if file not in csv_collections:
                    csv_collections[file] = []
                csv_collections[file].append(df)

    # Concatenate and save each consolidated CSV
    for file_name, df_list in csv_collections.items():
        combined_df = pd.concat(df_list, ignore_index=True)
        output_path = os.path.join(consolidated_dir, file_name)
        combined_df.to_csv(output_path, index=False)
        print(f"✅ Consolidated {file_name} → {output_path}")


if __name__ == "__main__":
    parent_directory = r"D:\Migration\Metadata\Tableau"
    consolidate_csvs(parent_directory)
