import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from python_scripts import modelmigrator, visualextractor, visualmigrator
from python_scripts.config_loader import load_config

def run_pipeline():
   config = load_config()

   if config["options"].get("run_stage1", True):
      modelmigrator.main()
   if config["options"].get("run_stage2", True):
      visualextractor.main()
   if config["options"].get("run_stage3", True):
      visualmigrator.main()

if __name__ == "__main__":
    run_pipeline()
