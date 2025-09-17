import yaml
import os
from pathlib import Path

def load_config(config_path: str = None):
    # Always resolve path to VisualStudioCodeBase/config/config.yaml
    base_dir = Path(__file__).parent.parent.resolve()  # VisualStudioCodeBase absolute path
    config_file = base_dir / "config" / "config.yaml"
    if config_path:
        config_file = base_dir / config_path
    config_file = config_file.resolve()
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")
    with open(config_file, "r") as f:
        return yaml.safe_load(f)
