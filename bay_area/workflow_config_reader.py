"""
Shared config reader for populationsim workflow scripts.
Loads workflow_config.yaml from the bay_area directory.
"""
import pathlib
import yaml

_CONFIG = None
_DEFAULT_CONFIG_PATH = pathlib.Path(__file__).parent / "workflow_config.yaml"


def load_config(path=None):
    """Load and cache the workflow config YAML."""
    global _CONFIG
    if _CONFIG is None:
        p = pathlib.Path(path) if path else _DEFAULT_CONFIG_PATH
        with open(p) as f:
            _CONFIG = yaml.safe_load(f)
    return _CONFIG


def get(key, default=None):
    """Access config values with dot notation, e.g. get('seed.crosswalk_file')"""
    cfg = load_config()
    for part in key.split('.'):
        if isinstance(cfg, dict):
            cfg = cfg.get(part, default)
        else:
            return default
    return cfg


def data_path(filename):
    """Return full path to a file in the data directory."""
    cfg = load_config()
    return pathlib.Path(cfg['data_dir']) / filename
