import os
import tomli

def load_config(file_path="config.toml"):
    """Load and return configuration from a TOML file."""
    # Path to the directory containing the current script
    current_dir = os.path.dirname(__file__)

    # Path to the file
    file_path = os.path.join(current_dir, 'config.toml')

    with open(file_path, "rb") as f:
        toml_dict = tomli.load(f)
        return toml_dict

# Load the configuration when this module is imported
config = load_config()
