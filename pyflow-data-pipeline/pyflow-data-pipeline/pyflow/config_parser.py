#1. Create a config parser that reads YAML/JSON config files for ETL settings (file paths, db credentials, chunk size)

# Import libraries
import yaml        # used to read YAML files
import json        # used to read JSON files
from pathlib import Path  # used to check file path easily


# function to read configuration file
def read_config(config_path: str) -> dict:

    # convert the path string into Path object
    path = Path(config_path)

    # check if file exists
    if not path.exists():
        raise FileNotFoundError("Config file not found")

    try:
        # open the file in read mode
        with open(path, "r") as file:

            # check file type using extension

            # if file is YAML
            if path.suffix == ".yaml" or path.suffix == ".yml":
                config = yaml.safe_load(file)

            # if file is JSON
            elif path.suffix == ".json":
                config = json.load(file)

            # if file type is not supported
            else:
                raise ValueError("Only YAML and JSON files are supported")

        # return the configuration data
        return config

    # if any error occurs while reading file
    except Exception as e:
        raise RuntimeError(f"Error reading config file: {e}")


# this part runs only when we run this file directly
if __name__ == "__main__":

    # read config file
    config = read_config("config/config.yaml")

    # print the configuration
    print(config)