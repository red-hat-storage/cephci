import yaml


class ElementIDs:
    def __init__(self, yaml_file_path: str):
        """
        Initialize ElementIDs with the path to the YAML file containing element identifiers.
        :param yaml_file_path: Path to the YAML file.
        """
        self.yaml_file_path = yaml_file_path
        self.elements = self._load_ids()

    def _load_ids(self) -> dict:
        """
        Load element IDs from a YAML file.
        :return: A dictionary of element identifiers.
        """
        try:
            with open(self.yaml_file_path, "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise RuntimeError(f"YAML file not found: {self.yaml_file_path}")
        except yaml.YAMLError as e:
            raise RuntimeError(f"Error parsing YAML file: {e}")

    def get_element(self, key: str) -> dict:
        """
        Get the locator types for a given key.
        :param key: The key for the desired element.
        :return: A dictionary with locator types (id, name, xpath, etc.) for the element.
        """
        if key not in self.elements:
            raise KeyError(f"Element key '{key}' not found in YAML.")
        return self.elements[key]
