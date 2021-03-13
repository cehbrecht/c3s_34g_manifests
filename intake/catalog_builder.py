import os
import yaml
from pathlib import Path

here = Path(os.path.dirname(__file__))


def read_inventory():
    inv = None
    inv_path = here / ".." / "inventories" / "c3s-cmip6" / "c3s-cmip6_files_latest.yml"
    with open(inv_path) as fin:
        inv = yaml.load(fin, Loader=yaml.SafeLoader)
    return inv


if __name__ == '__main__':
    inv = read_inventory()
