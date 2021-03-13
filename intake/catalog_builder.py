import os
import yaml
from pathlib import Path
from tqdm import tqdm

import pandas as pd

here = Path(os.path.dirname(__file__))


def read_inventory():
    inv = None
    inv_path = here / ".." / "inventories" / "c3s-cmip6" / "c3s-cmip6_files_latest.yml"
    with open(inv_path) as fin:
        inv = yaml.load(fin, Loader=yaml.SafeLoader)
    return inv


def build_catalog(inventory):
    entries = []
    for item in tqdm(inventory):
        try:
            if "path" in item:
                for filename in item["files"]:
                    entry = {
                        "ds_id": item["ds_id"],
                        "path": f'{item["path"]}/{filename}',
                    }
                    entries.append(entry)
        except ValueError:
            print(f"Error: {item}")
    print("Build Dataframe ...")
    df = pd.DataFrame(entries)
    columns = [
        'ds_id',
        'path',
    ]
    return df[columns]


if __name__ == '__main__':
    print("Loading inventory ...")
    inv = read_inventory()
    print("Building catalog ...")
    df = build_catalog(inv)
    cat_name = here / "catalogs" / "c3s-cmip6_latest.csv"
    df.to_csv(cat_name, index=False)
    print(f"Catalog written: {cat_name}")
