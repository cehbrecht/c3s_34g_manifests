import click
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
    columns = [
        "ds_id",
        "mip_era",
        "activity_id",
        "institution_id",
        "source_id",
        "experiment_id",
        "member_id",
        "table_id",
        "variable_id",
        "grid_label",
        "version",
        "path",
    ]
    for item in tqdm(inventory):
        try:
            if "path" in item:
                for filename in item["files"]:
                    entry = {
                        columns[0]: item["ds_id"],
                        columns[1]: item["facets"]["mip_era"],
                        columns[2]: item["facets"]["activity_id"],
                        columns[3]: item["facets"]["institution_id"],
                        columns[4]: item["facets"]["source_id"],
                        columns[5]: item["facets"]["experiment_id"],
                        columns[6]: item["facets"]["member_id"],
                        columns[7]: item["facets"]["table_id"],
                        columns[8]: item["facets"]["variable_id"],
                        columns[9]: item["facets"]["grid_label"],
                        columns[10]: item["facets"]["version"],
                        columns[11]: f'{item["path"]}/{filename}',
                    }
                    entries.append(entry)
        except ValueError:
            click.echo(f"Error: {item}")
    click.echo("Build Dataframe ...")
    df = pd.DataFrame(entries)
    return df[columns]


def main():
    click.echo("Loading inventory ...")
    inv = read_inventory()
    click.echo("Building catalog ...")
    df = build_catalog(inv)
    cat_name = here / "catalogs" / "c3s-cmip6_latest.csv.gz"
    df.to_csv(cat_name, index=False, compression="gzip")
    click.echo(f"Catalog written: {cat_name}")


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
def cli():
    main()


if __name__ == '__main__':
    cli(obj={})
