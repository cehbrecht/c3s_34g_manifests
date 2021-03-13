import click
import os
import yaml
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

import pandas as pd

here = Path(os.path.dirname(__file__))


def parse_time(filename):
    time_part = filename.split("_")[-1].split(".nc")[0]
    if "-" not in time_part:
        return None, None
    start, end = time_part.split("-")
    # start year
    year = start[:4]
    month = start[4:6] or "01"
    day = start[6:8] or "01"
    start_time = datetime(int(year), int(month), int(day)).strftime('%Y-%m-%dT12:00:00')
    # end year
    year = end[:4]
    month = end[4:6] or "12"
    day = end[6:8] or "31"
    end_time = datetime(int(year), int(month), int(day)).strftime('%Y-%m-%dT12:00:00')
    return start_time, end_time


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
        "start_time",
        "end_time",
        "path",
    ]
    for item in tqdm(inventory):
        try:
            if "path" in item:
                for filename in item["files"]:
                    start_time, end_time = parse_time(filename)
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
                        columns[11]: start_time,
                        columns[12]: end_time,
                        columns[13]: f'{item["path"]}/{filename}',
                    }
                    entries.append(entry)
        except Exception as e:
            raise click.ClickException(f"filename={filename} - message={e}")
    click.echo("Build Dataframe ...")
    df = pd.DataFrame(entries)
    return df[columns]


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option('-z', '--compress', is_flag=True, help='Compress the resulting catalog with gzip.')
def cli(compress):
    click.echo("Loading inventory ...")
    inv = read_inventory()
    project = inv[0]["project"]
    click.echo(f"Building catalog for {project} ...")
    df = build_catalog(inv)
    last_updated = datetime.now().utcnow()
    timestamp = last_updated.strftime('%Y-%m-%dT%H:%M:%SZ')
    version = last_updated.strftime('v%Y%m%d')
    cat_name = f"{project}_{version}.csv"
    if compress:
        cat_name += ".gz"
        compression = "gzip"
    else:
        compression = None
    cat_path = here / "catalogs" / cat_name
    df.to_csv(cat_path, index=False, compression=compression)
    click.echo(f"Catalog written: {cat_path}")


if __name__ == '__main__':
    cli(obj={})
