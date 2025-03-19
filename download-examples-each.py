from pathlib import Path
import json
import gzip
from typing import Annotated, cast
import typer

from pyalex import Works, Authors, Sources, Institutions, Topics, Publishers, Funders
from pyalex.api import BaseOpenAlex
from tqdm import tqdm


def download_examples(kind: type[BaseOpenAlex], snapshot_dir: Path, num: int):
    folder = (
        snapshot_dir.joinpath("data")
        .joinpath(kind.__name__.lower())
        .joinpath("examples")
    )
    folder.mkdir(parents=True, exist_ok=True)

    for _ in tqdm(range(num), total=num):
        data = cast(dict, kind().random())

        with gzip.open(
            folder.joinpath(data["id"].replace("https://openalex.org/", "") + ".gz"),
            "wt",
        ) as file:
            json.dump(data, file)


def main(snapshot_dir: Path, num: Annotated[int, typer.Option("-n", "--num")]):
    for kind in [Works, Authors, Sources, Institutions, Topics, Publishers, Funders]:
        print("Downloading", kind.__name__)
        download_examples(kind, snapshot_dir, num)


if __name__ == "__main__":
    typer.run(main)
