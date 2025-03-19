from pathlib import Path
import json
import gzip
from typing import cast

from pyalex import Works, Authors, Sources, Institutions, Topics, Publishers, Funders
from pyalex.api import BaseOpenAlex
from tqdm import tqdm


SNAPSHOT_DIR = Path("openalex-snapshot")
NUM_EXAMPLES = 5


def download_examples(kind: type[BaseOpenAlex], num: int):
    folder = (
        SNAPSHOT_DIR.joinpath("data")
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


if __name__ == "__main__":
    for kind in [Works, Authors, Sources, Institutions, Topics, Publishers, Funders]:
        print("Downloading", kind.__name__)
        download_examples(kind, NUM_EXAMPLES)
