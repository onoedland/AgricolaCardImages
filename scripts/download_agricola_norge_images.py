"""
Download card images from Håkon's WebApp.

- Input: csv file with (relative) urls to download.
"""

import pathlib
import polars as pl
import requests
import shutil


if __name__ == "__main__":
    df = pl.read_csv("../data/agricola_norge_image_urls.csv", separator="\t")

    df_local = (
        df.with_columns(
            ("../images/agricola_norge/" + pl.col("image_url")).alias(
                "local_image_path"
            ),
            (
                pl.lit("https://hauk88.github.io/PlayAgricolaStatistics/img/")
                + pl.col("image_url")
            ).alias("original_url"),
        )
        .with_columns(pl.col("original_url").alias("image_url"))
        .drop("original_url")
    )

    for row in df_local.iter_rows(named=True):
        card_name = row.get("card_name")
        image_url = row.get("image_url")
        local_image_path = row.get("local_image_path")
        print(f'Downloading image for "{card_name}"...')
        # print(row)
        # break

        # Make sure local directory is created if it does not already exist.
        pathlib.Path(local_image_path).parent.mkdir(exist_ok=True, parents=True)

        try:
            response = requests.get(image_url, stream=True)
            with open(local_image_path, "wb") as out_file:
                shutil.copyfileobj(response.raw, out_file)
            del response
        except Exception as err:
            print(
                f'Something went wrong when downloading image for card "{card_name}": {err}.'
            )
