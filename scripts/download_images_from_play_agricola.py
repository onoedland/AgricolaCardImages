"""
Download card images from play-agricola.com.

- Input: csv file with urls to download.
"""

import pathlib
import polars as pl
import requests
import shutil


if __name__ == "__main__":
    df = pl.read_csv("../data/play_agricola_image_urls.csv", separator="\t")

    df_local = df.with_columns(
        (
            "../images/play_agricola/"
            + pl.col("image_url").str.strip_prefix("http://play-agricola.com/")
        ).alias("local_image_path")
    )

    for row in df_local.iter_rows(named=True):
        card_name = row.get("card_name")
        image_url = row.get("image_url")
        local_image_path = row.get("local_image_path")
        print(f'Downloading image for "{card_name}"...')

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
