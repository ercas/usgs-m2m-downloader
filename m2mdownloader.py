#!/usr/bin/env python3

import datetime
import gzip
import io
import itertools
import logging
import os
import json
import re
import tempfile
import time
import typing

import joblib
import requests  # For interacting with the API
import pycurl  # For large downloads
import tqdm

URL_BASE = "https://m2m.cr.usgs.gov/api/api/json/stable/{endpoint}"

EE_USER_ENVIRONMENT_VAR = "EEUSER"

EE_PASSWORD_ENVIRONMENT_VAR = "EEPASS"

DEFAULT_CHUNK_SIZE = 5000


# %% Class development

# https://stackoverflow.com/a/8991553
def grouper(iterable: typing.Iterable, n: int) -> typing.Iterable[tuple]:
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def download_file(url: str,
                  output_path: typing.Optional[str] = None,
                  output_dir: str = ".",
                  timeout: int = 60):
    """ Download a file using PyCurl.

    Args:
        url: The URL to download.
        output_path: The path that the URL should be downloaded to. If no path
            is specified, the function will attempt to guess the filename from
            the HTTP return headers.
        output_dir: The directory where temporary files will be saved to, and,
            if no `output_path` is given, also where downloads will be saved to.
        timeout: Passed to PyCurl as the CONNECTTIMEOUT option.
    """
    if output_path and os.path.isfile(output_path):
        logging.info("Skipping {} ({} exists)".format(url, output_path))
    else:
        header_sink = io.BytesIO()
        (temp_fd, temp_file) = tempfile.mkstemp(dir=output_dir)

        logging.info("Downloading {} => {}".format(url, temp_file))
        with os.fdopen(temp_fd, "wb") as f:
            curl = pycurl.Curl()
            curl.setopt(pycurl.URL, url)
            curl.setopt(pycurl.WRITEDATA, f)
            curl.setopt(pycurl.HEADERFUNCTION, header_sink.write)
            curl.setopt(pycurl.CONNECTTIMEOUT, timeout)
            curl.perform()
            curl.close()

        # Extract remote file name from headers
        if not output_path:
            try:
                output_filename = re.search(
                    "filename=\"(.*)\"", header_sink.getvalue().decode()
                ).group(1)
                output_path = os.path.join(output_dir, output_filename)
            except:
                header_path = tempfile.mktemp(
                    suffix="-header",
                    prefix="unknown-download-",
                    dir=output_dir
                )
                output_path = header_path.replace("-header", "")
                logging.warning("Could not guess filename for {}; saving to {}".format(url, output_path))
                with open(header_path, "wb") as f2:
                    f2.write(header_sink.getvalue())

        logging.info("Renaming {} => {}".format(temp_file, output_path))
        os.rename(temp_file, output_path)


class M2MDownloader:
    API_URL_TEMPLATE = "https://m2m.cr.usgs.gov/api/api/json/stable/{endpoint}"

    def __init__(self,
                 username: str,
                 password: str,
                 output_dir: str = "output/",
                 cache_dir: str = "cache/"):
        """ Initialize M2MDownloader.

        Args:
            username: USGS Earth Explorer login username.
            password: USGS Earth Explorer login password.
            output_dir: Directory where files should be saved to.
            cache_dir: Directory where the cache should be stored.
        """

        logging.info("Authenticating as {}".format(username))
        auth_request = requests.post(
            self.API_URL_TEMPLATE.format(endpoint="login"),
            json={
                "username": username,
                "password": password
            }
        )
        auth_request.raise_for_status()
        self.api_key = auth_request.json()["data"]
        logging.debug("API key = {}".format(self.api_key))

        logging.debug("Setting up cache at {}".format(cache_dir))
        self.cache_dir = cache_dir
        if not os.path.isdir(cache_dir):
            os.makedirs(cache_dir)
        # self.memory = joblib.Memory(cache_dir, verbose=0)

        logging.debug("Setting up output directory at {}".format(output_dir))
        self.output_dir = output_dir
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir)

    def request(self,
                endpoint: str,
                data: dict = {},
                method: typing.Callable = requests.post
                ) -> dict:
        """ Send a request to the M2M API.

        Args:
            endpoint: The API endpoint.
            data: A dict that will be passed as a JSON body.
            method: The `requests` function to use to send the request.

        Returns: A dict containing the response from the API.
        """

        request = method(
            self.API_URL_TEMPLATE.format(endpoint=endpoint),
            json=data,
            headers={"X-Auth-Token": self.api_key}
        )
        request.raise_for_status()
        return request.json()["data"]

    def search_product_ids(self,
                           dataset_name: str) -> dict:
        """ Retrieve the ID for an Earth Explorer product.

        Args:
            dataset_name: The name of the data set.

        Returns: The response from the API.
        """

        return self.request(
            endpoint="dataset-download-options",
            data={"datasetName": dataset_name}
        )

    def get_product_id(self,
                       dataset_name: str,
                       product_name: str
                       ) -> str:
        """ Retrieve the ID for an Earth Explorer product.

        Args:
            dataset_name: The name of the data set.
            product_name: The name of the product.

        Returns: A string corresponding to the product ID.
        """

        logging.debug("Retrieving product IDs for dataset {}".format(dataset_name))
        products = self.request(
            endpoint="dataset-download-options",
            data={"datasetName": dataset_name}
        )
        product_id = next(
            product["productId"]
            for product in products
            if product["productName"] == product_name
        )
        logging.debug("Product ID for {}::{}: {}".format(
            dataset_name, product_name, product_id
        ))
        return product_id

    def search_downloads(self, **kwargs) -> dict:
        """ Small wrapper around download-search. """

        return self.request(
            endpoint="download-search",
            data=kwargs
        )

    def search_scenes(self,
                      dataset_name: str,
                      chunk_size: int = 10000) -> typing.Iterable[dict]:
        """ Search for scenes from a given data set.

        Args:
            dataset_name: The name of the data set.
            chunk_size: The number of scenes to retrieve in each request.

        Returns: An iterable yielding dicts that contain scene information.
        """

        scenes_dir = os.path.join(self.output_dir, "scenes")
        scenes_file = os.path.join(scenes_dir, "{}.ndjson.gz".format(dataset_name))

        if not os.path.isdir(scenes_dir):
            os.makedirs(scenes_dir)

        if not os.path.isfile(scenes_file):
            logging.info("{} does not exist; fetching scenes from API".format(scenes_file))
            temp_file = scenes_file + ".part"
            total_scenes = "???"
            total_pages = "???"
            page = 0
            with gzip.open(temp_file, "wt") as f:
                while True:

                    start_at = chunk_size * page + 1

                    logging.debug("Requesting: page {} / {} (results {}-{} / {})".format(
                        page, total_pages, start_at, start_at + chunk_size, total_scenes
                    ))

                    scenes_response = self.request(
                        endpoint="scene-search",
                        data={
                            "datasetName": dataset_name,
                            "maxResults": chunk_size,
                            "startingNumber": chunk_size * page
                        }
                    )

                    for scene in scenes_response["results"]:
                        f.write(json.dumps(scene))
                        f.write("\n")

                    total_scenes = scenes_response["totalHits"]
                    total_pages = int(total_scenes / chunk_size) + 1
                    page += 1

                    if scenes_response["recordsReturned"] < chunk_size:
                        logging.debug("Finished retrieving scene metadata")
                        break

            os.rename(temp_file, scenes_file)

        else:
            logging.debug("Reading scenes from {}".format(scenes_file))

        with gzip.open(scenes_file, "rt") as f:
            for line in f:
                yield json.loads(line)

    def search_products(self,
                        dataset_name: str) -> dict:
        """ Search available products for a given data set.

        Args:
            dataset_name: The name of the data set.

        Returns: The response from the API.
        """

        return self.request(
            endpoint="dataset-download-options",
            data={"datasetName": dataset_name}
        )

    def request_downloads(self,
                          scenes: typing.Iterable[dict],
                          product_id: str,
                          label: typing.Optional[str] = None
                          ) -> dict:
        """ Request downloads from the M2M API.

        Args:
            scenes: A list of scenes produced by `self.search_scenes`.
            product_id: An Earth Explorer product ID produced by
                `self.get_product_id`
            label: A string to label the download requests with.

        Returns: The response from the API.
        """

        if not label:
            label = datetime.datetime.now().isoformat()
            logging.debug("No download label specified; using {}".format(label))

        if type(scenes) is not list:
            scenes = list(scenes)

        logging.debug("Sending download request ({} scenes)".format(len(scenes)))
        return self.request(
            endpoint="download-request",
            data={
                "downloads": [
                    {
                        "entityId": scene["entityId"],
                        "productId": product_id
                    }
                    for scene in scenes
                ],
                "label": label
            }
        )

    def retrieve_downloads(self,
                           label: typing.Optional[str] = None,
                           loop: bool = True,
                           loop_delay: int = 2) -> list[dict]:
        """ Retrieve processed downloads from the M2M API.

        Args:
            label: A string previously used to label downloads in
                `self.request_downloads`.
            loop: If True, continue checking for downloads until the processing
                queue is empty, then return all downloads from the last check.
            loop_delay: How long to wait between loops.

        Returns: A list of processed and active downloads.
        """

        first = False

        while True:
            try:
                data = {}
                if label:
                    data = {"label": label}

                downloads = self.request(
                    endpoint="download-retrieve",
                    data=data
                )

                logging.debug("{} scenes in queue".format(downloads["queueSize"]))

                if (not loop) or (downloads["queueSize"] == 0):
                    return downloads["available"]

                if not first:
                    logging.info("Waiting for queue to empty ({} items)".format(downloads["queueSize"]))
                    first = True

                time.sleep(loop_delay)

            except KeyboardInterrupt:
                break

    def download_scenes(self,
                        scenes: typing.Iterable[dict],
                        dataset_name: str,
                        product_id: str = None,
                        threads: int = 10,
                        show_progress: bool = True):
        """ Download scenes from Earth Explorer.

        This function runs the full pipeline of request_downloads ->
        download_scenes.

        Args:
            scenes: A list of scenes produced by `self.search_scenes`.
            dataset_name: The name of the data set.
            product_id: An Earth Explorer product ID produced by
                `self.get_product_id`
            threads: The number of threads to use to retrieve scenes.
            show_progress: If True, shows the download progress on a per-file
                basis using `tqdm.tqdm`.
        """

        downloads_dir = os.path.join(self.output_dir, "downloads", dataset_name)

        label = datetime.datetime.now().isoformat()

        if not os.path.isdir(downloads_dir):
            os.makedirs(downloads_dir)

        self.request_downloads(
            scenes=scenes, product_id=product_id, label=label
        )

        downloads = self.retrieve_downloads(label)
        if show_progress:
            downloads = tqdm.tqdm(
                downloads,
                desc="Downloading files",
                unit=" files",
                miniters=1
            )

        joblib.Parallel(n_jobs=threads)(
            joblib.delayed(download_file)(
                url=download["url"], output_dir=downloads_dir
            )
            for download in downloads
        )

    def download_scenes_chunked(self,
                                scenes: typing.Iterable[dict],
                                dataset_name: str,
                                product_id: str,
                                chunk_size: int = DEFAULT_CHUNK_SIZE,
                                *args, **kwargs):
        for i, chunk in enumerate(grouper(scenes, chunk_size)):
            logging.info("Downloading chunk {} (scenes {}-{})".format(
                i, i * chunk_size, (i + 1) * chunk_size - 1
            ))
            self.download_scenes(
                scenes=chunk,
                dataset_name=dataset_name,
                product_id=product_id,
                *args, **kwargs
            )


def main():
    import argparse
    import getpass

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u", "--user",
        help="USGS Earth Explorer login username (will also check ${} or prompt if not given)".format(
            EE_USER_ENVIRONMENT_VAR)
    )
    parser.add_argument(
        "-p", "--password",
        help="USGS Earth explorer login password (will also check ${} prompt if not given)".format(
            EE_PASSWORD_ENVIRONMENT_VAR)
    )
    parser.add_argument(
        "-v", "--verbose",
        help="Enable verbose logging",
        default=False, action="store_true"
    )

    subparsers = parser.add_subparsers(dest="command")

    subparser_list_products = subparsers.add_parser("list-products")
    subparser_list_products.add_argument("dataset_name")

    subparser_download_scenes = subparsers.add_parser("download-scenes")
    subparser_download_scenes.add_argument(
        "-c", "--chunk-size",
        help="If specified, split scene requests into chunks of this size",
        type=int
    )

    subparser_count_scenes = subparsers.add_parser("count-scenes")

    for subparser in [subparser_download_scenes, subparser_count_scenes]:
        subparser.add_argument("dataset_name")
        subparser.add_argument("product_name")
        subparser.add_argument(
            "-d", "--min-date",
            help="If specified, filter scenes such that scenes[].temporalCoverage.endDate is on or after this date"
                 " (in ISO-8601 format)",
            type=datetime.datetime.fromisoformat
        )
        subparser.add_argument(
            "-D", "--max-date",
            help="If specified, filter scenes such that scenes[].temporalCoverage.startDate is on or before this date"
                 " (in ISO-8601 format)",
            type=datetime.datetime.fromisoformat
        )
        subparser.add_argument(
            "-g", "--geo-intersects",
            help="If specified, filter scenes such that scenes[].spatialCoverage intersects this geometry"
                 " (must be readable by GDAL; requires `shapely` and `fiona` to be installed)"
        )

    args = parser.parse_args()

    username = (
            args.user
            or os.environ.get(EE_USER_ENVIRONMENT_VAR)
            or input("USGS Earth Explorer username: ")
    ).strip()

    password = (
            args.password
            or os.environ.get(EE_PASSWORD_ENVIRONMENT_VAR)
            or getpass.getpass("USGS Earth Explorer password (will not echo): ")
    ).strip()

    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=args.verbose and logging.DEBUG or logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    downloader = M2MDownloader(
        username=username,
        password=password
    )

    if args.command == "list-products":
        print(json.dumps(
            downloader.search_products(dataset_name=args.dataset_name),
            indent=4
        ))

    elif args.command in ["download-scenes", "count-scenes"]:
        scenes = downloader.search_scenes(dataset_name=args.dataset_name)
        if args.verbose:
            scenes = tqdm.tqdm(scenes, desc="Reading scenes", unit=" scenes", delay=1)
        if args.min_date:
            start_ymd = args.min_date.strftime("%Y-%m-%d")
            logging.info("Applying temporal filter: scenes[].temporalCoverage.endDate >= {}".format(start_ymd))
            scenes = (
                scene
                for scene in scenes
                if scene["temporalCoverage"]["endDate"][:10] >= start_ymd
            )
        if args.max_date:
            end_ymd = args.max_date.strftime("%Y-%m-%d")
            logging.info("Applying temporal filter: scenes[].temporalCoverage.startDate <= {}".format(end_ymd))
            scenes = (
                scene
                for scene in scenes
                if scene["temporalCoverage"]["startDate"][:10] <= end_ymd
            )
        if args.geo_intersects:
            logging.info("Applying spatial filter: scenes[].spatialCoverage.intersects({})".format(args.geo_intersects))
            # Packing all of the spatial code into here so it doesn't become a dependency unless needed

            import fiona
            import shapely.geometry
            import shapely.ops

            with fiona.open(args.geo_intersects, "r") as f:
                geo = [
                    shapely.geometry.shape(feature["geometry"])
                    for feature in f
                ]

            if len(geo) == 1:
                geo = geo[0]
            else:
                logging.debug("Calculating unary union of {} geographies".format(len(geo)))
                geo = shapely.ops.unary_union(geo)

            scenes = (
                scene
                for scene in scenes
                if shapely.geometry.shape(scene["spatialCoverage"]).intersects(geo)
            )

        if args.command == "download-scenes":
            product_id = downloader.get_product_id(
                dataset_name=args.dataset_name, product_name=args.product_name
            )
            if args.chunk_size:
                downloader.download_scenes_chunked(
                    scenes=scenes,
                    dataset_name=args.dataset_name,
                    product_id=product_id,
                    chunk_size=args.chunk_size
                )
            else:
                downloader.download_scenes(
                    scenes=scenes,
                    dataset_name=args.dataset_name,
                    product_id=product_id
                )

        elif args.command == "count-scenes":
            print("Total scenes after filtering: {}".format(sum(1 for _ in scenes)))

    logging.info("ALL OK")


if __name__ == "__main__":
    main()
