import pandas as pd
from bs4 import BeautifulSoup
import logging
import argparse
import json
from google.cloud import storage
from google.cloud.storage import Blob
from urllib.request import urlopen

logging.getLogger().setLevel(logging.INFO)

project_name = 'RawImportSweGridAreasGeo'


def hist_table_from_html(html, year):
    logging.debug("Started hist_table_from_html year={}".format(year))
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.findAll('table')
    for table in tables:
        length = len(table.findAll('tr'))
        logging.debug("length={}".format(length))
        if length in [55,56]:
            df_list = pd.read_html(str(table))
            if len(df_list) == 1:
                df = df_list[0]\
                    .dropna(axis=0, how='all')\
                    .drop(columns=[2,3,4,5])\
                    .drop(0)\
                    .assign(Year=year)\
                    .rename(columns={0: 'Week', 1: 'Spot'})
                logging.debug(df)
                return df


def upload_blob_string(bucket_name, csvString, destination_blob_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = Blob(destination_blob_name, bucket)
    return blob.upload_from_string(
        data=csvString,
        content_type='text/csv')


def run(request):
    logging.info("Starting {}".format(project_name))
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', help='json with parameters')
    args = parser.parse_args()
    if request is None:
       logging.info('run(...): data=' + args.data)
       input_json = json.loads(str(args.data).replace("'", ""))
    else:
       input_json = request.get_json()
    logging.info("request.args: {}".format(input_json))
    bucket_name = input_json['bucket_target']
    base_url = input_json['url']
    destination_blob_name = input_json['destination_blob_name']
    logging.info("\nbucket_name: {}\nurl: {}\ndestination_blob_name: {}".format(
       bucket_name, base_url, destination_blob_name))
    # TODO add metadata handling
    df = pd.DataFrame(columns = ['Year', 'Week', 'Spot'])
    for y in [i for i in range(2005, 2022)]:
        url = '{}{}/'.format(base_url, y)
        html = urlopen(url).read()
        logging.debug("html size={}".format(len(html)))
        logging.debug("appende df. df={}".format(df))
        df = df.append(hist_table_from_html(html, str(y)))
    csv_string = df.to_csv(index=False, sep=';')
    upload_blob_string(bucket_name, csv_string, destination_blob_name)
    logging.info("Uploaded size={} to bucket {} and {}".format(df.size, bucket_name, destination_blob_name))


if __name__ == '__main__':
    run(None)