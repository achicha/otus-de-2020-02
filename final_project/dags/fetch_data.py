import logging
from tempfile import NamedTemporaryFile
import requests
import xmltodict

try:
    from airflow.hooks import GoogleCloudStorageHook
except:
    from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


def get_files_links(execution_date):
    """ get rides files from https://cycling.data.tfl.gov.uk/ """

    url = 'https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/'
    headers = {'User-Agent': 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'}
    data = requests.get(url=url, headers=headers, stream=True)

    xpars = xmltodict.parse(data.text)
    links = [ 'https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/' + i['Key']
              for i in xpars['ListBucketResult']['Contents']
              if i['Key'].startswith("usage") and i['LastModified'].startswith(execution_date)]
    logging.info(f'found {len(links)} files')
    return links


def fetch(file_url):
    """ download data for each file"""

    tmp_file_handle = NamedTemporaryFile(delete=True)
    headers = {'User-Agent': 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'}

    # download file and save to temp object
    with requests.get(file_url, headers=headers, stream=True) as r:
        tmp_file_handle.write(r.content)

    tmp_file_handle.flush()

    return tmp_file_handle


def upload_to_gcs(file_name, tmp_obj_name, google_cloud_storage_conn_id, gcs_bucket):
    """upload file to GCS"""

    gcs_hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=google_cloud_storage_conn_id)
    gcs_hook.upload(bucket=gcs_bucket,
                    object=file_name,
                    filename=tmp_obj_name,
                    gzip=True)
    logging.info(f'new file created {file_name}')


def upload_files(execution_date, google_cloud_storage_conn_id, gcs_bucket):
    links = get_files_links(execution_date=execution_date)

    if links:
        number_uploaded_files = len(links)
        for link in links:
            fn = f"{execution_date}/{link.split('/')[-1]}.csv.gz"
            tmp_obj = fetch(file_url=link)
            upload_to_gcs(fn, tmp_obj.name, google_cloud_storage_conn_id, gcs_bucket)
            tmp_obj.close()
    else:
        number_uploaded_files = 0

    return number_uploaded_files