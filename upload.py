from rbx_s3 import Rbx_client
import pandas as pd
from os.path import join, sep
import csv

import concurrent.futures

USER = 'user_test'
bucket = 'mediatheque-postcard'
rbx_client = Rbx_client(user=USER)

data_file_name = "files2load"
result_file = join("result", f"{data_file_name}_upload.csv")
prefix = 'data'

# On crée une liste de dictionnaires en entrée de la fonction rbx_upload_file :
def get_files2upload(data_file_name, rbx_client, bucket, prefix):
    files2upload = []

    data2upload = pd.read_csv(join("data", f"{data_file_name}.csv"))
    for file in data2upload.to_dict(orient='records'):
        file_path = file['path'].replace("/", sep)
        if prefix:
            file['file_name'] = join(prefix, file_path, file['name'])
        else:
            file['file_name'] = join(file_path, file['name'])

        tags = {
            'uuid': file['uuid'],
            'checksum_md5': file['checksum_md5']
        }
        file['tags_str'] = "&".join(f"{key}={value}" for key, value in tags.items())

        file['client'] = rbx_client
        file['bucket'] = bucket


        files2upload.append(file)

    return files2upload

# Fonction d'upload
def rbx_upload_file(file_data):
    upload_res = rbx_client.upload(file_data['file_name'],
                           file_data['bucket'],
                           file_data['key'],
                           ExtraArgs = {"Tagging": file_data['tags_str']})

    res2log = {
        'name': file_data['name'],
        'path': file_data['path'],
        'checksum_md5': file_data['checksum_md5'],
        'uuid': file_data['uuid'],
        'key': file_data['key'],
        'size': file_data['size'],
        'uploaded': upload_res['result'],
        'uploaded_file_size': None,
        'uploaded_file_lastmodified': None,
        'error': None
    }

    if 'error' in upload_res:
        res2log['error'] = upload_res['error']
    if 'LastModified' in upload_res:
        res2log['uploaded_file_lastmodified'] = upload_res['LastModified']
    if 'size' in upload_res:
        res2log['uploaded_file_size'] = upload_res['size']
        if res2log['uploaded_file_size'] != res2log['size']:
            res2log['error'] = 'cohérence tailles'

    return(res2log)

# Exécution
with open(result_file, 'w', newline='') as logfile:
    fieldnames = ['name', 'path', 'checksum_md5', 'uuid', 'size', 'key', 'uploaded',
                  'uploaded_file_size', 'uploaded_file_lastmodified', 'error']
    writer = csv.DictWriter(logfile, fieldnames=fieldnames)
    writer.writeheader()

    files2upload = get_files2upload(data_file_name, rbx_client, bucket, prefix)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for file_data in files2upload:
            futures.append(executor.submit(rbx_upload_file, file_data=file_data))
        for future in concurrent.futures.as_completed(futures):
            res2log = future.result()
            writer.writerow(res2log)
