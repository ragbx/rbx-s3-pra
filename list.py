from rbx_s3 import Rbx_resource, Rbx_client
import pandas as pd


def get_Tagset(s3_client, bucket_name, key):
    response = s3_client.get_object_tagging(
        Bucket=bucket_name,
        Key=key
    )

    tagset = {}
    for tag in response['TagSet']:
        if tag['Key'] == 'uuid':
            tagset['uuid'] = tag['Value']
        if tag['Key'] == 'checksum_md5':
            tagset['checksum_md5'] = tag['Value']
    return tagset

s3_resource = Rbx_resource(user='user_test').s3_resource
s3_client = Rbx_client(user='user_test').s3_client

bucket_name = 'mediatheque-postcard'

metadata = []
bucket = s3_resource.Bucket(bucket_name)
i = 0
for obj in bucket.objects.all():
    result = {
        'key': obj.key,
        'last_modified': obj.last_modified,
        'size': obj.size,
        'storage_class': obj.storage_class
    }

    tagset = get_Tagset(s3_client, bucket_name, result['key'])

    if 'uuid' in tagset:
        result['uuid'] = tagset['uuid']
    if 'checksum_md5' in tagset:
        result['checksum_md5'] = tagset['checksum_md5']

    metadata.append(result)
    print(i)
    i += 1

metadata_df = pd.DataFrame(metadata, columns=['key', 'last_modified', 'size', 'storage_class', 'uuid', 'checksum_md5'])
metadata_df.to_csv("result/listing-mediatheque-postcard.csv", index=False)
