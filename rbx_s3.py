from yaml import safe_load
import os.path
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

class Config():
    def __init__(self, user='user_ro'):
        my_path = os.path.abspath(os.path.dirname(__file__))
        config_file_path = os.path.join(my_path, 'conf.yml')
        with open(config_file_path, 'r') as stream:
            try:
                self.s3_conf = safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        self.user = user

    def get_endpoint_url(self):
        return self.s3_conf['s3_conf']['endpoint_url']

    def get_aws_access_key_id(self):
        return self.s3_conf['s3_conf'][self.user]['aws_access_key_id']

    def get_aws_secret_access_key(self):
        return self.s3_conf['s3_conf'][self.user]['aws_secret_access_key']

    def get_region_name(self):
        return self.s3_conf['s3_conf']['region_name']

class Rbx_resource():
    def __init__(self, user='user_ro'):
        config = Config(user=user)
        self.endpoint_url = config.get_endpoint_url()
        self.aws_access_key_id = config.get_aws_access_key_id()
        self.aws_secret_access_key = config.get_aws_secret_access_key()
        self.region_name = config.get_region_name()

        self.s3_resource = boto3.resource('s3',
                                          aws_access_key_id = self.aws_access_key_id,
                                          aws_secret_access_key = self.aws_secret_access_key,
                                          endpoint_url = self.endpoint_url,
                                          region_name = self.region_name)


class Rbx_client():
    def __init__(self, user='user_ro'):
        config = Config(user=user)
        self.endpoint_url = config.get_endpoint_url()
        self.aws_access_key_id = config.get_aws_access_key_id()
        self.aws_secret_access_key = config.get_aws_secret_access_key()
        self.region_name = config.get_region_name()

        self.s3_client = boto3.client('s3',
                                      aws_access_key_id = self.aws_access_key_id,
                                      aws_secret_access_key = self.aws_secret_access_key,
                                      endpoint_url = self.endpoint_url,
                                      region_name = self.region_name)

    def upload(self, file_name, bucket, object_name, ExtraArgs=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name.
        :return: True if file was uploaded, else False
        """

        # Upload the file
        upload_res = {"key": object_name}

        isfile = os.path.exists(file_name)
        if isfile:
            try:
                if ExtraArgs:
                    self.s3_client.upload_file(file_name, bucket, object_name, ExtraArgs=ExtraArgs)
                else:
                    self.s3_client.upload_file(file_name, bucket, object_name)
                response = self.s3_client.head_object(
                    Bucket=bucket,
                    Key=object_name)
                if 'LastModified' in response:
                    upload_res['LastModified'] = response['LastModified'].isoformat()#.strftime("%Y-%m-%dT%H:%M:%S")
                if 'ContentLength' in response:
                    upload_res['size'] = response['ContentLength']
                upload_res['result'] = True
            except ClientError as e:
                upload_res["result"] = False
                upload_res["error"] = e
        else:
            upload_res["result"] = False
            upload_res["error"] = "fichier absent"

        return upload_res
