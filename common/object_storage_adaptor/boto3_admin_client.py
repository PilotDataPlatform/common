# Copyright (C) 2022 Indoc Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import aioboto3

from botocore.client import Config

_SIGNATURE_VERSTION = 's3v4'


async def get_boto3_admin_client(endpoint:str, access_key:str, secret_key:str):

    mc = Boto3AdminClient(endpoint, access_key, secret_key)
    await mc.init_connection()

    return mc


class Boto3AdminClient:
    """
    Summary:
        The object client for minio admin operation. The class is based on
        the admin credentials to make the operations including:
            - create bucket in minio
            - create IAM role in minio
    """

    def __init__(self, endpoint:str, access_key:str, secret_key:str, https:bool=False) -> None:
        """
        Parameter:
            - endpoint(string): the endpoint of minio(no http schema)
            - access_key(str): the access key of minio
            - secret_key(str): the secret key of minio
            - https(bool): the bool to indicate if it is https connection
        """

        http_prefix = 'https://' if https else 'http://'
        self.endpoint = http_prefix + endpoint
        self.access_key = access_key
        self.secret_key = secret_key

        self._config = Config(signature_version=_SIGNATURE_VERSTION)
        self._session = None

    async def init_connection(self):

        self._session = aioboto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

    async def create_bucket(self, bucket:str):
        """
        Summary:
            The function will create new bucket in minio. The name contraint is following:
            - Bucket names must be between 3 and 63 characters long.
            - Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
            - Bucket names must begin and end with a letter or number.
            - Bucket names must not be formatted as an IP address (for example, 192.168.5.4).
            - Bucket names can't begin with xn-- (for buckets created after February 2020).
            - Bucket names must be unique within a partition.
            - Buckets used with Amazon S3 Transfer Acceleration can't have dots (.) 
                in their names. For more information about transfer acceleration, 
                see Amazon S3 Transfer Acceleration.

        Parameter:
            - bucket(str): the policy name

        return:
            - dict
        """

        async with self._session.client('s3', endpoint_url=self.endpoint, config=self._config) as s3:
            res = await s3.create_bucket(Bucket=bucket)

        return res
        