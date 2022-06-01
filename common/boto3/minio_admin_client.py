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
import httpx
import hashlib

from botocore.client import Config

from minio import Minio
from minio.signer import sign_v4_s3
from minio.helpers import url_replace

_SIGNATURE_VERSTION = 's3v4'

async def get_minio_admin_client(minio_endpoint:str, access_key:str, secret_key:str):

    mc = Minio_Admin_Client(minio_endpoint, access_key, secret_key)
    await mc.init_connection()

    return mc


class _Minio(Minio):
    """
    Summary:
        The inherit class from minio. The main reason is that the python
        minio deosn't support the IAM policy create. We setup the logic by
        our own.
    """

    async def create_IAM_policy(self, policy_name:str, content:str):
        # fetch the credential to generate headers
        creds = self._provider.retrieve() if self._provider else None
        
        # use native BaseURL class to follow the pattern
        url = self._base_url.build(
            "PUT",
            'us-east-1',
            query_params={"name": policy_name}
        )
        url = url_replace(url, path='/minio/admin/v3/add-canned-policy')

        headers = None
        headers, date = self._build_headers(url.netloc, headers, content, creds)
        # make the signiture of request
        headers = sign_v4_s3(
            "PUT",
            url,
            'us-east-1',
            headers,
            creds,
            hashlib.sha256(content.encode()).hexdigest(),
            date,
        )

        # sending to minio server to create IAM policy
        str_endpoint = url.scheme + "://" + url.netloc
        async with httpx.AsyncClient() as client:
            response = await client.put(
                str_endpoint + "/minio/admin/v3/add-canned-policy",
                params={"name": policy_name},
                headers=headers,
                data=content,
            )

            if response.status_code != 200:
                raise Exception("Fail to create minio policy")


class Minio_Admin_Client:
    """
    Summary:
        The object client for minio admin operation. The class is based on
        the admin credentials to make the operations including:
            - create bucket in minio
            - create IAM role in minio
    """

    def __init__(self, minio_endpoint:str, access_key:str, secret_key:str, https:bool=False) -> None:
        """
        Parameter:
            - minio_endpoint(string): the endpoint of minio(no http schema)
            - access_key(str): the access key of minio
            - secret_key(str): the secret key of minio
            - https(bool): the bool to indicate if it is https connection
        """

        http_prefix = 'https://' if https else 'http://'
        self.minio_endpoint = http_prefix + minio_endpoint
        self.access_key = access_key
        self.secret_key = secret_key

        self._config = Config(signature_version=_SIGNATURE_VERSTION)
        self._session = None

        self._minio = _Minio(minio_endpoint, access_key, secret_key, secure=https)


    async def init_connection(self):

        # todo in the thread pool
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

        async with self._session.client('s3', endpoint_url=self.minio_endpoint, config=self._config) as s3:
            res = await s3.create_bucket(Bucket=bucket)

        return res

    async def create_policy(self, policy_name:str, content:str):
        """
        Summary:
            The function will use create the IAM policy in minio server.
            for policy detail, please check the minio document:
            https://docs.min.io/minio/baremetal/security/minio-identity-management/policy-based-access-control.html

        Parameter:
            - policy_name(str): the policy name
            - content(str): the string content of policy

        return:
            - None
        """
        
        await self._minio.create_IAM_policy(policy_name, content)

        return
        