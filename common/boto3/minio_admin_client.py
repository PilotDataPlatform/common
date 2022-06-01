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

    async def create_IAM_policy(self, policy_name:str, content:str):
        
        creds = self._provider.retrieve() if self._provider else None
        
        url = self._base_url.build(
            "PUT",
            'us-east-1',
            query_params={"name": policy_name}
        )
        url = url_replace(url, path='/minio/admin/v3/add-canned-policy')

        headers = None
        headers, date = self._build_headers(url.netloc, headers, content, creds)

        headers = sign_v4_s3(
            "PUT",
            url,
            'us-east-1',
            headers,
            creds,
            hashlib.sha256(content.encode()).hexdigest(),
            date,
        )

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

    def __init__(self, minio_endpoint:str, access_key:str, secret_key:str, https:bool=False) -> None:

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

        async with self._session.client('s3', endpoint_url=self.minio_endpoint, config=self._config) as s3:
            res = await s3.create_bucket(Bucket=bucket)

        return res

    async def create_policy(self, policy_name:str, content:str):
        
        await self._minio.create_IAM_policy(policy_name, content)

        return
        