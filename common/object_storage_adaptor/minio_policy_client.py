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

import hashlib

import httpx
from minio import Minio
from minio.helpers import url_replace
from minio.signer import sign_v4_s3


class PolicyDoesNotExist(Exception):
    pass


async def get_minio_policy_client(endpoint: str, access_key: str, secret_key: str, https: bool = False):

    mc = MinioPolicyClient(endpoint, access_key, secret_key, secure=https)

    return mc


class MinioPolicyClient(Minio):
    """
    Summary:
        The inherit class from minio. The main reason is that the python
        minio deosn't support the IAM policy create. And the boto3.s3 cannot
        fulfill the requirement as well.

        The ONLY way to create policy is to use the minio client binary.
        Keeping the binary in common package is not a good idea. Thus, We
        setup the logic by our own.
    """

    async def create_IAM_policy(self, policy_name: str, content: str, region: str = 'us-east-1'):
        """
        Summary:
            The function will use create the IAM policy in minio server.
            for policy detail, please check the minio document:
            https://docs.min.io/minio/baremetal/security/minio-identity-management/policy-based-access-control.html

        Parameter:
            - policy_name(str): the policy name
            - content(str): the string content of policy
            - region(str): the region of service (default is us-east-1)

        return:
            - None
        """

        # fetch the credential to generate headers
        creds = self._provider.retrieve() if self._provider else None

        # use native BaseURL class to follow the pattern
        params = {'name': policy_name}
        url = self._base_url.build('PUT', region, query_params=params)
        url = url_replace(url, path='/minio/admin/v3/add-canned-policy')

        headers = None
        headers, date = self._build_headers(url.netloc, headers, content, creds)
        # make the signiture of request
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        headers = sign_v4_s3('PUT', url, region, headers, creds, content_hash, date)

        # sending to minio server to create IAM policy
        str_endpoint = url.scheme + '://' + url.netloc
        async with httpx.AsyncClient() as client:
            response = await client.put(
                str_endpoint + url.path,
                params=params,
                headers=headers,
                data=content,
            )

            if response.status_code != 200:
                raise Exception('Fail to create minio policy:' + str(response.text))

        return 'success'

    async def get_IAM_policy(self, policy_name: str, region: str = 'us-east-1'):
        """
        Summary:
            The function will use get the IAM policy in minio server.

        Parameter:
            - policy_name(str): the policy name to get
            - region(str): the region of service (default is us-east-1)

        return:
            - dict
        """

        # fetch the credential to generate headers
        creds = self._provider.retrieve() if self._provider else None

        # use native BaseURL class to follow the pattern
        params = {'name': policy_name, 'v': '2'}
        url = self._base_url.build('GET', region, query_params=params)
        url = url_replace(url, path='/minio/admin/v3/info-canned-policy')

        headers = None
        headers, date = self._build_headers(url.netloc, headers, '', creds)
        # make the signiture of request
        content_hash = hashlib.sha256(''.encode()).hexdigest()
        headers = sign_v4_s3('GET', url, region, headers, creds, content_hash, date)

        # sending to minio server to create IAM policy
        str_endpoint = url.scheme + '://' + url.netloc
        async with httpx.AsyncClient() as client:
            response = await client.get(
                str_endpoint + url.path,
                params=params,
                headers=headers,
            )

            if response.status_code == 404:
                raise PolicyDoesNotExist('Policy %s does not exist' % policy_name)
            elif response.status_code != 200:
                raise Exception('Fail to get minio policy' + str(response.text))

        return response.json()
