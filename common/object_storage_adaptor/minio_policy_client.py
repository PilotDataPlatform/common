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

import httpx
import hashlib

from minio import Minio
from minio.signer import sign_v4_s3
from minio.helpers import url_replace


async def get_minio_policy_client(endpoint:str, access_key:str, secret_key:str, https:bool=False):

    mc = Minio_Policy_Client(endpoint, access_key, secret_key, secure=https)

    return mc


class Minio_Policy_Client(Minio):
    """
    Summary:
        The inherit class from minio. The main reason is that the python
        minio deosn't support the IAM policy create. And the boto3.s3 cannot
        fulfill the requirement as well.

        The ONLY way to create policy is to use the minio client binary.
        Keeping the binary in common package is not a good idea. Thus, We
        setup the logic by our own.
    """

    async def create_IAM_policy(self, policy_name:str, content:str):
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