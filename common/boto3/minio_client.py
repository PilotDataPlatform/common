import aioboto3
import requests
import xmltodict
import asyncio

from botocore.client import Config


_SIGNATURE_VERSTION = 's3v4'

async def get_minio_client(minio_endpoint:str, token:str=None):
    mc = Minio_Client(minio_endpoint, token)
    await mc.init_connection()

    return mc


class Minio_Client:

    def __init__(self, minio_endpoint:str, token:str) -> None:
        
        self.minio_endpoint = minio_endpoint
        self.token= token

        self._config = Config(signature_version=_SIGNATURE_VERSTION)
        self._session = None


    async def init_connection(self):
        # ask minio to give the temperary credentials
        credentials = await self._get_sts(self.token)

        self._session = aioboto3.Session(
            aws_access_key_id=credentials.get("AccessKeyId"),
            aws_secret_access_key=credentials.get("SecretAccessKey"),
            aws_session_token=credentials.get("SessionToken")
        )

        return

    async def _get_sts(self, access_token: str, duration: int=86000) -> dict:

        result = requests.post(
            self.minio_endpoint,
            params={
                "Action": "AssumeRoleWithWebIdentity",
                "WebIdentityToken": access_token,
                "Version": "2011-06-15",
                "DurationSeconds": duration,
            },
            verify=False
        )

        # TODO add the secret
        sts_info = xmltodict.parse(result.text)\
            .get("AssumeRoleWithWebIdentityResponse", {})\
            .get("AssumeRoleWithWebIdentityResult", {})\
            .get("Credentials", {})

        return sts_info

    async def downlaod_object(self, bucket: str, key: str, local_path: str) -> None:

        async with self._session.client('s3', endpoint_url=self.minio_endpoint, config=self._config) as s3:
            await s3.download_file(bucket, key, local_path)

    
    async def copy_object(self, bucket: str, source: str, destination: str):

        async with self._session.client('s3', endpoint_url=self.minio_endpoint, config=self._config) as s3:
            await s3.copy_object(Bucket=bucket, CopySource=source, Key=destination)


    async def get_download_presigned_url(self, bucket: str, key: str, duration: int=3600) -> str:

        async with self._session.client('s3', endpoint_url=self.minio_endpoint, config=self._config) as s3:
            presigned_url = await s3.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket,
                    'Key': key
                },
                ExpiresIn=duration
            )

        return presigned_url

    async def prepare_multipart_upload(self, bucket: str, key: str) -> str:

        async with self._session.client('s3', endpoint_url=self.minio_endpoint, config=self._config) as s3:
            res = await s3.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = res.get("UploadId")

        return upload_id


    async def part_upload(self, bucket: str, key: str, upload_id: str, part_number: int, content: str) -> dict:

        async with self._session.client('s3', endpoint_url=self.minio_endpoint, config=self._config) as s3:
            signed_url = await s3.generate_presigned_url(
                ClientMethod='upload_part',
                Params={
                    'Bucket': bucket,
                    'Key': key,
                    'UploadId': upload_id,
                    'PartNumber': part_number
                }
            )
            res = requests.put(signed_url, data=content)

        etag = res.headers.get("ETag").replace("\"", "")
        
        return {'ETag': etag, 'PartNumber': part_number}

    
    async def combine_chunks(self, bucket: str, key: str, upload_id: str, parts: list) -> dict:

        async with self._session.client('s3', endpoint_url=self.minio_endpoint, config=self._config) as s3:
            res = await s3.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                MultipartUpload={'Parts': parts},
                UploadId=upload_id
            )

        return res
