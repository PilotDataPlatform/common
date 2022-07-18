from logging import ERROR, DEBUG

from unittest.mock import call
from unittest.mock import patch

from common.object_storage_adaptor.boto3_client import Boto3Client
from common.object_storage_adaptor.boto3_client import get_boto3_client
from tests.conftest import PROJECT_CREDENTIALS


async def test_boto3_client_check_log_level_debug():
    boto3_client = Boto3Client(endpoint='project', token='test')
    assert boto3_client.logger.level == ERROR

    await boto3_client.debug_on()
    assert boto3_client.logger.level == DEBUG


async def test_boto3_client_check_log_level_ERROR():
    boto3_client = Boto3Client(endpoint='project', token='test')
    await boto3_client.debug_on()
    assert boto3_client.logger.level == DEBUG
    
    await boto3_client.debug_off()
    assert boto3_client.logger.level == ERROR


@patch('aioboto3.Session')
async def test_boto3_client_init_connection_with_token_requests_credentials_creates_boto3_session(
    _session, mock_post_by_token
):
    boto3_client = Boto3Client(endpoint='project', token='test')
    await boto3_client.init_connection()

    _session.assert_called_with(
        aws_access_key_id=PROJECT_CREDENTIALS.get('AccessKeyId'),
        aws_secret_access_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
        aws_session_token=PROJECT_CREDENTIALS.get('SessionToken'),
    )
    # Asserting that boto3_client have a mocked boto3 session
    assert boto3_client._session == _session()


@patch('aioboto3.Session')
async def test_get_boto3_client(_session):
    boto3_client = await get_boto3_client(
        endpoint='project',
        access_key=PROJECT_CREDENTIALS.get('AccessKeyId'),
        secret_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
    )

    # Asserting that we get a correct boto3 client class
    assert isinstance(boto3_client, Boto3Client)
    _session.assert_called_with(
        aws_access_key_id=PROJECT_CREDENTIALS.get('AccessKeyId'),
        aws_secret_access_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
        aws_session_token=None,
    )


@patch('aioboto3.Session.client')
async def test_boto3_client_downloads_the_file_from_s3(_client):
    boto3_client = Boto3Client(
        endpoint='project',
        access_key=PROJECT_CREDENTIALS.get('AccessKeyId'),
        secret_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
    )
    await boto3_client.init_connection()
    args = ('test', '/test/path', './')
    await boto3_client.download_object(*args)

    assert _client.call_count == 1
    _client.assert_has_calls(
        [
            call().__aenter__().download_file(*args),
        ]
    )


@patch('aioboto3.Session.client')
async def test_boto3_client_copy_file_copies_the_file_from_s3(_client):
    boto3_client = Boto3Client(
        endpoint='project',
        access_key=PROJECT_CREDENTIALS.get('AccessKeyId'),
        secret_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
    )
    await boto3_client.init_connection()
    await boto3_client.copy_object('test', '/test/path', 'test', '/path/new')

    assert _client.call_count == 1
    _client.assert_has_calls(
        [
            call().__aenter__().copy_object(Bucket='test', CopySource='/test/path', Key='/path/new'),
        ]
    )


@patch('aioboto3.Session.client')
async def test_boto3_client_download_presigned_url_gets_download_presigned_url(_client):
    boto3_client = Boto3Client(
        endpoint='project',
        access_key=PROJECT_CREDENTIALS.get('AccessKeyId'),
        secret_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
    )
    await boto3_client.init_connection()
    await boto3_client.get_download_presigned_url('test', '/test/path')

    assert _client.call_count == 1
    _client.assert_has_calls(
        [
            call()
            .__aenter__()
            .generate_presigned_url('get_object', Params={'Bucket': 'test', 'Key': '/test/path'}, ExpiresIn=3600)
        ]
    )


@patch('aioboto3.Session.client')
async def test_boto3_client_prepare_multipart_upload_creates_multipart_upload(_client):
    boto3_client = Boto3Client(
        endpoint='project',
        access_key=PROJECT_CREDENTIALS.get('AccessKeyId'),
        secret_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
    )
    await boto3_client.init_connection()
    keys = ['/test/path', '/test/path2', '/test/path3']
    await boto3_client.prepare_multipart_upload('test', keys)

    # We are still calling client only once
    assert _client.call_count == 1
    _client.assert_has_calls(
        [
            call().__aenter__().create_multipart_upload(Bucket='test', Key='/test/path'),
            call().__aenter__().create_multipart_upload(Bucket='test', Key='/test/path2'),
            call().__aenter__().create_multipart_upload(Bucket='test', Key='/test/path3'),
        ],
        any_order=True,
    )


@patch('aioboto3.Session.client')
async def test_boto3_client_combine_chunks_combines_chunks(_client):
    boto3_client = Boto3Client(
        endpoint='project',
        access_key=PROJECT_CREDENTIALS.get('AccessKeyId'),
        secret_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
    )
    await boto3_client.init_connection()
    await boto3_client.combine_chunks('test', '/test/path', 'test', ['part_dict1', 'part_dict2', 'part_dict3'])

    assert _client.call_count == 1
    _client.assert_has_calls(
        [
            call()
            .__aenter__()
            .complete_multipart_upload(
                Bucket='test',
                Key='/test/path',
                MultipartUpload={'Parts': ['part_dict1', 'part_dict2', 'part_dict3']},
                UploadId='test',
            )
        ]
    )
