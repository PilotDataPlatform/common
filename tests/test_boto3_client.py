from unittest.mock import patch

import pytest

from common.object_storage_adaptor.boto3_client import Boto3_Client
from common.object_storage_adaptor.boto3_client import get_boto3_client
from tests.conftest import PROJECT_CREDENTIALS


@patch('aioboto3.Session')
@pytest.mark.asyncio
async def test_boto3_client_init_connection_with_token(_session, redis, mock_post_by_token):
    """Testing that Boto3_client requests credentials and creates boto3 session."""
    boto3_client = Boto3_Client(endpoint='project', token='test')
    await boto3_client.init_connection()

    # Asserting that boto3 session was created with correct credentials
    _session.assert_called_with(
        aws_access_key_id=PROJECT_CREDENTIALS.get('AccessKeyId'),
        aws_secret_access_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
        aws_session_token=PROJECT_CREDENTIALS.get('SessionToken'),
    )
    # Asserting that boto3_client have a mocked boto3 session
    assert boto3_client._session == _session()


@patch('aioboto3.Session')
@pytest.mark.asyncio
async def test_get_boto3_client(_session, redis):
    boto3_client = await get_boto3_client(endpoint='project', temp_credentials=PROJECT_CREDENTIALS)

    # Asserting that we get a correct boto3 client class
    assert isinstance(boto3_client, Boto3_Client)
    # Asserting that boto3 session was created with correct credentials
    _session.assert_called_with(
        aws_access_key_id=PROJECT_CREDENTIALS.get('AccessKeyId'),
        aws_secret_access_key=PROJECT_CREDENTIALS.get('SecretAccessKey'),
        aws_session_token=PROJECT_CREDENTIALS.get('SessionToken'),
    )


@patch('aioboto3.Session.client')
@pytest.mark.asyncio
async def test_boto3_client_download_file(_client, redis):
    """Testing that Boto3_client downloads the file from s3."""
    boto3_client = Boto3_Client(endpoint='project', temp_credentials=PROJECT_CREDENTIALS)
    await boto3_client.init_connection()
    args = ('test', '/test/path', '/home/test/path')
    await boto3_client.download_object(*args)

    # Asserting that boto3 download_file method gets called with correct args
    assert _client.call_count == 1
    assert args in (call.args for call in _client.mock_calls)


@patch('aioboto3.Session.client')
@pytest.mark.asyncio
async def test_boto3_client_copy_file(_client, redis):
    """Testing that Boto3_client copies the file from s3."""
    boto3_client = Boto3_Client(endpoint='project', temp_credentials=PROJECT_CREDENTIALS)
    await boto3_client.init_connection()
    await boto3_client.copy_object('test', '/test/path', '/test/path/new')

    # Asserting that boto3 copy_file method gets called with correct kwargs
    assert _client.call_count == 1
    kwargs = {'Bucket': 'test', 'CopySource': '/test/path', 'Key': '/test/path/new'}
    assert kwargs in (call.kwargs for call in _client.mock_calls)


@patch('aioboto3.Session.client')
@pytest.mark.asyncio
async def test_boto3_client_download_presigned_url(_client, redis):
    """Testing that Boto3_client gets download presigned_url."""
    boto3_client = Boto3_Client(endpoint='project', temp_credentials=PROJECT_CREDENTIALS)
    await boto3_client.init_connection()
    await boto3_client.get_download_presigned_url('test', '/test/path')

    # Asserting that boto3 generate_presigned_url method gets called with correct kwargs
    assert _client.call_count == 1
    kwargs = {'Params': {'Bucket': 'test', 'Key': '/test/path'}, 'ExpiresIn': 3600}
    assert kwargs in (call.kwargs for call in _client.mock_calls)


@patch('aioboto3.Session.client')
@pytest.mark.asyncio
async def test_boto3_client_prepare_multipart_upload(_client, redis):
    """Testing that Boto3_client creates multipart upload."""
    boto3_client = Boto3_Client(endpoint='project', temp_credentials=PROJECT_CREDENTIALS)
    await boto3_client.init_connection()
    keys = ['/test/path', '/test/path2', '/test/path3']
    await boto3_client.prepare_multipart_upload('test', keys)

    # Asserting that boto3 create_multipart_upload method gets called with correct kwargs
    # We are still calling client only once
    assert _client.call_count == 1
    # But calling create_multipart_upload len(keys) times
    for key in keys:
        kwargs = {'Bucket': 'test', 'Key': key}
        assert kwargs in (call.kwargs for call in _client.mock_calls)


@patch('aioboto3.Session.client')
@pytest.mark.asyncio
async def test_boto3_client_combine_chunks(_client, redis):
    """Testing that Boto3_client combines chunks."""
    boto3_client = Boto3_Client(endpoint='project', temp_credentials=PROJECT_CREDENTIALS)
    await boto3_client.init_connection()
    await boto3_client.combine_chunks('test', '/test/path', 'test', ['part_dict1', 'part_dict2', 'part_dict3'])

    # Asserting that boto3 client combine chunks method gets called with correct kwargs
    assert _client.call_count == 1
    kwargs = {
        'Bucket': 'test',
        'Key': '/test/path',
        'MultipartUpload': {'Parts': ['part_dict1', 'part_dict2', 'part_dict3']},
        'UploadId': 'test',
    }
    assert kwargs in (call.kwargs for call in _client.mock_calls)
