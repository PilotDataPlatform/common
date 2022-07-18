from logging import ERROR, DEBUG

from unittest.mock import call
from unittest.mock import patch

from common.object_storage_adaptor.boto3_admin_client import Boto3AdminClient
from common.object_storage_adaptor.boto3_admin_client import get_boto3_admin_client

async def test_boto3_client_check_log_level_debug():
    boto3_client = Boto3AdminClient(endpoint='project', access_key='access key', secret_key='secret key')
    assert boto3_client.logger.level == ERROR

    await boto3_client.debug_on()
    assert boto3_client.logger.level == DEBUG


async def test_boto3_client_check_log_level_ERROR():
    boto3_client = Boto3AdminClient(endpoint='project', access_key='access key', secret_key='secret key')
    await boto3_client.debug_on()
    assert boto3_client.logger.level == DEBUG
    
    await boto3_client.debug_off()
    assert boto3_client.logger.level == ERROR

async def test_get_boto3_admin_client_returns_class_instance():
    admin_client = await get_boto3_admin_client('project', access_key='access key', secret_key='secret key')
    assert isinstance(admin_client, Boto3AdminClient)


@patch('aioboto3.Session')
async def test_boto3_admin_client_init_connection(_session):
    admin_client = Boto3AdminClient('project', access_key='access key', secret_key='secret key')
    await admin_client.init_connection()

    _session.assert_called_with(
        aws_access_key_id='access key',
        aws_secret_access_key='secret key',
    )
    # Asserting that admin client have a mocked boto3 session
    assert admin_client._session == _session()


@patch('aioboto3.Session.client')
async def test_boto3_admin_client_creates_correct_bucket(_client):
    admin_client = Boto3AdminClient('project', access_key='access key', secret_key='secret key')
    await admin_client.init_connection()
    await admin_client.create_bucket('test.bucket')

    assert _client.call_count == 1
    _client.assert_has_calls(
        [
            call().__aenter__().create_bucket(Bucket='test.bucket'),
        ]
    )


@patch('aioboto3.Session.client')
async def test_boto3_admin_client_create_bucket_encryption(_client):
    admin_client = Boto3AdminClient('project', access_key='access key', secret_key='secret key')
    await admin_client.init_connection()
    bucket = 'test.bucket'
    algo = 'AES256'
    await admin_client.create_bucket_encryption(bucket, algo)

    assert _client.call_count == 1
    _client.assert_has_calls(
        [
            call().__aenter__().put_bucket_encryption(
                Bucket=bucket,
                ServerSideEncryptionConfiguration={
                    'Rules': [
                        {
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': algo,
                            },
                        },
                    ]
                },
            ),
        ]
    )


@patch('aioboto3.Session.client')
async def test_boto3_admin_client_set_bucket_versioning(_client):
    admin_client = Boto3AdminClient('project', access_key='access key', secret_key='secret key')
    await admin_client.init_connection()
    bucket = 'test.bucket'
    status = 'Enabled'
    await admin_client.set_bucket_versioning(bucket, status)

    assert _client.call_count == 1
    _client.assert_has_calls(
        [
            call().__aenter__().put_bucket_versioning(
                Bucket=bucket,
                VersioningConfiguration={
                    'MFADelete': 'Disabled',
                    'Status': status,
                },
            ),
        ]
    )