from unittest.mock import call
from unittest.mock import patch

from common.object_storage_adaptor.boto3_admin_client import Boto3_Admin_Client
from common.object_storage_adaptor.boto3_admin_client import get_boto3_admin_client


async def test_get_boto3_admin_client_returns_class_instance():
    admin_client = await get_boto3_admin_client('project', access_key='access key', secret_key='secret key')
    assert isinstance(admin_client, Boto3_Admin_Client)


@patch('aioboto3.Session')
async def test_boto3_admin_client_init_connection(_session):
    admin_client = Boto3_Admin_Client('project', access_key='access key', secret_key='secret key')
    await admin_client.init_connection()

    # Asserting that boto3 session was created with correct credentials
    _session.assert_called_with(
        aws_access_key_id='access key',
        aws_secret_access_key='secret key',
    )
    # Asserting that admin client have a mocked boto3 session
    assert admin_client._session == _session()


@patch('aioboto3.Session.client')
async def test_boto3_admin_client_creates_correct_bucket(_client):
    admin_client = Boto3_Admin_Client('project', access_key='access key', secret_key='secret key')
    await admin_client.init_connection()
    await admin_client.create_bucket('test.bucket')

    assert _client.call_count == 1
    _client.assert_has_calls(
        [
            call().__aenter__().create_bucket(Bucket='test.bucket'),
        ]
    )
