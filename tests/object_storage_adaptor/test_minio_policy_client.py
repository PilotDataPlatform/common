from logging import ERROR, DEBUG

from common.object_storage_adaptor.minio_policy_client import MinioPolicyClient, PolicyDoesNotExist
from common.object_storage_adaptor.minio_policy_client import get_minio_policy_client

async def test_boto3_client_check_log_level_debug():
    boto3_client = MinioPolicyClient(endpoint='project', access_key='access key', secret_key='secret key')
    assert boto3_client.logger.level == ERROR

    await boto3_client.debug_on()
    assert boto3_client.logger.level == DEBUG


async def test_boto3_client_check_log_level_ERROR():
    boto3_client = MinioPolicyClient(endpoint='project', access_key='access key', secret_key='secret key')
    await boto3_client.debug_on()
    assert boto3_client.logger.level == DEBUG
    
    await boto3_client.debug_off()
    assert boto3_client.logger.level == ERROR

async def test_get_minio_policy_client_returns_class_instance():
    minio_client = await get_minio_policy_client('project', access_key='access key', secret_key='secret key')
    assert isinstance(minio_client, MinioPolicyClient)


async def test_create_iam_policy(mock_put_add_policy):
    minio_client = MinioPolicyClient(
        'project', access_key='access key', secret_key='secret key', session_token='token'
    )
    await minio_client.create_IAM_policy('test policy', 'test content')


async def test_create_iam_policy(mock_get_get_policy):
    minio_client = MinioPolicyClient(
        'project', access_key='access key', secret_key='secret key', session_token='token'
    )
    # note name has to be the same with mock
    await minio_client.get_IAM_policy('test_policy')


