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


async def test_create_iam_policy_success(httpx_mock):
    httpx_mock.add_response(
        method='PUT', url='https://project/minio/admin/v3/add-canned-policy?name=test+policy', status_code=200
    )

    minio_client = MinioPolicyClient(
        'project', access_key='access key', secret_key='secret key', session_token='token'
    )
    await minio_client.create_IAM_policy('test policy', 'test content')


async def test_create_iam_policy_fail(httpx_mock):
    httpx_mock.add_response(
        method='PUT', url='https://project/minio/admin/v3/add-canned-policy?name=test+policy', status_code=500
    )

    minio_client = MinioPolicyClient(
        'project', access_key='access key', secret_key='secret key', session_token='token'
    )

    try:
        await minio_client.create_IAM_policy('test policy', 'test content')
    except Exception as e:
        assert str(e) == 'Fail to create minio policy:'


async def test_create_iam_policy(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='https://project/minio/admin/v3/info-canned-policy?name=test_policy&v=2',
        json={},
        status_code=200,
    )

    minio_client = MinioPolicyClient(
        'project', access_key='access key', secret_key='secret key', session_token='token'
    )
    # note name has to be the same with mock
    await minio_client.get_IAM_policy('test_policy')


async def test_create_iam_policy_not_exist(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='https://project/minio/admin/v3/info-canned-policy?name=test_policy&v=2',
        json={},
        status_code=404,
    )

    minio_client = MinioPolicyClient(
        'project', access_key='access key', secret_key='secret key', session_token='token'
    )
    # note name has to be the same with mock
    try:
        policy_name = "test_policy"
        await minio_client.get_IAM_policy(policy_name)
    except PolicyDoesNotExist as e:
        assert str(e) == 'Policy %s does not exist'%policy_name


async def test_create_iam_policy_error(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='https://project/minio/admin/v3/info-canned-policy?name=test_policy&v=2',
        json={},
        status_code=500,
    )

    minio_client = MinioPolicyClient(
        'project', access_key='access key', secret_key='secret key', session_token='token'
    )
    # note name has to be the same with mock
    try:
        policy_name = "test_policy"
        await minio_client.get_IAM_policy(policy_name)
    except Exception as e:
        assert str(e) == 'Fail to get minio policy:{}'