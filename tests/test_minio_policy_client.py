from common.object_storage_adaptor.minio_policy_client import Minio_Policy_Client
from common.object_storage_adaptor.minio_policy_client import get_minio_policy_client


async def test_get_minio_policy_client_returns_class_instance():
    minio_client = await get_minio_policy_client('project', access_key='access key', secret_key='secret key')
    assert isinstance(minio_client, Minio_Policy_Client)


async def test_create_iam_policy(mock_put_add_policy):
    minio_client = Minio_Policy_Client(
        'project', access_key='access key', secret_key='secret key', session_token='token'
    )
    await minio_client.create_IAM_policy('test policy', 'test content')
