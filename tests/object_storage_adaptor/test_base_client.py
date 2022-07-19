from logging import ERROR, DEBUG

from common.object_storage_adaptor.base_client import BaseClient


async def test_boto3_client_check_log_level_debug():
    boto3_client = BaseClient(client_name='test')
    assert boto3_client.logger.level == ERROR

    await boto3_client.debug_on()
    assert boto3_client.logger.level == DEBUG


async def test_boto3_client_check_log_level_ERROR():
    boto3_client = BaseClient(client_name='test')
    await boto3_client.debug_on()
    assert boto3_client.logger.level == DEBUG
    
    await boto3_client.debug_off()
    assert boto3_client.logger.level == ERROR