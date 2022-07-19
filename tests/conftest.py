import asyncio
from io import BytesIO
import json
from uuid import uuid4

import httpx
from httpx import Response
import pytest
from urllib3 import HTTPResponse
import pytest_asyncio
from dicttoxml import dicttoxml
from testcontainers.redis import RedisContainer

PROJECT_URL = 'http://project'

PROJECT_ID = str(uuid4())

PROJECT_DATA = {
    'id': PROJECT_ID,
    'code': 'unittestproject',
    'description': 'Test',
    'name': 'Unit Test Project',
    'tags': ['tag1', 'tag2'],
    'system_tags': ['system'],
}

PROJECT_CREDENTIALS = {
    'AccessKeyId': 'test',
    'SecretAccessKey': 'test',
    'SessionToken': 'test',
}


@pytest_asyncio.fixture(scope='session')
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
    asyncio.set_event_loop_policy(None)


@pytest.fixture
def mock_get_by_code(httpx_mock):
    code = PROJECT_DATA['code']
    httpx_mock.add_response(
        method='GET',
        url=PROJECT_URL + f'/v1/projects/{code}',
        json=PROJECT_DATA,
        status_code=200,
    )


@pytest.fixture
def mock_get_by_id(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url=PROJECT_URL + f'/v1/projects/{PROJECT_ID}',
        json=PROJECT_DATA,
        status_code=200,
    )


@pytest.fixture
def mock_post_by_token(httpx_mock):
    url = httpx.URL(
        PROJECT_URL,
        params={
            'Action': 'AssumeRoleWithWebIdentity',
            'WebIdentityToken': 'test',
            'Version': '2011-06-15',
            'DurationSeconds': 86000,
        },
    )
    xml = dicttoxml(
        {
            'AssumeRoleWithWebIdentityResponse': {
                'AssumeRoleWithWebIdentityResult': {'Credentials': PROJECT_CREDENTIALS}
            }
        },
        attr_type=False,
        root=False,
    ).decode('utf-8')
    httpx_mock.add_response(method='POST', url=url, status_code=200, text=xml)


@pytest.fixture(scope='session', autouse=True)
def redis():
    with RedisContainer('redis:latest') as redis:
        host = redis.get_container_host_ip()
        port = redis.get_exposed_port(redis.port_to_expose)
        redis.url = f'redis://{host}:{port}'
        yield redis
