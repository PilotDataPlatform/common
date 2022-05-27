# Project Client

A layer to simplify calling project APIs

## Usage

### Handle Exceptions

Setup error handles in flask/fastapi to allow for automically handling of the exception. Alternatively, you can catch the exceptions manually in your code.

Flask
```python
from common import ProjectException

@app.errorhandler(ProjectException)
def http_exception_handler(exc: ProjectException):
    return exc.content, exc.status_code
```

FastAPI
```python
from common import ProjectException

@app.exception_handler(ProjectException)
def http_exception_handler(request: Request, exc: ProjectException):
    return JSONResponse(
        status_code=exc.status_code,
        content=exc.content,
    )
```

Manually
```python
from common import ProjectException

try:
    <code>
except ProjectException as e:
    <code>
```

### Initialize client

There are two client, ProjectClient and ProjectClientSync. For async support use ProjectClient. Both clients should be identical other then async support.

REDIS_URL should be in the format `redis://<REDIS_USER>:<REDIS_PASS>@<REDIS_HOST>:<REDIS_PORT>` (`<REDIS_USER>` is the default user in this case)

```python
from common import ProjectClient

project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
```

### Get by code or id

```python
from common import ProjectClient


project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
project = await project_client.get(code='indoctestproject')
project = await project_client.get(id='6fc2201b-272a-4e1e-8fb8-a21ca84208d7')

print(project.name)
print(project.json()) # converts the project to a dict
```

### Search for project

```python
from common import ProjectClient


project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
results = await project_client.search(
    page=1,
    page_size=10,
    description='test'
)

# Sorting
results = await project_client.search(
    page=1,
    page_size=10,
    order_by='created_at', # name, code also supported
    order_type='asc', # or desc
)

# Create time range
results = await project_client.search(
    page=1,
    page_size=10,
    created_at_start=1653494559, # or datetime in ISO format
    created_at_start=1653494559,
)

# Tags format
results = await project_client.search(
    page=1,
    page_size=10,
    tags_all='test1,test2'
)

# Batch search by code
results = await project_client.search(
    page=1,
    page_size=10,
    code_any='code1,code2'
)

print(results["page"])
print(results["page_size"])
for project in results["results"]:
    print(project.name)
```


### Create project

```python
from common import ProjectClient


project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
project = await project_client.create(
    code="gregtestnewproject3",
    name="Greg Test New Project",
    description="Greg is testing new projects",
    tags=["greg", "test"],
    system_tags=["copied-to-core"],
    image_url="http://example.com/image.jpeg",
    is_discoverable=True,
)
```

### Update project

```python
from common import ProjectClient


project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
project = await project.get('indoctestproject')
await project.update(description="Changed3")
print(project.name)
```

### Upload logo

```python
from common import ProjectClient


project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
project = await project.get('indoctestproject')
project.upload_logo("<base64 encoded image data>")
```
