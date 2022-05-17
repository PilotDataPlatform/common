# Project Client

A layer to simplify calling project APIs

## Usage

### Initialize client

REDIS_URL should be in the format `redis://:<REDIS_PASS>@<REDIS_HOST>:<REDIS_PORT>`

```
from common import ProjectClient

project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
```

### Get by code or id

```
from common import ProjectClient


project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
project = project_client.get(code='indoctestproject')
project = project_client.get(code='6fc2201b-272a-4e1e-8fb8-a21ca84208d7')

print(project.name)
print(project.json()) # converts the project to a dict
```

### Search for project

```
from common import ProjectClient


project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
results = project_client.search(
    page=1,
    page_size=10,
    description='test'
)
print(results["page"])
print(results["page_size"])
for project in results["results"]:
    print(project.name)
```


### Create project

```
from common import ProjectClient


project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
project = project_client.create(
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

```
from common import ProjectClient


project_client = ProjectClient(ConfigClass.PROJECT_SERVICE, ConfigClass.REDIS_URL)
project = project.get('indoctestproject')
project.update(description="Changed3")
print(project.name)
```
