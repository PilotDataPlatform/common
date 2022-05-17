# Copyright (C) 2022 Indoc Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import httpx
import redis
import json
from http.client import HTTPException
from .project_exceptions import ProjectNotFoundException, ProjectException

CACHE_PREFIX = "project_client-"


class ProjectObject(object):
    attributes = [
        "id",
        "code",
        "name",
        "description",
        "image_url",
        "tags",
        "system_tags",
        "is_discoverable",
        "created_at",
        "updated_at",
    ]

    def __init__(self, data, project_client):
        self.project_client = project_client
        for attr in self.attributes:
            setattr(self, attr, data.get(attr))

    def __repr__(self):
        return f"<Project {self.code}>"

    def json(self):
        result = {}
        for attr in self.attributes:
            result[attr] = getattr(self, attr, "")
        return result

    def update(self, code=None, name=None, description=None, image_url=None, tags=None, system_tags=None, is_discoverable=None):
        data = {
            "code": code,
            "name": name,
            "description": description,
            "image_url": image_url,
            "tags": tags,
            "system_tags": system_tags,
            "is_discoverable": is_discoverable,
        }
        # remove blank items
        data = {k: v for k, v in data.items() if v is not None}

        response = httpx.patch(self.project_client.base_url + f"/v1/projects/{self.id}", json=data)
        if response.status_code == 404:
            raise ProjectNotFoundException
        elif response.status_code != 200:
            raise ProjectException(status_code=response.status_code, error_msg=response.json())

        self.project_client.redis.delete(CACHE_PREFIX +  self.id)
        self.project_client.redis.delete(CACHE_PREFIX +  self.code)
        for attr in self.attributes:
            setattr(self, attr, response.json().get(attr))
        return self


class ProjectClient(object):
    def __init__(self, project_url, redis_url, enable_cache=True):
        self.base_url = project_url
        self.redis = redis.from_url(redis_url)
        self.enable_cache = enable_cache

    def search(self, query):
        result = {}
        response = httpx.get(self.base_url + "/v1/projects/", params=query)
        if response.status_code == 404:
            raise ProjectNotFoundException
        elif response.status_code != 200:
            raise ProjectException(status_code=response.status_code, error_msg=response.json())
        result["result"] = [ProjectObject(item, self) for item in response.json()["result"]]
        return result

    def get(self, id="", code=""):
        project_id = ""
        if id:
            project_id = id
        else:
            project_id = code

        project_key = CACHE_PREFIX + project_id
        if self.enable_cache and self.redis.exists(project_key):
            return ProjectObject(json.loads(self.redis.get(project_key)), self)

        response = httpx.get(self.base_url + f"/v1/projects/{project_id}")
        if response.status_code == 404:
            raise ProjectNotFoundException
        elif response.status_code != 200:
            raise ProjectException(status_code=response.status_code, error_msg=response.json())

        if self.enable_cache:
            self.redis.setex(project_key, 30, json.dumps(response.json()))
        return ProjectObject(response.json(), self)

    def create(self, code, name, description, image_url=None, tags=[], system_tags=[], is_discoverable=True):
        data = {
            "code": code,
            "name": name,
            "description": description,
            "image_url": image_url,
            "tags": tags,
            "system_tags": system_tags,
            "is_discoverable": is_discoverable,
        }
        # remove blank items
        data = {k: v for k, v in data.items() if v is not None}

        response = httpx.post(self.base_url + f"/v1/projects/", json=data)
        if response.status_code != 200:
            raise ProjectException(status_code=response.status_code, error_msg=response.json())
        return ProjectObject(response.json(), self)
