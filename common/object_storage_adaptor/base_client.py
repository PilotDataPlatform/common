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


from logging import DEBUG
from logging import ERROR

from common.logger import LoggerFactory

class BaseClient:
    """
    Summary:
        The base client for all object storage class that will
        include some basic functions:
            - set logger level to DEBUG
            - set logger level to ERROR
    """

    def __init__(self, client_name: str) -> None:
        self.client_name = client_name
        
        # the flag to turn on class-wide logs
        self.logger = LoggerFactory('Boto3Client').get_logger()
        # initially only print out error info
        self.logger.setLevel(ERROR)

    async def debug_on(self):
        """
        Summary:
            The funtion will switch the log level to DEBUG
        """
        self.logger.setLevel(DEBUG)
        
        return

    async def debug_off(self):
        """
        Summary:
            The funtion will switch the log level to ERROR
        """
        self.logger.setLevel(ERROR)
        
        return
