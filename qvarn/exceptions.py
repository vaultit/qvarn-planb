from typing import Union

import apistar.exceptions


class HTTPException(apistar.exceptions.HTTPException):

    def __init__(self, detail: Union[str, dict]=None, status_code: int=None, headers: dict=None) -> None:
        self.detail = self.default_detail if detail is None else detail
        self.status_code = self.default_status_code if status_code is None else status_code
        self.headers = headers or {}
        assert self.detail is not None, '"detail" is required.'
        assert self.status_code is not None, '"status_code" is required.'


class Unauthorized(HTTPException):
    default_status_code = 401
    default_detail = 'Unauthorized'


class Forbidden(HTTPException):
    default_status_code = 403
    default_detail = 'Forbidden'


class NotFound(HTTPException):
    default_status_code = 404
    default_detail = 'Not found'
