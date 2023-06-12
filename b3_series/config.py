from enum import IntEnum

from pydantic import BaseModel


class FSType(IntEnum):
    local = 1
    s3 = 2


class Config(BaseModel):
    fs_type: FSType = FSType.local
    fs_path: str = "data"
