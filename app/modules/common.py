from typing import Optional
from pydantic import BaseModel


class p21_register(BaseModel):
    server: Optional[str] = None
    port: Optional[str] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None


class p21_input(BaseModel):
    company_id: Optional[str] = None
