from fastapi import Header, HTTPException
from config import DevelopmentConfig


async def verify_token(Authorization: str = Header(...)):
    if Authorization != DevelopmentConfig.AUTH_KEY:
        raise HTTPException(status_code=400, detail="Auth-Token header invalid")
