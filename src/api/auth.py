from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Annotated, Dict

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

JWT_SECRET = os.getenv("JWT_SECRET", "dev_only_secret")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

security = HTTPBearer()


class TokenPayload(dict):
    sub: str
    exp: int


def create_access_token(subject: str, expires_delta: timedelta | None = None) -> str:
    now = datetime.utcnow()
    if expires_delta is None:
        expires_delta = timedelta(hours=12)
    to_encode: Dict[str, str | int] = {"sub": subject, "iat": int(now.timestamp())}
    expire = now + expires_delta
    to_encode["exp"] = int(expire.timestamp())
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)


def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials, Depends(security)],
) -> str:
    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        sub: str = payload.get("sub")  # type: ignore[assignment]
        if sub is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid JWT payload"
            )
        return sub
    except jwt.PyJWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
        ) from exc
