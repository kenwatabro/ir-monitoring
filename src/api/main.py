from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel

from .auth import get_current_user

app = FastAPI(title="IR Monitoring API", version="0.1.0")


class DocResponse(BaseModel):
    doc_id: str
    source: str
    url: str


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/docs/{doc_id}", response_model=DocResponse)
def get_doc(doc_id: str, _: str = Depends(get_current_user)) -> DocResponse:  # noqa: D401
    # For now just return placeholder
    if not doc_id:
        raise HTTPException(status_code=404, detail="Document not found")
    return DocResponse(doc_id=doc_id, source="EDINET", url=f"/download/{doc_id}") 