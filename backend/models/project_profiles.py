"""Models for project profile generation and save flows."""

from __future__ import annotations

from pydantic import BaseModel


class ProjectProfileGenerateRequest(BaseModel):
    research_purpose: str = ""
    profile_name: str = ""
    filename: str = ""


class ProjectProfileGenerateResponse(BaseModel):
    status: str = "generated"
    filename: str = ""
    profile_name: str = ""
    content: str = ""


class ProjectProfileSaveRequest(BaseModel):
    content: str


class ProjectProfileSaveResponse(BaseModel):
    status: str = "saved"
    filename: str
    name: str
    content: str
