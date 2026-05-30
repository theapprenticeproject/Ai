from __future__ import annotations

from typing import Any, List, Optional
from pydantic import BaseModel, ConfigDict


class Enrollment(BaseModel):
    model_config = ConfigDict(extra="ignore")

    batch: Optional[str] = None
    course: Optional[str] = None
    grade: Optional[str] = None
    school: Optional[str] = None
    date_joining: Optional[Any] = None


class UserProfile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Optional[str] = None
    name: Optional[str] = None
    phone: Optional[str] = None
    grade: Optional[str] = None
    batch: Optional[str] = None
    glific_id: Optional[str] = None
    type: Optional[str] = None
    language: Optional[str] = None
    enrollments: List[Enrollment] = []
    current_enrollment: Optional[Enrollment] = None


class ContentDetails(BaseModel):
    # extra='allow' because field_map in dynamic config can include arbitrary keys
    model_config = ConfigDict(extra="allow")

    title: Optional[str] = None
    type: Optional[str] = None
    current_step: Optional[str] = None
    step: Optional[str] = None
