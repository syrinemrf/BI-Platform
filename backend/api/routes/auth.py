"""
Authentication API routes: register, login, profile, projects.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List
from datetime import datetime

from core.database import get_db
from core.models import User, Project
from services.auth_service import (
    create_user,
    authenticate_user,
    create_access_token,
    get_user_by_email,
    get_user_by_username,
    get_current_user,
    require_auth,
)

router = APIRouter(prefix="/auth", tags=["Authentication"])


# ========== Schemas ==========

class RegisterRequest(BaseModel):
    email: EmailStr
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6, max_length=100)
    full_name: Optional[str] = None


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: dict


class UserResponse(BaseModel):
    id: int
    email: str
    username: str
    full_name: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    config: Optional[dict] = None
    dataset_ids: Optional[list] = None
    dashboard_state: Optional[dict] = None


class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    config: Optional[dict] = None
    dataset_ids: Optional[list] = None
    dashboard_state: Optional[dict] = None


class ProjectResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    config: Optional[dict]
    dataset_ids: Optional[list]
    dashboard_state: Optional[dict]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ========== Auth Endpoints ==========

@router.post("/register", response_model=TokenResponse)
async def register(req: RegisterRequest, db: Session = Depends(get_db)):
    """Register a new user account."""
    if get_user_by_email(db, req.email):
        raise HTTPException(status_code=400, detail="Email already registered")
    if get_user_by_username(db, req.username):
        raise HTTPException(status_code=400, detail="Username already taken")

    user = create_user(db, req.email, req.username, req.password, req.full_name)
    token = create_access_token(data={"sub": user.id})

    return TokenResponse(
        access_token=token,
        user={
            "id": user.id,
            "email": user.email,
            "username": user.username,
            "full_name": user.full_name,
        },
    )


@router.post("/login", response_model=TokenResponse)
async def login(req: LoginRequest, db: Session = Depends(get_db)):
    """Login with email and password."""
    user = authenticate_user(db, req.email, req.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = create_access_token(data={"sub": user.id})

    return TokenResponse(
        access_token=token,
        user={
            "id": user.id,
            "email": user.email,
            "username": user.username,
            "full_name": user.full_name,
        },
    )


@router.get("/me", response_model=UserResponse)
async def get_profile(user: User = Depends(require_auth)):
    """Get current user profile."""
    return user


@router.put("/me", response_model=UserResponse)
async def update_profile(
    full_name: Optional[str] = None,
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Update user profile."""
    if full_name is not None:
        user.full_name = full_name
    db.commit()
    db.refresh(user)
    return user


# ========== Project Endpoints ==========

@router.get("/projects", response_model=List[ProjectResponse])
async def list_projects(user: User = Depends(require_auth), db: Session = Depends(get_db)):
    """List all projects for the authenticated user."""
    return db.query(Project).filter(Project.user_id == user.id).order_by(Project.updated_at.desc()).all()


@router.post("/projects", response_model=ProjectResponse)
async def create_project(
    req: ProjectCreate,
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Create a new project."""
    project = Project(
        user_id=user.id,
        name=req.name,
        description=req.description,
        config=req.config,
        dataset_ids=req.dataset_ids,
        dashboard_state=req.dashboard_state,
    )
    db.add(project)
    db.commit()
    db.refresh(project)
    return project


@router.get("/projects/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: int,
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Get a project by ID."""
    project = db.query(Project).filter(Project.id == project_id, Project.user_id == user.id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.put("/projects/{project_id}", response_model=ProjectResponse)
async def update_project(
    project_id: int,
    req: ProjectUpdate,
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Update a project."""
    project = db.query(Project).filter(Project.id == project_id, Project.user_id == user.id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    for field, value in req.model_dump(exclude_unset=True).items():
        setattr(project, field, value)

    db.commit()
    db.refresh(project)
    return project


@router.delete("/projects/{project_id}")
async def delete_project(
    project_id: int,
    user: User = Depends(require_auth),
    db: Session = Depends(get_db),
):
    """Delete a project."""
    project = db.query(Project).filter(Project.id == project_id, Project.user_id == user.id).first()
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    db.delete(project)
    db.commit()
    return {"detail": "Project deleted"}
