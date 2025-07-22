from fastapi import FastAPI, HTTPException, Depends, status, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Generator, Optional
import random
import time
from datetime import datetime, date, timedelta
import os
import pandas as pd

# --- SQLAlchemy Imports for Database ---
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# --- Import from local modules ---
import models
import schemas
import crud
from calculations import generate_dashboard_data_from_db

# --- FastAPI App Initialization ---
app = FastAPI(
    title="IRRBB Dashboard Backend",
    description="API for fetching simulated IRRBB metrics and data from a database.",
    version="0.1.0",
    openapi_url="/openapi.json"
)

# --- Enhanced CORS Configuration ---
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "https://irrbb-frontend.onrender.com",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods including OPTIONS
    allow_headers=["*"],
    expose_headers=["*"]
)

# --- Database Configuration ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://irrbb_user:irrbb_password@localhost:5432/irrbb_db")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Dependency to get a database session ---
def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Explicit OPTIONS handler for preflight requests ---
@app.options("/api/v1/dashboard/live-data")
async def options_live_data():
    return {"message": "OK"}

# --- API Endpoints ---
@app.get("/api/v1/dashboard/live-data", response_model=schemas.DashboardData)
async def get_live_dashboard_data(
    db: Session = Depends(get_db),
    nmd_effective_maturity_years: int = Query(5, ge=1, le=30),
    nmd_deposit_beta: float = Query(0.5, ge=0.0, le=1.0),
    prepayment_rate: float = Query(0.0, ge=0.0, le=1.0)
):
    assumptions = schemas.CalculationAssumptions(
        nmd_effective_maturity_years=nmd_effective_maturity_years,
        nmd_deposit_beta=nmd_deposit_beta,
        prepayment_rate=prepayment_rate
    )
    return generate_dashboard_data_from_db(db, assumptions)

# [Rest of your existing endpoints remain unchanged...]