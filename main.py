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

# --- LOAN Endpoints (using crud.py) ---
@app.get("/api/v1/loans", response_model=List[schemas.LoanResponse])
async def read_loans(db: Session = Depends(get_db)):
    """Fetches all loan instruments from the database."""
    loans = crud.get_loans(db)
    return loans

@app.post("/api/v1/loans", response_model=schemas.LoanResponse, status_code=status.HTTP_201_CREATED)
async def create_loan_endpoint(loan: schemas.LoanCreate, db: Session = Depends(get_db)):
    """Creates a new loan instrument in the database."""
    existing_loan = crud.get_loan(db, loan.instrument_id)
    if existing_loan:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Loan with this instrument_id already exists.")
    return crud.create_loan(db, loan)

@app.put("/api/v1/loans/{instrument_id}", response_model=schemas.LoanResponse)
async def update_loan_endpoint(instrument_id: str, loan_update: schemas.LoanCreate, db: Session = Depends(get_db)):
    """Updates an existing loan instrument."""
    db_loan = crud.get_loan(db, instrument_id)
    if not db_loan:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Loan not found")
    return crud.update_loan(db, instrument_id, loan_update)

@app.delete("/api/v1/loans/{instrument_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_loan_endpoint(instrument_id: str, db: Session = Depends(get_db)):
    """Deletes a loan instrument."""
    deleted = crud.delete_loan(db, instrument_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Loan not found")
    return {"message": "Loan deleted successfully"}

# --- DEPOSIT Endpoints (using crud.py) ---
@app.get("/api/v1/deposits", response_model=List[schemas.DepositResponse])
async def read_deposits(db: Session = Depends(get_db)):
    """Fetches all deposit instruments from the database."""
    deposits = crud.get_deposits(db)
    return deposits

@app.post("/api/v1/deposits", response_model=schemas.DepositResponse, status_code=status.HTTP_201_CREATED)
async def create_deposit_endpoint(deposit: schemas.DepositCreate, db: Session = Depends(get_db)):
    """Creates a new deposit instrument in the database."""
    existing_deposit = crud.get_deposit(db, deposit.instrument_id)
    if existing_deposit:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Deposit with this instrument_id already exists.")
    return crud.create_deposit(db, deposit)

@app.put("/api/v1/deposits/{instrument_id}", response_model=schemas.DepositResponse)
async def update_deposit_endpoint(instrument_id: str, deposit_update: schemas.DepositCreate, db: Session = Depends(get_db)):
    """Updates an existing deposit instrument."""
    db_deposit = crud.get_deposit(db, instrument_id)
    if not db_deposit:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deposit not found")
    return crud.update_deposit(db, instrument_id, deposit_update)

@app.delete("/api/v1/deposits/{instrument_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_deposit_endpoint(instrument_id: str, db: Session = Depends(get_db)):
    """Deletes a deposit instrument."""
    deleted = crud.delete_deposit(db, instrument_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deposit not found")
    return {"message": "Deposit deleted successfully"}

# --- DERIVATIVE Endpoints (using crud.py) ---
@app.get("/api/v1/derivatives", response_model=List[schemas.DerivativeResponse])
async def read_derivatives(db: Session = Depends(get_db)):
    """Fetches all derivative instruments from the database."""
    derivatives = crud.get_derivatives(db)
    return derivatives

@app.post("/api/v1/derivatives", response_model=schemas.DerivativeResponse, status_code=status.HTTP_201_CREATED)
async def create_derivative_endpoint(derivative: schemas.DerivativeCreate, db: Session = Depends(get_db)):
    """Creates a new derivative instrument in the database."""
    existing_derivative = crud.get_derivative(db, derivative.instrument_id)
    if existing_derivative:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Derivative with this instrument_id already exists.")
    return crud.create_derivative(db, derivative)

@app.put("/api/v1/derivatives/{instrument_id}", response_model=schemas.DerivativeResponse)
async def update_derivative_endpoint(instrument_id: str, derivative_update: schemas.DerivativeCreate, db: Session = Depends(get_db)):
    """Updates an existing derivative instrument."""
    db_derivative = crud.get_derivative(db, instrument_id)
    if not db_derivative:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Derivative not found")
    return crud.update_derivative(db, instrument_id, derivative_update)

@app.delete("/api/v1/derivatives/{instrument_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_derivative_endpoint(instrument_id: str, db: Session = Depends(get_db)):
    """Deletes a derivative instrument."""
    deleted = crud.delete_derivative(db, instrument_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Derivative not found")
    return {"message": "Derivative deleted successfully"}

# Root endpoint for basic check
@app.get("/")
async def root():
    return {"message": "IRRBB Backend is running! Access /docs for API documentation."}

# --- Additional Dashboard Insight Endpoints ---
from crud_dashboard import (
    get_latest_dashboard_metrics,
    get_eve_drivers_for_scenario,
    get_net_positions_for_scenario,
    get_bucket_constituents,
    get_portfolio_composition,
    get_nii_drivers_for_scenario_and_breakdown
)

@app.get("/api/v1/dashboard/snapshot")
def get_dashboard_snapshot(db: Session = Depends(get_db)):
    """Fetches saved dashboard metrics (EVE/NII/etc.) from DB."""
    return get_latest_dashboard_metrics(db)

@app.get("/api/v1/dashboard/eve-drivers")
def get_eve_drivers(scenario: str = "Parallel Up +200bps", db: Session = Depends(get_db)):
    """Fetches PVs of instruments (before/after shock) to explain EVE impact."""
    return get_eve_drivers_for_scenario(db, scenario)

@app.get("/api/v1/dashboard/net-positions")
def get_net_positions(scenario: str = "Base Case", db: Session = Depends(get_db)):
    """Returns net asset/liability positions across repricing buckets."""
    return get_net_positions_for_scenario(db, scenario)

@app.get("/api/v1/dashboard/bucket-constituents")
def get_bucket_instruments(scenario: str, bucket: str, db: Session = Depends(get_db)):
    """Fetches underlying instruments in a repricing bucket."""
    return get_bucket_constituents(db, scenario, bucket)

@app.get("/api/v1/portfolio/composition")
def get_portfolio_composition_summary(db: Session = Depends(get_db)):
    """Returns fixed/floating, maturity, and basis distribution."""
    result = get_portfolio_composition(db)
    return {
        'records': [r.__dict__ for r in result['records']],
        'total_loans': result['total_loans'],
        'total_deposits': result['total_deposits'],
        'total_derivatives': result['total_derivatives']
    }

@app.get("/api/v1/dashboard/nii-drivers")
def get_nii_drivers(scenario: str = "Base Case", breakdown: str = "instrument", db: Session = Depends(get_db)):
    """Fetches NII drivers for a scenario and breakdown (instrument, type, or bucket)."""
    return get_nii_drivers_for_scenario_and_breakdown(db, scenario, breakdown)

# --- Create missing dashboard tables (only runs once) ---
try:
    from models_dashboard import Base as DashboardBase
    DashboardBase.metadata.create_all(bind=engine)
except Exception as e:
    print(f"Error creating dashboard tables: {e}")
