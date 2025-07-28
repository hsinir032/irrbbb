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
import schemas_dashboard
from calculations import generate_dashboard_data_from_db
from fastapi import Query
from typing import List
from models_dashboard import CashflowLadder, RepricingBucket

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
    get_bucket_constituents,
    get_portfolio_composition,
    get_nii_drivers_for_scenario_and_breakdown,
    get_yield_curves
)

@app.get("/api/v1/dashboard/snapshot")
def get_dashboard_snapshot(db: Session = Depends(get_db)):
    """Fetches saved dashboard metrics (EVE/NII/etc.) from DB."""
    return get_latest_dashboard_metrics(db)

@app.get("/api/v1/dashboard/eve-drivers")
def get_eve_drivers(
    scenarios: Optional[str] = Query(None, description="Comma-separated list of scenarios"),
    db: Session = Depends(get_db)
):
    """
    Fetches PVs of instruments (before/after shock) to explain EVE impact.
    If 'scenarios' is provided, returns drivers for all listed scenarios as a flat list.
    """
    if scenarios:
        scenario_list = [s.strip() for s in scenarios.split(",")]
        all_drivers = []
        for scenario in scenario_list:
            drivers = get_eve_drivers_for_scenario(db, scenario)
            for drv in drivers:
                drv_dict = drv.__dict__.copy()
                drv_dict["scenario"] = scenario
                all_drivers.append(drv_dict)
        return all_drivers
    else:
        return get_eve_drivers_for_scenario(db, "Parallel Up +200bps")



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
def get_nii_drivers(
    scenarios: Optional[str] = Query(None, description="Comma-separated list of scenarios"),
    breakdown: str = "instrument",
    db: Session = Depends(get_db)
):
    """
    Fetches NII drivers for one or more scenarios and breakdown (instrument, type, or bucket).
    If 'scenarios' is provided, returns drivers for all listed scenarios as a flat list.
    """
    if scenarios:
        scenario_list = [s.strip() for s in scenarios.split(",")]
        all_drivers = []
        for scenario in scenario_list:
            drivers = get_nii_drivers_for_scenario_and_breakdown(db, scenario, breakdown)
            for drv in drivers:
                drv_dict = drv.__dict__.copy()
                drv_dict["scenario"] = scenario
                all_drivers.append(drv_dict)
        return all_drivers
    else:
        return get_nii_drivers_for_scenario_and_breakdown(db, "Base Case", breakdown)

@app.get("/api/v1/yield-curves")
def get_yield_curves_endpoint(scenario: Optional[str] = None, db: Session = Depends(get_db)):
    """Fetches yield curves from database, optionally filtered by scenario."""
    curves = get_yield_curves(db, scenario)
    return [schemas_dashboard.YieldCurveResponse.from_orm(curve) for curve in curves]

@app.get("/api/v1/cashflow-ladder")
def get_cashflow_ladder(
    scenario: str = Query("Base Case"),
    instrument_type: str = Query("all"),
    aggregation: str = Query("assets"),  # 'assets', 'liabilities', 'net'
    cashflow_type: str = Query("pv"),    # 'total' or 'pv'
    db: Session = Depends(get_db)
):
    # Query and filter cashflow ladder
    q = db.query(CashflowLadder).filter(CashflowLadder.scenario == scenario)
    if instrument_type != "all":
        q = q.filter(CashflowLadder.instrument_type == instrument_type)
    records = q.all()
    # Group by time (month/year)
    buckets = {}
    for rec in records:
        # Only aggregate by asset/liability as requested
        if aggregation == "assets" and rec.asset_liability != "A":
            continue
        if aggregation == "liabilities" and rec.asset_liability != "L":
            continue
        key = rec.cashflow_date.strftime("%Y-%m")
        if key not in buckets:
            buckets[key] = {"time_label": key, "fixed": 0.0, "floating": 0.0}
        val_fixed = rec.fixed_component if cashflow_type == "total" else rec.pv * (rec.fixed_component / rec.total_cashflow) if rec.total_cashflow else 0.0
        val_floating = rec.floating_component if cashflow_type == "total" else rec.pv * (rec.floating_component / rec.total_cashflow) if rec.total_cashflow else 0.0
        buckets[key]["fixed"] += val_fixed
        buckets[key]["floating"] += val_floating
    # If net, subtract liabilities from assets
    if aggregation == "net":
        # Re-query for liabilities
        qL = db.query(CashflowLadder).filter(CashflowLadder.scenario == scenario)
        if instrument_type != "all":
            qL = qL.filter(CashflowLadder.instrument_type == instrument_type)
        recsL = [r for r in qL.all() if r.asset_liability == "L"]
        for rec in recsL:
            key = rec.cashflow_date.strftime("%Y-%m")
            val_fixed = rec.fixed_component if cashflow_type == "total" else rec.pv * (rec.fixed_component / rec.total_cashflow) if rec.total_cashflow else 0.0
            val_floating = rec.floating_component if cashflow_type == "total" else rec.pv * (rec.floating_component / rec.total_cashflow) if rec.total_cashflow else 0.0
            if key not in buckets:
                buckets[key] = {"time_label": key, "fixed": 0.0, "floating": 0.0}
            buckets[key]["fixed"] -= val_fixed
            buckets[key]["floating"] -= val_floating
    # Return sorted by time
    return [buckets[k] for k in sorted(buckets.keys())]

@app.get("/api/v1/cashflow-ladder/instrument-types")
def get_cashflow_ladder_instrument_types(db: Session = Depends(get_db)):
    types = db.query(CashflowLadder.instrument_type).distinct().all()
    return [t[0] for t in types if t[0]]

@app.get("/api/v1/repricing-gap")
def get_repricing_gap(scenario: str = "Base Case", db: Session = Depends(get_db)):
    """Returns repricing gap data for the bar chart - aggregated from detailed instrument data."""
    # Get all repricing bucket records for the scenario
    records = db.query(RepricingBucket).filter(RepricingBucket.scenario == scenario).all()
    
    # Group by bucket and calculate totals
    buckets = {}
    for record in records:
        if record.bucket not in buckets:
            buckets[record.bucket] = {
                "bucket": record.bucket,
                "assets": 0.0,
                "liabilities": 0.0,
                "net": 0.0,
                "instruments": []
            }
        
        # Add to totals
        if record.position == "asset":
            buckets[record.bucket]["assets"] += record.notional
        else:
            buckets[record.bucket]["liabilities"] += record.notional
        
        # Add instrument detail
        buckets[record.bucket]["instruments"].append({
            "instrument_id": record.instrument_id,
            "instrument_type": record.instrument_type,
            "position": record.position,
            "amount": record.notional
        })
    
    # Calculate net for each bucket
    for bucket_data in buckets.values():
        bucket_data["net"] = bucket_data["assets"] - bucket_data["liabilities"]
    
    # Return sorted by bucket order
    bucket_order = ["0-3 Months", "3-6 Months", "6-12 Months", "1-5 Years", ">5 Years", "Fixed Rate / Non-Sensitive"]
    return [buckets.get(bucket, {"bucket": bucket, "assets": 0.0, "liabilities": 0.0, "net": 0.0, "instruments": []}) 
            for bucket in bucket_order if bucket in buckets]

@app.get("/api/v1/repricing-gap/drill-down/{bucket}")
def get_repricing_gap_drill_down(bucket: str, scenario: str = "Base Case", db: Session = Depends(get_db)):
    """Returns instrument-level drill-down data for a specific bucket."""
    print(f"Drill-down requested for bucket: {bucket}, scenario: {scenario}")
    
    records = db.query(RepricingBucket).filter(
        RepricingBucket.scenario == scenario,
        RepricingBucket.bucket == bucket
    ).all()
    
    print(f"Found {len(records)} records for bucket {bucket}")
    
    # Group by instrument type and position
    drill_down = {
        "assets": [],
        "liabilities": []
    }
    
    for record in records:
        instrument_data = {
            "instrument_id": record.instrument_id,
            "instrument_type": record.instrument_type,
            "amount": record.notional
        }
        
        if record.position == "asset":
            drill_down["assets"].append(instrument_data)
        else:
            drill_down["liabilities"].append(instrument_data)
    
    # Sort by amount (descending)
    drill_down["assets"].sort(key=lambda x: x["amount"], reverse=True)
    drill_down["liabilities"].sort(key=lambda x: x["amount"], reverse=True)
    
    print(f"Returning drill-down data: {len(drill_down['assets'])} assets, {len(drill_down['liabilities'])} liabilities")
    
    return drill_down

@app.get("/api/v1/debug/derivatives")
def get_debug_derivatives(db: Session = Depends(get_db)):
    """Debug endpoint to check what derivatives exist in the database."""
    from models import Derivative
    derivatives = db.query(Derivative).all()
    result = []
    for derivative in derivatives:
        result.append({
            "instrument_id": derivative.instrument_id,
            "type": derivative.type,
            "subtype": derivative.subtype,
            "start_date": derivative.start_date,
            "end_date": derivative.end_date,
            "notional": derivative.notional,
            "fixed_rate": derivative.fixed_rate,
            "floating_spread": derivative.floating_spread
        })
    return result

# --- Create missing dashboard tables (only runs once) ---
try:
    from models_dashboard import Base as DashboardBase
    DashboardBase.metadata.create_all(bind=engine)
except Exception as e:
    print(f"Error creating dashboard tables: {e}")
