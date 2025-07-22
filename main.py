# main.py
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

# --- CORS Configuration ---
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "https://irrbbb-backend.onrender.com" # Your deployed Render backend URL
    "https://irrbb-frontend.onrender.com",  # Your deployed frontend	
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
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

# --- Database Initialization and Data Population on Startup ---
@app.on_event("startup")
def on_startup():
    print(f"DEBUG: FastAPI app starting up. Allowed CORS origins: {ALLOWED_ORIGINS}")
    
    # Create database tables (if they don't exist)
    models.Base.metadata.create_all(bind=engine)
    print("Database tables created/checked.")

    db = SessionLocal()

    # Populate Loans if table is empty
    if db.query(models.Loan).count() == 0:
        print("Adding initial dummy loan data...")
        current_date = date.today()
        dummy_loans = []
        for i in range(1, 101): # 100 loans
            loan_type = random.choice(["Fixed Rate Loan", "Floating Rate Loan"])
            notional = round(random.uniform(50000, 5000000), 2)
            interest_rate = round(random.uniform(0.03, 0.07), 4) # 3% to 7%
            origination_date = current_date - timedelta(days=random.randint(30, 1000))
            maturity_date = origination_date + timedelta(days=random.randint(365, 365 * 10)) # 1 to 10 years

            benchmark_rate_type = None
            spread = None
            repricing_frequency = None
            next_repricing_date = None # New field for repricing date
            payment_frequency = random.choice(["Monthly", "Quarterly", "Semi-Annually", "Annually"])

            if loan_type == "Floating Rate Loan":
                benchmark_rate_type = random.choice(["SOFR", "Prime"])
                spread = round(random.uniform(0.005, 0.02), 4) # 0.5% to 2% spread
                repricing_frequency = random.choice(["Monthly", "Quarterly", "Annually"])
                if repricing_frequency == "Monthly":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 30))
                elif repricing_frequency == "Quarterly":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 90))
                elif repricing_frequency == "Annually":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 365))
                interest_rate = None # Floating rates are calculated, not fixed

            dummy_loans.append(
                models.Loan(
                    instrument_id=f"LOAN{i:03d}",
                    type=loan_type,
                    notional=notional,
                    interest_rate=interest_rate,
                    maturity_date=maturity_date,
                    origination_date=origination_date,
                    benchmark_rate_type=benchmark_rate_type,
                    spread=spread,
                    repricing_frequency=repricing_frequency,
                    next_repricing_date=next_repricing_date,
                    payment_frequency=payment_frequency
                )
            )
        db.add_all(dummy_loans)
        db.commit()
        print(f"{len(dummy_loans)} dummy loan data added.")

    # Populate Deposits if table is empty
    if db.query(models.Deposit).count() == 0:
        print("Adding initial dummy deposit data...")
        current_date = date.today()
        dummy_deposits = []
        for i in range(1, 101): # 100 deposits
            deposit_type = random.choice(["Checking", "Savings", "CD"])
            balance = round(random.uniform(1000, 1000000), 2)
            interest_rate = round(random.uniform(0.001, 0.02), 4) # 0.1% to 2%
            open_date = current_date - timedelta(days=random.randint(10, 500))
            maturity_date = None
            repricing_frequency = None
            next_repricing_date = None
            payment_frequency = None

            if deposit_type == "CD":
                maturity_date = open_date + timedelta(days=random.randint(90, 730)) # 3 months to 2 years
                payment_frequency = random.choice(["Monthly", "Quarterly", "Semi-Annually", "Annually"])
            elif deposit_type in ["Checking", "Savings"]:
                repricing_frequency = random.choice(["Monthly", "Quarterly"])
                if repricing_frequency == "Monthly":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 30))
                elif repricing_frequency == "Quarterly":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 90))

            dummy_deposits.append(
                models.Deposit(
                    instrument_id=f"DEP{i:03d}",
                    type=deposit_type,
                    balance=balance,
                    interest_rate=interest_rate,
                    open_date=open_date,
                    maturity_date=maturity_date,
                    repricing_frequency=repricing_frequency,
                    next_repricing_date=next_repricing_date,
                    payment_frequency=payment_frequency
                )
            )
        db.add_all(dummy_deposits)
        db.commit()
        print(f"{len(dummy_deposits)} dummy deposit data added.")

    # Populate Derivatives if table is empty
    if db.query(models.Derivative).count() == 0:
        print("Adding initial dummy derivative data...")
        current_date = date.today()
        dummy_derivatives = []
        for i in range(1, 21): # 20 derivatives (e.g., swaps)
            derivative_type = "Interest Rate Swap"
            subtype = random.choice(["Payer Swap", "Receiver Swap"])
            notional = round(random.uniform(1000000, 10000000), 2) # Larger notionals for derivatives
            start_date = current_date - timedelta(days=random.randint(0, 365))
            end_date = start_date + timedelta(days=random.randint(365 * 2, 365 * 10)) # 2 to 10 year swaps

            fixed_rate = round(random.uniform(0.02, 0.05), 4)
            floating_rate_index = random.choice(["SOFR", "LIBOR"])
            floating_spread = round(random.uniform(-0.001, 0.001), 4) # Small spread around index

            fixed_payment_frequency = random.choice(["Quarterly", "Semi-Annually"])
            floating_payment_frequency = random.choice(["Monthly", "Quarterly"])

            dummy_derivatives.append(
                models.Derivative(
                    instrument_id=f"SWAP{i:02d}",
                    type=derivative_type,
                    subtype=subtype,
                    notional=notional,
                    start_date=start_date,
                    end_date=end_date,
                    fixed_rate=fixed_rate,
                    floating_rate_index=floating_rate_index,
                    floating_spread=floating_spread,
                    fixed_payment_frequency=fixed_payment_frequency,
                    floating_payment_frequency=floating_payment_frequency
                )
            )
        db.add_all(dummy_derivatives)
        db.commit()
        print(f"{len(dummy_derivatives)} dummy derivative data added.")
    db.close()

# --- API Endpoints ---
@app.get("/api/v1/dashboard/live-data", response_model=schemas.DashboardData)
async def get_live_dashboard_data(
    db: Session = Depends(get_db),
    nmd_effective_maturity_years: int = Query(5, ge=1, le=30, description="Effective maturity in years for Non-Maturity Deposits (NMDs) for EVE calculation."),
    nmd_deposit_beta: float = Query(0.5, ge=0.0, le=1.0, description="Deposit beta (0-1) for NMD interest rate sensitivity."),
    prepayment_rate: float = Query(0.0, ge=0.0, le=1.0, description="Annual prepayment rate (CPR) for loans (0-1).")
):
    """
    Fetches live IRRBB dashboard data, calculated from database instruments
    including scenario-based EVE/NII and portfolio composition.
    Allows for configurable NMD behavioral and prepayment assumptions.
    """
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
