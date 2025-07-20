# main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Generator
import random
import time
from datetime import datetime, date, timedelta
import os
import pandas as pd # Import pandas

# --- SQLAlchemy Imports for Database ---
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

# --- FastAPI App Initialization ---
app = FastAPI(
    title="IRRBB Dashboard Backend",
    description="API for fetching simulated IRRBB metrics and data from a database.",
    version="0.1.0"
)

# --- CORS Configuration ---
ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "https://irrbbb-backend.onrender.com"
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

# --- Database Models ---
class Loan(Base):
    __tablename__ = "loans"
    id = Column(Integer, primary_key=True, index=True)
    instrument_id = Column(String, unique=True, index=True)
    type = Column(String) # e.g., "Fixed Rate Loan", "Floating Rate Loan"
    notional = Column(Float)
    interest_rate = Column(Float) # Annual rate
    maturity_date = Column(Date)
    origination_date = Column(Date)
    # New fields for more realistic data
    benchmark_rate_type = Column(String, nullable=True) # e.g., "LIBOR", "SOFR", "Prime"
    spread = Column(Float, nullable=True) # Spread over benchmark for floating rates
    repricing_frequency = Column(String, nullable=True) # e.g., "Monthly", "Quarterly", "Annually"

    def __repr__(self):
        return f"<Loan(id={self.id}, instrument_id='{self.instrument_id}', notional={self.notional})>"

class Deposit(Base):
    __tablename__ = "deposits"
    id = Column(Integer, primary_key=True, index=True)
    instrument_id = Column(String, unique=True, index=True)
    type = Column(String) # e.g., "Checking", "Savings", "CD" (Certificate of Deposit)
    balance = Column(Float)
    interest_rate = Column(Float) # Annual rate
    open_date = Column(Date)
    maturity_date = Column(Date, nullable=True) # For CDs, null for checking/savings

    def __repr__(self):
        return f"<Deposit(id={self.id}, instrument_id='{self.instrument_id}', balance={self.balance})>"


# Create database tables (if they don't exist) and populate with dummy data
@app.on_event("startup")
def on_startup():
    print(f"DEBUG: FastAPI app starting up. Allowed CORS origins: {ALLOWED_ORIGINS}")
    Base.metadata.create_all(bind=engine)
    print("Database tables created/checked.")

    db = SessionLocal()

    # Populate Loans if table is empty
    if db.query(Loan).count() == 0:
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

            if loan_type == "Floating Rate Loan":
                benchmark_rate_type = random.choice(["SOFR", "Prime"])
                spread = round(random.uniform(0.005, 0.02), 4) # 0.5% to 2% spread
                repricing_frequency = random.choice(["Monthly", "Quarterly", "Annually"])
                interest_rate = None # Floating rates are calculated, not fixed

            dummy_loans.append(
                Loan(
                    instrument_id=f"LOAN{i:03d}",
                    type=loan_type,
                    notional=notional,
                    interest_rate=interest_rate,
                    maturity_date=maturity_date,
                    origination_date=origination_date,
                    benchmark_rate_type=benchmark_rate_type,
                    spread=spread,
                    repricing_frequency=repricing_frequency
                )
            )
        db.add_all(dummy_loans)
        db.commit()
        print(f"{len(dummy_loans)} dummy loan data added.")

    # Populate Deposits if table is empty
    if db.query(Deposit).count() == 0:
        print("Adding initial dummy deposit data...")
        current_date = date.today()
        dummy_deposits = []
        for i in range(1, 101): # 100 deposits
            deposit_type = random.choice(["Checking", "Savings", "CD"])
            balance = round(random.uniform(1000, 1000000), 2)
            interest_rate = round(random.uniform(0.001, 0.02), 4) # 0.1% to 2%
            open_date = current_date - timedelta(days=random.randint(10, 500))
            maturity_date = None

            if deposit_type == "CD":
                maturity_date = open_date + timedelta(days=random.randint(90, 730)) # 3 months to 2 years

            dummy_deposits.append(
                Deposit(
                    instrument_id=f"DEP{i:03d}",
                    type=deposit_type,
                    balance=balance,
                    interest_rate=interest_rate,
                    open_date=open_date,
                    maturity_date=maturity_date
                )
            )
        db.add_all(dummy_deposits)
        db.commit()
        print(f"{len(dummy_deposits)} dummy deposit data added.")
    db.close()


# --- Dependency to get a database session ---
def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Pydantic Models for API Request/Response ---
class LoanBase(BaseModel):
    instrument_id: str
    type: str
    notional: float
    interest_rate: float | None = None # Can be None for floating
    maturity_date: date
    origination_date: date
    benchmark_rate_type: str | None = None
    spread: float | None = None
    repricing_frequency: str | None = None

class LoanCreate(LoanBase):
    pass

class LoanResponse(LoanBase):
    id: int
    class Config:
        from_attributes = True

class DepositBase(BaseModel):
    instrument_id: str
    type: str
    balance: float
    interest_rate: float
    open_date: date
    maturity_date: date | None = None

class DepositCreate(DepositBase):
    pass

class DepositResponse(DepositBase):
    id: int
    class Config:
        from_attributes = True

class DashboardData(BaseModel):
    eve_sensitivity: float # Will be calculated
    nii_sensitivity: float # Will be calculated
    portfolio_value: float # Sum of notional/balance
    yield_curve_data: List[Dict[str, Any]]
    scenario_data: List[Dict[str, Any]]
    total_loans: int
    total_deposits: int # New field
    total_assets_value: float # New field
    total_liabilities_value: float # New field
    net_interest_income: float # New calculated field
    economic_value_of_equity: float # New calculated field (simplified)


# --- Simulated Data Generation Logic (Modified to use DB for portfolio value and NII/EVE) ---
_scenario_history: List[Dict[str, Any]] = []
MAX_SCENARIO_HISTORY = 10

def calculate_nii_and_eve(db: Session) -> Dict[str, Any]:
    """
    Calculates Net Interest Income (NII) and Economic Value of Equity (EVE)
    based on data from the database.
    This is a simplified calculation for demonstration purposes.
    """
    loans = db.query(Loan).all()
    deposits = db.query(Deposit).all()

    # Convert to pandas DataFrames for easier calculation
    loans_df = pd.DataFrame([loan.__dict__ for loan in loans])
    deposits_df = pd.DataFrame([deposit.__dict__ for deposit in deposits])

    # --- NII Calculation (Simplified) ---
    total_loan_interest_income = 0.0
    if not loans_df.empty:
        # For fixed loans, use interest_rate. For floating, assume a current benchmark + spread.
        # This is a simplification; real floating rates would change over time.
        loans_df['effective_rate'] = loans_df.apply(
            lambda row: row['interest_rate'] if row['type'] == 'Fixed Rate Loan' else (0.04 + row['spread'] if pd.notna(row['spread']) else 0.04), # Assume 4% benchmark for floating
            axis=1
        )
        total_loan_interest_income = (loans_df['notional'] * loans_df['effective_rate']).sum()

    total_deposit_interest_expense = 0.0
    if not deposits_df.empty:
        total_deposit_interest_expense = (deposits_df['balance'] * deposits_df['interest_rate']).sum()

    net_interest_income = total_loan_interest_income - total_deposit_interest_expense

    # --- EVE Calculation (Highly Simplified for demonstration) ---
    # EVE is typically the present value of assets minus present value of liabilities.
    # Sensitivity of EVE to rate changes is key.
    # For simplicity, let's represent EVE as a metric derived from total assets/liabilities
    # and then apply a simulated sensitivity for the dashboard.
    total_assets_value = loans_df['notional'].sum() if not loans_df.empty else 0.0
    total_liabilities_value = deposits_df['balance'].sum() if not deposits_df.empty else 0.0
    
    # A very basic proxy for EVE, could be (Assets - Liabilities)
    economic_value_of_equity = total_assets_value - total_liabilities_value

    # Simulate sensitivity values (these are still random, but now they are "sensitivities"
    # of the calculated EVE/NII, rather than being the EVE/NII themselves).
    eve_sensitivity = round(random.uniform(-0.5, 0.5), 2) # % change in EVE for a rate shock
    nii_sensitivity = round(random.uniform(-0.2, 0.2), 2) # % change in NII for a rate shock

    return {
        "net_interest_income": net_interest_income,
        "economic_value_of_equity": economic_value_of_equity,
        "total_assets_value": total_assets_value,
        "total_liabilities_value": total_liabilities_value,
        "eve_sensitivity": eve_sensitivity,
        "nii_sensitivity": nii_sensitivity,
        "total_loans": len(loans),
        "total_deposits": len(deposits)
    }


def generate_dashboard_data_from_db(db: Session) -> DashboardData:
    """Generates dashboard data by fetching from DB and performing calculations."""
    global _scenario_history

    # Get calculated financial metrics
    calculated_metrics = calculate_nii_and_eve(db)

    # Yield curve and scenario data remain largely simulated for now
    # In a real app, yield curve would be fetched from market data.
    new_yield_curve_data = [
        {"name": "1M", "yield": round(random.uniform(0.5, 0.7), 2)},
        {"name": "3M", "yield": round(random.uniform(0.7, 1.0), 2)},
        {"name": "6M", "yield": round(random.uniform(1.0, 1.4), 2)},
        {"name": "1Y", "yield": round(random.uniform(1.5, 2.0), 2)},
        {"name": "2Y", "yield": round(random.uniform(2.0, 2.6), 2)},
        {"name": "5Y", "yield": round(random.uniform(2.5, 3.2), 2)},
        {"name": "10Y", "yield": round(random.uniform(3.0, 3.8), 2)},
    ]

    now = datetime.now()
    new_scenario_point = {
        "time": now.strftime("%H:%M:%S"),
        "base_case": round(random.uniform(97.5, 102.5), 2),
        "plus_100bps": round(random.uniform(92.5, 97.5), 2),
        "minus_100bps": round(random.uniform(102.5, 107.5), 2),
    }

    _scenario_history.append(new_scenario_point)
    _scenario_history = _scenario_history[-MAX_SCENARIO_HISTORY:]

    return DashboardData(
        eve_sensitivity=calculated_metrics["eve_sensitivity"],
        nii_sensitivity=calculated_metrics["nii_sensitivity"],
        portfolio_value=calculated_metrics["total_assets_value"], # Portfolio value now represents total assets
        yield_curve_data=new_yield_curve_data,
        scenario_data=_scenario_history,
        total_loans=calculated_metrics["total_loans"],
        total_deposits=calculated_metrics["total_deposits"],
        total_assets_value=calculated_metrics["total_assets_value"],
        total_liabilities_value=calculated_metrics["total_liabilities_value"],
        net_interest_income=calculated_metrics["net_interest_income"],
        economic_value_of_equity=calculated_metrics["economic_value_of_equity"]
    )

# --- API Endpoints ---
@app.get("/api/v1/dashboard/live-data", response_model=DashboardData)
async def get_live_dashboard_data(db: Session = Depends(get_db)):
    """
    Fetches live IRRBB dashboard data, calculated from database instruments.
    """
    return generate_dashboard_data_from_db(db)

@app.get("/api/v1/loans", response_model=List[LoanResponse])
async def get_loans(db: Session = Depends(get_db)):
    """Fetches all loan instruments from the database."""
    loans = db.query(Loan).all()
    return loans

@app.post("/api/v1/loans", response_model=LoanResponse, status_code=201)
async def create_loan(loan: LoanCreate, db: Session = Depends(get_db)):
    """Creates a new loan instrument in the database."""
    db_loan = Loan(**loan.model_dump())
    db.add(db_loan)
    db.commit()
    db.refresh(db_loan)
    return db_loan

@get("/api/v1/deposits", response_model=List[DepositResponse])
async def get_deposits(db: Session = Depends(get_db)):
    """Fetches all deposit instruments from the database."""
    deposits = db.query(Deposit).all()
    return deposits

@app.post("/api/v1/deposits", response_model=DepositResponse, status_code=201)
async def create_deposit(deposit: DepositCreate, db: Session = Depends(get_db)):
    """Creates a new deposit instrument in the database."""
    db_deposit = Deposit(**deposit.model_dump())
    db.add(db_deposit)
    db.commit()
    db.refresh(db_deposit)
    return db_deposit

# Root endpoint for basic check
@app.get("/")
async def root():
    return {"message": "IRRBB Backend is running! Access /docs for API documentation."}
