# main.py
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Generator
import random
import time
from datetime import datetime, date

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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# --- Database Configuration ---
# Replace with your PostgreSQL connection details
# If using Docker setup:
# DATABASE_URL = "postgresql://irrbb_user:irrbb_password@localhost:5432/irrbb_db"
# If using Nhost PostgreSQL (example, replace with your actual connection string from Nhost):
# DATABASE_URL = "postgresql://<user>:<password>@<host>:<port>/<database_name>"
DATABASE_URL = "postgresql://irrbb_user:irrbb_password@localhost:5432/irrbb_db" # Default local Docker

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Database Models ---
# This defines the structure of your tables in the database
class Loan(Base):
    __tablename__ = "loans" # Table name in the database

    id = Column(Integer, primary_key=True, index=True)
    instrument_id = Column(String, unique=True, index=True)
    type = Column(String, default="Fixed Rate Loan") # e.g., "Fixed Rate Loan", "Floating Rate Loan"
    notional = Column(Float) # Principal amount
    interest_rate = Column(Float) # Annual interest rate
    maturity_date = Column(Date) # Date when the loan matures
    origination_date = Column(Date) # Date when the loan was originated

    def __repr__(self):
        return f"<Loan(id={self.id}, instrument_id='{self.instrument_id}', notional={self.notional})>"

# Create database tables (if they don't exist)
# This should ideally be run once, or managed by a proper migration tool.
# For simplicity in this example, we'll call it on startup.
@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)
    print("Database tables created/checked.")
    # Optional: Add some initial dummy data if the table is empty
    db = SessionLocal()
    if db.query(Loan).count() == 0:
        print("Adding initial dummy loan data...")
        dummy_loans = [
            Loan(instrument_id="LOAN001", type="Fixed Rate Loan", notional=1000000.0, interest_rate=0.045, maturity_date=date(2028, 12, 31), origination_date=date(2023, 1, 15)),
            Loan(instrument_id="LOAN002", type="Floating Rate Loan", notional=2500000.0, interest_rate=0.030, maturity_date=date(2030, 6, 30), origination_date=date(2022, 5, 1)),
            Loan(instrument_id="LOAN003", type="Fixed Rate Loan", notional=750000.0, interest_rate=0.050, maturity_date=date(2027, 3, 1), origination_date=date(2024, 2, 10)),
        ]
        db.add_all(dummy_loans)
        db.commit()
        print("Dummy loan data added.")
    db.close()


# --- Dependency to get a database session ---
# This function will be used by FastAPI's dependency injection system
def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Pydantic Models for API Request/Response ---
# These define the data structure for what the API expects/returns
class LoanBase(BaseModel):
    instrument_id: str
    type: str
    notional: float
    interest_rate: float
    maturity_date: date
    origination_date: date

class LoanCreate(LoanBase):
    pass # No additional fields for creation for now

class LoanResponse(LoanBase):
    id: int
    class Config:
        from_attributes = True # For SQLAlchemy 2.0, use from_attributes instead of orm_mode

class DashboardData(BaseModel):
    eve_sensitivity: float
    nii_sensitivity: float
    portfolio_value: float
    yield_curve_data: List[Dict[str, Any]] # Keeping as Dict for now, will refine
    scenario_data: List[Dict[str, Any]] # Keeping as Dict for now, will refine
    total_loans: int # New field to show data from DB


# --- Simulated Data Generation Logic (Modified to use DB for portfolio value) ---
_scenario_history: List[Dict[str, Any]] = []
MAX_SCENARIO_HISTORY = 10

def generate_simulated_data(db: Session, total_loans_from_db: int) -> DashboardData:
    """Generates a new set of simulated IRRBB dashboard data."""
    # Moved global declaration to the top of the function
    global _scenario_history

    new_eve_sensitivity = round(random.uniform(-2.5, 2.5), 2)
    new_nii_sensitivity = round(random.uniform(-1.5, 1.5), 2)

    # Now, portfolio_value can be linked to actual data, or still simulated
    # For now, we'll still simulate, but you can imagine summing notional from DB
    new_portfolio_value = round(random.uniform(900, 1100), 2) # Still simulated for simplicity

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
        "Base Case": round(random.uniform(97.5, 102.5), 2),
        "+100bps": round(random.uniform(92.5, 97.5), 2),
        "-100bps": round(random.uniform(102.5, 107.5), 2),
    }

    _scenario_history.append(new_scenario_point)
    _scenario_history = _scenario_history[-MAX_SCENARIO_HISTORY:] # This line requires `global` because it's re-assigning the name

    return DashboardData(
        eve_sensitivity=new_eve_sensitivity,
        nii_sensitivity=new_nii_sensitivity,
        portfolio_value=new_portfolio_value,
        yield_curve_data=new_yield_curve_data,
        scenario_data=_scenario_history,
        total_loans=total_loans_from_db # Pass the count from the DB
    )

# --- API Endpoints ---
@app.get("/api/v1/dashboard/live-data", response_model=DashboardData)
async def get_live_dashboard_data(db: Session = Depends(get_db)):
    """
    Fetches simulated live IRRBB dashboard data, now including
    a count of loans from the database.
    """
    total_loans = db.query(Loan).count() # Get count of loans from the database
    return generate_simulated_data(db, total_loans)

@app.get("/api/v1/loans", response_model=List[LoanResponse])
async def get_loans(db: Session = Depends(get_db)):
    """Fetches all loan instruments from the database."""
    loans = db.query(Loan).all()
    return loans

@app.post("/api/v1/loans", response_model=LoanResponse, status_code=201)
async def create_loan(loan: LoanCreate, db: Session = Depends(get_db)):
    """Creates a new loan instrument in the database."""
    db_loan = Loan(**loan.model_dump()) # Use .model_dump() for Pydantic v2
    db.add(db_loan)
    db.commit()
    db.refresh(db_loan) # Refresh to get the ID generated by the DB
    return db_loan

# Root endpoint for basic check
@app.get("/")
async def root():
    return {"message": "IRRBB Backend is running! Access /docs for API documentation."}
