# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, timedelta
import random
import os
import pandas as pd # Still needed for pandas.isna in on_startup data generation

# Import modules using absolute paths (assuming project root is on PYTHONPATH)
from database import Base, engine, SessionLocal, get_db
from models import Loan, Deposit, Derivative # Import new Derivative model
from schemas import (
    LoanBase, LoanCreate, LoanResponse,
    DepositBase, DepositCreate, DepositResponse,
    DerivativeBase, DerivativeCreate, DerivativeResponse, # Import new Derivative schemas
    GapBucket, DashboardData, EVEScenarioResult # Import new EVE Scenario Result
)
from crud import (
    get_loan, get_loans, create_loan,
    get_deposit, get_deposits, create_deposit,
    # New CRUD functions for derivatives will be added later in crud.py
)
from calculations import (
    get_bucket, calculate_nii_and_eve,
    calculate_gap_analysis, generate_dashboard_data_from_db
)
from routers import dashboard, instruments # Import the API routers


# --- FastAPI App Initialization ---
app = FastAPI(
    title="IRRBB Dashboard Backend",
    description="API for fetching simulated IRRBB metrics and data from a database.",
    version="0.1.0",
    openapi_url="/openapi.json" # Ensure OpenAPI spec is generated
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

# --- Database Initialization and Data Population on Startup ---
@app.on_event("startup")
def on_startup():
    print(f"DEBUG: FastAPI app starting up. Allowed CORS origins: {ALLOWED_ORIGINS}")
    
    # Create database tables (if they don't exist)
    Base.metadata.create_all(bind=engine)
    print("Database tables created/checked.")

    db = SessionLocal()

    # Populate Loans if table is empty
    if db.query(Loan).count() == 0: # Use imported Loan model
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
            next_repricing_date = None
            payment_frequency = random.choice(["Monthly", "Quarterly", "Semi-Annually", "Annually"]) # New

            if loan_type == "Floating Rate Loan":
                benchmark_rate_type = random.choice(["SOFR", "Prime"])
                spread = round(random.uniform(0.005, 0.02), 4) # 0.5% to 2% spread
                repricing_frequency = random.choice(["Monthly", "Quarterly", "Annually"])
                # Simulate next repricing date for floating loans
                if repricing_frequency == "Monthly":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 30))
                elif repricing_frequency == "Quarterly":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 90))
                elif repricing_frequency == "Annually":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 365))
                interest_rate = None # Floating rates are calculated, not fixed

            dummy_loans.append(
                Loan( # Use imported Loan model
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
                    payment_frequency=payment_frequency # New
                )
            )
        db.add_all(dummy_loans)
        db.commit()
        print(f"{len(dummy_loans)} dummy loan data added.")

    # Populate Deposits if table is empty
    if db.query(Deposit).count() == 0: # Use imported Deposit model
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
            payment_frequency = None # Default for checking/savings

            if deposit_type == "CD":
                maturity_date = open_date + timedelta(days=random.randint(90, 730)) # 3 months to 2 years
                payment_frequency = random.choice(["Monthly", "Quarterly", "Semi-Annually", "Annually"]) # New for CDs
            elif deposit_type in ["Savings", "Checking"]: # Assume these can reprice, even if not explicitly "floating"
                repricing_frequency = random.choice(["Monthly", "Quarterly"])
                if repricing_frequency == "Monthly":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 30))
                elif repricing_frequency == "Quarterly":
                    next_repricing_date = current_date + timedelta(days=random.randint(0, 90))

            dummy_deposits.append(
                Deposit( # Use imported Deposit model
                    instrument_id=f"DEP{i:03d}",
                    type=deposit_type,
                    balance=balance,
                    interest_rate=interest_rate,
                    open_date=open_date,
                    maturity_date=maturity_date,
                    repricing_frequency=repricing_frequency,
                    next_repricing_date=next_repricing_date,
                    payment_frequency=payment_frequency # New
                )
            )
        db.add_all(dummy_deposits)
        db.commit()
        print(f"{len(dummy_deposits)} dummy deposit data added.")

    # Populate Derivatives if table is empty (NEW)
    if db.query(Derivative).count() == 0:
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
                Derivative(
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

# --- Include API Routers ---
app.include_router(dashboard.router)
app.include_router(instruments.router)

# Root endpoint for basic check
@app.get("/")
async def root():
    return {"message": "IRRBB Backend is running! Access /docs for API documentation."}
