# crud.py
from sqlalchemy.orm import Session
from typing import List
from datetime import date

# Corrected: Import models and schemas using absolute paths
from models import Loan, Deposit
from schemas import LoanCreate, DepositCreate

# --- CRUD Operations for Loans ---

def get_loan(db: Session, loan_id: int):
    """Retrieve a single loan by its ID."""
    return db.query(Loan).filter(Loan.id == loan_id).first()

def get_loans(db: Session, skip: int = 0, limit: int = 100) -> List[Loan]:
    """Retrieve multiple loans with pagination."""
    return db.query(Loan).offset(skip).limit(limit).all()

def create_loan(db: Session, loan: LoanCreate) -> Loan:
    """Create a new loan in the database."""
    db_loan = Loan(**loan.model_dump())
    db.add(db_loan)
    db.commit()
    db.refresh(db_loan)
    return db_loan

# --- CRUD Operations for Deposits ---

def get_deposit(db: Session, deposit_id: int):
    """Retrieve a single deposit by its ID."""
    return db.query(Deposit).filter(Deposit.id == deposit_id).first()

def get_deposits(db: Session, skip: int = 0, limit: int = 100) -> List[Deposit]:
    """Retrieve multiple deposits with pagination."""
    return db.query(Deposit).offset(skip).limit(limit).all()

def create_deposit(db: Session, deposit: DepositCreate) -> Deposit:
    """Create a new deposit in the database."""
    db_deposit = Deposit(**deposit.model_dump())
    db.add(db_deposit)
    db.commit()
    db.refresh(db_deposit)
    return db_deposit
