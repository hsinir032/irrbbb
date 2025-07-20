# crud.py
from sqlalchemy.orm import Session
from typing import List
from datetime import date

# Import models and schemas
from . import models, schemas

# --- CRUD Operations for Loans ---

def get_loan(db: Session, loan_id: int):
    """Retrieve a single loan by its ID."""
    return db.query(models.Loan).filter(models.Loan.id == loan_id).first()

def get_loans(db: Session, skip: int = 0, limit: int = 100) -> List[models.Loan]:
    """Retrieve multiple loans with pagination."""
    return db.query(models.Loan).offset(skip).limit(limit).all()

def create_loan(db: Session, loan: schemas.LoanCreate) -> models.Loan:
    """Create a new loan in the database."""
    db_loan = models.Loan(**loan.model_dump())
    db.add(db_loan)
    db.commit()
    db.refresh(db_loan)
    return db_loan

# --- CRUD Operations for Deposits ---

def get_deposit(db: Session, deposit_id: int):
    """Retrieve a single deposit by its ID."""
    return db.query(models.Deposit).filter(models.Deposit.id == deposit_id).first()

def get_deposits(db: Session, skip: int = 0, limit: int = 100) -> List[models.Deposit]:
    """Retrieve multiple deposits with pagination."""
    return db.query(models.Deposit).offset(skip).limit(limit).all()

def create_deposit(db: Session, deposit: schemas.DepositCreate) -> models.Deposit:
    """Create a new deposit in the database."""
    db_deposit = models.Deposit(**deposit.model_dump())
    db.add(db_deposit)
    db.commit()
    db.refresh(db_deposit)
    return db_deposit
