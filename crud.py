# crud.py
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import date

import models
import schemas # Import your schemas here

# --- LOAN CRUD Operations ---

def get_loan(db: Session, instrument_id: str):
    """Fetches a single loan by its instrument_id."""
    return db.query(models.Loan).filter(models.Loan.instrument_id == instrument_id).first()

def get_loans(db: Session, skip: int = 0, limit: int = 100):
    """Fetches a list of loans."""
    return db.query(models.Loan).offset(skip).limit(limit).all()

def create_loan(db: Session, loan: schemas.LoanCreate):
    """Creates a new loan record."""
    db_loan = models.Loan(**loan.model_dump())
    db.add(db_loan)
    db.commit()
    db.refresh(db_loan)
    return db_loan

def update_loan(db: Session, instrument_id: str, loan_update: schemas.LoanCreate):
    """Updates an existing loan record."""
    db_loan = db.query(models.Loan).filter(models.Loan.instrument_id == instrument_id).first()
    if db_loan:
        for key, value in loan_update.model_dump(exclude_unset=True).items():
            setattr(db_loan, key, value)
        db.commit()
        db.refresh(db_loan)
    return db_loan

def delete_loan(db: Session, instrument_id: str):
    """Deletes a loan record."""
    db_loan = db.query(models.Loan).filter(models.Loan.instrument_id == instrument_id).first()
    if db_loan:
        db.delete(db_loan)
        db.commit()
        return True # Indicate successful deletion
    return False # Indicate not found or failed

# --- DEPOSIT CRUD Operations ---

def get_deposit(db: Session, instrument_id: str):
    """Fetches a single deposit by its instrument_id."""
    return db.query(models.Deposit).filter(models.Deposit.instrument_id == instrument_id).first()

def get_deposits(db: Session, skip: int = 0, limit: int = 100):
    """Fetches a list of deposits."""
    return db.query(models.Deposit).offset(skip).limit(limit).all()

def create_deposit(db: Session, deposit: schemas.DepositCreate):
    """Creates a new deposit record."""
    db_deposit = models.Deposit(**deposit.model_dump())
    db.add(db_deposit)
    db.commit()
    db.refresh(db_deposit)
    return db_deposit

def update_deposit(db: Session, instrument_id: str, deposit_update: schemas.DepositCreate):
    """Updates an existing deposit record."""
    db_deposit = db.query(models.Deposit).filter(models.Deposit.instrument_id == instrument_id).first()
    if db_deposit:
        for key, value in deposit_update.model_dump(exclude_unset=True).items():
            setattr(db_deposit, key, value)
        db.commit()
        db.refresh(db_deposit)
    return db_deposit

def delete_deposit(db: Session, instrument_id: str):
    """Deletes a deposit record."""
    db_deposit = db.query(models.Deposit).filter(models.Deposit.instrument_id == instrument_id).first()
    if db_deposit:
        db.delete(db_deposit)
        db.commit()
        return True
    return False

# --- DERIVATIVE CRUD Operations ---

def get_derivative(db: Session, instrument_id: str):
    """Fetches a single derivative by its instrument_id."""
    return db.query(models.Derivative).filter(models.Derivative.instrument_id == instrument_id).first()

def get_derivatives(db: Session, skip: int = 0, limit: int = 100):
    """Fetches a list of derivatives."""
    return db.query(models.Derivative).offset(skip).limit(limit).all()

def create_derivative(db: Session, derivative: schemas.DerivativeCreate):
    """Creates a new derivative record."""
    db_derivative = models.Derivative(**derivative.model_dump())
    db.add(db_derivative)
    db.commit()
    db.refresh(db_derivative)
    return db_derivative

def update_derivative(db: Session, instrument_id: str, derivative_update: schemas.DerivativeCreate):
    """Updates an existing derivative record."""
    db_derivative = db.query(models.Derivative).filter(models.Derivative.instrument_id == instrument_id).first()
    if db_derivative:
        for key, value in derivative_update.model_dump(exclude_unset=True).items():
            setattr(db_derivative, key, value)
        db.commit()
        db.refresh(db_derivative)
    return db_derivative

def delete_derivative(db: Session, instrument_id: str):
    """Deletes a derivative record."""
    db_derivative = db.query(models.Derivative).filter(models.Derivative.instrument_id == instrument_id).first()
    if db_derivative:
        db.delete(db_derivative)
        db.commit()
        return True
    return False
