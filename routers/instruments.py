# routers/instruments.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

# Import dependencies from your local project
from .. import schemas, crud
from ..database import get_db

# Create an API Router instance
router = APIRouter(
    prefix="/api/v1", # This router will handle /api/v1/loans and /api/v1/deposits
    tags=["Instruments"] # Tags for Swagger UI documentation
)

# --- Loan Endpoints ---
@router.get("/loans", response_model=List[schemas.LoanResponse])
async def get_all_loans(db: Session = Depends(get_db), skip: int = 0, limit: int = 100):
    """
    Fetches all loan instruments from the database with pagination.
    """
    loans = crud.get_loans(db, skip=skip, limit=limit)
    return loans

@router.get("/loans/{loan_id}", response_model=schemas.LoanResponse)
async def get_single_loan(loan_id: int, db: Session = Depends(get_db)):
    """
    Fetches a single loan instrument by its ID.
    """
    db_loan = crud.get_loan(db, loan_id=loan_id)
    if db_loan is None:
        raise HTTPException(status_code=404, detail="Loan not found")
    return db_loan

@router.post("/loans", response_model=schemas.LoanResponse, status_code=status.HTTP_201_CREATED)
async def create_new_loan(loan: schemas.LoanCreate, db: Session = Depends(get_db)):
    """
    Creates a new loan instrument in the database.
    """
    return crud.create_loan(db, loan=loan)

# --- Deposit Endpoints ---
@router.get("/deposits", response_model=List[schemas.DepositResponse])
async def get_all_deposits(db: Session = Depends(get_db), skip: int = 0, limit: int = 100):
    """
    Fetches all deposit instruments from the database with pagination.
    """
    deposits = crud.get_deposits(db, skip=skip, limit=limit)
    return deposits

@router.get("/deposits/{deposit_id}", response_model=schemas.DepositResponse)
async def get_single_deposit(deposit_id: int, db: Session = Depends(get_db)):
    """
    Fetches a single deposit instrument by its ID.
    """
    db_deposit = crud.get_deposit(db, deposit_id=deposit_id)
    if db_deposit is None:
        raise HTTPException(status_code=404, detail="Deposit not found")
    return db_deposit

@router.post("/deposits", response_model=schemas.DepositResponse, status_code=status.HTTP_201_CREATED)
async def create_new_deposit(deposit: schemas.DepositCreate, db: Session = Depends(get_db)):
    """
    Creates a new deposit instrument in the database.
    """
    return crud.create_deposit(db, deposit=deposit)
