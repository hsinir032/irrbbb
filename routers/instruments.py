# routers/instruments.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

# Corrected: Import dependencies using absolute paths from the root package
from schemas import LoanResponse, LoanCreate, DepositResponse, DepositCreate # Import specific schemas
from crud import get_loan, get_loans, create_loan, get_deposit, get_deposits, create_deposit # Import specific CRUD functions
from database import get_db # Import specific dependency

# Create an API Router instance
router = APIRouter(
    prefix="/api/v1", # This router will handle /api/v1/loans and /api/v1/deposits
    tags=["Instruments"] # Tags for Swagger UI documentation
)

# --- Loan Endpoints ---
@router.get("/loans", response_model=List[LoanResponse])
async def get_all_loans(db: Session = Depends(get_db), skip: int = 0, limit: int = 100):
    """
    Fetches all loan instruments from the database with pagination.
    """
    loans = get_loans(db, skip=skip, limit=limit) # Use imported crud function
    return loans

@router.get("/loans/{loan_id}", response_model=LoanResponse)
async def get_single_loan(loan_id: int, db: Session = Depends(get_db)):
    """
    Fetches a single loan instrument by its ID.
    """
    db_loan = get_loan(db, loan_id=loan_id) # Use imported crud function
    if db_loan is None:
        raise HTTPException(status_code=404, detail="Loan not found")
    return db_loan

@router.post("/loans", response_model=LoanResponse, status_code=status.HTTP_201_CREATED)
async def create_new_loan(loan: LoanCreate, db: Session = Depends(get_db)):
    """
    Creates a new loan instrument in the database.
    """
    return create_loan(db, loan=loan) # Use imported crud function

# --- Deposit Endpoints ---
@router.get("/deposits", response_model=List[DepositResponse])
async def get_all_deposits(db: Session = Depends(get_db), skip: int = 0, limit: int = 100):
    """
    Fetches all deposit instruments from the database with pagination.
    """
    deposits = get_deposits(db, skip=skip, limit=limit) # Use imported crud function
    return deposits

@router.get("/deposits/{deposit_id}", response_model=DepositResponse)
async def get_single_deposit(deposit_id: int, db: Session = Depends(get_db)):
    """
    Fetches a single deposit instrument by its ID.
    """
    db_deposit = get_deposit(db, deposit_id=deposit_id) # Use imported crud function
    if db_deposit is None:
        raise HTTPException(status_code=404, detail="Deposit not found")
    return db_deposit

@router.post("/deposits", response_model=DepositResponse, status_code=status.HTTP_201_CREATED)
async def create_new_deposit(deposit: DepositCreate, db: Session = Depends(get_db)):
    """
    Creates a new deposit instrument in the database.
    """
    return create_deposit(db, deposit=deposit) # Use imported crud function
