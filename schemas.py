# schemas.py
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import date

# --- Pydantic Models for API Request/Response ---

# Loan Schemas
class LoanBase(BaseModel):
    instrument_id: str
    type: str
    notional: float
    interest_rate: Optional[float] = None # Can be None for floating
    maturity_date: date
    origination_date: date
    benchmark_rate_type: Optional[str] = None
    spread: Optional[float] = None
    repricing_frequency: Optional[str] = None
    next_repricing_date: Optional[date] = None

class LoanCreate(LoanBase):
    pass

class LoanResponse(LoanBase):
    id: int
    class Config:
        from_attributes = True # For SQLAlchemy 2.0, use from_attributes instead of orm_mode

# Deposit Schemas
class DepositBase(BaseModel):
    instrument_id: str
    type: str
    balance: float
    interest_rate: float
    open_date: date
    maturity_date: Optional[date] = None
    repricing_frequency: Optional[str] = None
    next_repricing_date: Optional[date] = None

class DepositCreate(DepositBase):
    pass

class DepositResponse(DepositBase):
    id: int
    class Config:
        from_attributes = True

# Gap Analysis Schemas
class GapBucket(BaseModel):
    bucket: str
    assets: float
    liabilities: float
    gap: float # assets - liabilities

# Dashboard Data Schema
class DashboardData(BaseModel):
    eve_sensitivity: float
    nii_sensitivity: float
    portfolio_value: float
    yield_curve_data: List[Dict[str, Any]]
    scenario_data: List[Dict[str, Any]]
    total_loans: int
    total_deposits: int
    total_assets_value: float
    total_liabilities_value: float
    net_interest_income: float
    economic_value_of_equity: float
    nii_repricing_gap: List[GapBucket]
    eve_maturity_gap: List[GapBucket]
