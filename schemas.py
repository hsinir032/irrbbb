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
    payment_frequency: Optional[str] = None # New

class LoanCreate(LoanBase):
    pass

class LoanResponse(LoanBase):
    id: int
    class Config:
        from_attributes = True

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
    payment_frequency: Optional[str] = None # New

class DepositCreate(DepositBase):
    pass

class DepositResponse(DepositBase):
    id: int
    class Config:
        from_attributes = True

# Derivative Schemas (NEW)
class DerivativeBase(BaseModel):
    instrument_id: str
    type: str
    subtype: str
    notional: float
    start_date: date
    end_date: date
    fixed_rate: Optional[float] = None
    floating_rate_index: Optional[str] = None
    floating_spread: Optional[float] = None
    fixed_payment_frequency: Optional[str] = None
    floating_payment_frequency: Optional[str] = None

class DerivativeCreate(DerivativeBase):
    pass

class DerivativeResponse(DerivativeBase):
    id: int
    class Config:
        from_attributes = True

# Gap Analysis Schemas (existing)
class GapBucket(BaseModel):
    bucket: str
    assets: float
    liabilities: float
    gap: float # assets - liabilities

# New: EVE Scenario Result
class EVEScenarioResult(BaseModel):
    scenario_name: str
    eve_value: float

# Dashboard Data Schema (UPDATED for EVE scenarios)
class DashboardData(BaseModel):
    eve_sensitivity: float # This might become less relevant or represent base case sensitivity
    nii_sensitivity: float # This might become less relevant or represent base case sensitivity
    portfolio_value: float
    yield_curve_data: List[Dict[str, Any]]
    scenario_data: List[Dict[str, Any]] # This is for the historical EVE chart, maybe rename
    total_loans: int
    total_deposits: int
    total_derivatives: int # New field
    total_assets_value: float
    total_liabilities_value: float
    net_interest_income: float
    economic_value_of_equity: float # This will be the base case EVE
    nii_repricing_gap: List[GapBucket]
    eve_maturity_gap: List[GapBucket]
    eve_scenarios: List[EVEScenarioResult] # New: EVE results for all 6 scenarios
