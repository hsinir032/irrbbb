# schemas.py
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
from datetime import date

# --- Instrument Schemas ---
class LoanBase(BaseModel):
    instrument_id: str
    type: str # e.g., "Fixed Rate Loan", "Floating Rate Loan"
    notional: float
    interest_rate: Optional[float] = None # For fixed rate loans
    maturity_date: date
    origination_date: date
    
    # For floating rate loans
    benchmark_rate_type: Optional[str] = None # e.g., "SOFR", "Prime"
    spread: Optional[float] = None
    repricing_frequency: Optional[str] = None # e.g., "Monthly", "Quarterly", "Annually"
    next_repricing_date: Optional[date] = None
    payment_frequency: str # e.g., "Monthly", "Quarterly", "Semi-Annually", "Annually"

class LoanCreate(LoanBase):
    pass

class LoanResponse(LoanBase):
    id: int # Database ID
    class Config:
        from_attributes = True

class DepositBase(BaseModel):
    instrument_id: str
    type: str # e.g., "Checking", "Savings", "CD"
    balance: float
    interest_rate: float # Current rate paid
    open_date: date
    
    # For CDs
    maturity_date: Optional[date] = None
    payment_frequency: Optional[str] = None # For CDs

    # For Checking/Savings (NMDs)
    repricing_frequency: Optional[str] = None # e.g., "Monthly", "Quarterly" for NMDs
    next_repricing_date: Optional[date] = None # Next date for rate review/change

class DepositCreate(DepositBase):
    pass

class DepositResponse(DepositBase):
    id: int
    class Config:
        from_attributes = True

class DerivativeBase(BaseModel):
    instrument_id: str
    type: str # e.g., "Interest Rate Swap"
    subtype: str # e.g., "Payer Swap", "Receiver Swap"
    notional: float
    start_date: date
    end_date: date
    fixed_rate: Optional[float] = None
    floating_rate_index: Optional[str] = None # e.g., "SOFR", "LIBOR"
    floating_spread: Optional[float] = None
    fixed_payment_frequency: Optional[str] = None
    floating_payment_frequency: Optional[str] = None

class DerivativeCreate(DerivativeBase):
    pass

class DerivativeResponse(DerivativeBase):
    id: int
    class Config:
        from_attributes = True

# --- Dashboard Data Schemas ---
class YieldCurvePoint(BaseModel):
    name: str # Tenor, e.g., "1Y", "5Y"
    rate: float
class ScenarioDataPoint(BaseModel):
    time: str # Timestamp for historical data
    # Dynamic fields for scenario values, e.g., "Base Case", "+200bps"
    data: Dict[str, float] # Allows for dynamic keys like 'Base Case', '+200bps'

class GapBucket(BaseModel):
    bucket: str
    assets: float
    liabilities: float
    gap: float

class EVEScenarioResult(BaseModel):
    scenario_name: str
    eve_value: float

class NIIScenarioResult(BaseModel):
    scenario_name: str
    nii_value: float

# Updated Schema for NMD and Prepayment Assumptions
class CalculationAssumptions(BaseModel):
    nmd_effective_maturity_years: int = Field(5, description="Effective maturity in years for Non-Maturity Deposits (NMDs) for EVE calculation.")
    nmd_deposit_beta: float = Field(0.5, description="Deposit beta (0-1) for NMD interest rate sensitivity.")
    prepayment_rate: float = Field(0.0, ge=0.0, le=1.0, description="Annual prepayment rate (CPR) for loans (0-1).")


class DashboardData(BaseModel):
    eve_sensitivity: float
    nii_sensitivity: float
    portfolio_value: float
    yield_curve_data: List[YieldCurvePoint]
    scenario_data: List[ScenarioDataPoint]
    total_loans: int
    total_deposits: int
    total_derivatives: int
    total_assets_value: float
    total_liabilities_value: float
    net_interest_income: float
    economic_value_of_equity: float
    nii_repricing_gap: List[GapBucket]
    eve_maturity_gap: List[GapBucket]
    eve_scenarios: List[EVEScenarioResult]
    nii_scenarios: List[NIIScenarioResult]
    loan_composition: Dict[str, float]
    deposit_composition: Dict[str, float]
    derivative_composition: Dict[str, float]
    # Include the current assumptions in the response for the frontend to display
    current_assumptions: CalculationAssumptions

