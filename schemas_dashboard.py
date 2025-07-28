from pydantic import BaseModel
from typing import Optional
from datetime import date
from datetime import datetime

class DashboardMetricCreate(BaseModel):
    timestamp: date
    scenario: str
    eve_value: float
    nii_value: float
    eve_sensitivity: float
    nii_sensitivity: float
    total_assets_value: float
    total_liabilities_value: float
    portfolio_value: float

class EveDriverCreate(BaseModel):
    scenario: str
    instrument_id: str
    instrument_type: str
    base_pv: float
    shocked_pv: Optional[float] = None
    duration: Optional[float] = None

class RepricingBucketCreate(BaseModel):
    scenario: str
    bucket: str
    instrument_id: str
    instrument_type: str
    notional: float
    position: str  # asset or liability



class PortfolioCompositionCreate(BaseModel):
    timestamp: date
    instrument_type: str
    category: str
    subcategory: Optional[str]
    volume_count: int
    total_amount: float
    average_interest_rate: Optional[float] = None

class NiiDriverCreate(BaseModel):
    scenario: str
    instrument_id: Optional[str] = None
    instrument_type: Optional[str] = None
    nii_contribution: float
    breakdown_type: Optional[str] = None
    breakdown_value: Optional[str] = None

class YieldCurveCreate(BaseModel):
    scenario: str
    tenor: str
    rate: float
    timestamp: Optional[datetime] = None

class YieldCurveResponse(BaseModel):
    id: int
    scenario: str
    tenor: str
    rate: float
    timestamp: Optional[datetime] = None

    class Config:
        from_attributes = True

class CashflowLadderCreate(BaseModel):
    scenario: str
    instrument_id: str
    instrument_type: str
    asset_liability: str
    cashflow_date: date
    time_months: int
    fixed_component: float
    floating_component: float
    total_cashflow: float
    discount_factor: float
    pv: float

class CashflowLadderResponse(CashflowLadderCreate):
    id: int
    created_at: Optional[datetime] = None
