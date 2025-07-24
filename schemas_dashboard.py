from pydantic import BaseModel
from typing import Optional
from datetime import date

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

class RepricingNetPositionCreate(BaseModel):
    scenario: str
    bucket: str
    total_assets: float
    total_liabilities: float
    net_position: float
    nii_base: float
    nii_shocked: float

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
