from sqlalchemy import Column, Integer, String, Float, Date, DateTime
from sqlalchemy.orm import relationship
from database import Base
from datetime import datetime

class DashboardMetric(Base):
    __tablename__ = "dashboard_metrics"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(Date)
    scenario = Column(String)
    eve_value = Column(Float)
    nii_value = Column(Float)
    eve_sensitivity = Column(Float)
    nii_sensitivity = Column(Float)
    total_assets_value = Column(Float)
    total_liabilities_value = Column(Float)
    portfolio_value = Column(Float)

class EveDriver(Base):
    __tablename__ = "eve_drivers"
    id = Column(Integer, primary_key=True, index=True)
    scenario = Column(String)
    instrument_id = Column(String)
    instrument_type = Column(String)  # Loan, Deposit, Derivative
    base_pv = Column(Float)
    shocked_pv = Column(Float)
    duration = Column(Float, nullable=True)

class RepricingBucket(Base):
    __tablename__ = "repricing_buckets"
    id = Column(Integer, primary_key=True, index=True)
    scenario = Column(String)
    bucket = Column(String)
    instrument_id = Column(String)
    instrument_type = Column(String)
    notional = Column(Float)
    position = Column(String)  # asset / liability



class PortfolioComposition(Base):
    __tablename__ = "portfolio_composition"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(Date)
    instrument_type = Column(String)  # Loan, Deposit, Derivative
    category = Column(String)         # e.g., Fixed, Floating, SOFR, etc.
    subcategory = Column(String)      # optional subtype or basis
    volume_count = Column(Integer)
    total_amount = Column(Float)
    average_interest_rate = Column(Float, nullable=True)

class NiiDriver(Base):
    __tablename__ = "nii_drivers"
    id = Column(Integer, primary_key=True, index=True)
    scenario = Column(String)
    instrument_id = Column(String)
    instrument_type = Column(String)  # Loan, Deposit, Derivative
    nii_contribution = Column(Float)
    breakdown_type = Column(String, nullable=True)  # e.g., instrument, type, bucket
    breakdown_value = Column(String, nullable=True)  # e.g., instrument id, type name, bucket name

class YieldCurve(Base):
    __tablename__ = "yield_curves"
    id = Column(Integer, primary_key=True, index=True)
    scenario = Column(String, nullable=False)  # e.g., "Base Case", "Parallel Up +200bps"
    tenor = Column(String, nullable=False)     # e.g., "1M", "3M", "1Y", etc.
    rate = Column(Float, nullable=False)       # e.g., 0.045 for 4.5%
    timestamp = Column(DateTime, nullable=True)  # Optional: when this curve was generated

class CashflowLadder(Base):
    __tablename__ = "cashflow_ladder"
    id = Column(Integer, primary_key=True, index=True)
    scenario = Column(String(50))
    instrument_id = Column(String(50))
    instrument_type = Column(String(50))
    asset_liability = Column(String(1))  # 'A' or 'L'
    cashflow_date = Column(Date)
    time_months = Column(Integer)
    fixed_component = Column(Float)
    floating_component = Column(Float)
    total_cashflow = Column(Float)
    discount_factor = Column(Float)
    pv = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)