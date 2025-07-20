# models.py
from sqlalchemy import Column, Integer, String, Float, Date
from sqlalchemy.ext.declarative import declarative_base

# Import Base from your database.py
from .database import Base

# --- Database Models ---
class Loan(Base):
    __tablename__ = "loans"
    id = Column(Integer, primary_key=True, index=True)
    instrument_id = Column(String, unique=True, index=True)
    type = Column(String) # e.g., "Fixed Rate Loan", "Floating Rate Loan"
    notional = Column(Float)
    interest_rate = Column(Float) # Annual rate (for fixed)
    maturity_date = Column(Date)
    origination_date = Column(Date)
    benchmark_rate_type = Column(String, nullable=True) # e.g., "LIBOR", "SOFR", "Prime"
    spread = Column(Float, nullable=True) # Spread over benchmark for floating rates
    repricing_frequency = Column(String, nullable=True) # e.g., "Monthly", "Quarterly", "Annually"
    next_repricing_date = Column(Date, nullable=True) # New field for floating rates

    def __repr__(self):
        return f"<Loan(id={self.id}, instrument_id='{self.instrument_id}', notional={self.notional})>"

class Deposit(Base):
    __tablename__ = "deposits"
    id = Column(Integer, primary_key=True, index=True)
    instrument_id = Column(String, unique=True, index=True)
    type = Column(String) # e.g., "Checking", "Savings", "CD" (Certificate of Deposit)
    balance = Column(Float)
    interest_rate = Column(Float) # Annual rate
    open_date = Column(Date)
    maturity_date = Column(Date, nullable=True) # For CDs, null for checking/savings
    repricing_frequency = Column(String, nullable=True) # e.g., "Monthly", "Quarterly", "Annually"
    next_repricing_date = Column(Date, nullable=True) # New field for floating/adjustable deposits

    def __repr__(self):
        return f"<Deposit(id={self.id}, instrument_id='{self.instrument_id}', balance={self.balance})>"
