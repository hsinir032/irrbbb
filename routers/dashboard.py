# routers/dashboard.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

# Corrected: Import dependencies using absolute paths from the root package
from schemas import DashboardData # Import specific schema
from calculations import generate_dashboard_data_from_db # Import specific function
from database import get_db # Import specific dependency

# Create an API Router instance
router = APIRouter(
    prefix="/api/v1/dashboard",
    tags=["Dashboard"] # Tags for Swagger UI documentation
)

@router.get("/live-data", response_model=DashboardData) # Use imported DashboardData
async def get_live_dashboard_data(db: Session = Depends(get_db)): # Use imported get_db
    """
    Fetches live IRRBB dashboard data, calculated from database instruments
    including NII Repricing Gap and EVE Maturity Gap.
    """
    return generate_dashboard_data_from_db(db) # Use imported function
