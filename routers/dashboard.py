# routers/dashboard.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

# Import dependencies from your local project
from .. import schemas, calculations
from ..database import get_db

# Create an API Router instance
router = APIRouter(
    prefix="/api/v1/dashboard",
    tags=["Dashboard"] # Tags for Swagger UI documentation
)

@router.get("/live-data", response_model=schemas.DashboardData)
async def get_live_dashboard_data(db: Session = Depends(get_db)):
    """
    Fetches live IRRBB dashboard data, calculated from database instruments
    including NII Repricing Gap and EVE Maturity Gap.
    """
    return calculations.generate_dashboard_data_from_db(db)
