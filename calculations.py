# calculations.py
import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy.orm import Session

# Corrected: Import models and schemas using absolute paths
from models import Loan, Deposit
from schemas import GapBucket, DashboardData

# In-memory store for scenario history (for demonstration)
_scenario_history: List[Dict[str, Any]] = []
MAX_SCENARIO_HISTORY = 10

def get_bucket(item_date: date, today: date, buckets: Dict[str, int]) -> str:
    """Assigns an item to a time bucket based on its date relative to today."""
    if item_date is None:
        return "Non-Sensitive / Undefined" # For items without a clear repricing/maturity date

    days_diff = (item_date - today).days

    for bucket_name, days_limit in buckets.items():
        if days_limit == -1: # Represents ">X days" or "Longer" bucket
            # For the last bucket (e.g., ">5 Years"), check if days_diff is greater than the previous bucket's upper limit
            # This logic assumes buckets are ordered from shortest to longest
            previous_bucket_limit = 0
            found_previous = False
            for prev_name, prev_limit in buckets.items():
                if prev_name == bucket_name:
                    break
                previous_bucket_limit = prev_limit
                found_previous = True
            
            if days_diff > previous_bucket_limit:
                return bucket_name
        elif days_diff <= days_limit:
            return bucket_name
    return "Non-Sensitive / Undefined" # Fallback for dates beyond defined buckets


def calculate_nii_and_eve(db: Session) -> Dict[str, Any]:
    """
    Calculates Net Interest Income (NII) and Economic Value of Equity (EVE)
    based on data from the database.
    This is a simplified calculation for demonstration purposes.
    """
    loans = db.query(Loan).all()
    deposits = db.query(Deposit).all()

    # Convert to pandas DataFrames for easier calculation
    loans_df = pd.DataFrame([loan.__dict__ for loan in loans])
    deposits_df = pd.DataFrame([deposit.__dict__ for deposit in deposits])

    # --- NII Calculation (Simplified) ---
    total_loan_interest_income = 0.0
    if not loans_df.empty:
        # For fixed loans, use interest_rate. For floating, assume a current benchmark + spread.
        # This is a simplification; real floating rates would change over time.
        loans_df['effective_rate'] = loans_df.apply(
            lambda row: row['interest_rate'] if row['type'] == 'Fixed Rate Loan' else (0.04 + row['spread'] if pd.notna(row['spread']) else 0.04), # Assume 4% benchmark for floating
            axis=1
        )
        total_loan_interest_income = (loans_df['notional'] * loans_df['effective_rate']).sum()

    total_deposit_interest_expense = 0.0
    if not deposits_df.empty:
        total_deposit_interest_expense = (deposits_df['balance'] * deposits_df['interest_rate']).sum()

    net_interest_income = total_loan_interest_income - total_deposit_interest_expense

    # --- EVE Calculation (Highly Simplified for demonstration) ---
    total_assets_value = loans_df['notional'].sum() if not loans_df.empty else 0.0
    total_liabilities_value = deposits_df['balance'].sum() if not deposits_df.empty else 0.0
    
    economic_value_of_equity = total_assets_value - total_liabilities_value

    # Simulate sensitivity values (these are still random, but now they are "sensitivities"
    # of the calculated EVE/NII, rather than being the EVE/NII themselves).
    eve_sensitivity = round(random.uniform(-0.5, 0.5), 2) # % change in EVE for a rate shock
    nii_sensitivity = round(random.uniform(-0.2, 0.2), 2) # % change in NII for a rate shock

    return {
        "net_interest_income": net_interest_income,
        "economic_value_of_equity": economic_value_of_equity,
        "total_assets_value": total_assets_value,
        "total_liabilities_value": total_liabilities_value,
        "eve_sensitivity": eve_sensitivity,
        "nii_sensitivity": nii_sensitivity,
        "total_loans": len(loans),
        "total_deposits": len(deposits)
    }


def calculate_gap_analysis(db: Session) -> Dict[str, List[GapBucket]]:
    """
    Calculates NII Repricing Gap and EVE Maturity Gap.
    """
    loans = db.query(Loan).all()
    deposits = db.query(Deposit).all()

    loans_df = pd.DataFrame([loan.__dict__ for loan in loans])
    deposits_df = pd.DataFrame([deposit.__dict__ for deposit in deposits])

    today = date.today()

    # --- NII Repricing Gap Buckets (in days from today) ---
    nii_buckets_def = {
        "0-3 Months": 90,
        "3-6 Months": 180,
        "6-12 Months": 365,
        "1-5 Years": 365 * 5,
        ">5 Years": 365 * 100, # Effectively "longer"
        "Fixed Rate / Non-Sensitive": -1 # Special bucket for fixed or non-repricing items
    }
    nii_bucket_order = list(nii_buckets_def.keys())

    nii_gap_data: Dict[str, Dict[str, float]] = {
        bucket: {"assets": 0.0, "liabilities": 0.0, "gap": 0.0}
        for bucket in nii_bucket_order
    }

    # Process Loans for NII Gap
    if not loans_df.empty:
        for index, row in loans_df.iterrows():
            if row['type'] == "Fixed Rate Loan" or pd.isna(row['next_repricing_date']):
                bucket_name = "Fixed Rate / Non-Sensitive"
            else:
                bucket_name = get_bucket(row['next_repricing_date'], today, nii_buckets_def)
            
            nii_gap_data[bucket_name]["assets"] += row['notional']

    # Process Deposits for NII Gap
    if not deposits_df.empty:
        for index, row in deposits_df.iterrows():
            if pd.isna(row['next_repricing_date']) and pd.isna(row['maturity_date']): # Assume non-repricing
                bucket_name = "Fixed Rate / Non-Sensitive"
            elif row['type'] == "CD" and pd.notna(row['maturity_date']): # CDs reprice at maturity
                bucket_name = get_bucket(row['maturity_date'], today, nii_buckets_def)
            elif pd.notna(row['next_repricing_date']): # Other deposits with repricing
                 bucket_name = get_bucket(row['next_repricing_date'], today, nii_buckets_def)
            else:
                bucket_name = "Fixed Rate / Non-Sensitive" # Fallback
            
            nii_gap_data[bucket_name]["liabilities"] += row['balance']

    # Calculate NII Gap
    nii_gap_results = []
    for bucket in nii_bucket_order:
        assets = nii_gap_data[bucket]["assets"]
        liabilities = nii_gap_data[bucket]["liabilities"]
        gap = assets - liabilities
        nii_gap_results.append(GapBucket(bucket=bucket, assets=assets, liabilities=liabilities, gap=gap))

    # --- EVE Maturity Gap Buckets (in days from today) ---
    eve_buckets_def = {
        "0-1 Year": 365,
        "1-3 Years": 365 * 3,
        "3-5 Years": 365 * 5,
        "5-10 Years": 365 * 10,
        ">10 Years": 365 * 100, # Effectively "longer"
        "Non-Maturity": -1 # Special bucket for non-maturity deposits (checking/savings)
    }
    eve_bucket_order = list(eve_buckets_def.keys())

    eve_gap_data: Dict[str, Dict[str, float]] = {
        bucket: {"assets": 0.0, "liabilities": 0.0, "gap": 0.0}
        for bucket in eve_bucket_order
    }

    # Process Loans for EVE Gap (based on maturity)
    if not loans_df.empty:
        for index, row in loans_df.iterrows():
            bucket_name = get_bucket(row['maturity_date'], today, eve_buckets_def)
            eve_gap_data[bucket_name]["assets"] += row['notional']

    # Process Deposits for EVE Gap (based on maturity)
    if not deposits_df.empty:
        for index, row in deposits_df.iterrows():
            if row['type'] == "CD" and pd.notna(row['maturity_date']):
                bucket_name = get_bucket(row['maturity_date'], today, eve_buckets_def)
            else: # Checking/Savings are non-maturity deposits
                bucket_name = "Non-Maturity"
            eve_gap_data[bucket_name]["liabilities"] += row['balance']

    # Calculate EVE Gap
    eve_gap_results = []
    for bucket in eve_bucket_order:
        assets = eve_gap_data[bucket]["assets"]
        liabilities = eve_gap_data[bucket]["liabilities"]
        gap = assets - liabilities
        eve_gap_results.append(GapBucket(bucket=bucket, assets=assets, liabilities=liabilities, gap=gap))

    return {
        "nii_repricing_gap": nii_gap_results,
        "eve_maturity_gap": eve_gap_results
    }


def generate_dashboard_data_from_db(db: Session) -> DashboardData:
    """Generates dashboard data by fetching from DB and performing calculations."""
    global _scenario_history

    # Get calculated financial metrics
    calculated_metrics = calculate_nii_and_eve(db)
    gap_analysis_metrics = calculate_gap_analysis(db)

    # Yield curve and scenario data remain largely simulated for now
    new_yield_curve_data = [
        {"name": "1M", "yield": round(random.uniform(0.5, 0.7), 2)},
        {"name": "3M", "yield": round(random.uniform(0.7, 1.0), 2)},
        {"name": "6M", "yield": round(random.uniform(1.0, 1.4), 2)},
        {"name": "1Y", "yield": round(random.uniform(1.5, 2.0), 2)},
        {"name": "2Y", "yield": round(random.uniform(2.0, 2.6), 2)},
        {"name": "5Y", "yield": round(random.uniform(2.5, 3.2), 2)},
        {"name": "10Y", "yield": round(random.uniform(3.0, 3.8), 2)},
    ]

    now = datetime.now()
    new_scenario_point = {
        "time": now.strftime("%H:%M:%S"),
        "base_case": round(random.uniform(97.5, 102.5), 2),
        "plus_100bps": round(random.uniform(92.5, 97.5), 2),
        "minus_100bps": round(random.uniform(102.5, 107.5), 2),
    }

    _scenario_history.append(new_scenario_point)
    _scenario_history = _scenario_history[-MAX_SCENARIO_HISTORY:]

    return DashboardData(
        eve_sensitivity=calculated_metrics["eve_sensitivity"],
        nii_sensitivity=calculated_metrics["nii_sensitivity"],
        portfolio_value=calculated_metrics["total_assets_value"],
        yield_curve_data=new_yield_curve_data,
        scenario_data=_scenario_history,
        total_loans=calculated_metrics["total_loans"],
        total_deposits=calculated_metrics["total_deposits"],
        total_assets_value=calculated_metrics["total_assets_value"],
        total_liabilities_value=calculated_metrics["total_liabilities_value"],
        net_interest_income=calculated_metrics["net_interest_income"],
        economic_value_of_equity=calculated_metrics["economic_value_of_equity"],
        nii_repricing_gap=gap_analysis_metrics["nii_repricing_gap"],
        eve_maturity_gap=gap_analysis_metrics["eve_maturity_gap"]
    )
