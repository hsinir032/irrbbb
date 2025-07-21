# calculations.py
import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy.orm import Session
import math

# Corrected: Import models as a module, so models.Loan, models.Deposit, models.Derivative works
import models
# FIX: Import schemas as a module to resolve 'NameError: name 'schemas' is not defined'
import schemas

# In-memory store for scenario history (for demonstration)
_scenario_history: List[Dict[str, Any]] = []
MAX_SCENARIO_HISTORY = 10

# --- Helper function to convert frequency to periods per year ---
def get_periods_per_year(frequency: str) -> int:
    if frequency == "Monthly":
        return 12
    elif frequency == "Quarterly":
        return 4
    elif frequency == "Semi-Annually":
        return 2
    elif frequency == "Annually":
        return 1
    return 1 # Default

# --- Yield Curve and Scenario Definitions ---
BASE_YIELD_CURVE = {
    "1M": 0.0400, # 4.00%
    "3M": 0.0410, # 4.10%
    "6M": 0.0425, # 4.25%
    "1Y": 0.0450, # 4.50%
    "2Y": 0.0475, # 4.75%
    "3Y": 0.0500, # 5.00%
    "5Y": 0.0525, # 5.25%
    "7Y": 0.0540, # 5.40%
    "10Y": 0.0550, # 5.50%
    "15Y": 0.0560, # 5.60%
    "20Y": 0.0565, # 5.65%
    "30Y": 0.0570, # 5.70%
}

# Define 6 interest rate scenarios (parallel shifts and twists in basis points)
# Values are in basis points (1 bp = 0.0001)
INTEREST_RATE_SCENARIOS = {
    "Base Case": {"1M": 0, "3M": 0, "6M": 0, "1Y": 0, "2Y": 0, "3Y": 0, "5Y": 0, "7Y": 0, "10Y": 0, "15Y": 0, "20Y": 0, "30Y": 0},
    "Parallel Up +200bps": {"1M": 200, "3M": 200, "6M": 200, "1Y": 200, "2Y": 200, "3Y": 200, "5Y": 200, "7Y": 200, "10Y": 200, "15Y": 200, "20Y": 200, "30Y": 200},
    "Parallel Down -200bps": {"1M": -200, "3M": -200, "6M": -200, "1Y": -200, "2Y": -200, "3Y": -200, "5Y": -200, "7Y": -200, "10Y": -200, "15Y": -200, "20Y": -200, "30Y": -200},
    "Short Rates Up +100bps": {"1M": 100, "3M": 100, "6M": 50, "1Y": 25, "2Y": 0, "3Y": 0, "5Y": 0, "7Y": 0, "10Y": 0, "15Y": 0, "20Y": 0, "30Y": 0},
    "Short Rates Down -100bps": {"1M": -100, "3M": -100, "6M": -50, "1Y": -25, "2Y": 0, "3Y": 0, "5Y": 0, "7Y": 0, "10Y": 0, "15Y": 0, "20Y": 0, "30Y": 0},
    "Long Rates Up +100bps": {"1M": 0, "3M": 0, "6M": 0, "1Y": 0, "2Y": 25, "3Y": 50, "5Y": 100, "7Y": 100, "10Y": 100, "15Y": 50, "20Y": 25, "30Y": 0},
}

def shock_yield_curve(base_curve: Dict[str, float], shock_bps: Dict[str, int]) -> Dict[str, float]:
    """Applies a shock to the base yield curve."""
    shocked_curve = {}
    for tenor, rate in base_curve.items():
        shocked_curve[tenor] = rate + (shock_bps.get(tenor, 0) / 10000) # Convert bps to decimal
    return shocked_curve

def interpolate_rate(yield_curve: Dict[str, float], days_to_maturity: int) -> float:
    """
    Simple linear interpolation for a rate given days to maturity.
    Assumes yield_curve keys are sorted by tenor (e.g., "1M", "3M", "1Y", etc.).
    """
    tenor_map = {
        "1M": 30, "3M": 90, "6M": 180, "1Y": 365, "2Y": 365*2, "3Y": 365*3,
        "5Y": 365*5, "7Y": 365*7, "10Y": 365*10, "15Y": 365*15, "20Y": 365*20, "30Y": 365*30
    }
    tenors = sorted(tenor_map.keys(), key=lambda x: tenor_map[x])

    if days_to_maturity <= tenor_map[tenors[0]]:
        return yield_curve[tenors[0]]
    if days_to_maturity >= tenor_map[tenors[-1]]:
        return yield_curve[tenors[-1]]

    for i in range(len(tenors) - 1):
        t1, t2 = tenors[i], tenors[i+1]
        days1, days2 = tenor_map[t1], tenor_map[t2]
        rate1, rate2 = yield_curve[t1], yield_curve[t2]

        if days1 <= days_to_maturity <= days2:
            if days1 == days2: # Avoid division by zero
                return rate1
            # Linear interpolation
            return rate1 + (rate2 - rate1) * (days_to_maturity - days1) / (days2 - days1)
    return yield_curve[tenors[-1]] # Fallback


# --- Present Value Calculation Functions (Simplified) ---

def calculate_loan_pv(loan: models.Loan, yield_curve: Dict[str, float], today: date) -> float:
    """
    Simplified PV calculation for a loan.
    Assumes loan's value is primarily driven by its notional and remaining term.
    For floating rate loans, assumes repricing to current interpolated curve rate.
    """
    days_to_maturity = (loan.maturity_date - today).days
    if days_to_maturity <= 0:
        return 0.0 # Matured loan

    # Determine the effective interest rate for discounting
    if loan.type == "Fixed Rate Loan":
        effective_rate = loan.interest_rate
    else: # Floating Rate Loan
        # For floating, assume it reprices to the current interpolated rate for its remaining term
        effective_rate = interpolate_rate(yield_curve, days_to_maturity) + (loan.spread if loan.spread is not None else 0)

    # Simple discounting of notional at maturity
    # In reality, this would involve projecting and discounting all future cash flows (interest + principal)
    discount_factor = 1 / (1 + effective_rate * (days_to_maturity / 365))
    pv = loan.notional * discount_factor
    return pv

def calculate_deposit_pv(deposit: models.Deposit, yield_curve: Dict[str, float], today: date) -> float:
    """
    Simplified PV calculation for a deposit.
    For non-maturity deposits (checking/savings), value is approx balance.
    For CDs, it's discounted balance.
    """
    if deposit.type in ["Checking", "Savings"]:
        # Non-maturity deposits are often valued at par or with a small discount/premium
        # For simplicity, assume PV is current balance.
        return deposit.balance
    elif deposit.type == "CD" and deposit.maturity_date:
        days_to_maturity = (deposit.maturity_date - today).days
        if days_to_maturity <= 0:
            return 0.0 # Matured CD

        # Use deposit's fixed rate for discounting its balance
        effective_rate = deposit.interest_rate
        discount_factor = 1 / (1 + effective_rate * (days_to_maturity / 365))
        pv = deposit.balance * discount_factor
        return pv
    return 0.0 # Fallback

def calculate_derivative_pv(derivative: models.Derivative, yield_curve: Dict[str, float], today: date) -> float:
    """
    Highly simplified PV calculation for an Interest Rate Swap.
    This is a very basic conceptual model, not a true swap valuation.
    Assumes net present value is based on difference in fixed vs. interpolated floating rate
    applied to notional for remaining term.
    """
    if derivative.type != "Interest Rate Swap":
        return 0.0 # Only handling swaps for now

    days_to_end = (derivative.end_date - today).days
    if days_to_end <= 0:
        return 0.0 # Matured derivative

    # Interpolate current floating rate based on remaining term
    current_floating_rate = interpolate_rate(yield_curve, days_to_end) + (derivative.floating_spread if derivative.floating_spread is not None else 0)

    fixed_rate = derivative.fixed_rate if derivative.fixed_rate is not None else 0

    # Simplified net interest differential per year
    if derivative.subtype == "Payer Swap": # Bank pays fixed, receives floating
        net_rate_diff = current_floating_rate - fixed_rate
    elif derivative.subtype == "Receiver Swap": # Bank receives fixed, pays floating
        net_rate_diff = fixed_rate - current_floating_rate
    else:
        net_rate_diff = 0

    # Approximate PV: Notional * (Net Rate Diff) * Remaining Years, discounted
    remaining_years = days_to_end / 365.0
    
    # Use interpolated rate for discounting
    discount_rate = interpolate_rate(yield_curve, days_to_end)
    discount_factor = 1 / (1 + discount_rate * remaining_years)

    pv = derivative.notional * net_rate_diff * remaining_years * discount_factor
    return pv


# --- Existing Functions (Adapted for new models) ---

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


def calculate_nii_and_eve_for_curve(db_session: Session, yield_curve: Dict[str, float]) -> Dict[str, Any]:
    """
    Calculates Net Interest Income (NII) and Economic Value of Equity (EVE)
    based on data from the database for a given yield curve.
    This is a simplified calculation for demonstration purposes.
    """
    loans = db_session.query(models.Loan).all()
    deposits = db_session.query(models.Deposit).all()
    derivatives = db_session.query(models.Derivative).all()

    # Get column names for each model to filter DataFrame rows
    loan_cols = {col.name for col in models.Loan.__table__.columns}
    deposit_cols = {col.name for col in models.Deposit.__table__.columns}
    derivative_cols = {col.name for col in models.Derivative.__table__.columns}

    # When converting SQLAlchemy objects to DataFrames, filter out internal attributes
    loans_data = [
        {k: v for k, v in loan.__dict__.items() if not k.startswith('_sa_')}
        for loan in loans
    ]
    deposits_data = [
        {k: v for k, v in deposit.__dict__.items() if not k.startswith('_sa_')}
        for deposit in deposits
    ]
    derivatives_data = [
        {k: v for k, v in derivative.__dict__.items() if not k.startswith('_sa_')}
        for derivative in derivatives
    ]

    loans_df = pd.DataFrame(loans_data)
    deposits_df = pd.DataFrame(deposits_data)
    derivatives_df = pd.DataFrame(derivatives_data)

    today = date.today()

    # --- NII Calculation (Scenario-sensitive) ---
    total_loan_interest_income = 0.0
    if not loans_df.empty:
        loans_df['effective_rate'] = loans_df.apply(
            lambda row: row['interest_rate'] if row['type'] == 'Fixed Rate Loan' else (
                interpolate_rate(yield_curve, (row['next_repricing_date'] - today).days if pd.notna(row['next_repricing_date']) else 365) + (row['spread'] if pd.notna(row['spread']) else 0)
            ),
            axis=1
        )
        total_loan_interest_income = (loans_df['notional'] * loans_df['effective_rate']).sum()

    total_deposit_interest_expense = 0.0
    if not deposits_df.empty:
        deposits_df['effective_rate'] = deposits_df.apply(
            lambda row: row['interest_rate'] if row['type'] == 'CD' else (
                interpolate_rate(yield_curve, (row['next_repricing_date'] - today).days if pd.notna(row['next_repricing_date']) else 30) # Assume 1M for non-maturity repricing
            ),
            axis=1
        )
        total_deposit_interest_expense = (deposits_df['balance'] * deposits_df['effective_rate']).sum()

    net_interest_income = total_loan_interest_income - total_deposit_interest_expense

    # --- EVE Calculation ---
    total_pv_assets = 0.0
    total_pv_liabilities = 0.0
    total_pv_derivatives = 0.0

    if not loans_df.empty:
        for index, row in loans_df.iterrows():
            # Filter row.to_dict() to only include actual model columns
            clean_row_dict = {k: v for k, v in row.to_dict().items() if k in loan_cols}
            loan_obj = models.Loan(**clean_row_dict)
            total_pv_assets += calculate_loan_pv(loan_obj, yield_curve, today)
    
    if not deposits_df.empty:
        for index, row in deposits_df.iterrows():
            # Filter row.to_dict() to only include actual model columns
            clean_row_dict = {k: v for k, v in row.to_dict().items() if k in deposit_cols}
            deposit_obj = models.Deposit(**clean_row_dict)
            total_pv_liabilities += calculate_deposit_pv(deposit_obj, yield_curve, today)

    if not derivatives_df.empty:
        for index, row in derivatives_df.iterrows():
            # Filter row.to_dict() to only include actual model columns
            clean_row_dict = {k: v for k, v in row.to_dict().items() if k in derivative_cols}
            derivative_obj = models.Derivative(**clean_row_dict)
            total_pv_derivatives += calculate_derivative_pv(derivative_obj, yield_curve, today)

    eve_value = total_pv_assets - total_pv_liabilities + total_pv_derivatives

    return {
        "net_interest_income": net_interest_income,
        "economic_value_of_equity": eve_value,
        "total_assets_value": total_pv_assets,
        "total_liabilities_value": total_pv_liabilities,
        "total_derivatives_value": total_pv_derivatives # New: PV of derivatives
    }


def calculate_gap_analysis(db: Session) -> Dict[str, List[schemas.GapBucket]]:
    """
    Calculates NII Repricing Gap and EVE Maturity Gap.
    """
    loans = db.query(models.Loan).all()
    deposits = db.query(models.Deposit).all()
    derivatives = db.query(models.Derivative).all() # Fetch derivatives

    # Filter out _sa_instance_state before creating DataFrames
    loans_data = [{k: v for k, v in loan.__dict__.items() if not k.startswith('_sa_')} for loan in loans]
    deposits_data = [{k: v for k, v in deposit.__dict__.items() if not k.startswith('_sa_')} for deposit in deposits]
    derivatives_data = [{k: v for k, v in derivative.__dict__.items() if not k.startswith('_sa_')} for derivative in derivatives]

    loans_df = pd.DataFrame(loans_data)
    deposits_df = pd.DataFrame(deposits_data)
    derivatives_df = pd.DataFrame(derivatives_data)

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
            if row['type'] == "Fixed Rate Loan" or pd.isna(row.get('next_repricing_date')): # Use .get() for safety
                bucket_name = "Fixed Rate / Non-Sensitive"
            else:
                bucket_name = get_bucket(row['next_repricing_date'], today, nii_buckets_def)
            
            nii_gap_data[bucket_name]["assets"] += row['notional']

    # Process Deposits for NII Gap
    if not deposits_df.empty:
        for index, row in deposits_df.iterrows():
            # Use .get() for safety as next_repricing_date might not exist for all deposit types
            if pd.isna(row.get('next_repricing_date')) and pd.isna(row.get('maturity_date')): # Assume non-repricing
                bucket_name = "Fixed Rate / Non-Sensitive"
            elif row['type'] == "CD" and pd.notna(row.get('maturity_date')): # CDs reprice at maturity
                bucket_name = get_bucket(row['maturity_date'], today, nii_buckets_def)
            elif pd.notna(row.get('next_repricing_date')): # Other deposits with repricing
                 bucket_name = get_bucket(row['next_repricing_date'], today, nii_buckets_def)
            else:
                bucket_name = "Fixed Rate / Non-Sensitive" # Fallback
            
            nii_gap_data[bucket_name]["liabilities"] += row['balance']

    # Derivatives also have repricing characteristics, but for NII gap, it's more complex
    # For simplicity, we'll exclude them from NII repricing gap for now.
    # A full NII gap would require analyzing the fixed vs floating legs' repricing.

    # Calculate NII Gap
    nii_gap_results = []
    for bucket in nii_bucket_order:
        assets = nii_gap_data[bucket]["assets"]
        liabilities = nii_gap_data[bucket]["liabilities"]
        gap = assets - liabilities
        nii_gap_results.append(schemas.GapBucket(bucket=bucket, assets=assets, liabilities=liabilities, gap=gap))

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
            if row['type'] == "CD" and pd.notna(row.get('maturity_date')): # Use .get() for safety
                bucket_name = get_bucket(row['maturity_date'], today, eve_buckets_def)
            else: # Checking/Savings are non-maturity deposits
                bucket_name = "Non-Maturity"
            eve_gap_data[bucket_name]["liabilities"] += row['balance']

    # Derivatives for EVE Gap (based on end_date)
    if not derivatives_df.empty:
        for index, row in derivatives_df.iterrows():
            bucket_name = get_bucket(row['end_date'], today, eve_buckets_def)
            # Derivatives can be assets or liabilities depending on their PV.
            # For simplicity, we'll assign their notional to assets/liabilities based on subtype
            # A more accurate approach would be to assign their PV to the gap bucket.
            if row['subtype'] == "Payer Swap": # Bank pays fixed, receives floating (often asset-like if rates rise)
                eve_gap_data[bucket_name]["assets"] += row['notional']
            elif row['subtype'] == "Receiver Swap": # Bank receives fixed, pays floating (often liability-like if rates rise)
                eve_gap_data[bucket_name]["liabilities"] += row['notional']


    # Calculate EVE Gap
    eve_gap_results = []
    for bucket in eve_bucket_order:
        assets = eve_gap_data[bucket]["assets"]
        liabilities = eve_gap_data[bucket]["liabilities"]
        gap = assets - liabilities
        eve_gap_results.append(schemas.GapBucket(bucket=bucket, assets=assets, liabilities=liabilities, gap=gap))

    return {
        "nii_repricing_gap": nii_gap_results,
        "eve_maturity_gap": eve_gap_results
    }


def generate_dashboard_data_from_db(db: Session) -> schemas.DashboardData:
    """
    Generates dashboard data by fetching from DB and performing calculations,
    including scenario-based EVE/NII and portfolio composition.
    """
    global _scenario_history

    today = date.today()
    loans = db.query(models.Loan).all()
    deposits = db.query(models.Deposit).all()
    derivatives = db.query(models.Derivative).all()

    # --- Calculate Portfolio Composition ---
    loan_composition: Dict[str, float] = {}
    for loan in loans:
        loan_composition[loan.type] = loan_composition.get(loan.type, 0.0) + loan.notional

    deposit_composition: Dict[str, float] = {}
    for deposit in deposits:
        deposit_composition[deposit.type] = deposit_composition.get(deposit.type, 0.0) + deposit.balance
    
    derivative_composition: Dict[str, float] = {}
    for derivative in derivatives:
        derivative_composition[derivative.type] = derivative_composition.get(derivative.type, 0.0) + derivative.notional


    # --- Calculate EVE and NII for all Scenarios ---
    eve_scenario_results: List[schemas.EVEScenarioResult] = []
    nii_scenario_results: List[schemas.NIIScenarioResult] = []
    
    base_case_eve = 0.0
    base_case_nii = 0.0
    total_assets_value_base = 0.0
    total_liabilities_value_base = 0.0
    portfolio_value_base = 0.0

    for scenario_name, shock_bps in INTEREST_RATE_SCENARIOS.items():
        shocked_curve = shock_yield_curve(BASE_YIELD_CURVE, shock_bps)
        
        # Calculate NII and EVE for the current shocked curve
        metrics_for_curve = calculate_nii_and_eve_for_curve(db, shocked_curve)
        
        eve_scenario_results.append(schemas.EVEScenarioResult(
            scenario_name=scenario_name,
            eve_value=metrics_for_curve["economic_value_of_equity"]
        ))
        nii_scenario_results.append(schemas.NIIScenarioResult(
            scenario_name=scenario_name,
            nii_value=metrics_for_curve["net_interest_income"]
        ))

        if scenario_name == "Base Case":
            base_case_eve = metrics_for_curve["economic_value_of_equity"]
            base_case_nii = metrics_for_curve["net_interest_income"]
            total_assets_value_base = metrics_for_curve["total_assets_value"]
            total_liabilities_value_base = metrics_for_curve["total_liabilities_value"]
            portfolio_value_base = total_assets_value_base - total_liabilities_value_base + metrics_for_curve["total_derivatives_value"]


    # --- Calculate Sensitivities ---
    eve_sensitivity = 0.0
    nii_sensitivity = 0.0

    # Find the "Parallel Up +200bps" scenario for sensitivity calculation
    eve_up_200bps = next((res.eve_value for res in eve_scenario_results if res.scenario_name == "Parallel Up +200bps"), None)
    nii_up_200bps = next((res.nii_value for res in nii_scenario_results if res.scenario_name == "Parallel Up +200bps"), None)

    if base_case_eve != 0 and eve_up_200bps is not None:
        eve_sensitivity = ((eve_up_200bps - base_case_eve) / base_case_eve) * 100
        eve_sensitivity = round(eve_sensitivity, 2)

    if base_case_nii != 0 and nii_up_200bps is not None:
        nii_sensitivity = ((nii_up_200bps - base_case_nii) / base_case_nii) * 100
        nii_sensitivity = round(nii_sensitivity, 2)


    # --- Gap Analysis Metrics (unchanged, still uses current state) ---
    gap_analysis_metrics = calculate_gap_analysis(db)

    # --- Yield Curve Data for Display (Base Case) ---
    yield_curve_data_for_display = [{"name": tenor, "yield": rate * 100} for tenor, rate in BASE_YIELD_CURVE.items()]

    # --- Historical Scenario Data (for the EVE chart over time) ---
    now = datetime.now()
    new_scenario_point = {
        "time": now.strftime("%H:%M:%S"),
        "Base Case": base_case_eve, # Use actual calculated base case EVE
        "+200bps": next((res.eve_value for res in eve_scenario_results if res.scenario_name == "Parallel Up +200bps"), base_case_eve),
        "-200bps": next((res.eve_value for res in eve_scenario_results if res.scenario_name == "Parallel Down -200bps"), base_case_eve),
    }

    _scenario_history.append(new_scenario_point)
    _scenario_history = _scenario_history[-MAX_SCENARIO_HISTORY:]


    return schemas.DashboardData(
        eve_sensitivity=eve_sensitivity,
        nii_sensitivity=nii_sensitivity,
        portfolio_value=portfolio_value_base,
        yield_curve_data=yield_curve_data_for_display,
        scenario_data=_scenario_history,
        total_loans=len(loans),
        total_deposits=len(deposits),
        total_derivatives=len(derivatives),
        total_assets_value=total_assets_value_base,
        total_liabilities_value=total_liabilities_value_base,
        net_interest_income=base_case_nii,
        economic_value_of_equity=base_case_eve,
        nii_repricing_gap=gap_analysis_metrics["nii_repricing_gap"],
        eve_maturity_gap=gap_analysis_metrics["eve_maturity_gap"],
        eve_scenarios=eve_scenario_results,
        nii_scenarios=nii_scenario_results, # NEW
        loan_composition=loan_composition, # NEW
        deposit_composition=deposit_composition, # NEW
        derivative_composition=derivative_composition # NEW
    )
