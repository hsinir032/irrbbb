# calculations.py
import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy.orm import Session
import math

# Corrected: Import models as a module, so models.Loan, models.Deposit, models.Derivative works
import models
from schemas import GapBucket, DashboardData, EVEScenarioResult

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


def calculate_nii_and_eve(db: Session) -> Dict[str, Any]:
    """
    Calculates Net Interest Income (NII) and Economic Value of Equity (EVE)
    based on data from the database.
    This is a simplified calculation for demonstration purposes.
    """
    loans = db.query(models.Loan).all()
    deposits = db.query(models.Deposit).all()
    derivatives = db.query(models.Derivative).all() # Fetch derivatives

    loans_df = pd.DataFrame([loan.__dict__ for loan in loans])
    deposits_df = pd.DataFrame([deposit.__dict__ for deposit in deposits])
    derivatives_df = pd.DataFrame([derivative.__dict__ for derivative in derivatives]) # Convert to df

    today = date.today()

    # --- NII Calculation (Simplified) ---
    total_loan_interest_income = 0.0
    if not loans_df.empty:
        loans_df['effective_rate'] = loans_df.apply(
            lambda row: row['interest_rate'] if row['type'] == 'Fixed Rate Loan' else (BASE_YIELD_CURVE["1Y"] + row['spread'] if pd.notna(row['spread']) else BASE_YIELD_CURVE["1Y"]), # Assume 1Y benchmark for floating
            axis=1
        )
        total_loan_interest_income = (loans_df['notional'] * loans_df['effective_rate']).sum()

    total_deposit_interest_expense = 0.0
    if not deposits_df.empty:
        total_deposit_interest_expense = (deposits_df['balance'] * deposits_df['interest_rate']).sum()

    # Derivatives impact NII, but for simplicity, we'll only include them in EVE for now.
    # A full NII calculation would involve projecting cash flows from derivatives.

    net_interest_income = total_loan_interest_income - total_deposit_interest_expense

    # --- EVE Calculation (Comprehensive with Scenarios) ---
    eve_scenario_results: List[EVEScenarioResult] = []

    for scenario_name, shock_bps in INTEREST_RATE_SCENARIOS.items():
        shocked_curve = shock_yield_curve(BASE_YIELD_CURVE, shock_bps)
        
        total_pv_assets = 0.0
        total_pv_liabilities = 0.0
        total_pv_derivatives = 0.0

        if not loans_df.empty:
            for index, row in loans_df.iterrows():
                loan_obj = models.Loan(**row.to_dict()) # Reconstruct model object
                total_pv_assets += calculate_loan_pv(loan_obj, shocked_curve, today)
        
        if not deposits_df.empty:
            for index, row in deposits_df.iterrows():
                deposit_obj = models.Deposit(**row.to_dict()) # Reconstruct model object
                total_pv_liabilities += calculate_deposit_pv(deposit_obj, shocked_curve, today)

        if not derivatives_df.empty:
            for index, row in derivatives_df.iterrows():
                derivative_obj = models.Derivative(**row.to_dict()) # Reconstruct model object
                total_pv_derivatives += calculate_derivative_pv(derivative_obj, shocked_curve, today)

        # EVE = PV(Assets) - PV(Liabilities) + PV(Derivatives)
        eve_value = total_pv_assets - total_pv_liabilities + total_pv_derivatives
        eve_scenario_results.append(EVEScenarioResult(scenario_name=scenario_name, eve_value=eve_value))

    # Base Case EVE for dashboard display
    base_case_eve = [res.eve_value for res in eve_scenario_results if res.scenario_name == "Base Case"][0]

    # Portfolio value for dashboard display (sum of base case PVs)
    # This is a more realistic "portfolio value" than just notional sum
    base_curve = BASE_YIELD_CURVE # Use base curve for portfolio value
    total_assets_value_base = sum(calculate_loan_pv(models.Loan(**row.to_dict()), base_curve, today) for index, row in loans_df.iterrows()) if not loans_df.empty else 0.0
    total_liabilities_value_base = sum(calculate_deposit_pv(models.Deposit(**row.to_dict()), base_curve, today) for index, row in deposits_df.iterrows()) if not deposits_df.empty else 0.0
    total_derivatives_value_base = sum(calculate_derivative_pv(models.Derivative(**row.to_dict()), base_curve, today) for index, row in derivatives_df.iterrows()) if not derivatives_df.empty else 0.0
    
    portfolio_value = total_assets_value_base - total_liabilities_value_base + total_derivatives_value_base


    # Simulate sensitivity values (these are now derived from scenario results)
    # For simplicity, let's just use the base case EVE and a random sensitivity for the main display
    # A true sensitivity would be (EVE_shocked - EVE_base) / shock_amount
    eve_sensitivity = round(random.uniform(-0.5, 0.5), 2) # % change in EVE for a rate shock
    nii_sensitivity = round(random.uniform(-0.2, 0.2), 2) # % change in NII for a rate shock

    return {
        "net_interest_income": net_interest_income,
        "economic_value_of_equity": base_case_eve, # Base case EVE
        "total_assets_value": total_assets_value_base,
        "total_liabilities_value": total_liabilities_value_base,
        "total_derivatives": len(derivatives), # New
        "eve_sensitivity": eve_sensitivity,
        "nii_sensitivity": nii_sensitivity,
        "total_loans": len(loans),
        "total_deposits": len(deposits),
        "portfolio_value": portfolio_value, # Updated to be based on PV
        "eve_scenarios": eve_scenario_results # New: EVE results for all scenarios
    )
