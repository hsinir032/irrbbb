# calculations.py
import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from sqlalchemy.orm import Session
import math

import models
import schemas

from crud_dashboard import *
from schemas_dashboard import *
from models_dashboard import NiiDriver, EveDriver, RepricingBucket, RepricingNetPosition, PortfolioComposition, YieldCurve

# In-memory store for scenario history (for demonstration)
_scenario_history: List[Dict[str, Any]] = []
MAX_SCENARIO_HISTORY = 10

# Define NII horizon in days (e.g., 1 year for NII calculations)
NII_HORIZON_DAYS = 365

# --- Helper function to convert frequency to periods per year ---
def get_periods_per_year(frequency: Optional[str]) -> int:
    if frequency == "Monthly":
        return 12
    elif frequency == "Quarterly":
        return 4
    elif frequency == "Semi-Annually":
        return 2
    elif frequency == "Annually":
        return 1
    return 0 # For non-periodic or undefined

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

    if days_to_maturity <= 0:
        return yield_curve[tenors[0]] # Use shortest rate for immediate cash flows
    if days_to_maturity <= tenor_map[tenors[0]]:
        return yield_curve[tenors[0]]
    if days_to_maturity >= tenor_map[tenors[-1]]:
        return yield_curve[tenors[-1]]

    for i in range(len(tenors) - 1):
        t1, t2 = tenors[i], tenors[i+1]
        days1, days2 = tenor_map[t1], tenor_map[t2]
        rate1, rate2 = yield_curve[t1], yield_curve[t2]

        if days1 == days2: # Avoid division by zero
            return rate1

        if days1 <= days_to_maturity <= days2:
            # Linear interpolation
            return rate1 + (rate2 - rate1) * (days_to_maturity - days1) / (days2 - days1)
    return yield_curve[tenors[-1]] # Fallback for very long dates


# --- Core Cash Flow Projection and PV Calculation ---

def calculate_pv_of_cashflows(cashflows: List[Tuple[date, float]], yield_curve: Dict[str, float], today: date) -> float:
    """
    Calculates the present value of a list of (date, amount) cash flows.
    Each cash flow is discounted using the interpolated rate for its time to payment.
    """
    total_pv = 0.0
    for cf_date, cf_amount in cashflows:
        if cf_date <= today:
            if cf_date == today:
                total_pv += cf_amount # Include today's cash flows at face value
            continue

        days_to_payment = (cf_date - today).days
        if days_to_payment <= 0: # Should be caught by cf_date <= today, but as a safeguard
            total_pv += cf_amount # Treat as immediate if very close
            continue

        discount_rate = interpolate_rate(yield_curve, days_to_payment)
        discount_factor = 1 / (1 + discount_rate * (days_to_payment / 365.0)) # Simple annual compounding
        total_pv += cf_amount * discount_factor
    return total_pv

def generate_loan_cashflows(loan: models.Loan, yield_curve: Dict[str, float], today: date, 
                            include_principal: bool = True, prepayment_rate: float = 0.0) -> List[Tuple[date, float]]:
    """
    Generates projected interest and principal cash flows for a loan,
    incorporating a constant annual prepayment rate (CPR).
    Returns a list of (date, amount) tuples. Positive for income.
    """
    cashflows = []
    current_balance = loan.notional
    payment_periods_per_year = get_periods_per_year(loan.payment_frequency)
    
    if payment_periods_per_year == 0 and loan.maturity_date:
        if include_principal and loan.maturity_date > today:
            cashflows.append((loan.maturity_date, current_balance))
        return cashflows
    elif payment_periods_per_year == 0:
        return []

    interval_days = 365 / payment_periods_per_year
    prepayment_per_period = (1 - (1 - prepayment_rate)**(interval_days / 365.0)) # Convert CPR to period rate

    next_payment_date = loan.origination_date
    while next_payment_date <= today:
        next_payment_date += timedelta(days=interval_days)

    if next_payment_date <= today:
        next_payment_date = today + timedelta(days=interval_days)

    current_repricing_date = loan.next_repricing_date if loan.next_repricing_date else loan.origination_date
    if current_repricing_date < today:
        current_repricing_date = today

    while next_payment_date <= loan.maturity_date and current_balance > 0.01: # Continue as long as balance > 0
        # Determine effective rate for this period
        effective_rate = loan.interest_rate

        if loan.type == "Floating Rate Loan":
            if current_repricing_date <= next_payment_date:
                days_to_reprice = (current_repricing_date - today).days if current_repricing_date > today else 0
                effective_rate = interpolate_rate(yield_curve, days_to_reprice) + (loan.spread if loan.spread is not None else 0)
                
                repricing_freq_days = 365 / get_periods_per_year(loan.repricing_frequency) if loan.repricing_frequency else 0
                
                if repricing_freq_days > 0:
                    current_repricing_date += timedelta(days=repricing_freq_days)
                else:
                    current_repricing_date = loan.maturity_date + timedelta(days=1)

            if effective_rate is None:
                 effective_rate = interpolate_rate(yield_curve, (next_payment_date - today).days) + (loan.spread if loan.spread is not None else 0)

        # Calculate interest for the period
        interest_amount = current_balance * effective_rate * (interval_days / 365.0)
        cashflows.append((next_payment_date, interest_amount))

        # Apply prepayment and reduce balance
        if prepayment_rate > 0 and include_principal:
            prepayment_amount = current_balance * prepayment_per_period
            current_balance -= prepayment_amount
            if current_balance < 0: # Ensure balance doesn't go negative
                current_balance = 0

        next_payment_date += timedelta(days=interval_days)

    # Add remaining principal at maturity if not fully prepaid
    if include_principal and current_balance > 0.01 and loan.maturity_date and loan.maturity_date > today:
        cashflows.append((loan.maturity_date, current_balance))

    return cashflows


def generate_deposit_cashflows(deposit: models.Deposit, yield_curve: Dict[str, float], today: date, 
                                include_principal: bool = True, nmd_effective_maturity_years: int = 5, 
                                nmd_deposit_beta: float = 0.5) -> List[Tuple[date, float]]:
    """
    Generates projected interest and principal cash flows for a deposit,
    incorporating behavioral assumptions for Non-Maturity Deposits (NMDs).
    Returns a list of (date, amount) tuples. Negative for expense.
    """
    cashflows = []
    current_balance = deposit.balance
    
    # Handle CDs (Certificate of Deposits) and Wholesale Funding
    if deposit.type in ["CD", "Wholesale Funding"]:
        if not deposit.maturity_date or deposit.maturity_date <= today:
            return [] # Matured or missing maturity
        payment_periods_per_year = get_periods_per_year(deposit.payment_frequency)
        if payment_periods_per_year == 0:
            interval_days = (deposit.maturity_date - deposit.open_date).days
        else:
            interval_days = 365 / payment_periods_per_year
        next_payment_date = deposit.open_date
        while next_payment_date <= today:
            next_payment_date += timedelta(days=interval_days)
        if next_payment_date <= today:
            next_payment_date = today + timedelta(days=interval_days)
        while next_payment_date <= deposit.maturity_date:
            interest_expense = current_balance * deposit.interest_rate * (interval_days / 365.0)
            cashflows.append((next_payment_date, -interest_expense))
            next_payment_date += timedelta(days=interval_days)
        if include_principal:
            cashflows.append((deposit.maturity_date, -current_balance))
        return cashflows

    # Handle Non-Maturity Deposits (Checking/Savings)
    elif deposit.type in ["Checking", "Savings"]:
        conceptual_maturity_date = today + timedelta(days=365 * nmd_effective_maturity_years)
        
        repricing_freq_days = 365 / get_periods_per_year(deposit.repricing_frequency) if deposit.repricing_frequency else 30
        
        base_market_rate_for_nmd = interpolate_rate(BASE_YIELD_CURVE, repricing_freq_days)
        shocked_market_rate_for_nmd = interpolate_rate(yield_curve, repricing_freq_days)
        
        market_rate_change = shocked_market_rate_for_nmd - base_market_rate_for_nmd
        
        effective_rate = deposit.interest_rate + (market_rate_change * nmd_deposit_beta)
        effective_rate = max(0.0001, effective_rate) # Small floor

        pay_freq_days = 365 / get_periods_per_year(deposit.payment_frequency) if deposit.payment_frequency else 30

        next_payment_date = today + timedelta(days=pay_freq_days)
        
        projection_end_date = conceptual_maturity_date if include_principal else today + timedelta(days=NII_HORIZON_DAYS)

        while next_payment_date <= projection_end_date:
            interest_expense = current_balance * effective_rate * (pay_freq_days / 365.0)
            cashflows.append((next_payment_date, -interest_expense)) # Negative for expense
            next_payment_date += timedelta(days=pay_freq_days)
        
        if include_principal:
            cashflows.append((conceptual_maturity_date, -current_balance)) # Principal outflow
        
        return cashflows

    return []


def calculate_derivative_pv(derivative: models.Derivative, yield_curve: Dict[str, float], today: date) -> float:
    """
    Calculates the present value of a derivative (interest rate swap).
    For swaps, PV = (Fixed Rate - Floating Rate) * Notional * Remaining Term
    """
    if derivative.start_date > today or derivative.end_date <= today:
        return 0.0  # Swap hasn't started or has ended
    
    # Calculate remaining term in years
    remaining_days = (derivative.end_date - today).days
    remaining_years = remaining_days / 365.0
    
    if remaining_years <= 0:
        return 0.0
    
    # Get current floating rate (interpolated from yield curve)
    current_floating_rate_for_valuation = interpolate_rate(yield_curve, remaining_days) + (derivative.floating_spread if derivative.floating_spread is not None else 0)
    
    # Calculate net annual payment
    fixed_rate = derivative.fixed_rate if derivative.fixed_rate is not None else 0
    if derivative.subtype == "Payer Swap":
        net_annual_payment = (current_floating_rate_for_valuation - fixed_rate) * derivative.notional
    elif derivative.subtype == "Receiver Swap":
        net_annual_payment = (fixed_rate - current_floating_rate_for_valuation) * derivative.notional
    else:
        return 0.0  # Unknown swap type
    
    # Calculate PV using simple discounting
    discount_rate = interpolate_rate(yield_curve, remaining_days)
    discount_factor = 1 / (1 + discount_rate * remaining_years)
    pv = net_annual_payment * remaining_years * discount_factor
    
    return pv

def generate_derivative_cashflows(derivative: models.Derivative, yield_curve: Dict[str, float], today: date) -> List[Tuple[date, float]]:
    """
    Generates projected cash flows for a derivative (interest rate swap).
    Returns a list of (date, amount) tuples.
    """
    if derivative.start_date > today or derivative.end_date <= today:
        return []  # Swap hasn't started or has ended
    
    cashflows = []
    fixed_rate = derivative.fixed_rate if derivative.fixed_rate is not None else 0
    
    # Generate cash flows for each payment period
    payment_date = derivative.start_date
    while payment_date <= derivative.end_date and payment_date > today:
        # Calculate floating rate for this period
        days_to_payment = (payment_date - today).days
        if days_to_payment <= 0:
            payment_date += timedelta(days=365)  # Move to next year
            continue
            
        floating_rate = interpolate_rate(yield_curve, days_to_payment) + (derivative.floating_spread if derivative.floating_spread is not None else 0)
        
        # Calculate net payment
        if derivative.subtype == "Payer Swap":
            net_payment = (floating_rate - fixed_rate) * derivative.notional
        elif derivative.subtype == "Receiver Swap":
            net_payment = (fixed_rate - floating_rate) * derivative.notional
        else:
            net_payment = 0
        
        cashflows.append((payment_date, net_payment))
        payment_date += timedelta(days=365)  # Annual payments
    
    return cashflows


def get_bucket(item_date: date, today: date, buckets: Dict[str, int]) -> str:
    """Assigns an item to a time bucket based on its date relative to today."""
    if item_date is None:
        return "Non-Sensitive / Undefined"

    days_diff = (item_date - today).days

    sorted_buckets = sorted(buckets.items(), key=lambda item: item[1])

    for bucket_name, days_limit in sorted_buckets:
        if days_limit == -1:
            continue
        elif days_diff <= days_limit:
            return bucket_name
    
    if sorted_buckets and days_diff > sorted_buckets[-1][1]:
        return sorted_buckets[-1][0]
    
    return "Non-Sensitive / Undefined"


def calculate_nii_and_eve_for_curve(db_session: Session, yield_curve: Dict[str, float], 
                                    nmd_effective_maturity_years: int = 5, 
                                    nmd_deposit_beta: float = 0.5,
                                    prepayment_rate: float = 0.0) -> Dict[str, Any]:
    """
    Calculates Net Interest Income (NII) over NII_HORIZON_DAYS and Economic Value of Equity (EVE)
    based on data from the database for a given yield curve and NMD/Prepayment assumptions.
    """
    loans = db_session.query(models.Loan).all()
    deposits = db_session.query(models.Deposit).all()
    derivatives = db_session.query(models.Derivative).all()

    today = date.today()
    nii_horizon_date = today + timedelta(days=NII_HORIZON_DAYS)

    # --- NII Calculation (over NII_HORIZON_DAYS) ---
    total_nii_income = 0.0
    total_nii_expense = 0.0

    for loan in loans:
        if loan.type == "Cash":
            continue
        # HTM Securities are treated as fixed rate, fixed maturity assets (no special handling needed)
        loan_cfs = generate_loan_cashflows(loan, yield_curve, today, include_principal=False, prepayment_rate=prepayment_rate)
        for cf_date, cf_amount in loan_cfs:
            if today < cf_date <= nii_horizon_date:
                total_nii_income += cf_amount

    for deposit in deposits:
        if deposit.type == "Equity":
            continue
        deposit_cfs = generate_deposit_cashflows(deposit, yield_curve, today, include_principal=False,
                                                 nmd_effective_maturity_years=nmd_effective_maturity_years,
                                                 nmd_deposit_beta=nmd_deposit_beta)
        for cf_date, cf_amount in deposit_cfs:
            if today < cf_date <= nii_horizon_date:
                total_nii_expense += abs(cf_amount)

    for derivative in derivatives:
        if derivative.type == "Interest Rate Swap" and derivative.start_date <= today and  derivative.end_date > today:
            fixed_rate = derivative.fixed_rate if derivative.fixed_rate is not None else 0
            floating_rate_for_nii = interpolate_rate(yield_curve, 365) + (derivative.floating_spread if derivative.floating_spread is not None else 0)

            if derivative.subtype == "Payer Swap":
                net_annual_interest = (floating_rate_for_nii - fixed_rate) * derivative.notional
            elif derivative.subtype == "Receiver Swap":
                net_annual_interest = (fixed_rate - floating_rate_for_nii) * derivative.notional
            else:
                net_annual_interest = 0

            if (derivative.end_date - today).days > 0:
                proration_factor = min(1.0, (min(derivative.end_date, nii_horizon_date) - today).days / 365.0)
                total_nii_income += net_annual_interest * proration_factor

    net_interest_income = total_nii_income - total_nii_expense

    # --- EVE Calculation (Present Value of all future cash flows) ---
    total_pv_assets = 0.0
    total_pv_liabilities = 0.0
    total_pv_derivatives = 0.0

    print('--- EVE Asset PVs (Loans) ---')
    for loan in loans:
        if loan.type == "Cash":
            continue
        loan_cfs = generate_loan_cashflows(loan, yield_curve, today, include_principal=True, prepayment_rate=prepayment_rate)
        pv = calculate_pv_of_cashflows(loan_cfs, yield_curve, today)
        print(f"Loan {loan.instrument_id} ({loan.type}): PV = {pv:,.2f}")
        total_pv_assets += pv

    print('--- EVE Liability PVs (Deposits) ---')
    for deposit in deposits:
        if deposit.type == "Equity":
            continue
        deposit_cfs = generate_deposit_cashflows(deposit, yield_curve, today, include_principal=True,
                                                 nmd_effective_maturity_years=nmd_effective_maturity_years,
                                                 nmd_deposit_beta=nmd_deposit_beta)
        pv = calculate_pv_of_cashflows(deposit_cfs, yield_curve, today)
        print(f"Deposit {deposit.instrument_id} ({deposit.type}): PV = {pv:,.2f}")
        total_pv_liabilities += pv

    print('--- EVE Derivative PVs (Receiver Swaps) ---')
    for derivative in derivatives:
        if getattr(derivative, 'subtype', None) != "Receiver Swap":
            continue
        pv = calculate_derivative_pv(derivative, yield_curve, today)
        print(f"Derivative {derivative.instrument_id} (Receiver Swap): PV = {pv:,.2f}")
        total_pv_derivatives += pv

    eve_value = total_pv_assets - abs(total_pv_liabilities) + total_pv_derivatives

    return {
        "net_interest_income": net_interest_income,
        "economic_value_of_equity": eve_value,
        "total_assets_value": total_pv_assets,
        "total_liabilities_value": abs(total_pv_liabilities),
        "total_derivatives_value": total_pv_derivatives
    }


def calculate_gap_analysis(db: Session) -> Dict[str, List[schemas.GapBucket]]:
    """
    Calculates NII Repricing Gap and EVE Maturity Gap.
    This still uses the repricing/maturity dates from the instruments directly,
    as gap analysis is typically based on contractual or first repricing dates.
    """
    loans = db.query(models.Loan).all()
    deposits = db.query(models.Deposit).all()
    derivatives = db.query(models.Derivative).all()

    today = date.today()

    # --- NII Repricing Gap Buckets (in days from today) ---
    nii_buckets_def = {
        "0-3 Months": 90,
        "3-6 Months": 180,
        "6-12 Months": 365,
        "1-5 Years": 365 * 5,
        ">5 Years": 365 * 100,
        "Fixed Rate / Non-Sensitive": -1
    }
    nii_bucket_order = ["0-3 Months", "3-6 Months", "6-12 Months", "1-5 Years", ">5 Years", "Fixed Rate / Non-Sensitive"]

    nii_gap_data: Dict[str, Dict[str, float]] = {
        bucket: {"assets": 0.0, "liabilities": 0.0, "gap": 0.0}
        for bucket in nii_bucket_order
    }

    for loan in loans:
        if loan.type == "Cash":
            continue
        if loan.type == "HTM Securities":
            bucket_name = "Fixed Rate / Non-Sensitive"
        elif not loan.next_repricing_date:
            bucket_name = "Fixed Rate / Non-Sensitive"
        else:
            bucket_name = get_bucket(loan.next_repricing_date, today, nii_buckets_def)
        nii_gap_data[bucket_name]["assets"] += loan.notional

    for deposit in deposits:
        if deposit.type == "Equity":
            continue
        if deposit.type in ["CD", "Wholesale Funding"]:
            if deposit.maturity_date:
                bucket_name = get_bucket(deposit.maturity_date, today, nii_buckets_def)
            else:
                bucket_name = "Fixed Rate / Non-Sensitive"
        elif deposit.type in ["Checking", "Savings"]:
            if deposit.next_repricing_date:
                bucket_name = get_bucket(deposit.next_repricing_date, today, nii_buckets_def)
            else:
                bucket_name = "Fixed Rate / Non-Sensitive"
        else:
            bucket_name = "Fixed Rate / Non-Sensitive"
        nii_gap_data[bucket_name]["liabilities"] += deposit.balance

    for derivative in derivatives:
        if derivative.type == "Interest Rate Swap":
            repricing_freq_days = 0
            if derivative.floating_payment_frequency == "Monthly": repricing_freq_days = 30
            elif derivative.floating_payment_frequency == "Quarterly": repricing_freq_days = 90
            elif derivative.floating_payment_frequency == "Semi-Annually": repricing_freq_days = 182
            elif derivative.floating_payment_frequency == "Annually": repricing_freq_days = 365

            if repricing_freq_days > 0:
                next_repricing_date = today + timedelta(days=repricing_freq_days)
                bucket_name = get_bucket(next_repricing_date, today, nii_buckets_def)
                
                if derivative.subtype == "Payer Swap":
                    nii_gap_data[bucket_name]["assets"] += derivative.notional
                elif derivative.subtype == "Receiver Swap":
                    nii_gap_data[bucket_name]["liabilities"] += derivative.notional
            else:
                nii_gap_data["Fixed Rate / Non-Sensitive"]["assets"] += derivative.notional

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
        ">10 Years": 365 * 100,
        "Non-Maturity": -1
    }
    eve_bucket_order = ["0-1 Year", "1-3 Years", "3-5 Years", "5-10 Years", ">10 Years", "Non-Maturity"]

    eve_gap_data: Dict[str, Dict[str, float]] = {
        bucket: {"assets": 0.0, "liabilities": 0.0, "gap": 0.0}
        for bucket in eve_bucket_order
    }

    for loan in loans:
        if loan.type == "Cash":
            continue
        bucket_name = get_bucket(loan.maturity_date, today, eve_buckets_def)
        eve_gap_data[bucket_name]["assets"] += loan.notional

    for deposit in deposits:
        if deposit.type == "Equity":
            continue
        if deposit.type in ["CD", "Wholesale Funding"] and deposit.maturity_date:
            bucket_name = get_bucket(deposit.maturity_date, today, eve_buckets_def)
        else:
            bucket_name = "Non-Maturity"
        eve_gap_data[bucket_name]["liabilities"] += deposit.balance

    for derivative in derivatives:
        bucket_name = get_bucket(derivative.end_date, today, eve_buckets_def)
        if derivative.subtype == "Payer Swap":
            eve_gap_data[bucket_name]["assets"] += derivative.notional
        elif derivative.subtype == "Receiver Swap":
            eve_gap_data[bucket_name]["liabilities"] += derivative.notional

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


def generate_dashboard_data_from_db(db: Session, assumptions: schemas.CalculationAssumptions) -> schemas.DashboardData:
    """
    Generates dashboard data by fetching from DB and performing calculations,
    including scenario-based EVE/NII and portfolio composition.
    Accepts NMD behavioral and prepayment assumptions.
    """
    global _scenario_history

    today = date.today()
    loans = db.query(models.Loan).all()
    deposits = db.query(models.Deposit).all()
    derivatives = db.query(models.Derivative).all()

    # --- Calculate EVE and NII for all Scenarios ---
    eve_scenario_results: List[schemas.EVEScenarioResult] = []
    nii_scenario_results: List[schemas.NIIScenarioResult] = []
    
    base_case_eve = 0.0
    base_case_nii = 0.0
    total_assets_value_base = 0.0
    total_liabilities_value_base = 0.0
    portfolio_value_base = 0.0

    # --- Save Yield Curves to Database ---
    yield_curve_records = []
    now = datetime.now()

    for scenario_name, shock_bps in INTEREST_RATE_SCENARIOS.items():
        if scenario_name == "Base Case":
            curve = BASE_YIELD_CURVE
        else:
            curve = shock_yield_curve(BASE_YIELD_CURVE, shock_bps)
        
        # Save yield curve data for this scenario
        for tenor, rate in curve.items():
            yield_curve_records.append(schemas_dashboard.YieldCurveCreate(
                scenario=scenario_name,
                tenor=tenor,
                rate=rate,
                timestamp=now
            ))
        
        # Pass all assumptions to the calculation function
        metrics_for_curve = calculate_nii_and_eve_for_curve(
            db, curve,
            nmd_effective_maturity_years=assumptions.nmd_effective_maturity_years,
            nmd_deposit_beta=assumptions.nmd_deposit_beta,
            prepayment_rate=assumptions.prepayment_rate
        )
        
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
            portfolio_value_base = total_assets_value_base + total_liabilities_value_base + metrics_for_curve["total_derivatives_value"]

    # Delete and save yield curves
    from crud_dashboard import delete_yield_curves, save_yield_curves
    delete_yield_curves(db)
    save_yield_curves(db, yield_curve_records)

    # --- Calculate Sensitivities ---
    eve_sensitivity = 0.0
    nii_sensitivity = 0.0

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
    yield_curve_data_for_display = [schemas.YieldCurvePoint(name=tenor, rate=rate*100) for tenor, rate in BASE_YIELD_CURVE.items()]

    # --- Historical Scenario Data (for the EVE chart over time) ---
    new_scenario_point = {
        "time": now.strftime("%H:%M:%S"),
        "data":{
        "Base Case": base_case_eve,
            "+200bps": next((res.eve_value for res in eve_scenario_results if res.scenario_name == "Parallel Up +200bps"), base_case_eve),
           "-200bps": next((res.eve_value for res in eve_scenario_results if res.scenario_name == "Parallel Down -200bps"), base_case_eve),
        }
    }

    _scenario_history.append(new_scenario_point)
    _scenario_history = _scenario_history[-MAX_SCENARIO_HISTORY:]
    
    today = date.today()
    save_dashboard_metric(db, DashboardMetricCreate(
        timestamp=today,
        scenario="Base Case",
        eve_value=base_case_eve,
        nii_value=base_case_nii,
        eve_sensitivity=eve_sensitivity,
        nii_sensitivity=nii_sensitivity,
        total_assets_value=total_assets_value_base,
        total_liabilities_value=total_liabilities_value_base,
        portfolio_value=portfolio_value_base
    ))
    
    # Save EVE drivers for all scenarios
    eve_driver_records = []
    for scenario_name, shock_bps in INTEREST_RATE_SCENARIOS.items():
        if scenario_name == "Base Case":
            curve = BASE_YIELD_CURVE
        else:
            curve = shock_yield_curve(BASE_YIELD_CURVE, shock_bps)
        for loan in loans:
            if loan.type == "Cash":
                continue
            # HTM Securities are treated as fixed rate, fixed maturity assets (no special handling needed)
            loan_cfs = generate_loan_cashflows(loan, curve, today, include_principal=True, prepayment_rate=assumptions.prepayment_rate)
            base_pv = calculate_pv_of_cashflows(loan_cfs, curve, today)
            duration = calculate_modified_duration(loan_cfs, curve, today)
            eve_driver_records.append(EveDriverCreate(
                scenario=scenario_name,
                instrument_id=str(loan.id),
                instrument_type=loan.type,
                base_pv=base_pv,
                shocked_pv=None,
                duration=duration
            ))
        for deposit in deposits:
            if deposit.type == "Equity":
                continue
            deposit_cfs = generate_deposit_cashflows(deposit, curve, today, include_principal=True,
                                                 nmd_effective_maturity_years=assumptions.nmd_effective_maturity_years,
                                                 nmd_deposit_beta=assumptions.nmd_deposit_beta)
            base_pv = calculate_pv_of_cashflows(deposit_cfs, curve, today)
            duration = calculate_modified_duration(deposit_cfs, curve, today)
            eve_driver_records.append(EveDriverCreate(
                scenario=scenario_name,
                instrument_id=str(deposit.id),
                instrument_type=deposit.type,
                base_pv=base_pv,
                shocked_pv=None,
                duration=duration
            ))
        for derivative in derivatives:
            if getattr(derivative, 'subtype', None) != "Receiver Swap":
                continue
            # Use the same PV calculation method as EVE scenario table
            base_pv = calculate_derivative_pv(derivative, curve, today)
            # Generate cash flows for duration calculation only
            derivative_cfs = generate_derivative_cashflows(derivative, curve, today)
            duration = calculate_modified_duration(derivative_cfs, curve, today)
            eve_driver_records.append(EveDriverCreate(
                scenario=scenario_name,
                instrument_id=str(derivative.id),
                instrument_type=derivative.type,
                base_pv=base_pv,
                shocked_pv=None,
                duration=duration
            ))
    # Delete and save EVE drivers for all scenarios
    db.query(EveDriver).delete(synchronize_session=False)
    save_eve_drivers(db, eve_driver_records)

    # Save NII drivers for all scenarios
    nii_driver_records = []
    for scenario_name, shock_bps in INTEREST_RATE_SCENARIOS.items():
        if scenario_name == "Base Case":
            curve = BASE_YIELD_CURVE
        else:
            curve = shock_yield_curve(BASE_YIELD_CURVE, shock_bps)
        
        # Loans
        for loan in loans:
            if loan.type == "Cash":
                continue
            loan_cfs = generate_loan_cashflows(loan, curve, today, include_principal=False, prepayment_rate=assumptions.prepayment_rate)
            nii_contribution = sum(cf_amount for cf_date, cf_amount in loan_cfs if today < cf_date <= today + timedelta(days=NII_HORIZON_DAYS))
            bucket = get_bucket(loan.next_repricing_date if loan.next_repricing_date else loan.maturity_date, today, {
                "0-3 Months": 90,
                "3-6 Months": 180,
                "6-12 Months": 365,
                "1-5 Years": 365 * 5,
                ">5 Years": 365 * 100,
                "Fixed Rate / Non-Sensitive": -1
            })
            nii_driver_records.append(NiiDriverCreate(
                scenario=scenario_name,
                instrument_id=str(loan.id),
                instrument_type=loan.type,
                nii_contribution=nii_contribution,
                breakdown_type=loan.type,
                breakdown_value=bucket
            ))
        
        # Deposits
        for deposit in deposits:
            if deposit.type == "Equity":
                continue
            deposit_cfs = generate_deposit_cashflows(deposit, curve, today, include_principal=False,
                                                 nmd_effective_maturity_years=assumptions.nmd_effective_maturity_years,
                                                 nmd_deposit_beta=assumptions.nmd_deposit_beta)
            nii_contribution = -sum(abs(cf_amount) for cf_date, cf_amount in deposit_cfs if today < cf_date <= today + timedelta(days=NII_HORIZON_DAYS))
            bucket = get_bucket(deposit.next_repricing_date if deposit.next_repricing_date else deposit.maturity_date, today, {
                "0-3 Months": 90,
                "3-6 Months": 180,
                "6-12 Months": 365,
                "1-5 Years": 365 * 5,
                ">5 Years": 365 * 100,
                "Fixed Rate / Non-Sensitive": -1
            })
            nii_driver_records.append(NiiDriverCreate(
                scenario=scenario_name,
                instrument_id=str(deposit.id),
                instrument_type=deposit.type,
                nii_contribution=nii_contribution,
                breakdown_type=deposit.type,
                breakdown_value=bucket
            ))
        
        # Derivatives
        for derivative in derivatives:
            if getattr(derivative, 'subtype', None) != "Receiver Swap":
                continue
            if derivative.start_date <= today and derivative.end_date > today:
                fixed_rate = derivative.fixed_rate if derivative.fixed_rate is not None else 0
                floating_rate_for_nii = interpolate_rate(curve, 365) + (derivative.floating_spread if derivative.floating_spread is not None else 0)

                if derivative.subtype == "Payer Swap":
                    net_annual_interest = (floating_rate_for_nii - fixed_rate) * derivative.notional
                elif derivative.subtype == "Receiver Swap":
                    net_annual_interest = (fixed_rate - floating_rate_for_nii) * derivative.notional
                else:
                    net_annual_interest = 0

                if (derivative.end_date - today).days > 0:
                    proration_factor = min(1.0, (min(derivative.end_date, today + timedelta(days=NII_HORIZON_DAYS)) - today).days / 365.0)
                    nii_contribution = net_annual_interest * proration_factor
                else:
                    nii_contribution = 0
            else:
                nii_contribution = 0

            bucket = get_bucket(derivative.end_date, today, {
                "0-3 Months": 90,
                "3-6 Months": 180,
                "6-12 Months": 365,
                "1-5 Years": 365 * 5,
                ">5 Years": 365 * 100,
                "Fixed Rate / Non-Sensitive": -1
            })
            nii_driver_records.append(NiiDriverCreate(
                scenario=scenario_name,
                instrument_id=str(derivative.id),
                instrument_type=derivative.type,
                nii_contribution=nii_contribution,
                breakdown_type=derivative.type,
                breakdown_value=bucket
            ))
    
    # Delete and save NII drivers for all scenarios
    db.query(NiiDriver).delete(synchronize_session=False)
    save_nii_drivers(db, nii_driver_records)

    # Populate repricing_buckets for all instruments
    repricing_buckets = []
    nii_buckets_def = {
        "0-3 Months": 90,
        "3-6 Months": 180,
        "6-12 Months": 365,
        "1-5 Years": 365 * 5,
        ">5 Years": 365 * 100,
        "Fixed Rate / Non-Sensitive": -1
    }
    # Loans (assets)
    for loan in loans:
        if loan.type == "Cash":
            continue
        if loan.type == "HTM Securities":
            bucket_name = "Fixed Rate / Non-Sensitive"
        else:
            bucket_name = get_bucket(loan.next_repricing_date, today, nii_buckets_def)
        repricing_buckets.append(RepricingBucketCreate(
            scenario="Base Case",
            bucket=bucket_name,
            instrument_type="Loan",
            instrument_id=str(loan.id),
            notional=loan.notional,
            position="asset"
        ))
    # Deposits (liabilities)
    for deposit in deposits:
        if deposit.type == "Equity":
            continue
        if deposit.type == "Wholesale Funding":
            bucket_name = get_bucket(deposit.maturity_date, today, nii_buckets_def)
        else:
            bucket_name = get_bucket(deposit.next_repricing_date, today, nii_buckets_def)
        repricing_buckets.append(RepricingBucketCreate(
            scenario="Base Case",
            bucket=bucket_name,
            instrument_type="Deposit",
            instrument_id=str(deposit.id),
            notional=deposit.balance,
            position="liability"
        ))
    # Derivatives
    for derivative in derivatives:
        if getattr(derivative, 'subtype', None) != "Receiver Swap":
            continue
        if derivative.end_date and derivative.end_date > today:
            next_repricing_date = derivative.end_date
        else:
            next_repricing_date = derivative.start_date + timedelta(days=365)  # Default to 1 year
        bucket_name = get_bucket(next_repricing_date, today, nii_buckets_def)
        if derivative.subtype == "Payer Swap":
            repricing_buckets.append(RepricingBucketCreate(
                scenario="Base Case",
                bucket=bucket_name,
                instrument_type="Derivative",
                instrument_id=str(derivative.id),
                notional=derivative.notional,
                position="asset"
            ))
        elif derivative.subtype == "Receiver Swap":
            repricing_buckets.append(RepricingBucketCreate(
                scenario="Base Case",
                bucket=bucket_name,
                instrument_type="Derivative",
                instrument_id=str(derivative.id),
                notional=derivative.notional,
                position="liability"
            ))
        else:
            repricing_buckets.append(RepricingBucketCreate(
                scenario="Base Case",
                bucket="Fixed Rate / Non-Sensitive",
                instrument_type="Derivative",
                instrument_id=str(derivative.id),
                notional=derivative.notional,
                position="asset"
            ))
    # Delete and save repricing buckets
    db.query(RepricingBucket).filter(RepricingBucket.scenario == "Base Case").delete(synchronize_session=False)
    save_repricing_buckets(db, repricing_buckets)

    # Populate repricing_net_positions
    net_positions = []
    for bucket in gap_analysis_metrics["nii_repricing_gap"]:
        net_positions.append(RepricingNetPositionCreate(
            scenario="Base Case",
            bucket=bucket.bucket,
            total_assets=bucket.assets,
            total_liabilities=bucket.liabilities,
            net_position=bucket.gap,
            nii_base=base_case_nii,
            nii_shocked=nii_up_200bps if nii_up_200bps else base_case_nii
        ))
    # Delete and save net positions
    db.query(RepricingNetPosition).filter(RepricingNetPosition.scenario == "Base Case").delete(synchronize_session=False)
    save_repricing_net_positions(db, net_positions)

    # --- Save Portfolio Composition ---
    portfolio_records = []
    # Loans
    for loan in loans:
        if loan.type == "Cash":
            continue
        portfolio_records.append(PortfolioCompositionCreate(
            timestamp=today,
            instrument_type="Loan",
            category=loan.type,
            subcategory=getattr(loan, 'subtype', None),
            volume_count=1,
            total_amount=loan.notional,
            average_interest_rate=loan.interest_rate
        ))
    # Deposits
    for deposit in deposits:
        if deposit.type == "Equity":
            continue
        portfolio_records.append(PortfolioCompositionCreate(
            timestamp=today,
            instrument_type="Deposit",
            category=deposit.type,
            subcategory=None,
            volume_count=1,
            total_amount=deposit.balance,
            average_interest_rate=deposit.interest_rate
        ))
    # Derivatives
    for derivative in derivatives:
        if getattr(derivative, 'subtype', None) != "Receiver Swap":
            continue
        portfolio_records.append(PortfolioCompositionCreate(
            timestamp=today,
            instrument_type="Derivative",
            category=derivative.type,
            subcategory="Receiver Swap",
            volume_count=1,
            total_amount=derivative.notional,
            average_interest_rate=derivative.fixed_rate
        ))
    db.query(PortfolioComposition).filter(PortfolioComposition.instrument_type.in_(["Loan", "Deposit", "Derivative"])).delete(synchronize_session=False)
    save_portfolio_composition(db, portfolio_records)
    
    # --- Save Cashflow Ladder ---
    delete_all_cashflow_ladder(db)
    cashflow_ladder_records = []
    for scenario_name, shock_bps in INTEREST_RATE_SCENARIOS.items():
        curve = BASE_YIELD_CURVE if scenario_name == "Base Case" else shock_yield_curve(BASE_YIELD_CURVE, shock_bps)
        for loan in loans:
            cfs = generate_loan_cashflows(loan, curve, today, include_principal=True, prepayment_rate=assumptions.prepayment_rate)
            for cf_date, cf_amount in cfs:
                months = (cf_date.year - today.year) * 12 + (cf_date.month - today.month)
                # For fixed loans, all interest is fixed; for floating, split (simplified logic)
                if loan.type in ["Fixed Rate Loan", "HTM Securities"]:
                    fixed_component = cf_amount
                    floating_component = 0.0
                else:
                    fixed_component = 0.0
                    floating_component = cf_amount
                total_cashflow = fixed_component + floating_component
                days_to_maturity = (cf_date - today).days
                discount_rate = interpolate_rate(curve, days_to_maturity)
                discount_factor = 1 / (1 + discount_rate * (days_to_maturity / 365))
                pv = total_cashflow * discount_factor
                cashflow_ladder_records.append(CashflowLadderCreate(
                    scenario=scenario_name,
                    instrument_id=str(loan.id),
                    instrument_type=loan.type,
                    asset_liability="A",
                    cashflow_date=cf_date,
                    time_months=months,
                    fixed_component=fixed_component,
                    floating_component=floating_component,
                    total_cashflow=total_cashflow,
                    discount_factor=discount_factor,
                    pv=pv
                ))
        for deposit in deposits:
            cfs = generate_deposit_cashflows(deposit, curve, today, include_principal=True,
                                             nmd_effective_maturity_years=assumptions.nmd_effective_maturity_years,
                                             nmd_deposit_beta=assumptions.nmd_deposit_beta)
            for cf_date, cf_amount in cfs:
                months = (cf_date.year - today.year) * 12 + (cf_date.month - today.month)
                if deposit.type in ["CD", "Wholesale Funding"]:
                    fixed_component = cf_amount
                    floating_component = 0.0
                else:
                    fixed_component = 0.0
                    floating_component = cf_amount
                total_cashflow = fixed_component + floating_component
                days_to_maturity = (cf_date - today).days
                discount_rate = interpolate_rate(curve, days_to_maturity)
                discount_factor = 1 / (1 + discount_rate * (days_to_maturity / 365))
                pv = total_cashflow * discount_factor
                cashflow_ladder_records.append(CashflowLadderCreate(
                    scenario=scenario_name,
                    instrument_id=str(deposit.id),
                    instrument_type=deposit.type,
                    asset_liability="L",
                    cashflow_date=cf_date,
                    time_months=months,
                    fixed_component=fixed_component,
                    floating_component=floating_component,
                    total_cashflow=total_cashflow,
                    discount_factor=discount_factor,
                    pv=pv
                ))
        for derivative in derivatives:
            if getattr(derivative, 'subtype', None) != "Receiver Swap":
                continue
            cfs = generate_derivative_cashflows(derivative, curve, today)
            for cf_date, cf_amount in cfs:
                months = (cf_date.year - today.year) * 12 + (cf_date.month - today.month)
                # For swaps, split into fixed/floating if possible (simplified: all to floating)
                fixed_component = 0.0
                floating_component = cf_amount
                total_cashflow = fixed_component + floating_component
                days_to_maturity = (cf_date - today).days
                discount_rate = interpolate_rate(curve, days_to_maturity)
                discount_factor = 1 / (1 + discount_rate * (days_to_maturity / 365))
                pv = total_cashflow * discount_factor
                cashflow_ladder_records.append(CashflowLadderCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id),
                    instrument_type=derivative.type,
                    asset_liability="L",
                    cashflow_date=cf_date,
                    time_months=months,
                    fixed_component=fixed_component,
                    floating_component=floating_component,
                    total_cashflow=total_cashflow,
                    discount_factor=discount_factor,
                    pv=pv
                ))
    save_cashflow_ladder(db, cashflow_ladder_records)

    return schemas.DashboardData(
        eve_sensitivity=eve_sensitivity,
        nii_sensitivity=nii_sensitivity,
        portfolio_value=portfolio_value_base,
        yield_curve_data=yield_curve_data_for_display,
        scenario_data=_scenario_history,
        total_assets_value=total_assets_value_base,
        total_liabilities_value=total_liabilities_value_base,
        net_interest_income=base_case_nii,
        economic_value_of_equity=base_case_eve,
        nii_repricing_gap=gap_analysis_metrics["nii_repricing_gap"],
        eve_maturity_gap=gap_analysis_metrics["eve_maturity_gap"],
        eve_scenarios=eve_scenario_results,
        nii_scenarios=nii_scenario_results,
        current_assumptions=assumptions # Pass assumptions back to frontend
    )


def calculate_modified_duration(cashflows: List[Tuple[date, float]], yield_curve: Dict[str, float], today: date) -> Optional[float]:
    """
    Calculates the modified duration of a series of cash flows using the yield curve.
    Returns None if PV is zero or cashflows are empty.
    """
    if not cashflows:
        return None
    pv = 0.0
    weighted_sum = 0.0
    for cf_date, cf_amount in cashflows:
        if cf_date <= today:
            continue
        days_to_payment = (cf_date - today).days
        t = days_to_payment / 365.0  # time in years
        discount_rate = interpolate_rate(yield_curve, days_to_payment)
        discount_factor = 1 / (1 + discount_rate * t)
        pv_cf = cf_amount * discount_factor
        pv += pv_cf
        weighted_sum += t * pv_cf
    if pv == 0.0:
        return None
    macaulay_duration = weighted_sum / pv
    # Use average yield for all cash flows for modified duration denominator
    avg_yield = sum(interpolate_rate(yield_curve, (cf_date - today).days) for cf_date, _ in cashflows if cf_date > today) / max(1, len([1 for cf_date, _ in cashflows if cf_date > today]))
    modified_duration = macaulay_duration / (1 + avg_yield)
    return modified_duration