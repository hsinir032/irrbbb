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
from models_dashboard import NiiDriver, EveDriver, RepricingBucket, PortfolioComposition, YieldCurve, CashflowLadder
from schemas_dashboard import CashflowLadderCreate, RepricingBucketCreate

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

    # Calculate derivative NII using separate leg cashflows
    for derivative in derivatives:
        if derivative.start_date <= today and derivative.end_date > today:
            # Generate separate cashflows for NII calculation
            fixed_cfs = generate_fixed_leg_cashflows(derivative, yield_curve, today)
            floating_cfs = generate_floating_leg_cashflows(derivative, yield_curve, today)
            
            # Calculate NII from cashflows within the horizon
            fixed_nii = sum(cf_amount for cf_date, cf_amount in fixed_cfs 
                           if today < cf_date <= nii_horizon_date)
            floating_nii = sum(cf_amount for cf_date, cf_amount in floating_cfs 
                              if today < cf_date <= nii_horizon_date)
            
            # Add to income/expense based on swap type
            if derivative.subtype == "Payer Swap":
                # Payer Swap: Pay fixed (expense), receive floating (income)
                total_nii_expense += abs(fixed_nii)
                total_nii_income += abs(floating_nii)
            elif derivative.subtype == "Receiver Swap":
                # Receiver Swap: Receive fixed (income), pay floating (expense)
                total_nii_income += abs(fixed_nii)
                total_nii_expense += abs(floating_nii)
            else:
                # For other derivative types, treat as separate legs
                total_nii_income += abs(fixed_nii)
                total_nii_expense += abs(floating_nii)

    # Calculate NII using separate leg cashflows (consistent with NII drivers)
    nii_from_separate_legs = total_nii_income - total_nii_expense
    
    # Also calculate using the old method for comparison (can be removed later)
    net_interest_income = nii_from_separate_legs

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

    print('--- EVE Derivative PVs (All Derivatives) ---')
    for derivative in derivatives:
        fixed_pv = calculate_fixed_leg_pv(derivative, yield_curve, today)
        floating_pv = calculate_floating_leg_pv(derivative, yield_curve, today)
        print(f"Derivative {derivative.instrument_id} ({derivative.subtype}): Fixed PV = {fixed_pv:,.2f}, Floating PV = {floating_pv:,.2f}")
        # For EVE, we add the PV to the appropriate side based on swap type
        if derivative.subtype == "Receiver Swap":
            # Receiver Swap: We receive fixed (asset), pay floating (liability)
            total_pv_assets += fixed_pv
            total_pv_liabilities += floating_pv
        elif derivative.subtype == "Payer Swap":
            # Payer Swap: We pay fixed (liability), receive floating (asset)
            total_pv_assets += floating_pv
            total_pv_liabilities += fixed_pv
        else:
            # For other swap types, treat as separate legs
            total_pv_assets += fixed_pv
            total_pv_liabilities += floating_pv

    eve_value = total_pv_assets - abs(total_pv_liabilities) + total_pv_derivatives

    return {
        "net_interest_income": net_interest_income,
        "net_interest_income_from_separate_legs": net_interest_income,  # This is now calculated from separate legs
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
            nii_gap_data[bucket_name]["assets"] += loan.notional
        elif not loan.next_repricing_date:
            bucket_name = "Fixed Rate / Non-Sensitive"
            nii_gap_data[bucket_name]["assets"] += loan.notional
        else:
            # For floating rate loans, include all repricing points over the life
            current_date = loan.next_repricing_date
            while current_date and current_date <= loan.maturity_date:
                bucket_name = get_bucket(current_date, today, nii_buckets_def)
                nii_gap_data[bucket_name]["assets"] += loan.notional
                # Move to next repricing date
                if loan.repricing_frequency == "Monthly":
                    current_date = current_date + timedelta(days=30)
                elif loan.repricing_frequency == "Quarterly":
                    current_date = current_date + timedelta(days=90)
                elif loan.repricing_frequency == "Semi-Annually":
                    current_date = current_date + timedelta(days=182)
                elif loan.repricing_frequency == "Annually":
                    current_date = current_date + timedelta(days=365)
                else:
                    break  # Unknown frequency, stop

    for deposit in deposits:
        if deposit.type == "Equity":
            continue
        if deposit.type in ["CD", "Wholesale Funding"]:
            if deposit.maturity_date:
                bucket_name = get_bucket(deposit.maturity_date, today, nii_buckets_def)
                nii_gap_data[bucket_name]["liabilities"] += deposit.balance
            else:
                bucket_name = "Fixed Rate / Non-Sensitive"
                nii_gap_data[bucket_name]["liabilities"] += deposit.balance
        elif deposit.type in ["Checking", "Savings"]:
            if deposit.next_repricing_date:
                # For floating deposits, include all repricing points over the life
                current_date = deposit.next_repricing_date
                # Assume NMDs have effective maturity of 5 years for gap analysis
                effective_maturity = today + timedelta(days=365*5)
                while current_date and current_date <= effective_maturity:
                    bucket_name = get_bucket(current_date, today, nii_buckets_def)
                    nii_gap_data[bucket_name]["liabilities"] += deposit.balance
                    # Move to next repricing date
                    if deposit.repricing_frequency == "Monthly":
                        current_date = current_date + timedelta(days=30)
                    elif deposit.repricing_frequency == "Quarterly":
                        current_date = current_date + timedelta(days=90)
                    elif deposit.repricing_frequency == "Semi-Annually":
                        current_date = current_date + timedelta(days=182)
                    elif deposit.repricing_frequency == "Annually":
                        current_date = current_date + timedelta(days=365)
                    else:
                        break  # Unknown frequency, stop
            else:
                bucket_name = "Fixed Rate / Non-Sensitive"
                nii_gap_data[bucket_name]["liabilities"] += deposit.balance
        else:
            bucket_name = "Fixed Rate / Non-Sensitive"
            nii_gap_data[bucket_name]["liabilities"] += deposit.balance

    for derivative in derivatives:
        if derivative.type == "Interest Rate Swap":
            # Split derivative into fixed and floating legs
            notional = derivative.notional
            
            # Fixed leg always goes in "Fixed Rate / Non-Sensitive"
            if derivative.subtype == "Payer Swap":
                # For Payer Swap: Fixed leg is liability
                nii_gap_data["Fixed Rate / Non-Sensitive"]["liabilities"] += notional
            elif derivative.subtype == "Receiver Swap":
                # For Receiver Swap: Fixed leg is asset
                nii_gap_data["Fixed Rate / Non-Sensitive"]["assets"] += notional
            else:
                # For other swap types, default to asset
                nii_gap_data["Fixed Rate / Non-Sensitive"]["assets"] += notional
            
            # Floating leg goes in appropriate time bucket based on repricing frequency
            # Include all repricing points over the life of the derivative
            if derivative.floating_payment_frequency:
                current_date = today
                # Start from next repricing date
                if derivative.floating_payment_frequency == "Monthly":
                    current_date = today + timedelta(days=30)
                elif derivative.floating_payment_frequency == "Quarterly":
                    current_date = today + timedelta(days=90)
                elif derivative.floating_payment_frequency == "Semi-Annually":
                    current_date = today + timedelta(days=182)
                elif derivative.floating_payment_frequency == "Annually":
                    current_date = today + timedelta(days=365)
                
                # Include all repricing points until maturity
                while current_date and current_date <= derivative.end_date:
                    bucket_name = get_bucket(current_date, today, nii_buckets_def)
                    
                    if derivative.subtype == "Payer Swap":
                        # For Payer Swap: Floating leg is asset
                        nii_gap_data[bucket_name]["assets"] += notional
                    elif derivative.subtype == "Receiver Swap":
                        # For Receiver Swap: Floating leg is liability
                        nii_gap_data[bucket_name]["liabilities"] += notional
                    else:
                        # For other swap types, default to asset
                        nii_gap_data[bucket_name]["assets"] += notional
                    
                    # Move to next repricing date
                    if derivative.floating_payment_frequency == "Monthly":
                        current_date = current_date + timedelta(days=30)
                    elif derivative.floating_payment_frequency == "Quarterly":
                        current_date = current_date + timedelta(days=90)
                    elif derivative.floating_payment_frequency == "Semi-Annually":
                        current_date = current_date + timedelta(days=182)
                    elif derivative.floating_payment_frequency == "Annually":
                        current_date = current_date + timedelta(days=365)
                    else:
                        break  # Unknown frequency, stop
            else:
                # If no repricing frequency, put floating leg in "Fixed Rate / Non-Sensitive"
                if derivative.subtype == "Payer Swap":
                    nii_gap_data["Fixed Rate / Non-Sensitive"]["assets"] += notional
                elif derivative.subtype == "Receiver Swap":
                    nii_gap_data["Fixed Rate / Non-Sensitive"]["liabilities"] += notional
                else:
                    nii_gap_data["Fixed Rate / Non-Sensitive"]["assets"] += notional

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
        # Split derivatives into fixed and floating legs
        if derivative.subtype == "Payer Swap":
            # Payer Swap: Fixed leg is liability, floating leg is asset
            bucket_name = get_bucket(derivative.end_date, today, eve_buckets_def)
            eve_gap_data[bucket_name]["liabilities"] += derivative.notional  # Fixed leg
            eve_gap_data[bucket_name]["assets"] += derivative.notional       # Floating leg
        elif derivative.subtype == "Receiver Swap":
            # Receiver Swap: Fixed leg is asset, floating leg is liability
            bucket_name = get_bucket(derivative.end_date, today, eve_buckets_def)
            eve_gap_data[bucket_name]["assets"] += derivative.notional       # Fixed leg
            eve_gap_data[bucket_name]["liabilities"] += derivative.notional  # Floating leg

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
        
        # Calculate NII from separate leg contributions (consistent with NII drivers)
        nii_from_separate_legs = metrics_for_curve["net_interest_income_from_separate_legs"]
        nii_scenario_results.append(schemas.NIIScenarioResult(
            scenario_name=scenario_name,
            nii_value=nii_from_separate_legs
        ))

        if scenario_name == "Base Case":
            base_case_eve = metrics_for_curve["economic_value_of_equity"]
            base_case_nii = nii_from_separate_legs
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
            # Skip inactive derivatives
            if derivative.start_date > today or derivative.end_date < today:
                continue
            
            print(f"Processing derivative {derivative.instrument_id} for EVE drivers")
            
            # Generate separate cashflows for fixed and floating legs for duration calculation
            fixed_cfs = generate_fixed_leg_cashflows(derivative, curve, today)
            floating_cfs = generate_floating_leg_cashflows(derivative, curve, today)
            
            print(f"  Fixed leg cashflows: {len(fixed_cfs)}")
            print(f"  Floating leg cashflows: {len(floating_cfs)}")
            
            # Calculate PV of each leg using separate functions
            fixed_pv = calculate_fixed_leg_pv(derivative, curve, today)
            floating_pv = calculate_floating_leg_pv(derivative, curve, today)
            
            print(f"  Fixed PV: {fixed_pv:,.2f}")
            print(f"  Floating PV: {floating_pv:,.2f}")
            
            # Calculate duration for each leg using separate cashflows
            fixed_duration = calculate_modified_duration(fixed_cfs, curve, today) if fixed_cfs else None
            floating_duration = calculate_modified_duration(floating_cfs, curve, today) if floating_cfs else None
            
            print(f"  Fixed duration: {fixed_duration}")
            print(f"  Floating duration: {floating_duration}")
            
            # Create separate records for fixed and floating legs
            if derivative.subtype == "Receiver Swap":
                # Fixed leg is asset, floating leg is liability
                eve_driver_records.append(EveDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_fixed",
                    instrument_type="Derivative (Fixed Asset)",
                    base_pv=abs(fixed_pv),
                    shocked_pv=None,
                    duration=fixed_duration
                ))
                eve_driver_records.append(EveDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_floating",
                    instrument_type="Derivative (Floating Liability)",
                    base_pv=abs(floating_pv),
                    shocked_pv=None,
                    duration=floating_duration
                ))
            elif derivative.subtype == "Payer Swap":
                # Fixed leg is liability, floating leg is asset
                eve_driver_records.append(EveDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_fixed",
                    instrument_type="Derivative (Fixed Liability)",
                    base_pv=abs(fixed_pv),
                    shocked_pv=None,
                    duration=fixed_duration
                ))
                eve_driver_records.append(EveDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_floating",
                    instrument_type="Derivative (Floating Asset)",
                    base_pv=abs(floating_pv),
                    shocked_pv=None,
                    duration=floating_duration
                ))
            else:
                # For other derivative types, treat as separate legs
                fixed_pv = calculate_fixed_leg_pv(derivative, curve, today)
                floating_pv = calculate_floating_leg_pv(derivative, curve, today)
                # Use separate legs for duration calculation (consistent approach)
                fixed_cfs = generate_fixed_leg_cashflows(derivative, curve, today)
                floating_cfs = generate_floating_leg_cashflows(derivative, curve, today)
                # Calculate weighted average duration
                fixed_duration = calculate_modified_duration(fixed_cfs, curve, today) if fixed_cfs else None
                floating_duration = calculate_modified_duration(floating_cfs, curve, today) if floating_cfs else None
                # Use the leg with the larger PV for duration
                if abs(fixed_pv) > abs(floating_pv):
                    duration = fixed_duration
                else:
                    duration = floating_duration
                # Use the larger PV for the record
                base_pv = abs(fixed_pv) if abs(fixed_pv) > abs(floating_pv) else abs(floating_pv)
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
            if derivative.start_date <= today and derivative.end_date > today:
                fixed_rate = derivative.fixed_rate if derivative.fixed_rate is not None else 0
                floating_rate_for_nii = interpolate_rate(curve, 365) + (derivative.floating_spread if derivative.floating_spread is not None else 0)

                # Calculate fixed and floating NII contributions
                fixed_nii = fixed_rate * derivative.notional
                floating_nii = floating_rate_for_nii * derivative.notional

                if (derivative.end_date - today).days > 0:
                    proration_factor = min(1.0, (min(derivative.end_date, today + timedelta(days=NII_HORIZON_DAYS)) - today).days / 365.0)
                    fixed_nii_contribution = fixed_nii * proration_factor
                    floating_nii_contribution = floating_nii * proration_factor
                else:
                    fixed_nii_contribution = 0
                    floating_nii_contribution = 0
            else:
                fixed_nii_contribution = 0
                floating_nii_contribution = 0

            bucket = get_bucket(derivative.end_date, today, {
                "0-3 Months": 90,
                "3-6 Months": 180,
                "6-12 Months": 365,
                "1-5 Years": 365 * 5,
                ">5 Years": 365 * 100,
                "Fixed Rate / Non-Sensitive": -1
            })

            # Create separate records for fixed and floating legs
            if derivative.subtype == "Receiver Swap":
                # Receiver Swap: Receive fixed (income), pay floating (expense)
                nii_driver_records.append(NiiDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_fixed",
                    instrument_type="Derivative (Fixed Asset)",
                    nii_contribution=fixed_nii_contribution,  # Positive (income)
                    breakdown_type=derivative.type,
                    breakdown_value=bucket
                ))
                nii_driver_records.append(NiiDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_floating",
                    instrument_type="Derivative (Floating Liability)",
                    nii_contribution=-floating_nii_contribution,  # Negative (expense)
                    breakdown_type=derivative.type,
                    breakdown_value=bucket
                ))
            elif derivative.subtype == "Payer Swap":
                # Payer Swap: Pay fixed (expense), receive floating (income)
                nii_driver_records.append(NiiDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_fixed",
                    instrument_type="Derivative (Fixed Liability)",
                    nii_contribution=-fixed_nii_contribution,  # Negative (expense)
                    breakdown_type=derivative.type,
                    breakdown_value=bucket
                ))
                nii_driver_records.append(NiiDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_floating",
                    instrument_type="Derivative (Floating Asset)",
                    nii_contribution=floating_nii_contribution,  # Positive (income)
                    breakdown_type=derivative.type,
                    breakdown_value=bucket
                ))
            else:
                # For other derivative types, treat as separate legs
                nii_driver_records.append(NiiDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_fixed",
                    instrument_type="Derivative (Fixed)",
                    nii_contribution=fixed_nii_contribution,  # Positive
                    breakdown_type=derivative.type,
                    breakdown_value=bucket
                ))
                nii_driver_records.append(NiiDriverCreate(
                    scenario=scenario_name,
                    instrument_id=str(derivative.id) + "_floating",
                    instrument_type="Derivative (Floating)",
                    nii_contribution=-floating_nii_contribution,  # Negative
                    breakdown_type=derivative.type,
                    breakdown_value=bucket
                ))
    
    # Delete and save NII drivers for all scenarios
    db.query(NiiDriver).delete(synchronize_session=False)
    save_nii_drivers(db, nii_driver_records)





    # Populate repricing_buckets for all instruments (for drill-down capability)
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
            repricing_buckets.append(RepricingBucketCreate(
                scenario="Base Case",
                bucket=bucket_name,
                instrument_type=loan.type,
                instrument_id=str(loan.id),
                notional=loan.notional,
                position="asset"
            ))
        elif not loan.next_repricing_date:
            bucket_name = "Fixed Rate / Non-Sensitive"
            repricing_buckets.append(RepricingBucketCreate(
                scenario="Base Case",
                bucket=bucket_name,
                instrument_type=loan.type,
                instrument_id=str(loan.id),
                notional=loan.notional,
                position="asset"
            ))
        else:
            # For floating rate loans, include all repricing points over the life
            current_date = loan.next_repricing_date
            while current_date and current_date <= loan.maturity_date:
                bucket_name = get_bucket(current_date, today, nii_buckets_def)
                repricing_buckets.append(RepricingBucketCreate(
                    scenario="Base Case",
                    bucket=bucket_name,
                    instrument_type=loan.type,
                    instrument_id=str(loan.id),
                    notional=loan.notional,
                    position="asset"
                ))
                # Move to next repricing date
                if loan.repricing_frequency == "Monthly":
                    current_date = current_date + timedelta(days=30)
                elif loan.repricing_frequency == "Quarterly":
                    current_date = current_date + timedelta(days=90)
                elif loan.repricing_frequency == "Semi-Annually":
                    current_date = current_date + timedelta(days=182)
                elif loan.repricing_frequency == "Annually":
                    current_date = current_date + timedelta(days=365)
                else:
                    break  # Unknown frequency, stop
    
    # Deposits (liabilities)
    for deposit in deposits:
        if deposit.type == "Equity":
            continue
        if deposit.type in ["CD", "Wholesale Funding"]:
            if deposit.maturity_date:
                bucket_name = get_bucket(deposit.maturity_date, today, nii_buckets_def)
            else:
                bucket_name = "Fixed Rate / Non-Sensitive"
            repricing_buckets.append(RepricingBucketCreate(
                scenario="Base Case",
                bucket=bucket_name,
                instrument_type=deposit.type,
                instrument_id=str(deposit.id),
                notional=deposit.balance,
                position="liability"
            ))
        elif deposit.type in ["Checking", "Savings"]:
            if deposit.next_repricing_date:
                # For floating deposits, include all repricing points over the life
                current_date = deposit.next_repricing_date
                # Assume NMDs have effective maturity of 5 years for gap analysis
                effective_maturity = today + timedelta(days=365*5)
                while current_date and current_date <= effective_maturity:
                    bucket_name = get_bucket(current_date, today, nii_buckets_def)
                    repricing_buckets.append(RepricingBucketCreate(
                        scenario="Base Case",
                        bucket=bucket_name,
                        instrument_type=deposit.type,
                        instrument_id=str(deposit.id),
                        notional=deposit.balance,
                        position="liability"
                    ))
                    # Move to next repricing date
                    if deposit.repricing_frequency == "Monthly":
                        current_date = current_date + timedelta(days=30)
                    elif deposit.repricing_frequency == "Quarterly":
                        current_date = current_date + timedelta(days=90)
                    elif deposit.repricing_frequency == "Semi-Annually":
                        current_date = current_date + timedelta(days=182)
                    elif deposit.repricing_frequency == "Annually":
                        current_date = current_date + timedelta(days=365)
                    else:
                        break  # Unknown frequency, stop
            else:
                bucket_name = "Fixed Rate / Non-Sensitive"
                repricing_buckets.append(RepricingBucketCreate(
                    scenario="Base Case",
                    bucket=bucket_name,
                    instrument_type=deposit.type,
                    instrument_id=str(deposit.id),
                    notional=deposit.balance,
                    position="liability"
                ))
        else:
            bucket_name = "Fixed Rate / Non-Sensitive"
            repricing_buckets.append(RepricingBucketCreate(
                scenario="Base Case",
                bucket=bucket_name,
                instrument_type=deposit.type,
                instrument_id=str(deposit.id),
                notional=deposit.balance,
                position="liability"
            ))
    
    # Derivatives
    for derivative in derivatives:
        if derivative.type == "Interest Rate Swap":
            notional = derivative.notional
            
            # Fixed leg always goes in "Fixed Rate / Non-Sensitive"
            if derivative.subtype == "Payer Swap":
                # For Payer Swap: Fixed leg is liability
                repricing_buckets.append(RepricingBucketCreate(
                    scenario="Base Case",
                    bucket="Fixed Rate / Non-Sensitive",
                    instrument_type="Derivative (Fixed)",
                    instrument_id=str(derivative.id) + "_fixed",
                    notional=notional,
                    position="liability"
                ))
            elif derivative.subtype == "Receiver Swap":
                # For Receiver Swap: Fixed leg is asset
                repricing_buckets.append(RepricingBucketCreate(
                    scenario="Base Case",
                    bucket="Fixed Rate / Non-Sensitive",
                    instrument_type="Derivative (Fixed)",
                    instrument_id=str(derivative.id) + "_fixed",
                    notional=notional,
                    position="asset"
                ))
            else:
                # For other swap types, default to asset
                repricing_buckets.append(RepricingBucketCreate(
                    scenario="Base Case",
                    bucket="Fixed Rate / Non-Sensitive",
                    instrument_type="Derivative (Fixed)",
                    instrument_id=str(derivative.id) + "_fixed",
                    notional=notional,
                    position="asset"
                ))
            
            # Floating leg goes in appropriate time bucket based on repricing frequency
            # Include all repricing points over the life of the derivative
            if derivative.floating_payment_frequency:
                current_date = today
                # Start from next repricing date
                if derivative.floating_payment_frequency == "Monthly":
                    current_date = today + timedelta(days=30)
                elif derivative.floating_payment_frequency == "Quarterly":
                    current_date = today + timedelta(days=90)
                elif derivative.floating_payment_frequency == "Semi-Annually":
                    current_date = today + timedelta(days=182)
                elif derivative.floating_payment_frequency == "Annually":
                    current_date = today + timedelta(days=365)
                
                # Include all repricing points until maturity
                while current_date and current_date <= derivative.end_date:
                    bucket_name = get_bucket(current_date, today, nii_buckets_def)
                    
                    if derivative.subtype == "Payer Swap":
                        # For Payer Swap: Floating leg is asset
                        repricing_buckets.append(RepricingBucketCreate(
                            scenario="Base Case",
                            bucket=bucket_name,
                            instrument_type="Derivative (Floating)",
                            instrument_id=str(derivative.id) + "_floating",
                            notional=notional,
                            position="asset"
                        ))
                    elif derivative.subtype == "Receiver Swap":
                        # For Receiver Swap: Floating leg is liability
                        repricing_buckets.append(RepricingBucketCreate(
                            scenario="Base Case",
                            bucket=bucket_name,
                            instrument_type="Derivative (Floating)",
                            instrument_id=str(derivative.id) + "_floating",
                            notional=notional,
                            position="liability"
                        ))
                    else:
                        # For other swap types, default to asset
                        repricing_buckets.append(RepricingBucketCreate(
                            scenario="Base Case",
                            bucket=bucket_name,
                            instrument_type="Derivative (Floating)",
                            instrument_id=str(derivative.id) + "_floating",
                            notional=notional,
                            position="asset"
                        ))
                    
                    # Move to next repricing date
                    if derivative.floating_payment_frequency == "Monthly":
                        current_date = current_date + timedelta(days=30)
                    elif derivative.floating_payment_frequency == "Quarterly":
                        current_date = current_date + timedelta(days=90)
                    elif derivative.floating_payment_frequency == "Semi-Annually":
                        current_date = current_date + timedelta(days=182)
                    elif derivative.floating_payment_frequency == "Annually":
                        current_date = current_date + timedelta(days=365)
                    else:
                        break  # Unknown frequency, stop
            else:
                # If no repricing frequency, put floating leg in "Fixed Rate / Non-Sensitive"
                if derivative.subtype == "Payer Swap":
                    repricing_buckets.append(RepricingBucketCreate(
                        scenario="Base Case",
                        bucket="Fixed Rate / Non-Sensitive",
                        instrument_type="Derivative (Floating)",
                        instrument_id=str(derivative.id) + "_floating",
                        notional=notional,
                        position="asset"
                    ))
                elif derivative.subtype == "Receiver Swap":
                    repricing_buckets.append(RepricingBucketCreate(
                        scenario="Base Case",
                        bucket="Fixed Rate / Non-Sensitive",
                        instrument_type="Derivative (Floating)",
                        instrument_id=str(derivative.id) + "_floating",
                        notional=notional,
                        position="liability"
                    ))
                else:
                    repricing_buckets.append(RepricingBucketCreate(
                        scenario="Base Case",
                        bucket="Fixed Rate / Non-Sensitive",
                        instrument_type="Derivative (Floating)",
                        instrument_id=str(derivative.id) + "_floating",
                        notional=notional,
                        position="asset"
                    ))
    
    # Delete and save repricing buckets
    db.query(RepricingBucket).filter(RepricingBucket.scenario == "Base Case").delete(synchronize_session=False)
    save_repricing_buckets(db, repricing_buckets)

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
        portfolio_records.append(PortfolioCompositionCreate(
            timestamp=today,
            instrument_type="Derivative",
            category=derivative.type,
            subcategory=derivative.subtype,
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
                days_to_maturity = (cf_date - today).days
                accrual_period = get_accrual_period(getattr(loan, 'payment_frequency', 'Annually'))
                if loan.type in ["Fixed Rate Loan", "HTM Securities"]:
                    fixed_component = cf_amount
                    floating_component = 0.0
                elif loan.type == "Floating Rate Loan":
                    spread = loan.spread if hasattr(loan, 'spread') and loan.spread is not None else 0.0
                    scenario_rate = interpolate_rate(curve, days_to_maturity)
                    notional = loan.notional
                    fixed_component = spread * notional * accrual_period
                    floating_component = scenario_rate * notional * accrual_period
                else:
                    fixed_component = 0.0
                    floating_component = cf_amount
                total_cashflow = fixed_component + floating_component
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
                days_to_maturity = (cf_date - today).days
                accrual_period = get_accrual_period(getattr(deposit, 'payment_frequency', 'Annually'))
                if deposit.type in ["CD", "Wholesale Funding"]:
                    fixed_component = cf_amount
                    floating_component = 0.0
                elif deposit.type in ["Checking", "Savings"]:
                    spread = getattr(deposit, 'spread', 0.0) or 0.0
                    scenario_rate = interpolate_rate(curve, days_to_maturity)
                    notional = deposit.balance
                    fixed_component = spread * notional * accrual_period
                    floating_component = scenario_rate * notional * accrual_period
                else:
                    fixed_component = 0.0
                    floating_component = cf_amount
                total_cashflow = fixed_component + floating_component
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
            # Skip inactive derivatives
            if derivative.start_date > today or derivative.end_date < today:
                continue
                
            print(f"Processing derivative {derivative.instrument_id} for cashflow ladder")
            # Generate separate cashflows for fixed and floating legs
            fixed_cfs = generate_fixed_leg_cashflows(derivative, curve, today)
            floating_cfs = generate_floating_leg_cashflows(derivative, curve, today)
            print(f"  Fixed leg cashflows: {len(fixed_cfs)}")
            print(f"  Floating leg cashflows: {len(floating_cfs)}")
            
            # Process fixed leg cashflows
            for cf_date, cf_amount in fixed_cfs:
                months = (cf_date.year - today.year) * 12 + (cf_date.month - today.month)
                days_to_maturity = (cf_date - today).days
                
                # Calculate discount factor for this specific payment
                discount_rate = interpolate_rate(curve, days_to_maturity)
                discount_factor = 1 / (1 + discount_rate * (days_to_maturity / 365))
                
                # Calculate PV of fixed component
                fixed_pv = cf_amount * discount_factor
                
                # Determine asset/liability based on swap type
                if derivative.subtype == "Receiver Swap":
                    # Fixed leg is asset, floating leg is liability
                    # Asset (Fixed)
                    cashflow_ladder_records.append(CashflowLadderCreate(
                        scenario=scenario_name,
                        instrument_id=str(derivative.id) + "_fixed",
                        instrument_type=derivative.type + " (Fixed)",
                        asset_liability="A",
                        cashflow_date=cf_date,
                        time_months=months,
                        fixed_component=cf_amount,
                        floating_component=0.0,
                        total_cashflow=cf_amount,
                        discount_factor=discount_factor,
                        pv=fixed_pv
                    ))
                elif derivative.subtype == "Payer Swap":
                    # Fixed leg is liability, floating leg is asset
                    # Liability (Fixed)
                    cashflow_ladder_records.append(CashflowLadderCreate(
                        scenario=scenario_name,
                        instrument_id=str(derivative.id) + "_fixed",
                        instrument_type=derivative.type + " (Fixed)",
                        asset_liability="L",
                        cashflow_date=cf_date,
                        time_months=months,
                        fixed_component=cf_amount,
                        floating_component=0.0,
                        total_cashflow=cf_amount,
                        discount_factor=discount_factor,
                        pv=fixed_pv
                    ))
                else:
                    # For other derivative types, treat as separate legs
                    cashflow_ladder_records.append(CashflowLadderCreate(
                        scenario=scenario_name,
                        instrument_id=str(derivative.id) + "_fixed",
                        instrument_type=derivative.type + " (Fixed)",
                        asset_liability="A",  # Default to asset
                        cashflow_date=cf_date,
                        time_months=months,
                        fixed_component=cf_amount,
                        floating_component=0.0,
                        total_cashflow=cf_amount,
                        discount_factor=discount_factor,
                        pv=fixed_pv
                    ))
            
            # Process floating leg cashflows
            for cf_date, cf_amount in floating_cfs:
                months = (cf_date.year - today.year) * 12 + (cf_date.month - today.month)
                days_to_maturity = (cf_date - today).days
                
                # Calculate discount factor for this specific payment
                discount_rate = interpolate_rate(curve, days_to_maturity)
                discount_factor = 1 / (1 + discount_rate * (days_to_maturity / 365))
                
                # Calculate PV of floating component
                floating_pv = cf_amount * discount_factor
                
                # Determine asset/liability based on swap type
                if derivative.subtype == "Receiver Swap":
                    # Fixed leg is asset, floating leg is liability
                    # Liability (Floating)
                    cashflow_ladder_records.append(CashflowLadderCreate(
                        scenario=scenario_name,
                        instrument_id=str(derivative.id) + "_floating",
                        instrument_type=derivative.type + " (Floating)",
                        asset_liability="L",
                        cashflow_date=cf_date,
                        time_months=months,
                        fixed_component=0.0,
                        floating_component=cf_amount,
                        total_cashflow=cf_amount,
                        discount_factor=discount_factor,
                        pv=floating_pv
                    ))
                elif derivative.subtype == "Payer Swap":
                    # Fixed leg is liability, floating leg is asset
                    # Asset (Floating)
                    cashflow_ladder_records.append(CashflowLadderCreate(
                        scenario=scenario_name,
                        instrument_id=str(derivative.id) + "_floating",
                        instrument_type=derivative.type + " (Floating)",
                        asset_liability="A",
                        cashflow_date=cf_date,
                        time_months=months,
                        fixed_component=0.0,
                        floating_component=cf_amount,
                        total_cashflow=cf_amount,
                        discount_factor=discount_factor,
                        pv=floating_pv
                    ))
                else:
                    # For other derivative types, treat as separate legs
                    cashflow_ladder_records.append(CashflowLadderCreate(
                        scenario=scenario_name,
                        instrument_id=str(derivative.id) + "_floating",
                        instrument_type=derivative.type + " (Floating)",
                        asset_liability="A",  # Default to asset
                        cashflow_date=cf_date,
                        time_months=months,
                        fixed_component=0.0,
                        floating_component=cf_amount,
                        total_cashflow=cf_amount,
                        discount_factor=discount_factor,
                        pv=floating_pv
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

def get_accrual_period(payment_frequency: str) -> float:
    if payment_frequency == "Monthly":
        return 1/12
    elif payment_frequency == "Quarterly":
        return 1/4
    elif payment_frequency == "Semi-Annually":
        return 1/2
    elif payment_frequency == "Annually":
        return 1.0
    return 1.0  # Default to annual if missing

def generate_fixed_leg_cashflows(derivative: models.Derivative, yield_curve: Dict[str, float], today: date) -> List[Tuple[date, float]]:
    """Generate cashflows for the fixed leg of a derivative."""
    if derivative.fixed_rate is None:
        return []
    
    cashflows = []
    payment_date = derivative.start_date
    
    # If the start date is in the past, find the next payment date
    while payment_date <= today:
        payment_date += timedelta(days=365)  # Move to next year
    
    accrual_period = get_accrual_period(getattr(derivative, 'fixed_payment_frequency', 'Annually'))
    fixed_payment = derivative.fixed_rate * derivative.notional * accrual_period
    
    while payment_date <= derivative.end_date:
        cashflows.append((payment_date, fixed_payment))
        payment_date += timedelta(days=365)
    
    return cashflows

def generate_floating_leg_cashflows(derivative: models.Derivative, yield_curve: Dict[str, float], today: date) -> List[Tuple[date, float]]:
    """Generate cashflows for the floating leg of a derivative."""
    cashflows = []
    payment_date = derivative.start_date
    
    # If the start date is in the past, find the next payment date
    while payment_date <= today:
        payment_date += timedelta(days=365)  # Move to next year
    
    accrual_period = get_accrual_period(getattr(derivative, 'floating_payment_frequency', 'Annually'))
    
    while payment_date <= derivative.end_date:
        days_to_payment = (payment_date - today).days
        if days_to_payment <= 0:
            payment_date += timedelta(days=365)
            continue
        
        # Calculate floating rate at payment date
        floating_rate = interpolate_rate(yield_curve, days_to_payment) + (derivative.floating_spread if derivative.floating_spread is not None else 0)
        floating_payment = floating_rate * derivative.notional * accrual_period
        
        cashflows.append((payment_date, floating_payment))
        payment_date += timedelta(days=365)
    
    return cashflows

def calculate_fixed_leg_pv(derivative: models.Derivative, yield_curve: Dict[str, float], today: date) -> float:
    """Calculate PV of the fixed leg of a derivative."""
    if derivative.fixed_rate is None:
        return 0.0
    
    fixed_cfs = generate_fixed_leg_cashflows(derivative, yield_curve, today)
    return calculate_pv_of_cashflows(fixed_cfs, yield_curve, today)

def calculate_floating_leg_pv(derivative: models.Derivative, yield_curve: Dict[str, float], today: date) -> float:
    """Calculate PV of the floating leg of a derivative."""
    floating_cfs = generate_floating_leg_cashflows(derivative, yield_curve, today)
    return calculate_pv_of_cashflows(floating_cfs, yield_curve, today)