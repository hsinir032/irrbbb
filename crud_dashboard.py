from sqlalchemy.orm import Session
import models_dashboard, schemas_dashboard
from datetime import date

def save_dashboard_metric(db: Session, metric: schemas_dashboard.DashboardMetricCreate):
    record = models_dashboard.DashboardMetric(**metric.dict())
    db.add(record)
    db.commit()

def save_eve_drivers(db: Session, drivers: list[schemas_dashboard.EveDriverCreate]):
    for drv in drivers:
        db.add(models_dashboard.EveDriver(**drv.dict()))
    db.commit()

def save_repricing_buckets(db: Session, buckets: list[schemas_dashboard.RepricingBucketCreate]):
    for bucket in buckets:
        db.add(models_dashboard.RepricingBucket(**bucket.dict()))
    db.commit()

def save_repricing_net_positions(db: Session, net_data: list[schemas_dashboard.RepricingNetPositionCreate]):
    for row in net_data:
        db.add(models_dashboard.RepricingNetPosition(**row.dict()))
    db.commit()

def save_portfolio_composition(db: Session, records: list[schemas_dashboard.PortfolioCompositionCreate]):
    for rec in records:
        db.add(models_dashboard.PortfolioComposition(**rec.dict()))
    db.commit()

def save_nii_drivers(db: Session, drivers: list[schemas_dashboard.NiiDriverCreate]):
    for drv in drivers:
        db.add(models_dashboard.NiiDriver(**drv.dict()))
    db.commit()

def get_latest_dashboard_metrics(db: Session):
    return db.query(models_dashboard.DashboardMetric).order_by(models_dashboard.DashboardMetric.timestamp.desc()).all()

def get_eve_drivers_for_scenario(db: Session, scenario: str):
    return db.query(models_dashboard.EveDriver).filter(models_dashboard.EveDriver.scenario == scenario).all()

def get_net_positions_for_scenario(db: Session, scenario: str):
    return db.query(models_dashboard.RepricingNetPosition).filter(models_dashboard.RepricingNetPosition.scenario == scenario).all()

def get_bucket_constituents(db: Session, scenario: str, bucket: str):
    return db.query(models_dashboard.RepricingBucket).filter(
        models_dashboard.RepricingBucket.scenario == scenario,
        models_dashboard.RepricingBucket.bucket == bucket
    ).all()

def get_portfolio_composition(db: Session):
    records = db.query(models_dashboard.PortfolioComposition).all()
    total_loans = sum(r.volume_count for r in records if r.instrument_type == 'Loan')
    total_deposits = sum(r.volume_count for r in records if r.instrument_type == 'Deposit')
    total_derivatives = sum(r.volume_count for r in records if r.instrument_type == 'Derivative')
    return {
        'records': records,
        'total_loans': total_loans,
        'total_deposits': total_deposits,
        'total_derivatives': total_derivatives
    }

def delete_eve_drivers_for_scenario_and_date(db: Session, scenario: str, timestamp: date):
    db.query(models_dashboard.EveDriver).filter(
        models_dashboard.EveDriver.scenario == scenario
    ).delete(synchronize_session=False)
    db.commit()

def get_nii_drivers_for_scenario_and_breakdown(db: Session, scenario: str, breakdown_type: str):
    return db.query(models_dashboard.NiiDriver).filter(
        models_dashboard.NiiDriver.scenario == scenario,
        models_dashboard.NiiDriver.breakdown_type == breakdown_type
    ).all()
