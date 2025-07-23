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
    return db.query(models_dashboard.PortfolioComposition).all()

def delete_eve_drivers_for_scenario_and_date(db: Session, scenario: str, timestamp: date):
    db.query(models_dashboard.EveDriver).filter(
        models_dashboard.EveDriver.scenario == scenario
    ).delete(synchronize_session=False)
    db.commit()
