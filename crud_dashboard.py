from sqlalchemy.orm import Session
import models_dashboard, schemas_dashboard

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