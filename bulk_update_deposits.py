from irrbbb.database import SessionLocal
from irrbbb.models import Deposit

# Target total balance (in millions)
TARGET_TOTAL = 260_000_000

def main():
    db = SessionLocal()
    deposits = db.query(Deposit).all()
    current_total = sum(dep.balance for dep in deposits)
    if current_total == 0:
        print("No deposits found. Exiting.")
        return
    scaling_factor = TARGET_TOTAL / current_total
    print(f"Scaling all deposit balances by factor: {scaling_factor:.2f}")
    for dep in deposits:
        dep.balance *= scaling_factor
    db.commit()
    db.close()
    print(f"All deposit balances have been scaled up. New total: {TARGET_TOTAL}")

if __name__ == "__main__":
    main() 