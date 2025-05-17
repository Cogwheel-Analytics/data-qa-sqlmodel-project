from sqlmodel import text
from apps.utils.database import get_session  # Your session context manager

# Query to get all inactive hotels
query = text("SELECT * FROM public.hotel WHERE is_active = FALSE")


def get_inactive_hotels():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# Example usage
inactive_hotels = get_inactive_hotels()

# Print each inactive hotel row
if inactive_hotels:
    for row in inactive_hotels:
        print(row)

    # Print total count of inactive hotels
    print(f"\nTotal inactive hotels: {len(inactive_hotels)}")
else:
    print("No inactive hotels found.")
