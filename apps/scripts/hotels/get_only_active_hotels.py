from sqlmodel import text
from apps.database import get_session

# Raw SQL query to get only active hotels
query = text("SELECT * FROM public.hotel WHERE is_active = TRUE ORDER BY id ASC;")


# Function to fetch active hotels
def get_active_hotels():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# Example usage
active_hotels = get_active_hotels()

# Print the results
if active_hotels:
    for row in active_hotels:
        print(row)
    print(f"\nTotal active hotels: {len(active_hotels)}")
else:
    print("No active hotels found.")
