from sqlmodel import text
from apps.utils.database import get_session

# Raw SQL query to get all hotels
query = text("SELECT * FROM public.hotel ORDER BY id ASC;")


# Function to fetch all hotels
def get_all_hotels():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# Example usage
all_hotels = get_all_hotels()

# Print the results
if all_hotels:
    for row in all_hotels:
        print(row)  # Each row is a tuple of hotel columns
    print(f"\nTotal hotels: {len(all_hotels)}")
else:
    print("No hotels found.")
