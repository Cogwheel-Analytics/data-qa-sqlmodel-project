from sqlmodel import text
from apps.utils.database import get_session  # Import your session manager

# Query to get all duplicate hotels based on hotel_code
query = text(
    """
    SELECT *
    FROM public.hotel
    WHERE code IN (
        SELECT code
        FROM public.hotel
        GROUP BY code
        HAVING COUNT(*) > 1
    )
"""
)


def get_duplicate_hotels():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# Example usage
duplicate_hotels = get_duplicate_hotels()

# Print results
if duplicate_hotels:
    print("Duplicate hotels:")
    for row in duplicate_hotels:
        print(row)  # Each row will contain all columns of the hotel
    print(f"\nTotal duplicate hotels: {len(duplicate_hotels)}")
else:
    print("No duplicate hotels found.")
