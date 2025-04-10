from sqlmodel import text
from apps.database import get_session

# SQL query to fetch only hotel codes from paid_media where hotel is active
query = text(
    """
    SELECT DISTINCT h.code AS hotel_code
    FROM public.paid_media pm
    JOIN public.hotel h ON pm.hotel_id = h.id
    WHERE h.is_active = TRUE
    ORDER BY h.code;
    """
)


def get_active_paid_media_hotel_codes():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# Example usage
hotel_codes = get_active_paid_media_hotel_codes()

# Print results
if hotel_codes:
    print("Active hotel codes from Paid Media data:\n")
    for row in hotel_codes:
        print(row.hotel_code)
    print(f"\nTotal active hotel codes: {len(hotel_codes)}")
else:
    print("No active hotel codes found in Paid Media data.")
