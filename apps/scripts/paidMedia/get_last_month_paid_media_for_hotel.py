from sqlmodel import text
from apps.database import get_session

# Replace with the desired hotel_id
hotel_id = "44ba4ddf-b600-47aa-a50a-cb9e9199c0bf"
# Raw SQL query to fetch paid media data for last month for a specific hotel_id
query = text(
    f"""
    SELECT *
    FROM public.paid_media
    WHERE hotel_id = '{hotel_id}'
      AND date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
      AND date < DATE_TRUNC('month', CURRENT_DATE)
    ORDER BY date;
"""
)


def get_last_month_paid_media_for_hotel(hotel_id: str):
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


# Example usage
paid_media_data = get_last_month_paid_media_for_hotel(hotel_id)

# Print results
if paid_media_data:
    print("Paid Media data for last month for hotel_id {hotel_id}:\n")
    for row in paid_media_data:
        print(row)  # Print each row (a tuple with the column data)
    print(f"\nTotal records: {len(paid_media_data)}")
else:
    print("No data found for the last month for the specified hotel_id.")
