from sqlmodel import text
from apps.database import get_session

hotel_id = "b80acf46-4456-4972-a575-1709f4ee7d1a"

# SQL query to fetch channel_mix data for the specific hotel and last month
query = text(
    """
    SELECT *
    FROM public.channel_mix
    WHERE date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 month'
      AND date < DATE_TRUNC('month', CURRENT_DATE)
      AND hotel_id = :hotel_id
    ORDER BY date;
    """
)


def get_last_month_channel_mix_by_hotel(hotel_id: str):
    with get_session() as session:
        result = session.execute(query, {"hotel_id": hotel_id}).fetchall()
        return result


# Example usage
channel_mix_data = get_last_month_channel_mix_by_hotel(hotel_id)

if channel_mix_data:
    print("Channel Mix data for last month (single hotel):\n")
    for row in channel_mix_data:
        print(row)
    print(f"\nTotal records: {len(channel_mix_data)}")
else:
    print("No data found for this hotel in last month.")
