from sqlmodel import text
from apps.database import get_session

hotel_id = "b80acf46-4456-4972-a575-1709f4ee7d1a"

# SQL query to fetch channel_mix data for the specific active hotel and last x months
query = text(
    """
    SELECT cm.*
    FROM public.channel_mix cm
    JOIN public.hotel h ON cm.hotel_id = h.id
    WHERE cm.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
      AND cm.date < DATE_TRUNC('month', CURRENT_DATE)
      AND cm.hotel_id = :hotel_id
      AND h.is_active = true
    ORDER BY cm.date;
    """
)


def get_last_month_channel_mix_by_hotel(hotel_id: str):
    with get_session() as session:
        result = session.execute(query, {"hotel_id": hotel_id}).fetchall()
        return result


# Example usage
channel_mix_data = get_last_month_channel_mix_by_hotel(hotel_id)

if channel_mix_data:
    print("Channel Mix data for the last x months (active hotel):\n")
    for row in channel_mix_data:
        print(row)
    print(f"\nTotal records: {len(channel_mix_data)}")
else:
    print("No data found for this hotel in the last x months or the hotel is inactive.")
