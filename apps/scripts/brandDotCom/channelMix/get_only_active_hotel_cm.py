from sqlmodel import text
from apps.database import get_session

# Raw SQL query to fetch all active hotel data from channel_mix table
query = text(
    """
    SELECT cm.*
FROM public.channel_mix cm
JOIN public.hotel hotel ON cm.hotel_id = hotel.id
WHERE cm.hotel_id IS NOT NULL
  AND hotel.is_active = true;
"""
)


def get_only_active_hotel_cm():
    with get_session() as session:
        result = session.execute(query).fetchall()
        return result


only_active_hotel_channel_mix_data = get_only_active_hotel_cm()

if only_active_hotel_channel_mix_data:
    for row in only_active_hotel_channel_mix_data:
        print(row)
    print(f"\nTotal records: {len(only_active_hotel_channel_mix_data)}")
else:
    print("No data found in channel_mix table.")
