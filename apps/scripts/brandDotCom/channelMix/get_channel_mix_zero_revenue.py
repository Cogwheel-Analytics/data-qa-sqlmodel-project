from sqlmodel import text
from apps.database import get_session

# Raw SQL query to find channel mix records with revenue = 0
query = text(
    """
    SELECT cm.*
FROM public.channel_mix cm
JOIN public.hotel hotel ON cm.hotel_id = hotel.id
WHERE cm.revenue = 0
  AND cm.hotel_id IS NOT NULL
  AND hotel.is_active = true;
"""
)


# Function to retrieve the channel mix records with zero revenue
def get_channel_mix_zero_revenue():
    with get_session() as session:
        # Execute the raw SQL query
        result = session.execute(query).fetchall()
        return result


# Example usage
zero_revenue_channel_mix = get_channel_mix_zero_revenue()

# Printing the results
if zero_revenue_channel_mix:
    for row in zero_revenue_channel_mix:
        print(row)
    print(f"\nTotal records: {len(zero_revenue_channel_mix)}")
else:
    print("No channel mix records found with zero revenue.")
