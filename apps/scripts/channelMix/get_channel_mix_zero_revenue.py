from sqlmodel import text
from apps.database import get_session

# Raw SQL query to find channel mix records with revenue = 0
query = text(
    """
    SELECT * 
    FROM public.channel_mix 
    WHERE revenue = 0 AND hotel_id IS NOT NULL;
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
        print(row)  # Each row will be a tuple with the column data
else:
    print("No channel mix records found with zero revenue.")
