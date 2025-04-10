from sqlmodel import text
from apps.database import get_session

# Raw SQL query to fetch all data from channel_mix table for March 2024
query = text(
    """
    SELECT * 
    FROM public.channel_mix 
    WHERE date BETWEEN '2024-03-01' AND '2024-03-31'
    ORDER BY id ASC
"""
)


# Function to retrieve March 2024 data from the channel_mix table
def get_march_2024_channel_mix():
    with get_session() as session:
        # Execute the raw SQL query
        result = session.execute(query).fetchall()
        return result


# Example usage
channel_mix_data_march_2024 = get_march_2024_channel_mix()

# Printing the results
if channel_mix_data_march_2024:
    for row in channel_mix_data_march_2024:
        print(row)  # Each row will be a tuple with the column data
else:
    print("No data found for March 2024 in the channel_mix table.")
