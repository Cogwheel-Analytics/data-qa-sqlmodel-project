from sqlmodel import text
from apps.database import get_session

# Raw SQL query to calculate the total revenue for February and March 2024
query = text(
    """
    SELECT
        SUM(CASE WHEN date BETWEEN '2024-02-01' AND '2024-02-29' THEN revenue END) AS february_total_revenue,
        SUM(CASE WHEN date BETWEEN '2024-03-01' AND '2024-03-31' THEN revenue END) AS march_total_revenue
    FROM public.channel_mix;
"""
)


def get_february_march_total_revenue():
    with get_session() as session:
        result = session.execute(query).fetchone()
        return result


# Example usage
february_march_revenue = get_february_march_total_revenue()

if february_march_revenue:
    february_revenue = february_march_revenue[0] if february_march_revenue[0] else 0
    march_revenue = february_march_revenue[1] if february_march_revenue[1] else 0

    # Calculate the difference
    revenue_difference = february_revenue - march_revenue

    print(f"Total Revenue for February 2024: {february_revenue}")
    print(f"Total Revenue for March 2024: {march_revenue}")
    print(f"Revenue Difference (February - March): {revenue_difference}")
else:
    print("No revenue data found for February or March 2024.")
