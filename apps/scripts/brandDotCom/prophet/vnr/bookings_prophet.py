import pandas as pd
from apps.database import get_session
from sqlalchemy import text
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os


def fetch_vnr_data(hotel_code: str) -> pd.DataFrame:
    query = """
        SELECT
            h.code AS hotel_code,
            DATE_TRUNC('month', vnr.date) AS month,
            SUM(vnr.booking) AS booking
        FROM public.visit_revenue vnr
        JOIN public.hotel h ON vnr.hotel_id = h.id
        WHERE vnr.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '36 month'
        AND vnr.date < DATE_TRUNC('month', CURRENT_DATE)
        AND h.code = :hotel_code
        AND h.is_active = TRUE
        GROUP BY h.code, DATE_TRUNC('month', vnr.date)
        ORDER BY month
    """
    with get_session() as session:
        result = session.execute(text(query), {"hotel_code": hotel_code})
        df = pd.DataFrame(result.fetchall(), columns=["hotel_code", "ds", "y"])
    return df


def generate_forecast(df: pd.DataFrame) -> tuple:
    df["ds"] = pd.to_datetime(df["ds"]).dt.tz_localize(None)
    df = df.sort_values("ds")

    if len(df) < 6:
        return None, None, None, None, None

    # Train-test split
    split_index = int(len(df) * 0.75)
    train = df.iloc[:split_index]
    test = df.iloc[split_index:]

    # Fit full data
    model = Prophet(yearly_seasonality=True)
    model.fit(df)

    future = df[["ds"]].copy()
    forecast = model.predict(future)

    y_min, y_max = df["y"].min(), df["y"].max()
    forecast["yhat"] = forecast["yhat"].clip(lower=y_min, upper=y_max)

    return model, forecast, train, test, df


def evaluate_forecast(df: pd.DataFrame, forecast: pd.DataFrame, test: pd.DataFrame):
    forecast = forecast[forecast["ds"] <= df["ds"].max()]
    forecast_filtered = (
        forecast[["ds", "yhat"]].set_index("ds").join(test.set_index("ds"))
    )
    forecast_filtered.dropna(inplace=True)

    if forecast_filtered.empty:
        return None

    mae = mean_absolute_error(forecast_filtered["y"], forecast_filtered["yhat"])
    rmse = np.sqrt(
        mean_squared_error(forecast_filtered["y"], forecast_filtered["yhat"])
    )
    mape = (
        np.mean(
            np.abs(
                (forecast_filtered["y"] - forecast_filtered["yhat"])
                / forecast_filtered["y"]
            )
        )
        * 100
    )
    return mae, rmse, mape


def plot_forecast(
    model: Prophet,
    forecast: pd.DataFrame,
    df: pd.DataFrame,
    test: pd.DataFrame,
    hotel_code: str,
    output_dir: str,
):

    fig = model.plot(forecast)
    ax = fig.gca()

    # Plot actuals
    ax.scatter(df["ds"], df["y"], color="black", label="Actual", zorder=5)

    # Format Y-axis to show thousands with commas (e.g., 10,000)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{int(x):,}"))

    # Mark train/test split
    split_index = int(len(df) * 0.75)
    ax.axvline(
        df["ds"].iloc[split_index],
        color="red",
        linestyle="--",
        label="Train/Test Split",
    )

    # X-axis formatting
    months = pd.date_range(df["ds"].min(), df["ds"].max(), freq="MS")
    ax.set_xticks(months)
    ax.set_xticklabels(
        [d.strftime("%b %Y") for d in months], rotation=45, ha="right", fontsize=8
    )

    ax.set_title(f"Forecast vs Actual - {hotel_code}")
    ax.legend()

    # Save plot
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{output_dir}/forecast.png"
    fig.savefig(filename, bbox_inches="tight")
    plt.close()
    print(f"Saved: {filename}")


def forecast_vnr_for_hotel(hotel_code: str):
    df = fetch_vnr_data(hotel_code)
    if df.empty:
        print(f"No VNR data for hotel: {hotel_code}")
        return

    model, forecast, train, test, full_df = generate_forecast(df)
    if model is None:
        print(f"Skipping {hotel_code}: Not enough data")
        return

    metrics = evaluate_forecast(full_df, forecast, test)
    if not metrics:
        print(f"Skipping {hotel_code}: No overlapping test/forecast data")
        return

    mae, rmse, mape = metrics
    print(f"\n{hotel_code} - VNR")
    print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%")

    output_dir = f"forecast_plots/VNR/Bookings/{hotel_code}"
    plot_forecast(model, forecast, full_df, test, hotel_code, output_dir)


def main():
    hotel_code = "BOSFRUP"
    forecast_vnr_for_hotel(hotel_code)


if __name__ == "__main__":
    main()
