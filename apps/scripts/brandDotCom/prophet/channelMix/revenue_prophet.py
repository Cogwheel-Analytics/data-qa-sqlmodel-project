import pandas as pd
from apps.database import get_session
from sqlalchemy import text
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
import matplotlib.pyplot as plt
import os
import re


def fetch_data():
    query = """
        SELECT
            h.code AS hotel_code,
            ct.name AS channel_type,
            DATE_TRUNC('month', cm.date) AS month,
            SUM(cm.revenue) AS revenue
        FROM public.channel_mix cm
        JOIN public.hotel h ON cm.hotel_id = h.id
        JOIN public.channel_type ct ON cm.channel_type_id = ct.id
        WHERE cm.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '24 month'
        AND cm.date < DATE_TRUNC('month', CURRENT_DATE)
        AND h.code = 'BOSFRUP'
        AND h.is_active = TRUE
        GROUP BY h.code, ct.name, DATE_TRUNC('month', cm.date)
        ORDER BY ct.name, month
    """
    with get_session() as session:
        result = session.execute(text(query))
        df = pd.DataFrame(
            result.fetchall(), columns=["hotel_code", "channel_type", "ds", "y"]
        )
    return df


def evaluate_and_plot(df: pd.DataFrame, hotel_code: str, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    for channel in df["channel_type"].unique():
        df_channel = df[df["channel_type"] == channel].copy()
        df_channel["ds"] = pd.to_datetime(df_channel["ds"]).dt.tz_localize(None)
        df_channel = df_channel.sort_values("ds")

        if len(df_channel) < 6:
            print(f"Skipping {channel} (not enough data)")
            continue

        # Train/test split
        split_index = int(len(df_channel) * 0.75)
        train = df_channel.iloc[:split_index]
        test = df_channel.iloc[split_index:]

        # Fit Prophet model
        model = Prophet(yearly_seasonality=True)
        model.fit(train)

        # Forecast
        future = model.make_future_dataframe(periods=len(test), freq="MS")
        forecast = model.predict(future)

        forecast = forecast[forecast["ds"] <= df_channel["ds"].max()]

        # Evaluation
        forecast_filtered = (
            forecast[["ds", "yhat"]].set_index("ds").join(test.set_index("ds"))
        )
        forecast_filtered.dropna(inplace=True)

        if forecast_filtered.empty:
            print(f"Skipping {channel} (no overlapping forecast & test data)")
            continue

        mae = mean_absolute_error(forecast_filtered["y"], forecast_filtered["yhat"])
        mse = mean_squared_error(forecast_filtered["y"], forecast_filtered["yhat"])
        rmse = np.sqrt(mse)
        mape = (
            np.mean(
                np.abs(
                    (forecast_filtered["y"] - forecast_filtered["yhat"])
                    / forecast_filtered["y"]
                )
            )
            * 100
        )

        print(f"\nForecast Evaluation for {hotel_code} - {channel}:")
        print(f"MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {mape:.2f}%")

        # Plot and save
        fig = model.plot(forecast)

        # Plot actual values (test data) with markers
        plt.scatter(test["ds"], test["y"], color="red", label="Actual", zorder=5)

        # Mark the train/test split
        plt.axvline(
            test["ds"].iloc[0], color="red", linestyle="--", label="Train/Test Split"
        )

        # Add title and legend
        plt.title(f"Forecast vs Actual for {channel}")
        plt.legend()

        # Save the plot
        safe_channel = re.sub(r"[^\w\-_.]", "_", channel)
        filename = f"{output_dir}/{safe_channel}.png"
        fig.savefig(filename)
        plt.close()
        print(f"Saved plot: {filename}")


def main():
    df = fetch_data()
    if df.empty:
        print("No data found.")
        return

    hotel_code = df["hotel_code"].iloc[0]
    output_dir = f"forecast_plots/{hotel_code}"
    evaluate_and_plot(df, hotel_code, output_dir)


if __name__ == "__main__":
    main()
