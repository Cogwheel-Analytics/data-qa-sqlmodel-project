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
        WHERE cm.date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '36 month'
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

        # OPTIONAL: Remove extreme outliers (e.g., negative or very high values)
        upper_limit = df_channel["y"].quantile(0.99)
        lower_limit = df_channel["y"].quantile(0.01)
        df_channel = df_channel[
            (df_channel["y"] >= lower_limit) & (df_channel["y"] <= upper_limit)
        ]

        # Train/test split
        split_index = int(len(df_channel) * 0.75)
        train = df_channel.iloc[:split_index].copy()
        test = df_channel.iloc[split_index:].copy()

        # Ensure 'cap' column is not present (to avoid logistic growth issues)
        if "cap" in train.columns:
            train = train.drop(columns=["cap"])

        # Fit Prophet model with improved settings
        model = Prophet(
            yearly_seasonality=True,
            seasonality_mode="multiplicative",
            changepoint_prior_scale=0.2,
            seasonality_prior_scale=5.0,
        )
        model.fit(train)

        # Forecast
        future = model.make_future_dataframe(periods=len(test), freq="MS")
        forecast = model.predict(future)

        # Clip yhat to within train data range
        y_min, y_max = train["y"].min(), train["y"].max()
        forecast["yhat"] = forecast["yhat"].clip(lower=y_min, upper=y_max)

        # Ensure forecast includes all test dates
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

        # Show all monthly ticks on x-axis
        all_months = pd.date_range(
            start=df_channel["ds"].min(), end=df_channel["ds"].max(), freq="MS"
        )
        ax = fig.gca()
        ax.set_xticks(all_months)
        ax.set_xticklabels(
            [d.strftime("%b %Y") for d in all_months],
            rotation=45,
            ha="right",
            fontsize=8,
        )

        # Add title and legend
        plt.title(f"Forecast vs Actual for {channel}")
        plt.legend()

        # Save the plot
        safe_channel = re.sub(r"[^\w\-_.]", "_", channel)
        filename = f"{output_dir}/{safe_channel}.png"
        fig.savefig(filename, bbox_inches="tight")
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
