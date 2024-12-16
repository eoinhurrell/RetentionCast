from lifetimes import BetaGeoFitter
import pandas as pd
import sqlalchemy
import numpy as np
from scipy.special import expit


def find_age_below_probability(
    frequency,
    recency,
    target_probability=0.7,
    max_age=1000,  # Reasonable upper bound
    tolerance=1e-6,  # Precision of probability calculation
):
    """
    Find the age at which the conditional probability of being alive
    falls below the target probability.

    Parameters
    ----------
    frequency: int or float
        Historical frequency of customer transactions.
    recency: int or float
        Historical recency of customer's last transaction.
    target_probability: float, optional (default=0.7)
        Probability threshold to find age for.
    max_age: int or float, optional (default=1000)
        Maximum age to search up to.
    tolerance: float, optional (default=1e-6)
        Precision for probability calculation.

    Returns
    -------
    float
        Age at which probability falls below target probability.
    """
    # Retrieve model parameters
    model_params = {
        "r": 5.449462,
        "alpha": 9.599969,
        "a": 1.137758,
        "beta": 17.670075,
        "b": 17.670075,
    }
    r = model_params["r"]
    alpha = model_params["alpha"]
    a = model_params["a"]
    b = model_params["b"]

    def _conditional_probability_alive(T):
        """
        Internal method to compute conditional probability alive at a given age.
        Matches the original implementation in the class method.
        """
        log_div = (r + frequency) * np.log((alpha + T) / (alpha + recency)) + np.log(
            a / (b + np.maximum(frequency, 1) - 1)
        )
        return np.atleast_1d(np.where(frequency == 0, 1.0, expit(-log_div)))[0]

    # Binary search to find the age
    left, right = recency, max_age
    while right - left > tolerance:
        mid = (left + right) / 2
        prob = _conditional_probability_alive(mid)

        if prob > target_probability:
            left = mid
        else:
            right = mid

    return right if right < max_age else 0  # None


# Create database connection string

connection_string = f"postgresql://postgres:postgres@localhost:5432/postgres"

# Create SQLAlchemy engine
engine = sqlalchemy.create_engine(connection_string)

# Read entire table into pandas DataFrame
sql = """WITH study_period AS (
    SELECT 
        MAX(last_purchase_date) AS end_date,
        MIN(first_purchase_date) AS start_date
    FROM 
        public.rfm
),
customer_metrics AS (
    SELECT 
        r.user_id,
        -- Frequency: Keep as-is from the table
        r.frequency,
        
        -- T: Total customer age in days from start of study to first purchase
        EXTRACT(DAYS FROM (
            (SELECT end_date FROM study_period) - r.first_purchase_date)
        ) AS T,
        
        -- Recency: Days from first to last purchase
        EXTRACT(DAYS FROM (r.last_purchase_date - r.first_purchase_date)) AS recency,
        
        -- Monetary Value: Average order value
        r.avg_order_value AS monetary_value,
        
        -- Additional context columns
        r.first_purchase_date,
        r.last_purchase_date,
        r.total_order_value
    FROM 
        public.rfm r,
        study_period
)

SELECT 
    user_id,
    frequency,
    ROUND(T::numeric, 2) AS T,
    ROUND(recency::numeric, 2) AS recency,
    ROUND(monetary_value::numeric, 2) AS monetary_value,
    first_purchase_date
FROM 
    customer_metrics;
"""
data = pd.read_sql_query(sql, engine)

# similar API to scikit-learn and lifelines.
bgf = BetaGeoFitter(penalizer_coef=0.0)
bgf.fit(data["frequency"], data["recency"], data["t"])
print(bgf)
print(bgf.summary)

data["age"] = data.apply(
    lambda x: find_age_below_probability(x["frequency"], x["recency"]), axis=1
)

data["retention_target_date"] = data.apply(
    lambda x: pd.to_datetime(x["first_purchase_date"])
    + pd.Timedelta(days=int(x["age"])),
    axis=1,
)
__import__("ipdb").set_trace()
