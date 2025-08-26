import pandas as pd
import statsmodels.api as sm
from pathlib import Path

# Load the final roadmap
ROADMAP = Path("picks/survivor/survivor_roadmap_expanded.csv")

df = pd.read_csv(ROADMAP)

# Ensure the spot_value_score column exists
if "spot_value_score" not in df.columns:
    raise SystemExit("No 'spot_value_score' column found — run spot_value_updates.py first.")

# Select potential predictors (adjust list as needed based on roadmap columns)
predictors = [
    "projected_win_prob",
    "rating_gap",
    "team_dvoa",
    "opp_dvoa",
    "injury_adjustment",
    "future_scarcity_bonus",
]

# Keep only predictors that exist in the dataframe
predictors = [col for col in predictors if col in df.columns]

# Drop rows with missing values in target or predictors
df_model = df.dropna(subset=["spot_value_score"] + predictors).copy()

# Define X and y
X = df_model[predictors]
y = df_model["spot_value_score"]

# Add constant for intercept
X = sm.add_constant(X)

# Fit OLS regression
model = sm.OLS(y, X).fit()

# Print regression summary
print(model.summary())

# Optional: display coefficients sorted by absolute value
print("\nPredictor Importance (abs value):")
print(model.params.reindex(model.params.abs().sort_values(ascending=False).index))
