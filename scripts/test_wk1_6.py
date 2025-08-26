import pandas as pd

df = pd.read_csv('picks/survivor/survivor_roadmap_expanded.csv')
cols = ['week','team','opponent','home_or_away','projected_win_prob','spot_value','spot_value_score']

out = (df.loc[df['week'].between(1,6), cols]
         .sort_values(['week','spot_value_score'], ascending=[True,False]))

out.to_csv('reports/survivor_matrix_wk1_6.csv', index=False)
print('Wrote reports/survivor_matrix_wk1_6.csv')

# Optional checks
checks = {
    'projected_win_prob': df['projected_win_prob'].isna().sum(),
    'home_or_away': df['home_or_away'].isna().sum(),
    'spot_value_score': df['spot_value_score'].isna().sum(),
}
print("NA counts:", checks)

print("Holiday flags sums:",
      int(df.get('is_thanksgiving',0).sum()),
      int(df.get('is_black_friday',0).sum()),
      int(df.get('is_christmas',0).sum()))