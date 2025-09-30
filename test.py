import duckdb
import pandas as pd
import os

from dc_parser_lark import translate_dc_to_sql_lark
from dc_parser_parsimonious import translate_dc_to_sql_parsimonious

# --- Example ---

data = {
    "EmpID": ["E1", "E2", "E3", "E1"],
    "ProjID": ["P1", "P1", "P1", "P2"],
    "Role": ["Manager", "Developer", "Developer", "Manager"],
    "Hours": [4, 2, 4, 4],
    "Bonus": [100, 200, 150, 120],
}

df_hours = pd.DataFrame(data)
csv_filename = "hours.csv"
df_hours.to_csv(csv_filename, index=False)

dc_string = "¬(t.Role = t'.Role ∧ t.Hours > t'.Hours ∧ t.Bonus < t'.Bonus)"

try:
    # Choose parser here:
    sql_query = translate_dc_to_sql_lark(dc_string, csv_filename)
    # sql_query = translate_dc_to_sql_parsimonious(dc_string, csv_filename)

    print("Consulta SQL Gerada:")
    print(sql_query)

    results_df = duckdb.sql(sql_query).df()
    print("\nResultados:")
    print(results_df if not results_df.empty else "Nenhuma violação encontrada.")

finally:
    if os.path.exists(csv_filename):
        os.remove(csv_filename)