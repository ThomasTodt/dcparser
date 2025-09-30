import pandas as pd
import duckdb
import os
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
from parsimonious.exceptions import ParseError

# --- Definições do Parser com Parsimonious ---

# def translate_dc_to_sql_parsimonious(dc_string: str, table_name: str) -> str:
    # """
    # Analisa uma string de Denial Constraint usando Parsimonious e a traduz para SQL.
    # """
    # dc_grammar_parsimonious = r"""
    #     start                 = "¬(" ws predicate_conjunction ws ")"
    #     predicate_conjunction = predicate (ws "∧" ws predicate)*
    #     predicate             = tuple_variable dot column op tuple_variable dot column
    #     tuple_variable        = "t'" / "t"
    #     column                = ~r"[a-zA-Z_]\w*"
    #     op                    = ws operator ws
    #     operator              = "<=" / ">=" / "!=" / "<" / ">" / "="
    #     dot                   = "."
    #     ws                    = ~r"\s*"
    # """

    # class DcToSqlVisitor(NodeVisitor):
    #     #... (classe DcToSqlVisitor definida acima)...
    #     def __init__(self, table_name):
    #         self.table_name = table_name
    #         self.grammar = Grammar(dc_grammar_parsimonious)

    #     def visit_column(self, n, vc): return n.text
    #     def visit_operator(self, n, vc): return n.text.strip()
    #     def visit_tuple_variable(self, n, vc): return n.text
    #     def visit_op(self, n, vc): return vc[2]
        
    #     # def visit_predicate(self, n, vc):
    #     #     t_var1, _, col1, op, t_var2, _, col2 = vc
    #     #     return f"{t_var1}.{col1} {op} {t_var2}.{col2}"
        
    #     def visit_predicate(self, n, vc):
    #         return f"{vc[0]}.{vc[2]} {vc[3]} {vc[4]}.{vc[6]}"


    #     def visit_predicate_conjunction(self, n, vc):
    #         first_pred, others = vc
    #         other_preds = [pred_node[1] for pred_node in others]
    #         return " AND ".join([first_pred] + other_preds)

    #     def visit_start(self, n, vc):
    #         _, _, where_clause, _, _ = vc
    #         return (f"SELECT t.*, t2.* "
    #                 f"FROM read_csv_auto('{self.table_name}') t, "
    #                 f"     read_csv_auto('{self.table_name}') t2 "
    #                 f"WHERE {where_clause}")


    #     def generic_visit(self, n, vc): return vc or n.text
        
    #     def parse(self, text):
    #         tree = self.grammar.parse(text)
    #         return self.visit(tree)

    # try:
    #     visitor = DcToSqlVisitor(table_name)
    #     return visitor.parse(dc_string)
    # except ParseError as e:
    #     raise ValueError(f"Erro de sintaxe na Denial Constraint: {e}")

def translate_dc_to_sql_parsimonious(dc_string: str, table_name: str) -> str:
    """
    Analisa uma string de Denial Constraint usando Parsimonious e a traduz para SQL.
    """

    dc_grammar_parsimonious = r"""
        start                 = "¬(" predicate_conjunction ")"
        predicate_conjunction = predicate (ws "∧" ws predicate)*
        predicate             = tuple_variable dot column ws operator ws tuple_variable dot column
        tuple_variable        = "t'" / "t"
        column                = ~r"[a-zA-Z_]\w*"
        operator              = "=" / "!=" / "<=" / ">=" / "<" / ">"
        dot                   = "."
        ws                    = ~r"\s*"
    """

    class DcToSqlVisitor(NodeVisitor):
        def __init__(self, table_name):
            self.table_name = table_name
            self.grammar = Grammar(dc_grammar_parsimonious)

        def visit_tuple_variable(self, n, vc):
            return "t1" if n.text == "t" else "t2"

        def visit_column(self, n, vc): return n.text
        def visit_operator(self, n, vc): return n.text

        def visit_predicate(self, n, vc):
            # vc = [t1, '.', col1, op, t2, '.', col2]
            # return f"{vc[0]}.{vc[2]} {vc[3]} {vc[4]}.{vc[6]}"
            return f"{vc[0]}.{vc[2]} {vc[4]} {vc[6]}.{vc[8]}"

        def visit_predicate_conjunction(self, n, vc):
            first_pred, others = vc[0], vc[1]
            preds = [first_pred] + [p[3] for p in others]  # skip ∧ and ws
            return " AND ".join(preds)

        def visit_start(self, n, vc):
            where_clause = vc[1]
            return (
                f"SELECT t1.*, t2.* "
                f"FROM read_csv_auto('{self.table_name}') t1, "
                f"     read_csv_auto('{self.table_name}') t2 "
                f"WHERE {where_clause}"
            )

        def generic_visit(self, n, vc): return vc or n.text

        def parse(self, text):
            tree = self.grammar.parse(text)
            return self.visit(tree)

    try:
        visitor = DcToSqlVisitor(table_name)
        return visitor.parse(dc_string)
    except ParseError as e:
        raise ValueError(f"Erro de sintaxe na Denial Constraint: {e}")

# --- Demonstração de Ponta a Ponta ---

# # 1. Preparação dos Dados (mesmo de antes)
# data = {
#     'EmpID': ['E1', 'E2', 'E3', 'E1'],
#     'ProjID': ['P1', 'P1', 'P1', 'P2'],
#     'Role': ['Manager', 'Developer', 'Developer', 'Manager'],
#     'Hours': [4, 2, 4, 4],
#     'Bonus': [100, 200, 150, 120]
# }

# df_hours = pd.DataFrame(data)
# csv_filename = 'hours.csv'
# df_hours.to_csv(csv_filename, index=False)
# print(f"Arquivo '{csv_filename}' criado.")
# print(df_hours)
# print("-" * 40)

# # 2. Tradução de DC para SQL com Parsimonious
# dc_string = "¬(t.Role = t'.Role ∧ t.Hours > t'.Hours ∧ t.Bonus < t'.Bonus)"
# # table_name_for_query = f"'{csv_filename}'"

# try:
#     sql_query = translate_dc_to_sql_parsimonious(dc_string, csv_filename)
#     print("Usando o parser Parsimonious:")
#     print("Denial Constraint de Entrada:")
#     print(dc_string)
#     print("\nConsulta SQL Gerada:")
#     print(sql_query)
#     print("-" * 40)

#     # 3. Execução com DuckDB
#     print("Executando a consulta com DuckDB...")
#     results_df = duckdb.sql(sql_query).df()
    
#     # 4. Análise dos Resultados
#     print("\nViolações encontradas:")
#     if results_df.empty:
#         print("Nenhuma violação encontrada.")
#     else:
#         print(results_df)

# except (ValueError, duckdb.Error) as e:
#     print(f"Ocorreu um erro: {e}")

# finally:
#     # Limpa o arquivo CSV criado
#     if os.path.exists(csv_filename):
#         os.remove(csv_filename)
#         print(f"\nArquivo '{csv_filename}' removido.")