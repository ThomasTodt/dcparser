import json
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
import duckdb
import pandas as pd
import argparse

def dc_to_sql(dc_json_string: str, table_name: str) -> str:

    dc_grammar = r"""

        dc_object = "{" ws "\"type\"" ws ":" ws "\"DenialConstraint\"" ws "," ws "\"predicates\"" ws ":" ws predicate_array ws "}"
        
        predicate_array = "[" ws predicate_list? ws "]"
        
        predicate_list  = predicate (ws "," ws predicate)*
        
        predicate = "{" ws "\"type\"" ws ":" ws escaped_string ws "," ws
                        "\"column1\"" ws ":" ws column_object ws "," ws
                        "\"index1\"" ws ":" ws signed_int ws "," ws
                        "\"op\"" ws ":" ws escaped_string ws "," ws
                        "\"column2\"" ws ":" ws column_object ws "," ws
                        "\"index2\"" ws ":" ws signed_int ws "}"
        
        column_object = "{" ws "\"tableIdentifier\"" ws ":" ws escaped_string ws "," ws
                            "\"columnIdentifier\"" ws ":" ws escaped_string ws "}"
        
        escaped_string = ~r'"(?:\\.|[^"\\])*"'
        
        signed_int = ~r"-?\d+"
        
        ws = ~r"\s*"
    """

    class DcToSqlVisitor(NodeVisitor):
        def __init__(self, table_name):
            super().__init__()
            self.table_name = table_name
            self.op_map = {
                "EQUAL": "=",
                "UNEQUAL": "!=", 
                "LESS": "<", 
                "LESS_EQUAL": "<=",
                "GREATER": ">", 
                "GREATER_EQUAL": ">="
            }
        
        def generic_visit(self, node, visited_children):
            return visited_children or node

        def visit_escaped_string(self, node, visited_children):
            return json.loads(node.text)

        def visit_signed_int(self, node, visited_children):
            return int(node.text)

        def visit_column_object(self, node, visited_children):
            return visited_children[14]

        def visit_predicate(self, node, visited_children):
            col1 = visited_children[14]
            idx1 = visited_children[22]

            op_str = visited_children[30]
            
            col2 = visited_children[38]
            idx2 = visited_children[46]

            sql_op = self.op_map.get(op_str, "???")

            t_var1 = "t1" if idx1 == 0 else "t2"
            t_var2 = "t1" if idx2 == 0 else "t2"

            return f"{t_var1}.{col1} {sql_op} {t_var2}.{col2}"

        def visit_predicate_list(self, node, visited_children):
            first_pred = visited_children[0]

            other_preds_groups = visited_children[1]

            all_preds = [first_pred]

            for group in other_preds_groups:
                all_preds.append(group[3])

            return " AND ".join(all_preds)

        def visit_predicate_array(self, node, visited_children):
            predicate_list_result = visited_children[2]

            if isinstance(predicate_list_result, list) and predicate_list_result:
                return predicate_list_result[0]

            return ""

        def visit_dc_object(self, node, visited_children):
            conjunction = visited_children[14]

            if not conjunction:
                return f"SELECT 1 FROM {self.table_name} WHERE 1=0;"

            return (
                f"SELECT t1.*, t2.* "
                f"FROM {self.table_name} t1, {self.table_name} t2 "
                f"WHERE {conjunction};"
            )
        
        def visit_start(self, node, visited_children):
            return visited_children[1]

    try:
        grammar = Grammar(dc_grammar)
        parse_tree = grammar.parse(dc_json_string)
        visitor = DcToSqlVisitor(table_name)
        sql_query = visitor.visit(parse_tree)

        return sql_query

    except ValueError as e:
        print(e)

#############################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run Denial Constraints on a CSV using DuckDB")
    parser.add_argument(
        "--csv-file",
        type=str,
        default="flights.csv",
        help="Path to the CSV file "
    )
    parser.add_argument(
        "--results-file",
        type=str,
        default="results.txt",
        help="Path to the DCs JSON file"
    )

    args = parser.parse_args()
    csv_file = args.csv_file
    results_file = args.results_file


    json_objects = []
    with open(results_file, 'r', encoding='utf-8') as f:
        json_objects = [line.strip() for line in f if line.strip()]

    # pega o nome da tabela do primeiro DC
    dc0 = json.loads(json_objects[0])
    table_name = dc0['predicates'][0]['column1']['tableIdentifier'].split('.')[0]

    df = pd.read_csv(csv_file)
    con = duckdb.connect()
    con.register(table_name, df)

    # itera sobre a lista de JSONs lida do arquivo
    for i, dc_json in enumerate(json_objects):
        print(f"\nDC #{i+1} para a tabela '{table_name}'")
        print("Denial Constraint de Entrada (JSON):")
        print(dc_json)

        sql_query = dc_to_sql(dc_json, table_name)
        print("\nConsulta SQL Gerada:")
        print(sql_query)

        # Execute on DuckDB
        violations = con.execute(sql_query).df()
        print("\nResultados:")
        print(violations if not violations.empty else "Nenhuma violação encontrada.")
        print("-----------------------------------")
