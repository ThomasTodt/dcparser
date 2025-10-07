import json
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
import duckdb
# import pandas as pd
import argparse
import time
import os
from multiprocessing import Pool, cpu_count
import psutil


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
            # self.table_name = table_name
            self.table_name_or_path = f"read_csv_auto('{table_name}')"
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
                return f"SELECT 1 FROM {self.table_name_or_path} WHERE 1=0;"

            return (
                f"SELECT t1.*, t2.* "
                f"FROM {self.table_name_or_path} t1, {self.table_name_or_path} t2 "
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

def run_dc_query(args):
    dc_json, csv_path, i = args

    con = duckdb.connect()
    sql_query = dc_to_sql(dc_json, csv_path)

    if sql_query:
        violations = con.execute(sql_query).df()
        num_violations = len(violations)
        con.close()
        return i, num_violations
    
    return i, -1

#############################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run Denial Constraints on a CSV using DuckDB")
    parser.add_argument(
        "--csv-file",
        type=str,
        default="flights.csv",
        help="caminho para o csv com os dados"
    )
    parser.add_argument(
        "--results-file",
        type=str,
        default="results.txt",
        help="caminho para o json com DCs"
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="executa as queries em paralelo"
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

    process = psutil.Process(os.getpid())


    if args.parallel:
        # Paralelo entre queries
        print("paralela")

        mem_before = process.memory_info().rss / (1024 * 1024)
        start_time = time.time()
        
        tasks = [(dc_json, args.csv_file, i) for i, dc_json in enumerate(json_objects)]
        num_processes = os.cpu_count()
        print(f"{num_processes} processos\n")
        
        with Pool(processes=num_processes) as pool:
            results = pool.map(run_dc_query, tasks)
            
        for i, num_violations in sorted(results):
            if num_violations != -1:
                print(f"DC #{i+1}: {num_violations} violações")
            else:
                print(f"DC #{i+1}: erro")

        end_time = time.time()
        mem_after = process.memory_info().rss / (1024 * 1024)

        print(f"\nMemória: Início={mem_before:.2f} MB, Fim={mem_after:.2f} MB")
        print(f"✅ Tempo total (Paralelo): {end_time - start_time:.4f} segundos")


    else:
        print("sequencial")

        config = {'threads': '4'}
        con = duckdb.connect(database=':memory:', config=config)
        con = duckdb.connect()

        # num_threads = 1 # os.cpu_count()
        # con.execute(f"PRAGMA threads={num_threads}")
        # print(f"DuckDB configurado para usar {con.execute('PRAGMA threads').fetchone()[0]} threads.")

        mem_before = process.memory_info().rss / (1024 * 1024)
        start_time = time.time()
        
        for i, dc_json in enumerate(json_objects):
            sql_query = dc_to_sql(dc_json, args.csv_file)

            if sql_query:
                violations = con.execute(sql_query).df()
                print(f"DC #{i+1}: {len(violations)} violações")
                
                
        end_time = time.time()
        mem_after = process.memory_info().rss / (1024 * 1024)
        con.close()
        
        print(f"\nMemória: Início={mem_before:.2f} MB, Fim={mem_after:.2f} MB")
        print(f"✅ Tempo total (Sequencial): {end_time - start_time:.4f} segundos")








    # # itera sobre a lista de JSONs lida do arquivo
    # for i, dc_json in enumerate(json_objects):
    #     print(f"\nDC #{i+1} para a tabela '{table_name}'")
    #     print("Denial Constraint de Entrada (JSON):")
    #     print(dc_json)

    #     sql_query = dc_to_sql(dc_json, table_name)
    #     print("\nConsulta SQL Gerada:")
    #     print(sql_query)

    #     # Execute on DuckDB
    #     violations = con.execute(sql_query).df()
    #     print("\nResultados:")
    #     print(violations if not violations.empty else "Nenhuma violação encontrada.")
    #     print("-----------------------------------")
