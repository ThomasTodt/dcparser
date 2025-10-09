import json
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
import duckdb
import argparse
import time
import os
from multiprocessing import Pool, Process, Manager
import psutil
from threading import Thread, Event


######################################################
# obtenção de métricas
######################################################

class ResourceMonitor:
    """Monitora o uso de CPU e memória de um processo em um thread separado."""
    def __init__(self, process_pid, interval=0.01):
        self._process = psutil.Process(process_pid)
        self._interval = interval
        self._stop_event = Event()
        self._thread = Thread(target=self._monitor, daemon=True)
        self.peak_memory_mb = 0
        self.cpu_percents = []
        self._process.cpu_percent(interval=None) # chama cpu_percent uma vez para inicializá-lo e descartar o resultado.


    def _monitor(self):
        self.peak_memory_mb = self._process.memory_info().rss / (1024 ** 2)

        while not self._stop_event.is_set():
            try:
                mem_info = self._process.memory_info().rss / (1024 ** 2)

                if mem_info > self.peak_memory_mb:
                    self.peak_memory_mb = mem_info
                
                self.cpu_percents.append(self._process.cpu_percent(interval=self._interval))

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join(timeout=0.2)
        total_cpu_usage = sum(self.cpu_percents) / len(self.cpu_percents) if self.cpu_percents else 0
        return total_cpu_usage, self.peak_memory_mb

###########################################################
# tradução de results.txt para sql
###########################################################

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

###########################################################
# funções para execução de queries
###########################################################

def run_query_in_thread(main_connection, dc_json, csv_file, thread_n, results_list):
    """
    Executa uma única query de DC em uma thread
    """
    cursor = main_connection.cursor()
    
    sql_query = dc_to_sql(dc_json, csv_file)
    if sql_query:
        
        count_query = f"SELECT COUNT(*) FROM ({sql_query.replace(';', '')}) as violations_subquery;"
        num_violations = cursor.execute(count_query).fetchone()[0]

        # violations = cursor.execute(sql_query).df()
        # num_violations = len(violations)
        
        results_list.append((thread_n, num_violations))
        print(f"DC #{thread_n+1}: {num_violations} violacoes")

def run_sequential(thread_count, dc_json, csv_file, results_list):
    # pid = os.getpid()
    
    con = duckdb.connect(config={'threads': thread_count})
    # monitor = ResourceMonitor(pid)
    
    # monitor.start()
    # start_time = time.perf_counter()
    
    for i, dc_json in enumerate(dc_json):
        sql_query = dc_to_sql(dc_json, csv_file)

        if sql_query:
            # Executa a query e obtém o número de violações

            count_query = f"SELECT COUNT(*) FROM ({sql_query.replace(';', '')}) as violations_subquery;"
            num_violations = con.execute(count_query).fetchone()[0]

            results_list.append((i, num_violations))

            # num_violations = len(con.execute(sql_query).df())
            # Imprime a contagem imediatamente
            print(f"DC #{i+1}: Found {num_violations} violations.")
            
    # end_time = time.perf_counter()
    # total_cpu, peak_mem = monitor.stop()
    con.close()
    
    # result = {
    #     "threads": thread_count,
    #     "time_s": end_time - start_time,
    #     "peak_mem_mb": peak_mem,
    #     "total_cpu_pct": total_cpu
    # }

    # return results

###################################################
# Main
###################################################

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run Denial Constraints on a CSV using DuckDB")
    parser.add_argument("--csv-file", type=str, default="flights.csv", help="Caminho para o CSV com os dados")
    parser.add_argument("--results-file", type=str, default="results.txt", help="Caminho para o JSON com DCs")
    parser.add_argument("--parallel", action="store_true", help="Executa as queries em paralelo")
    args = parser.parse_args()

    # le o json de cada dc
    with open(args.results_file, 'r', encoding='utf-8') as f:
        json_objects = [line.strip() for line in f if line.strip()]

    process = psutil.Process(os.getpid())
    # num_logical_cores = os.cpu_count()
    results = [] # Lista para coletar os resultados dos threads

    if args.parallel:
        print("Executando em paralelo")

        pid = os.getpid()
        monitor = ResourceMonitor(pid)

        main_con = duckdb.connect()
        # main_con = duckdb.connect(config={'memory_limit': '3GB'})
        # main_con = duckdb.connect(config={'threads': 4})
        
        threads = []
        # results = [] # Lista para coletar os resultados dos threads
        
        monitor.start()
        start_time = time.perf_counter()

        # uma thread para cada query de DC
        for i, dc_json in enumerate(json_objects):
            thread = Thread(target=run_query_in_thread, 
                            args=(main_con, dc_json, args.csv_file, i, results))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        end_time = time.perf_counter()
        total_cpu, peak_mem = monitor.stop()

        main_con.close()


        # # Processa os resultados
        # print("\n--- Resultados (Threading) ---")
        # for i, num_violations in sorted(results):
        #      if num_violations != -1:
        #         print(f"DC #{i+1}: {num_violations} violações")
        #      else:
        #         print(f"DC #{i+1}: erro")
        
        # print("\n--- Resultados do Benchmark (Threading) ---")
        # print(f"Tempo total: {end_time - start_time:.4f} segundos")
        # print(f"Pico de Memória (MB): {peak_mem:.2f}")
        # print(f"Uso Médio de CPU (%): {total_cpu:.2f}")
        # print("-" * 40)


    else:
        print("Executando em modo sequencial")
        
        thread_count = 4 # os.cpu_count()
        # results = []

        pid = os.getpid()
        monitor = ResourceMonitor(pid)

        monitor.start()
        start_time = time.perf_counter()

        # A função agora imprime as contagens internamente
        run_sequential(thread_count, json_objects, args.csv_file, results)
        # results.append(result)

        end_time = time.perf_counter()
        total_cpu, peak_mem = monitor.stop()

        # # A impressão final do benchmark funciona para ambos os modos
        # print("\n--- Resultados Finais do Benchmark ---")
        # print(f"{'Configuração Threads':<20} | {'Tempo (s)':<15} | {'Pico Memória (MB)':<20} | {'Uso Total CPU (%)':<20}")
        # print("-" * 85)
        # for res in results:
        #     print(f"{res['threads']:<20} | {res['time_s']:<15.4f} | {res['peak_mem_mb']:<20.2f} | {res['total_cpu_pct']:<20.2f}")

    # Processa os resultados
    # print("\n--- Resultados (Threading) ---")
    # for i, num_violations in sorted(results):
    #     if num_violations != -1:
    #         print(f"DC #{i+1}: {num_violations} violações")
    #     else:
    #         print(f"DC #{i+1}: erro")

    print("\n--- Resultados do Benchmark (Threading) ---")
    print(f"Tempo total: {end_time - start_time:.4f} segundos")
    print(f"Pico de Memória (MB): {peak_mem:.2f}")
    print(f"Uso Médio de CPU (%): {total_cpu:.2f}")
    print("-" * 40)