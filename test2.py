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


# =============================================================================
# A classe ResourceMonitor do nosso relatório anterior é inserida aqui.
# Ela é essencial para medições precisas de pico de memória e uso de CPU.
# =============================================================================
class ResourceMonitor:
    """Monitora o uso de CPU e memória de um processo em um thread separado."""
    def __init__(self, process_pid, interval=0.01):
        self._process = psutil.Process(process_pid)
        self._interval = interval
        self._stop_event = Event()
        self._thread = Thread(target=self._monitor, daemon=True)
        self.peak_memory_mb = 0
        self.cpu_percents = []
        # CORREÇÃO: Chame cpu_percent uma vez para inicializá-lo e descartar o resultado.
        self._process.cpu_percent(interval=None)

    def _monitor(self):
        self.peak_memory_mb = self._process.memory_info().rss / (1024 ** 2)
        while not self._stop_event.is_set():
            try:
                mem_info = self._process.memory_info().rss / (1024 ** 2)
                if mem_info > self.peak_memory_mb:
                    self.peak_memory_mb = mem_info
                
                # Agora as leituras serão corretas
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


# =============================================================================
# Suas funções originais (sem modificações necessárias)
# =============================================================================
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

def run_dc_query_for_parallel(args):
    """Função alvo para o modo de paralelismo ENTRE-queries (multiprocessing)."""
    dc_json, csv_path, i = args
    # Cada processo cria sua própria conexão
    con = duckdb.connect()
    sql_query = dc_to_sql(dc_json, csv_path)
    if sql_query:
        num_violations = con.execute(sql_query).df().shape
        con.close()
        return i, num_violations
    return i, -1




def run_single_benchmark(thread_count, json_objects, csv_file, result_dict):
    """Executa a carga de trabalho completa para uma única configuração de threads."""
    pid = os.getpid()
    print(f"Iniciando teste com {thread_count} thread(s) (PID: {pid})...")
    
    con = duckdb.connect(config={'threads': thread_count})
    monitor = ResourceMonitor(pid)
    
    monitor.start()
    start_time = time.perf_counter()
    
    for dc_json in json_objects:
        sql_query = dc_to_sql(dc_json, csv_file)
        if sql_query:
            con.execute(sql_query).fetchone()
            
    end_time = time.perf_counter()
    total_cpu, peak_mem = monitor.stop()
    
    con.close()
    
    result = {
        "threads": thread_count,
        "time_s": end_time - start_time,
        "peak_mem_mb": peak_mem,
        "total_cpu_pct": total_cpu
    }
    result_dict[thread_count] = result
    print(f"Teste com {thread_count} thread(s) concluído.")

# =============================================================================
# Novo Bloco Principal
# =============================================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run Denial Constraints on a CSV using DuckDB")
    parser.add_argument("--csv-file", type=str, default="flights.csv", help="Caminho para o CSV com os dados")
    parser.add_argument("--results-file", type=str, default="results.txt", help="Caminho para o JSON com DCs")
    parser.add_argument("--parallel", action="store_true", help="Executa as queries em paralelo (paralelismo INTER-query)")
    args = parser.parse_args()

    with open(args.results_file, 'r', encoding='utf-8') as f:
        json_objects = [line.strip() for line in f if line.strip()]

    process = psutil.Process(os.getpid())
    num_logical_cores = os.cpu_count()

    if args.parallel:
        # --- MODO 1: PARALELISMO ENTRE QUERIES (INTER-QUERY) ---
        # Usa múltiplos processos para executar múltiplas queries ao mesmo tempo.
        print("Executando em modo PARALELO (INTER-QUERY)...")
        
        mem_before = process.memory_info().rss / (1024 * 1024)
        start_time = time.perf_counter()
        
        tasks = [(dc_json, args.csv_file, i) for i, dc_json in enumerate(json_objects)]
        
        print(f"Iniciando pool com {num_logical_cores} processos para {len(tasks)} tarefas.\n")
        
        with Pool(processes=num_logical_cores) as pool:
            results = pool.map(run_dc_query_for_parallel, tasks)
            
        for i, num_violations in sorted(results):
            if num_violations!= -1:
                print(f"DC #{i+1}: {num_violations} violações")
            else:
                print(f"DC #{i+1}: erro")

        end_time = time.perf_counter()
        mem_after = process.memory_info().rss / (1024 * 1024)

        print("\n--- Resultados do Benchmark (Paralelo INTER-QUERY) ---")
        print(f"Memória (processo principal): Início={mem_before:.2f} MB, Fim={mem_after:.2f} MB")
        print(f"✅ Tempo total: {end_time - start_time:.4f} segundos")

    else:
        # --- MODO 2: SEQUENCIAL COM PARALELISMO DENTRO DA QUERY (INTRA-QUERY) ---
        # Executa uma query de cada vez, mas permite que o DuckDB use múltiplos threads para cada uma.
        print("Executando em modo SEQUENCIAL (testando paralelismo INTRA-QUERY)...")
        
        # Vamos testar uma faixa de configurações de threads para ver o impacto
        thread_counts_to_test = [8] #, num_logical_cores]
        benchmark_results = []

        with Manager() as manager:
            result_dict = manager.dict()
            processes = []
            
            for thread_count in thread_counts_to_test:
                p = Process(target=run_single_benchmark, args=(thread_count, json_objects, args.csv_file, result_dict))
                processes.append(p)
                p.start()
            
            for p in processes:
                p.join() # Espera todos os processos terminarem

            # Converte o dict gerenciado em uma lista e ordena pelos threads
            final_results = sorted(list(result_dict.values()), key=lambda x: x['threads'])

        print("\n--- Resultados Finais do Benchmark (Corrigido) ---")
        print(f"{'Threads':<10} | {'Tempo (s)':<15} | {'Pico Memória (MB)':<20} | {'Uso Total CPU (%)':<20}")
        print("-" * 75)
        for res in final_results:
            print(f"{res['threads']:<10} | {res['time_s']:<15.4f} | {res['peak_mem_mb']:<20.2f} | {res['total_cpu_pct']:<20.2f}")
