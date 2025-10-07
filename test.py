import pandas as pd
import numpy as np
import os
import time
import psutil
from threading import Thread, Event
import duckdb
import pandas as pd
import numpy as np

def generate_large_csv(file_path, num_rows):
    """Gera um grande arquivo CSV para fins de benchmarking."""
    if os.path.exists(file_path):
        print(f"O arquivo {file_path} já existe. Pulando a geração.")
        return
    print(f"Gerando arquivo CSV com {num_rows} linhas em {file_path}...")
    categories = ['A', 'B', 'C', 'D', 'E']
    data = {
        'id': np.arange(num_rows),
        'value_float': np.random.rand(num_rows) * 1000,
        'value_int': np.random.randint(0, 100000, size=num_rows),
        'category': np.random.choice(categories, size=num_rows)
    }
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    print("Geração do arquivo concluída.")

# Exemplo de uso:
# NUM_ROWS_FOR_BENCHMARK = 2_000_000
# CSV_FILE_PATH = 'large_dataset.csv'
# generate_large_csv(CSV_FILE_PATH, NUM_ROWS_FOR_BENCHMARK)

class ResourceMonitor:
    """Monitora o uso de CPU e memória de um processo em um thread separado."""
    def __init__(self, process_pid, interval=0.01):
        self._process = psutil.Process(process_pid)
        self._interval = interval
        self._stop_event = Event()
        self._thread = Thread(target=self._monitor, daemon=True)
        self.peak_memory_mb = 0
        self.cpu_percents = []

    def _monitor(self):
        while not self._stop_event.is_set():
            try:
                # Memória RSS (Resident Set Size) em MB
                mem_info = self._process.memory_info().rss / (1024 ** 2)
                if mem_info > self.peak_memory_mb:
                    self.peak_memory_mb = mem_info
                
                # Uso de CPU
                self.cpu_percents.append(self._process.cpu_percent())
            except psutil.NoSuchProcess:
                break
            time.sleep(self._interval)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()
        avg_cpu = sum(self.cpu_percents) / len(self.cpu_percents) if self.cpu_percents else 0
        # O cpu_percent de psutil é relativo ao tempo desde a última chamada.
        # Para um valor total, dividimos pelo número de núcleos.
        num_cores = os.cpu_count()
        return avg_cpu / num_cores, self.peak_memory_mb
    

def run_benchmark(csv_path, query, thread_counts):
    """Executa um benchmark para uma consulta com diferentes contagens de threads."""
    results = []
    file_size_mb = os.path.getsize(csv_path) / (1024 ** 2)

    for threads in thread_counts:
        print(f"\nExecutando benchmark com {threads} thread(s)...")
        
        # Conecta ao DuckDB com a configuração de threads especificada
        con = duckdb.connect(config={'threads': threads})
        
        # Inicia o monitor de recursos
        monitor = ResourceMonitor(os.getpid())
        monitor.start()
        
        # Cronometra a execução da consulta
        start_time = time.perf_counter()
        con.execute(query.format(csv_path=csv_path)).fetchall()
        end_time = time.perf_counter()
        
        # Para o monitor e coleta os resultados
        avg_cpu, peak_mem = monitor.stop()
        
        execution_time = end_time - start_time
        throughput = file_size_mb / execution_time if execution_time > 0 else 0
        
        result = {
            'thread_count': threads,
            'execution_time_s': execution_time,
            'avg_cpu_util_pct': avg_cpu * 100, # Convertido para porcentagem
            'peak_process_mem_mb': peak_mem,
            'throughput_mb_s': throughput
        }
        results.append(result)
        
        print(f"  Tempo de execução: {result['execution_time_s']:.4f} s")
        print(f"  Pico de memória: {result['peak_process_mem_mb']:.2f} MB")
        print(f"  Uso médio de CPU: {result['avg_cpu_util_pct']:.2f}%")
        
        con.close()

    return pd.DataFrame(results)

if __name__ == '__main__':
    # --- Configuração do Experimento ---
    NUM_ROWS = 5_000_000
    CSV_FILE = 'benchmark_data.csv'
    
    # Gera o arquivo de dados se ele não existir
    generate_large_csv(CSV_FILE, NUM_ROWS)

    # Define a consulta a ser testada
    # Esta consulta realiza uma agregação que se beneficia do paralelismo
    QUERY_TO_TEST = """
    SELECT category, avg(value_float), count(*)
    FROM read_csv_auto('{csv_path}')
    GROUP BY category;
    """
    
    # Define a faixa de contagens de threads para testar
    num_logical_cores = os.cpu_count()
    THREAD_COUNTS_TO_TEST = [1, 2, 4, 8, num_logical_cores]
    # Garante que os valores sejam únicos e ordenados
    THREAD_COUNTS_TO_TEST = sorted(list(set(t for t in THREAD_COUNTS_TO_TEST if t <= num_logical_cores)))

    # --- Execução e Resultados ---
    benchmark_results = run_benchmark(CSV_FILE, QUERY_TO_TEST, THREAD_COUNTS_TO_TEST)
    
    print("\n--- Resultados Finais do Benchmark ---")
    print(benchmark_results.to_string(index=False))
    
    # Salva os resultados em um arquivo CSV para análise posterior
    benchmark_results.to_csv('benchmark_results.csv', index=False)
