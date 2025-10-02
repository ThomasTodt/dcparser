import json
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor

def translate_json_dc_to_sql_parsimonious(dc_json_string: str, table_name: str) -> str:
    """
    Analisa uma string de Denial Constraint em formato JSON (padrão Metanome)
    usando uma gramática Parsimonious e a traduz para uma consulta SQL.
    """
    print("\nIniciando análise da Denial Constraint com Parsimonious...")

    json_dc_grammar = r"""
        start = ws dc_object ws

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

    class JsonDcToSqlVisitor(NodeVisitor):
        def __init__(self, table_name):
            super().__init__()
            self.table_name = table_name
            self.op_map = {
                "EQUAL": "=", "UNEQUAL": "!=", "LESS": "<", "LESS_EQUAL": "<=",
                "GREATER": ">", "GREATER_EQUAL": ">="
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
            col1 = visited_children[14]; idx1 = visited_children[22]
            op_str = visited_children[30]; col2 = visited_children[38]
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
            # CORREÇÃO FINAL: O índice correto para predicate_array é 14.
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
        grammar = Grammar(json_dc_grammar)
        parse_tree = grammar.parse(dc_json_string)
        visitor = JsonDcToSqlVisitor(table_name)
        sql_query = visitor.visit(parse_tree)
        return sql_query
    except Exception as e:
        raise ValueError(f"Erro ao processar a DC com Parsimonious ({type(e).__name__}): {e}")

# ==============================================================================
# SEÇÃO 2: Exemplos de uso (MODIFICADO PARA LER DO ARQUIVO)
# ==============================================================================
if __name__ == '__main__':
    filename = "results.txt"
    json_objects = []

    # Bloco try/except para lidar com o caso de o arquivo não existir
    try:
        # Abre o arquivo para leitura ('r') com codificação utf-8
        # O 'with' garante que o arquivo seja fechado automaticamente
        with open(filename, 'r', encoding='utf-8') as f:
            # Lê cada linha do arquivo, remove espaços/quebras de linha extras
            # e ignora linhas em branco.
            json_objects = [line.strip() for line in f if line.strip()]
        
        if not json_objects:
            print(f"O arquivo '{filename}' está vazio ou não contém dados válidos.")
            exit()

    except FileNotFoundError:
        print(f"Erro: O arquivo '{filename}' não foi encontrado no diretório.")
        exit() # Encerra o script se o arquivo não existir

    # O resto da lógica permanece similar
    table_json = "unknown_table"
    try:
        # Pega o primeiro JSON da lista para extrair o nome da tabela
        first_dc = json.loads(json_objects[0])
        table_identifier = first_dc['predicates'][0]['column1']['tableIdentifier']
        table_json = table_identifier.split('.')[0]
    except (json.JSONDecodeError, IndexError, KeyError) as e:
        print(f"Não foi possível determinar o nome da tabela automaticamente: {e}")

    # Itera sobre a lista de JSONs lida do arquivo
    for i, dc_json in enumerate(json_objects):
        try:
            print(f"--- Processando DC JSON #{i+1} para a tabela '{table_json}' ---")
            sql_json = translate_json_dc_to_sql_parsimonious(dc_json, table_json)
            print("Denial Constraint de Entrada (JSON):")
            print(dc_json)
            print("\nConsulta SQL Gerada:")
            print(sql_json)
            print("-"*(42 + len(str(i+1)) + len(table_json)))
        except ValueError as e:
            print(e)
