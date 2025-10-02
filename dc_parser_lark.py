from lark import Lark, Transformer, ParseError, v_args
import json

# ==============================================================================
# SEÇÃO 1: Tradutor original para o formato "¬(t.col ...)"
# (Código omitido por brevidade, pois não foi alterado)
# ==============================================================================
def translate_dc_to_sql_lark(dc_string: str, table_name: str) -> str:
    """
    Analisa uma string de Denial Constraint no formato de predicados lógicos
    e a traduz para uma consulta SQL.
    """
    print("Iniciando análise da Denial Constraint com lark (formato original)...")
    dc_grammar = r"""
        start: "¬(" predicate_conjunction ")"

        predicate_conjunction: predicate ("∧" predicate)*

        predicate: tuple_variable "." column operator tuple_variable "." column

        tuple_variable: TUPLEVAR
        TUPLEVAR: "t" | "t'"

        column: CNAME

        operator: OP
        OP: "=" | "!=" | "<" | "<=" | ">" | ">="

        %import common.CNAME
        %import common.WS
        %ignore WS
    """

    @v_args(inline=True)
    class DcToSqlTransformer(Transformer):
        def __init__(self, table_name):
            super().__init__()
            self.table_name = table_name

        def column(self, token):
            return token.value

        def operator(self, token):
            return token.value

        def tuple_variable(self, token):
            # Mapeia as variáveis de tupla para aliases SQL t1 e t2
            return "t1" if token.value == "t" else "t2"

        def predicate(self, t_var1, col1, op, t_var2, col2):
            return f"{t_var1}.{col1} {op} {t_var2}.{col2}"

        def predicate_conjunction(self, *preds):
            return " AND ".join(preds)

        def start(self, conjunction):
            return (
                f"SELECT t1.*, t2.* "
                f"FROM {self.table_name} t1, {self.table_name} t2 "
                f"WHERE {conjunction};"
            )

    try:
        dc_parser = Lark(dc_grammar, start='start')
        parse_tree = dc_parser.parse(dc_string)
        transformer = DcToSqlTransformer(table_name)
        sql_query = transformer.transform(parse_tree)
        return sql_query
    except ParseError as e:
        raise ValueError(f"Erro de sintaxe na Denial Constraint: {e}")


# ==============================================================================
# SEÇÃO 2: Novo tradutor para o formato JSON (CORRIGIDO NOVAMENTE)
# ==============================================================================

def translate_json_dc_to_sql_lark(dc_json_string: str, table_name: str) -> str:
    """
    Analisa uma string de Denial Constraint em formato JSON usando Lark.
    """
    print("\nIniciando análise da Denial Constraint com lark (formato JSON)...")

    # Garanta que sua variável de gramática seja EXATAMENTE esta:
    json_dc_grammar = r"""
        ?start: dc_object

        dc_object: "{" "\"type\"" ":" "\"DenialConstraint\"" "," "\"predicates\"" ":" predicate_array "}"

        predicate_array: "[" [predicate ("," predicate)*] "]"

        predicate: "{" "\"type\"" ":" ESCAPED_STRING ","
                         "\" column1\" " ":" column_object ","
                         "\"index1\"" ":" SIGNED_INT ","
                         "\"op\"" ":" ESCAPED_STRING ","
                         "\"column2\"" ":" column_object ","
                         "\"index2\"" ":" SIGNED_INT
                   "}"

        column_object: "{" "\"tableIdentifier\"" ":" ESCAPED_STRING ","
                             "\"columnIdentifier\"" ":" ESCAPED_STRING
                       "}"

        %import common.ESCAPED_STRING
        %import common.SIGNED_INT
        %import common.WS
        %ignore WS
    """

    @v_args(inline=True)
    class JsonDcToSqlTransformer(Transformer):
        def __init__(self, table_name):
            super().__init__()
            self.table_name = table_name
            self.op_map = { "EQUAL": "=", "UNEQUAL": "!=", "LESS": "<", "LESS_EQUAL": "<=", "GREATER": ">", "GREATER_EQUAL": ">=" }
        def ESCAPED_STRING(self, s): return json.loads(s)
        def SIGNED_INT(self, n): return int(n)
        def column_object(self, table_identifier, column_identifier): return column_identifier
        def predicate(self, type_str, col1, idx1, op_str, col2, idx2):
            sql_op = self.op_map.get(op_str, "???")
            t_var1 = "t1" if idx1 == 0 else "t2"
            t_var2 = "t1" if idx2 == 0 else "t2"
            return f"{t_var1}.{col1} {sql_op} {t_var2}.{col2}"
        def predicate_array(self, *preds): return " AND ".join(preds)
        def dc_object(self, conjunction):
            if not conjunction:
                 return f"SELECT 1 FROM {self.table_name} WHERE 1=0;"
            return (f"SELECT t1.*, t2.* FROM {self.table_name} t1, {self.table_name} t2 WHERE {conjunction};")

    try:
        dc_parser = Lark(json_dc_grammar, start='start')
        parse_tree = dc_parser.parse(dc_json_string)
        transformer = JsonDcToSqlTransformer(table_name)
        sql_query = transformer.transform(parse_tree)
        return sql_query
    except ParseError as e:
        raise ValueError(f"Erro de sintaxe na Denial Constraint JSON: {e}")
    except Exception as e:
        raise ValueError(f"Erro ao transformar a Denial Constraint JSON: {e}")


# ==============================================================================
# SEÇÃO 3: Exemplos de uso
# ==============================================================================
if __name__ == '__main__':
    dc_json_stream = """{"type":"DenialConstraint","predicates":[{"type":"de.metanome.algorithm_integration.PredicateVariable","column1":{"tableIdentifier":"flights.csv","columnIdentifier":"passengers"},"index1":0,"op":"LESS","column2":{"tableIdentifier":"flights.csv","columnIdentifier":"passengers"},"index2":1},{"type":"de.metanome.algorithm_integration.PredicateVariable","column1":{"tableIdentifier":"flights.csv","columnIdentifier":"year"},"index1":0,"op":"LESS","column2":{"tableIdentifier":"flights.csv","columnIdentifier":"year"},"index2":1},{"type":"de.metanome.algorithm_integration.PredicateVariable","column1":{"tableIdentifier":"flights.csv","columnIdentifier":"month"},"index1":0,"op":"UNEQUAL","column2":{"tableIdentifier":"flights.csv","columnIdentifier":"month"},"index2":1}]}{"type":"DenialConstraint","predicates":[{"type":"de.metanome.algorithm_integration.PredicateVariable","column1":{"tableIdentifier":"flights.csv","columnIdentifier":"passengers"},"index1":0,"op":"EQUAL","column2":{"tableIdentifier":"flights.csv","columnIdentifier":"passengers"},"index2":1}]}{"type":"DenialConstraint","predicates":[{"type":"de.metanome.algorithm_integration.PredicateVariable","column1":{"tableIdentifier":"flights.csv","columnIdentifier":"month"},"index1":0,"op":"EQUAL","column2":{"tableIdentifier":"flights.csv","columnIdentifier":"month"},"index2":1},{"type":"de.metanome.algorithm_integration.PredicateVariable","column1":{"tableIdentifier":"flights.csv","columnIdentifier":"year"},"index1":0,"op":"EQUAL","column2":{"tableIdentifier":"flights.csv","columnIdentifier":"year"},"index2":1}]}"""

    json_objects = dc_json_stream.replace('}{', '}\n{').splitlines()

    table_json = "unknown_table"
    try:
        first_dc = json.loads(json_objects[0])
        table_identifier = first_dc['predicates'][0]['column1']['tableIdentifier']
        table_json = table_identifier.split('.')[0]
    except (json.JSONDecodeError, IndexError, KeyError) as e:
        print(f"Não foi possível determinar o nome da tabela automaticamente: {e}")


    for i, dc_json in enumerate(json_objects):
        try:
            print(f"--- Processando DC JSON #{i+1} para a tabela '{table_json}' ---")
            sql_json = translate_json_dc_to_sql_lark(dc_json, table_json)
            print("Denial Constraint de Entrada (JSON):")
            print(dc_json)
            print("\nConsulta SQL Gerada:")
            print(sql_json)
            print("-"*(42 + len(str(i+1)) + len(table_json)))
        except ValueError as e:
            print(e)