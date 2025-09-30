from lark import Lark, Transformer, ParseError, v_args

def translate_dc_to_sql_lark(dc_string: str, table_name: str) -> str:
    """
    Analisa uma string de Denial Constraint e a traduz para uma consulta SQL.
    """
    print("Iniciando análise da Denial Constraint com lark")
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
            # return token.value
            return "t1" if token.value == "t" else "t2"

        def predicate(self, t_var1, col1, op, t_var2, col2):
            return f"{t_var1}.{col1} {op} {t_var2}.{col2}"

        def predicate_conjunction(self, *preds):
            return " AND ".join(preds)

        def start(self, conjunction):
            return (
                f"SELECT t1.*, t2.* "
                f"FROM {self.table_name} t1, {self.table_name} t2 "
                f"WHERE {conjunction}"
            )

    try:
        dc_parser = Lark(dc_grammar, start='start')
        parse_tree = dc_parser.parse(dc_string)
        transformer = DcToSqlTransformer(table_name)
        sql_query = transformer.transform(parse_tree)
        return sql_query
    except ParseError as e:
        raise ValueError(f"Erro de sintaxe na Denial Constraint: {e}")


# Exemplo
if __name__ == '__main__':
    dc_example = "¬(t.Role = t'.Role ∧ t.Hours > t'.Hours ∧ t.Bonus < t'.Bonus)"
    table = "hours"

    try:
        sql = translate_dc_to_sql(dc_example, table)
        print("Denial Constraint de Entrada:")
        print(dc_example)
        print("\nConsulta SQL Gerada:")
        print(sql)
    except ValueError as e:
        print(e)
