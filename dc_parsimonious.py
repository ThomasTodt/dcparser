import json
from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor

def dc_to_sql(dc_json_string: str, table_name: str) -> str:

    dc_grammar = r"""
        ws = ~r"\s*"

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
        grammar = Grammar(json_dc_grammar)
        parse_tree = grammar.parse(dc_json_string)
        visitor = DcToSqlVisitor(table_name)
        sql_query = visitor.visit(parse_tree)

        return sql_query

    except ValueError as e:
        print(e)

#############################################

if __name__ == '__main__':
    filename = "results.txt"
    json_objects = []

    # pega o nome da tabela do primeiro DC
    dc0 = json.loads(json_objects[0])
    table_identifier = dc0['predicates'][0]['column1']['tableIdentifier']
    table_json = table_identifier.split('.')[0]

    # itera sobre a lista de JSONs lida do arquivo
    for i, dc_json in enumerate(json_objects):
        try:
            print(f"DC #{i+1} para a tabela '{table_json}'")
             
            sql_dc = dc_to_sql(dc_json, table_json)

            print("Denial Constraint de Entrada (JSON):")
            print(dc_json)

            print("\nConsulta SQL Gerada:")
            print(sql_dc)

            print("-----------------------------------")

        except ValueError as e:
            print(e)
