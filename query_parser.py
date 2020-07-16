import pickle
import query_objects as qo
import re

def parse_query(query):
    if query[-1] == ';':
       query = query[:-1]

    q = qo.QueryObject(query)
    
    q.add_select_statement(query.split('FROM')[0].split('SELECT')[1])

    # Parse (used) table information
    #
    for table_ident in query.split('WHERE')[0].split('FROM')[1].split(','):
        if ' AS ' in table_ident:
            table_name, alias = table_ident.split('AS')
            table_name = table_name.strip()
            alias = alias.strip()
            q.add_table(table_name, alias)
        else:
            q.add_table(table_ident)
    
    # Parse for JOIN and filter conditions
    # 
    pattern = re.compile('.+\..+\s\=\s.+\..+')

    filters = []
    joins = []
    
    # Decide between JOIN and filter condition
    # 
    for entry in query.split('WHERE')[1].split('AND'):
        if pattern.match(entry):
            joins.append(entry)
        else:
            filters.append(entry)

    # Add filter conditions to QueryObject
    # 
    q.add_filter_statement('AND'.join(filters))

    # Convert JOIN conditions to JoinObjects
    # 
    for join_ident in joins:
        left, right = join_ident.split(' = ')
        left = left.strip()
        right = right.strip()

        j = qo.JoinObject(left, right)
        q.add_join(j)

    return q

def main():
    with open('qp.pkl', 'rb') as f:
        data = pickle.load(f)

    all_queries = dict()

    # This loop iterates over queries and 
    #
    for key in data.keys():
        query = data[key]['query'].strip()
        all_queries[key] = parse_query(query)

        x = all_queries[key].generate_sql()

    # import code; code.interact(local=dict(globals(), **locals()))

if __name__ == "__main__":
    main()