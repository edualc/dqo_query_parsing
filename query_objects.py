import warnings

class QueryObject():
    def __init__(self, raw_query):
        self.raw_query = raw_query
        
        # Contains all the where-filters as a string
        # 
        self.filter_statement = ''

        # Contains the select statements as a string
        # 
        self.select_statement = ''
        
        # Contains all the JOIN conditions as JoinObjects
        self.joins = []

        # Dictionary containing the key-value pairs for
        # "TABLE AS t", where t is the key and TABLE the value. 
        # If there are no aliases, key and value are equal
        # 
        self.tables = dict()

    def add_filter_statement(self, filter_string):
        self.filter_statement = filter_string

    def add_join(self, join):
        join.set_query(self)
        self.joins.append(join)

    def add_select_statement(self, select):
        self.select_statement = select

    def add_table(self, table, alias=None):
        if alias is None:
            alias = table
        
        self.tables[alias] = table

    def print(self):
        print('')
        print("QueryObject")
        print('-'*30)
        print("SELECT\t{}".format(self.select_statement))
        print('')
        print("FROM\t{}".format(', '.join(self.tables.values())))
        print('')
        print("WHERE\t{}".format(self.filter_statement))
        print('')
        print("JOINS: [{}]".format(len(self.joins)))
        for j in self.joins:
            print("\t{}".format(j))
        print('')
        print(self.generate_sql(self.joins))
        print('')

    def tables_to_sql(self):
        return ', '.join(map(lambda key: "{} AS {}".format(self.tables[key], key), self.tables.keys()))

    def table_to_sql(self, key):
        return "{} AS {}".format(self.tables[key], key)

    def generate_sql(self, order=None):
        if order is None:
            order = self.joins

        base_query = "SELECT {} FROM".format(self.select_statement)
        base_query += " {} AS {}".format(self.tables[order[0].left_table], order[0].left_table)
        
        # Keep track of which tables(-aliases) are known
        # to the query result
        # 
        used_tables = [order[0].left_table]
        additional_filters = []

        for join in order:
            if join.left_table in used_tables:
                if join.right_table in used_tables:
                    # both tables already known -> use this as a filter
                    # 
                    additional_filters.append(join.join_condition())

                else:
                    # left already known, use right for join
                    # 
                    used_tables.append(join.right_table)
                    base_query += " INNER JOIN {} ON {}".format(self.table_to_sql(join.right_table), join.join_condition())
            
            else:
                if join.right_table in used_tables:
                    # right already known, use left for join
                    # 
                    used_tables.append(join.left_table)
                    base_query += " INNER JOIN {} ON {}".format(self.table_to_sql(join.left_table), join.join_condition())
                else:
                    # Neither tables are known. This is not a valid JOIN order
                    # 
                    warn_string = "The provided JOIN order is not valid.\n\tCurrently available table aliases are: {}\n\tThe requested join is: {}".format(used_tables, join.join_condition())
                    warnings.warn(warn_string)

        base_query += " WHERE {}".format(self.filter_statement)

        # JOIN conditions where both tables were already joined are treated
        # as additional WHERE filters instead
        # 
        if len(additional_filters) > 0:
            base_query += " AND {}".format(' AND '.join(additional_filters))

        return base_query

class JoinObject():
    def __init__(self, one, other):
        # Save join conditions ascending alphabetically
        # 
        if one < other:
            self.left = one
            self.right = other
        else:
            self.left = other
            self.right = one

        self.left_table = self.left.split('.')[0]
        self.right_table = self.right.split('.')[0]

        self.query = None

    def __str__(self):
        return self.left + ' = ' + self.right

    def __repr__(self):
        return self.left_table + '-' + self.right_table

    def join_condition(self):
        return self.left + ' = ' + self.right

    def set_query(self, query):
        self.query = query
