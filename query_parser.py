import pickle
import query_objects as qo
import re
import pandas as pd
import numpy as np
import datetime
import time
from database_connection import postgres_connection
import psycopg2
import random

OPTIMIZE_QUERIES = True
MAX_SQL_QUERY_GENERATION_FAIL_ATTEMPTS = 10000

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

# TODO
def parse_query_plan(query_plan):
    pass

def _force_join_order(cursor):
    cursor.execute("SET LOCAL join_collapse_limit = 1")

# The default value for the join_collapse_limit according to the
# documentation is 8:
# https://www.postgresql.org/docs/8.2/runtime-config-query.html#GUC-JOIN-COLLAPSE-LIMIT
#
def _reset_join_order(cursor):
    cursor.execute("SET LOCAL join_collapse_limit = 8")

# Set up the optimizer according to the OPTIMIZE_QUERIES
# constant to either force the join order or let the optimizer
# work out an optimal ordering
#
def config_optimizer(cursor):
    if OPTIMIZE_QUERIES:
        _reset_join_order(cursor)
    else:
        _force_join_order(cursor)

# Writes the query times and some meta data to the :execution_results table
# in Postgres and logs the data as a print statement
#
def write_to_db(cursor, conn, tmp, counter=0):
    job_id = tmp['job_id']
    order_ident = tmp['order_ident']
    execution_time = tmp['execution_time']
    # original_query = tmp['original_query']
    # executed_query = tmp['executed_query']
    executed_at = str(datetime.datetime.now())

    cursor.execute("INSERT INTO execution_results(job_id, order_ident, execution_time, executed_at, optimized) VALUES ({}, '{}', {}, '{}', '{}')".format(
        job_id,
        order_ident,
        execution_time,
        executed_at,
        OPTIMIZE_QUERIES
        ))

    conn.commit()

    print("[{}] JOB-Query #{}-{}\texecuted in {} (Order {})".format(executed_at, job_id, counter, round(execution_time, 2), order_ident))

# Generates a connection to the Postgres database and returns
# the necessary Connection and Cursor objects
#
def pg_connect():
    conn = psycopg2.connect(**postgres_connection())
    cursor = conn.cursor()

    return conn, cursor

# Add each key as many times as there are joins to be executed,
# to skew the number of executions per query towards queries with
# a larger number of different JOINs
#
def generate_query_keys_to_process(data):
    keys_proportional = []

    for key in data.keys():
        # Queries with few JOINs are almost trivial for our purposes
        # and have a limited number of permutations. It is sufficient
        # to have fewer number of executions for those "easier" queries
        #
        log2_num_joins = np.log2(data[key]['num_joins'])

        for i in range(int(round(log2_num_joins,0))):
            keys_proportional.append(key)

    random.shuffle(keys_proportional)
    return keys_proportional

def main():
    # Loads the queries and optimized execution plans (according to the official
    # JOB queries without order/explicit/implicit changes to the queries)
    #
    with open('job_query_execution_plans.pkl', 'rb') as f:
        data = pickle.load(f)

    # Loads the queries from the official JOB list, including the ID (which is missing
    # in the query_plan Pickle file)
    #
    queries = pd.read_csv('./job_queries.txt', header = None, sep='|')

    # This loop iterates over queries and adds useful metadata, as
    # well as parses the query and generates QueryObjects 
    #
    for key in data.keys():
        # Parse the JOB query into a <QueryObject>
        data[key]['parsed_query'] = parse_query(data[key]['query'].strip())
        
        # Evaluate the JOB Query ID for each query by comparing it to the JOB query list
        data[key]['query_job_id'] = queries.loc[queries[1] == data[key]['query']][0].to_numpy()[0]
        
        # Evaluate how many JOINs there are, that could be managed by rearranging the JOIN orders
        data[key]['num_joins'] = len(data[key]['parsed_query'].joins)
    
    query_keys_to_process = generate_query_keys_to_process(data)
    counter = 0

    # TODO: Code Block attempt to get back the specific SQL query
    # from an executed JOIN order
    # ====================================
    #

    # import code; code.interact(local=dict(globals(), **locals()))
    # order = 'L_26-2-25-15-8-6-19-27-23-21-11-16-5-3-17-0-4-24-13-9-10-12-7-14-20-1-22-18'
    # key = 111 - 1
    # # import code; code.interact(local=dict(globals(), **locals()))
    # perm = data[key]['parsed_query'].generate_sql_from_ident(order)
    # # data[key]['parsed_query'].generate_sql(perm)
    # exit()

    while True:
        counter += 1

        # Connect to Database and set up the optimizer
        #
        conn, cursor = pg_connect()
        config_optimizer(cursor)

        # Iterate over each Query and run one JOIN order
        #
        for key in query_keys_to_process:
            fail_counter = 0
            tmp = dict()

            tmp['job_id'] = data[key]['query_job_id']
            tmp['original_query'] = data[key]['query']

            # Evaluate, which JOIN orders have been executed before
            #
            cursor.execute("SELECT DISTINCT order_ident FROM execution_results WHERE optimized = {} AND job_id = {}".format(OPTIMIZE_QUERIES, tmp['job_id']))
            executed_permutations = list(map(lambda x: x[0], cursor.fetchall()))

            # Outer While Loop: Make sure that the permutation actually generates a 
            # sensible SQL query and try again the inner loop if that was not the case
            #
            while True:
                # Inner While Loop: Test out different permutations for ones that have
                # not been executed yet. Break out of this loop if one has been found
                #
                while True:
                    order_ident, perm = data[key]['parsed_query'].generate_permutation()

                    # Execute JOIN orders that have not yet been executed
                    if order_ident not in executed_permutations:
                        break

                    # Exit if too many attempts have been made
                    if fail_counter > MAX_SQL_QUERY_GENERATION_FAIL_ATTEMPTS:
                        break

                # Exit if too many attempts have been made
                if fail_counter > MAX_SQL_QUERY_GENERATION_FAIL_ATTEMPTS:
                    break

                try:
                    tmp['order_ident'] = order_ident
                    tmp['executed_query'] = data[key]['parsed_query'].generate_sql(perm)
                except ValueError:
                    fail_counter += 1
                    pass
                else:
                    break

            # Exit if too many attempts have been made
            if fail_counter > MAX_SQL_QUERY_GENERATION_FAIL_ATTEMPTS:
                print("\tCould not find a valid SQL query for Query #{} in {} attempts.".format(tmp['job_id'], MAX_SQL_QUERY_GENERATION_FAIL_ATTEMPTS))
                continue

            # Measure the runtime of a SQL query
            #
            start_time = time.time()

            cursor.execute(tmp['executed_query'])
            result = cursor.fetchall()

            time_taken = time.time() - start_time
            tmp['execution_time'] = time_taken

            # Write results to the database
            #
            write_to_db(cursor, conn, tmp, counter=counter)

        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
