import sys, os

from graphviz import Digraph
from sythesize_nodes import SynthesizedNode
from core.enum_operator  import Operator
import random
from database import Table, Field, Source
from tpc_ds_tables import store_returns, date_dim, store_sales, time_dim, item, customer, customer_demographics, \
    customer_address, household_demographics, store, promotion, reason
from synthesize_tables import JoinTable
import copy
import time

import logging
from lstm_sequences_generating import LSTMWrapper, LSTMTagger

table_index = 1
table_dict = {"store_returns": store_returns, "date_dim": date_dim}

id_to_nodes = {"Af": "FileScan parquetf", "Ad": "FileScan parquetd",
               "B": "BroadcastHashJoin",
               "C": "SortMergeJoin", "D": "Union", "E": ""}

id_to_operators = {"": "", "1": "Filter",
                   "2": "Project",
                   "3": "HashAggregate-HashAggregate",
                   "4": "Sort", "5": "TakeOrderedAndProject",
                   "6": "Expand", "7": "Window"}

db_name = "tpcds_100_parquet"


def get_with_table_index():
    global table_index
    index = table_index
    table_index = table_index + 1
    return str(index)


# return the join_nodes # key:node->id, value:node

# list, tow_dim_list
def display_adj(nodename, adj_next):
    print("The ADJ is:==========")
    print("%10s %38s \t|\t %10s %38s" % ("NodeID", "NodeName", "NextNodeID", "NextNodeName"))
    print("--------------------------------------------------------------------------------------------------")
    for i, nexti in enumerate(adj_next):
        ss = []
        print(nexti)
        for j in nexti:
            ss.append("(%s %s) " % (j, nodename[j]))
        print("%3s %45s \t|\t %s" % (i, nodename[i], ' '.join(ss)))


def length_one_branch(id_index, table: Table) -> SynthesizedNode:
    start_node = SynthesizedNode(id_index, "FileScan parquetd")
    start_node.input_table = table  # fix the input table
    start_node.input_table.with_table_name = "tb" + get_with_table_index()
    id_index += 1

    second_node = SynthesizedNode(id_index, "Filter")
    second_node.parent_node.append(start_node)
    second_node.input_table = start_node.input_table
    start_node.child_node = second_node

    third_node = SynthesizedNode(id_index, "Project")
    third_node.parent_node.append(second_node)
    third_node.input_table = second_node.input_table
    second_node.child_node = third_node
    return start_node, third_node


def generate_long_chain(given_len):
    b_pro = 10.7 / (10.7 + 3.75 + 0.4)  # BHJ
    s_pro = 3.75 / (10.7 + 3.75 + 0.4)  # SMJ
    union_pro = 0.4 / (10.7 + 3.75 + 0.4)  # Union（这部分是估算的 圆饼图里没有显示）
    given_len = given_len - 1
    long_chain = ["Af"]

    while given_len > 0:

        # print(random.random())

        r = random.random()
        if r < b_pro:
            long_chain.append("B")
        elif r < b_pro + s_pro:
            long_chain.append("C")
        else:
            long_chain.append("D")
        given_len -= 1
    long_chain.append("E")
    result = ""
    temp_len = len(long_chain)
    for i in range(0, temp_len - 1):
        result += (long_chain[i] + long_chain[i + 1]) + " "
    result = result[:-1]
    print(result)
    print(long_chain)
    lstm_wrapper = LSTMWrapper()
    lstm_prediction = lstm_wrapper.predict(result)
    full_list = []
    for i in range(0, temp_len - 1):
        start_item = id_to_nodes[long_chain[i]]
        # end_item = id_to_nodes[long_chain[i+1]]
        full_list.append(start_item)
        for item in lstm_prediction[i]:
            full_list.append(item)
    full_list.append(id_to_nodes[long_chain[temp_len - 1]])
    print("full list is {}".format(full_list))

    return full_list


def build_tree(long_chain):
    chain_len = len(long_chain)
    top_level_nodes = []
    id_index = 1
    previous_node = None
    start_node = None
    join_index = 0
    join_node_list = []
    actuaL_tables =None
    # 要在list中存很多表 第一个是根表 别的表都要跟这个表有连接。现在要存是那个field跟什么表连接(field1, field2, table2)
    # len = 9
    connected_tables_store_sales = [store_sales, date_dim, time_dim, item, customer, customer_demographics, household_demographics, customer_address, store, promotion]
    connected_tables_store_returns = [store_returns, date_dim, time_dim, item, customer, customer_demographics, household_demographics, customer_address, reason, store_sales]

    # length = 8
    #connected_tables_store_returns =[...]
    #
    # r = random()
    # if r <0.5:
    #     connected_tables_store_sales
    # else
    #     connected_tables_store_returns
    #
    actual_tables = connected_tables_store_sales[:chain_len]
    # if chain_len ==3:
    #     actual_tables = connected_tables_store_sales[:3]
    # elif chain_len ==4:
    #     actuaL_tables =connected_tables_store_sales[]

    for index, op in enumerate(long_chain):
        if op == 'FileScan parquetf':
            node = SynthesizedNode(id_index, "FileScan parquetf")
            table_new = actual_tables.pop(0)
            node.input_table = table_new  # fix the input table

            node.input_table.with_table_name = "tb" + get_with_table_index()
            id_index += 1
            previous_node = node
            start_node = node
            top_level_nodes.append(start_node)  # the node indexed at 0 is equivalent to the start_node

        elif op == 'FileScan parquetd':
            node = SynthesizedNode(id_index, "FileScan parquetd")
            node.input_table = date_dim  # fix the input table
            node.input_table.with_table_name = "tb" + get_with_table_index()
            id_index += 1
            previous_node = node
            start_node = node
            top_level_nodes.append(start_node)  # the node indexed at 0 is equivalent to the start_node

        elif op == 'Filter':
            node = SynthesizedNode(id_index, "Filter")
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node.child_node = node
            id_index += 1
            node.parent_node.append(previous_node)
            previous_node = node

        elif op == 'Project':
            node = SynthesizedNode(id_index, "Project")
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node.child_node = node
            node.parent_node.append(previous_node)
            id_index += 1
            previous_node = node

        elif op == 'HashAggregate':
            node = SynthesizedNode(id_index, "HashAggregate")
            previous_node.child_node = node
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node = node
            id_index += 1

        elif op == 'Sort':
            node = SynthesizedNode(id_index, "Sort")
            previous_node.child_node = node
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node = node
            id_index += 1

        elif op == "TakeOrderedAndProject":
            node = SynthesizedNode(id_index, "TakeOrderedAndProject")
            previous_node.child_node = node
            node.input_table = previous_node.input_table
            node.join_table = previous_node.join_table
            previous_node = node
            id_index += 1

        elif op == 'BroadcastHashJoin':
            node = SynthesizedNode(id_index, "BroadcastHashJoin")
            # date_dim_copy = copy.deepcopy(date_dim)
            # table_copy = copy.deepcopy(store_returns)
            table_copy = actual_tables.pop(0)
            start_node_branch, end_node_one_branch = length_one_branch(id_index, table_copy)
            top_level_nodes.append(start_node_branch)
            if join_index >= 1:

                node.join_table = JoinTable(t1_table=previous_node.join_table, t2_table=end_node_one_branch.input_table,
                                            with_table_name=get_with_table_index())

            else:
                node.join_table = JoinTable(t1_table=previous_node.input_table,
                                            t2_table=end_node_one_branch.input_table,
                                            with_table_name=get_with_table_index())

            node.join_table.relationships = node.join_table.t1_table.relationships
            node.join_table.fields = node.join_table.t1_table.fields
            node.join_table.join_field_str = node.join_table.t1_table.join_field_str

            node.parent_node.append(previous_node)
            node.parent_node.append(end_node_one_branch)
            previous_node.child_node = node
            end_node_one_branch.child_node = node
            previous_node = node
            # join_node = node
            join_node_list.append(node)
            join_index += 1

        elif op == 'SortMergeJoin':
            node = SynthesizedNode(id_index, "SortMergeJoin")
            # table_copy = copy.deepcopy(store_returns)
            table_copy = actual_tables.pop(0)
            start_node_branch, end_node_one_branch = length_one_branch(id_index, table_copy)
            top_level_nodes.append(start_node_branch)
            if join_index >= 1:

                node.join_table = JoinTable(t1_table=previous_node.join_table, t2_table=end_node_one_branch.input_table,
                                            with_table_name=get_with_table_index())
            else:
                node.join_table = JoinTable(t1_table=previous_node.input_table,
                                            t2_table=end_node_one_branch.input_table,
                                            with_table_name=get_with_table_index())

            node.join_table.relationships = node.join_table.t1_table.relationships
            node.join_table.fields = node.join_table.t1_table.fields
            node.join_table.join_field_str = node.join_table.t1_table.join_field_str
            node.parent_node.append(previous_node)
            node.parent_node.append(end_node_one_branch)
            previous_node.child_node = node
            end_node_one_branch.child_node = node
            previous_node = node
            # join_node = node
            join_node_list.append(node)
            join_index += 1

    return start_node, id_index, join_node_list, top_level_nodes


def generate_sql_from_tree(start_node, join_nodes, top_level_nodes):
    current_node = start_node
    final_sql = "WITH "
    while join_nodes:
        current_node = join_nodes.pop(0)
        current_join_table = current_node.join_table

        if current_join_table.t1_table is None or current_join_table.t2_table is None:
            join_nodes.append(current_node)
            continue
        else:
            current_join_table.join_fields.append(current_join_table.t1_table.join_field_str)
            current_join_table.join_fields.append(current_join_table.t2_table.join_field_str)
            t1_sql = current_join_table.t1_table.print_sql(use_as=True)
            t2_sql = current_join_table.t2_table.print_sql(use_as=True)
            final_sql += "\n" + t1_sql + " , \n " + t2_sql + " , "

        next_node = current_node.child_node

        while True:
            if next_node is None or next_node.operator == Operator.TakeOrderedAndProject:
                return current_node, final_sql
            if next_node.operator == Operator.BroadcastHashJoin:
                join_table = next_node.join_table
                if join_table.t1_table is None:  # pretty new node
                    join_table.t1_table = current_join_table
                    join_table.t2_table = None
                    join_nodes.append(next_node)
                elif join_table.t2_table is None:  # has a table already and been the queue
                    join_table.t2_table = current_join_table

                break  # jump up from the current loop since we encounter "HashJoin"
            elif next_node.operator == Operator.HashAggregate:
                print("hash_aggregate")
                temp_len= len(current_join_table.t1_table.fields)
                random_number = random.randint(2, temp_len)
                tmp_hash_list = random.sample(current_join_table.t1_table.fields, random_number)

                current_join_table.hash_agg = tmp_hash_list
                next_node.join_table = current_join_table
                # print(tmp_hash_list)
            elif next_node.operator == Operator.Project:
                print("Project")
                temp_len = len(current_join_table.t1_table.fields)
                random_number = random.randint(2, temp_len)
                tmp_pro_list = random.sample(current_join_table.t1_table.fields, random_number)

                current_join_table.tmp_pro_list = tmp_pro_list
                next_node.join_table = current_join_table
            next_node = next_node.child_node


if __name__ == "__main__":
    # ---------swx------------
    try:
        a = 5
        b = 3
        b_h = 2
        long_chain = generate_long_chain(a)
        start_node, id_final, join_node_list, top_level_nodes = build_tree(long_chain)
        # current_node = start_node
        # while current_node:
        #     print(current_node.name)
        #     current_node = current_node.child_node

        print("------------top-level nodes---------------")
        # for index, node in enumerate(top_level_nodes):
        #     print("index is {}, node.name is {}".format(index, node.name))
        #     current_node = node
        #     while current_node:
        #         print(current_node.name)
        #         current_node = current_node.child_node

        last_join_node, generated_sql = generate_sql_from_tree(start_node, join_node_list, top_level_nodes)
        # generated_sql = generate_sql(join_nodes)
        generated_sql = generated_sql[:-2]  # delete the last comma
        rest_sql = last_join_node.join_table.print_sql(use_as=False)
        generated_sql += rest_sql
        limit_sql = "\n LIMIT 100;"
        generated_sql += limit_sql
        print("--------------final generated sql ------------")

        print(generated_sql)

        sql_file_name = "single_branch_synthesized_sql.sql"
        result_dir = "../../results/generated_sqls/"
        sql_file = os.path.join(result_dir, sql_file_name)
        with open(sql_file, 'a') as file:
            time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
            # file.write(time_str)
            file.write("\n---new sql--{}-----\n".format(time_str))
            file.write(generated_sql)

        # planList = parsePhysicalPlan(fileName)
        # # print(planList)
        # # top_level_nodes, nodes_list, adj_next = generate_adj(planList, qname=fileName + "_operators")
        # top_level_nodes=None
        # join_nodes = handle_file_scan_nodes(top_level_nodes, None, None)
        # print(len(join_nodes))
        # print(join_nodes)
        # final_node, final_sql = handle_join_nodes(join_nodes, None, None)
        # print("final_sql is -----------------------------")
        #
        # if not final_node:
        #     print("error")
        # else:
        #     final_sql = final_sql[:-2]  # delete the comma
        #     final_sql += final_node.join_table.printSQL(use_as=False)
        #     final_sql += "LIMIT 100\n"
        # print(final_sql)

        # sql_file_name = 'generated_sql_{}.sql'.format(queryName)
        # result_dir = "../results/generated_sqls/"
        # sql_file = os.path.join(result_dir, sql_file_name)
        # with open(sql_file, 'w') as file:
        #     file.write(final_sql)


    except Exception as e:
        logging.exception("An error occurred:")
        print(e)
