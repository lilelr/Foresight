import sys, os

from graphviz import Digraph
from sythesize_nodes import SynthesizedNode
from core.enum_operator import Operator
import random
from database import Table, Field, Source
from tpc_ds_tables import store_returns, date_dim
from synthesize_tables import JoinTable

import logging

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


# def handle_top_level_nodes(top_level_nodes):
#     # result
#     join_nodes = []  # the join nodes collection
#
#     canProcessHashAggre = False
#     for t_node in top_level_nodes:
#         current_node = t_node.child_node
#         current_input_table = t_node.input_table
#         # temp_input_table = InputTable(line="", with_table_index=get_with_table_index())
#         while True:
#             if current_node.operator == Operator.BroadcastHashJoin:
#                 # current_node.
#                 # new_join_table = JoinTable(t1=current_input_table.name, t2="",
#                 #                            with_table_name=get_with_table_index(), t1_table=current_input_table,
#                 #                            t2_table=None)
#             current_node = current_node.child_node


def file_scan_branch(chain, id_index):
    start_node = None
    previous_node = None
    end_node = None
    for index, op in enumerate(chain):
        if op == 'FileScan parquetf':
            node = SynthesizedNode(id_index, "FileScan parquetf")
            node.input_table = store_returns  # fix the input table
            node.input_table.with_table_name = "tb"+get_with_table_index()
            id_index += 1
            previous_node = node
            start_node = node

        elif op == 'FileScan parquetd':
            node = SynthesizedNode(id_index, "FileScan parquetd")
            node.input_table = date_dim  # fix the input table
            node.input_table.with_table_name = "tb" + get_with_table_index()
            id_index += 1
            previous_node = node
            start_node = node
        elif op == 'Filter':
            node = SynthesizedNode(id_index, "Filter")
            node.input_table = previous_node.input_table
            previous_node.child_node = node
            id_index += 1
            node.parent_node.append(previous_node)
            previous_node = node

        elif op == 'Project':
            node = SynthesizedNode(id_index, "Project")
            node.input_table = previous_node.input_table
            previous_node.child_node = node
            node.parent_node.append(previous_node)
            id_index += 1
            previous_node = node
        elif op == 'BroadcastHashJoin':
            end_node = previous_node
    return start_node, end_node, id_index


def join_branch(chain, end_nodes, id_index):
    join_node = None
    previous_node = None
    for index, op in enumerate(chain):

        if op == 'BroadcastHashJoin':
            node = SynthesizedNode(id_index, "BroadcastHashJoin")
            node.join_table = JoinTable(t1_table=end_nodes[0].input_table, t2_table=end_nodes[1].input_table,with_table_name=get_with_table_index())
            node.parent_node = end_nodes
            previous_node = node
            join_node = node
        elif op == 'Filter':
            node = SynthesizedNode(id_index, "Filter")
            previous_node.child_node = node
            node.join_table = previous_node.join_table
            id_index += 1
            node.parent_node.append(previous_node)
            previous_node = node

        elif op == 'Project':
            node = SynthesizedNode(id_index, "Project")
            previous_node.child_node = node
            node.parent_node.append(previous_node)
            node.join_table = previous_node.join_table
            id_index += 1
            previous_node = node
        elif op == 'HashAggregate':
            node = SynthesizedNode(id_index, "HashAggregate")
            previous_node.child_node = node
            node.join_table = previous_node.join_table
            previous_node = node
            id_index += 1
        elif op == "TakeOrderedAndProject":
            node = SynthesizedNode(id_index, "TakeOrderedAndProject")
            previous_node.child_node = node
            node.join_table = previous_node.join_table
            previous_node = node
            id_index += 1

    return join_node, id_index


def construct_tree(topological_sorting_list):
    top_level_nodes = [] ##
    end_nodes = [] # two nodes
    id_index = 0
    previous_node = None
    resulting_join_nodes = []
    join_node = None
    join_nodes = []
    for outter_index, chain in enumerate(topological_sorting_list):
        if chain[0].find("FileScan") >= 0:
            start_node, end_node, id_index = file_scan_branch(chain, id_index)
            top_level_nodes.append(start_node)
            end_nodes.append(end_node)
        elif chain[0].find("Join") >= 0:
            join_node, id_index = join_branch(chain, end_nodes, id_index)
            join_nodes.append(join_node)

    current_node = join_node
    final_sql = "with "
    resulting_join_nodes.append(join_node)
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
            if next_node.operator == Operator.TakeOrderedAndProject:
                print("TOAP")
                return current_node, final_sql
            elif next_node.operator == Operator.BroadcastHashJoin:
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
                random_number = random.randint(2, 6)
                tmp_hash_list = random.sample(current_join_table.t1_table.fields, random_number)

                current_join_table.hash_agg = tmp_hash_list
                next_node.join_table = current_join_table
                # print(tmp_hash_list)
            elif next_node.operator == Operator.Project:
                print("Project")
                random_number = random.randint(2, 6)
                tmp_pro_list = random.sample(current_join_table.t1_table.fields, random_number)

                current_join_table.tmp_pro_list = tmp_pro_list
                next_node.join_table = current_join_table
            next_node = next_node.child_node

def generate_sql(join_nodes):
    final_sql = ""

    while join_nodes:
        current_node = join_nodes.pop(0)
        current_join_table = current_node.join_table

        if current_join_table.t1_table is None or current_join_table.t2_table is None:
            join_nodes.append(current_node)
            continue
        final_sql += current_join_table.print_sql(use_as=False)

    return final_sql


if __name__ == "__main__":
    # ---------swx------------
    try:
        # three tables, input:
        # output of conditional_probability_phy.py
        s1 = "Af B"  # AfB
        s2 = "Ad B"  # AdB
        s3 = "B E"  # BC CE
        # output of lstm_sequence_part_of_speech_tagging.ipynb
        predicted_middle_list1 = ["Filter"]
        predicted_middle_list2 = ["Filter-Project"]
        predicted_middle_list3 = ["Project-HashAggregate-HashAggregate-TakeOrderedAndProject"]

        predicted_frame_list1 = s1.split()
        predicted_frame_list2 = s2.split()
        predicted_frame_list3 = s3.split()

        len_seq1 = len(predicted_frame_list1)
        len_seq2 = len(predicted_frame_list2)
        len_seq3 = len(predicted_frame_list3)

        full_seq1_list = []
        for i in range(len_seq1):
            full_seq1_list.append(id_to_nodes[predicted_frame_list1[i]])
            if i != len_seq1 - 1:
                full_seq1_list.extend(predicted_middle_list1[i].split("-"))

        full_seq2_list = []
        for i in range(len_seq2):
            full_seq2_list.append(id_to_nodes[predicted_frame_list2[i]])
            if i != len_seq2 - 1:
                full_seq2_list.extend(predicted_middle_list2[i].split("-"))

        full_seq3_list = []
        for i in range(len_seq3):
            full_seq3_list.append(id_to_nodes[predicted_frame_list3[i]])
            if i != len_seq3 - 1:
                full_seq3_list.extend(predicted_middle_list3[i].split("-"))

        print(full_seq1_list)
        print(full_seq2_list)
        print(full_seq3_list)
        topological_sorting_list = []
        topological_sorting_list.append(full_seq1_list)
        topological_sorting_list.append(full_seq2_list)
        topological_sorting_list.append(full_seq3_list)

        last_join_node, generated_sql = construct_tree(topological_sorting_list)
        # generated_sql = generate_sql(join_nodes)
        generated_sql = generated_sql[:-2]  # delete the last comma
        rest_sql = last_join_node.join_table.print_sql(use_as=False)
        generated_sql += rest_sql
        print("--------------final generated sql ------------")

        print(generated_sql)

        sql_file_name = "test_synthesized_sql.sql"
        result_dir = "../../results/generated_sqls/"
        sql_file = os.path.join(result_dir, sql_file_name)
        with open(sql_file, 'a') as file:
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
