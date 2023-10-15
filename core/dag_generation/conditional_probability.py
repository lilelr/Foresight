import sys, os
import random
from graphviz import Digraph
from core.node_lele import Node
from core.enum_operator import Operator

from core.edit_distance import edit_dist, best_fit

from core.tables import InputTable, JoinTable, HashAggreTable, SortMergeJoinTable

from core.lele_operations_handling import handle_filter_with_input_table, handle_project, handle_hash_aggregate, handle_sort, \
    handle_hash_aggregate_joint_table, handle_two_join_fields
import logging

if __name__ == '__main__':

    diffs = [0 for i in range(0, 20)]
    diffs_pro = [0.0 for i in range(0, 20)]

    with open("./code_tree_directory/code_tree_TPC_DS_branch_number_2.txt", "r") as file:
        lines = file.readlines()

        total = len(lines)
        for line in lines:
            numbers = [int(item) for item in line.split()]
            a = numbers[0]
            b = numbers[1]
            c = numbers[2]
            diffs[(a-b)]+=1
        print(diffs)
        for index, item in enumerate(diffs):
            diffs_pro[index] = item / float(total)
            # print(index, item,diffs_pro[index] )
        print(diffs_pro)
    # random.seed(10)
    num_sequences = 10
    given_a = 7

    while num_sequences>0:

        # print(random.random())
        result ="{} ".format(given_a)
        r = random.random()
        if r < diffs_pro[0]:
            result+=" {} {}".format(given_a,2)
        elif r< diffs_pro[0]+diffs_pro[1]:
            result += " {} {}".format(given_a-1, 2)
        elif r< diffs_pro[0]+diffs_pro[1] + diffs_pro[2]:
            result += " {} {}".format(given_a - 2, 2)
        elif r< diffs_pro[0]+diffs_pro[1] + diffs_pro[2]+ diffs_pro[3]:
            result += " {} {}".format(given_a - 3, 2)
        else:
            result += " {} {}".format(given_a - 4, 2)
        print(result)
        num_sequences -=1