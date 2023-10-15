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
def generate_frame(len_frame)->int:
    if len_frame < 2:
        print("Invalid Length")
        return
    # B 10.7 S 3.75 U 0.4(估算 表中没有此数据）
    start_pro = [0.5,0.5]#Af Ad的频率还需要统计
    middle_pro = [0.0 for i in range(0, 3)]
    middle_pro[0] = 10.7/(10.7+3.75+0.4) # BHJ
    middle_pro[1] = 3.75/(10.7+3.75+0.4) # SMJ
    middle_pro[2] = 0.4/ (10.7 + 3.75 + 0.4) # Union（这部分是估算的 圆饼图里没有显示）

    # 头尾也计入长度
    num_sequences = len_frame-2

    rs1 = random.random()
    if rs1 < start_pro[0]:
        table1 = "Af"
    else:
        table1 = "Ad"
    rs2 = random.random()
    if rs2 < start_pro[0]:
        table2 = "Af"
    else:
        table2 = "Ad"

    result = table1
    while num_sequences>0:

        # print(random.random())
        r = random.random()
        if r < middle_pro[0]:
            result+=" B"
        elif r< middle_pro[0]+middle_pro[1]:
            result+=" C"
        else:
            result+=" D"
        num_sequences -=1
    result+=" E"
    if len_frame == 2:
        pass
    else:
        s1 = result[0:4]
        s2 = table2 + " " + result[3]
        s3 = result[3:]
        # s2
    print(s1)
    print(s2)
    print(s3)
    # 拓扑结构
    return s1, s2, s3

def normalize_frame(sequence)->str:
    seq_list = sequence.split()
    result = ""
    for i in range(len(seq_list)-1):
        result += seq_list[i]+seq_list[i+1]+" "
    result = result[:-1]
    print(result)

if __name__ == '__main__':
    s1, s2, s3 = generate_frame(3)

    seq1 = normalize_frame(s1)
    seq2 = normalize_frame(s2)
    seq3 = normalize_frame(s3)