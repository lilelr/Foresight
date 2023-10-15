import sys, os

from graphviz import Digraph

from core.edit_distance import edit_dist, best_fit

from core.enum_operator import Operator

from core.node_lele import Node

from core.tables import InputTable, JoinTable, HashAggreTable, SortMergeJoinTable

from core.lele_operations_handling import handle_filter_with_input_table, handle_project, handle_hash_aggregate, handle_sort, \
    handle_hash_aggregate_joint_table, handle_two_join_fields
import logging

# log_file_path = "../results/lele_dag_sql_ouput_to_file_logfile.log"
# logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

table_index = 1
operatorList = [
    "TakeOrderedAndProject",
    "ReusedExchange",
    "FileScan parquet",
    "Project",
    # SMJ
    "SortMergeJoin",
    "Sort",
    # BHJ
    "BroadcastHashJoin",
    "BroadcastExchange",
    "Exchange hashpartitioning",
    "Exchange SinglePartition",
    "HashAggregate",
    "Filter",
    "Subquery",
    "Expand",
    "Union",
    "Exchange",  # for formatted / final plan

]

rootList = [
    "TakeOrderedAndProject",
    "Sort",
    "CollectLimit 100",
    "Project"
]

operator_map = {
    "FileScan" :"a",
    "Filter":"b",
    "BroadcastHashJoin":"c",
    "BroadcastExchange":"d",
    "HashAggregate": "e",
    "Project":"f",
    "Sort":"g",
    "SortMergeJoin":"h",
    "Subquery":"i",
    "Union":"j",
    "Expand":"k",
    "Exchange hashpartitioning":"l",
    "Exchange SinglePartition":"m",
    "ReusedExchange":"n",
    "Empty":"o",
    "TakeOrderedAndProject":"p",
    "CollectLimit":"q",
    "error":"r",
    "Error":"r",
    "Exchange":"s"
}

def parsePhysicalPlan(fileName):
    fileHandler = open(fileName, "r")
    lineList = []
    while True:
        # Get next line from file
        line = fileHandler.readline()
        # If line is empty then end of file reached
        if not line:
            break
        # 去掉回车"\n"
        lineList.append(line.strip("\n"))
        # Close Close
    fileHandler.close()
    return lineList


def parseJobNum(planList):
    jobNum0 = 0
    jobNum1 = 0
    for plan in planList:
        if "- BroadcastExchange" in plan:
            jobNum0 += 1
        if "- Subquery subquery" in plan:
            jobNum1 += 1

    jobNum = jobNum0 + jobNum1 + 1
    return jobNum


def parseStageNum(planList, jobNum):
    shuffleNum = 0
    for plan in planList:
        if "+- Exchange hashpartitioning" in plan or "+- Exchange SinglePartition" in plan:
            shuffleNum += 1
    stageNum = jobNum + shuffleNum
    return stageNum


# 提取出物理计划中的具体表
def parseTableInfo(planList):
    tableInfoDict = {}
    for plan in planList:
        if "FileScan parquet" in plan:
            # 对FileScan类型特殊处理，找到数据源的表
            leftIndex = plan.find("FileScan parquet")
            rightIndex = plan.find("[")
            tableName = plan[leftIndex + 16: rightIndex].split(".")[1]
            if tableName not in tableInfoDict:
                # 找到相应表在hdfs上存储的位置
                leftIndex = plan.find("hdfs")
                locationInfo = ""
                for i in range(leftIndex, len(plan)):
                    if plan[i] == ']':
                        break
                    else:
                        locationInfo += plan[i]
                tableInfoDict[tableName] = locationInfo
    for table in tableInfoDict:
        tableLocation = tableInfoDict[table]
        os.system("bash hdfsCMD.sh " + tableLocation)
        f = open("tableInfo.txt", "r")  # 设置文件对象

        str = f.read().strip()  # 将txt文件的所有内容读入到字符串str中
        infoList = str.split(" ")
        tableInfoList = []
        for info in infoList:
            if info != "":
                tableInfoList.append(info)
        f.close()  # 将文件关闭
    return tableInfoDict


# 从计划中提取出物理操作符 + 表名 + 字段数量
def extractOperatorFromPlan(plan):
    # print(plan) # plan 表示当前行的内容 如:
    # +- FileScan parquet tpcds_100_parquet.date_dim[d_date_sk#24,d_year#30] Batched: true, DataFilter
    for op in operatorList:
        if op in plan:
            if op == "FileScan parquet":
                # 对FileScan类型特殊处理，找到数据源的表
                leftIndex = plan.find("FileScan parquet")
                rightIndex = plan.find("[")
                newOp = plan[leftIndex: leftIndex + 16] + plan[leftIndex + 16: rightIndex]

                left_square_bracket = rightIndex
                right_square_bracket = plan.find("]")
                table_fields = plan[leftIndex + 13:right_square_bracket + 1]
                fields_count = table_fields.count("#")
                return newOp, fields_count
            elif op == "Filter":
                fields_count = plan.count("#")
                return op, fields_count
            else:
                return op, 0

    print("Unsupported Operator found in line: {}".format(plan))
    return "error", 0


# 从计划中提取出根节点的名字
def extractRootOpFromPlan(plan):
    for op in rootList:
        if op in plan:
            return op
    return "Error"


def get_with_table_index():
    global table_index
    index = table_index
    table_index = table_index + 1
    return str(index)


def generate_adj(planList, qname="q", type="spark3"):
    n = len(planList) - 1
    # 名称; 所在图的层次（最后一个结点是0 或1;    stage号（op前的数字，无就-1）;  nextnode编号
    nodename, layer, stage, adj_next = [""], [0], [-1], [[] for i in range(n)]
    nodeLine = [""]  # 记录该node 对应的physical plan 原信息(就是物理计划的某一行除去
    nodes_fields_count = [0]
    planList = planList[1:] if type == "spark2" else (planList[2:] if type == "spark3" else planList[3:])
    # 1.1. Deal Root name
    line = planList[0]
    nodename[0] = extractRootOpFromPlan(line)
    i0 = line.find("*(")
    stage[0] = int(line[i0 + 2: line.find(")")]) if i0 > 0 else -1
    # 1.2. Deal nonRoot node

    for id, line in enumerate(planList[0:], start=1):
        if line.strip() == "":
            break  # 表明结束
        reuse_id = -1  # 重用哪个node
        # todo 注意q14b [i_item_sk#175, i_brand_id#182, i_class_id#184, i_category_id#186]
        # 注意 q23a 倒数第二个reuse   到底是用哪一个？
        # 注意data_id 有sum的情况
        if "ReusedExchange" in line:
            op = line[line.find("],") + 3:].strip()
            data_id = line[line.find("[") + 1: line.find("]")].strip()  # [d_date_sk#71] 这些,叫啥 啥意思？ todo
            # 不能有[]符号，因为q14a 有Project [i_item_sk#26 AS ss_item_sk#18] 
            for ti, tline in enumerate(planList):
                if op in tline and data_id in planList[ti + 1]:  # 一定会找到吧?
                    reuse_id = ti
                    break
            tmpname = "ReusedExchange_%s" % reuse_id
            fields_count = 0
        else:
            # for FileScan parquet, Filter
            tmpname, fields_count = extractOperatorFromPlan(line)

        # find layer
        # eg.         :        +- BroadcastHashJoin [ctr_store_sk#2], [s_store_sk#52], Inner, BuildRight, false
        i0 = line.find("-")
        if i0 < 0:  # 应该不会有这种情况吧
            continue
        tmplayer = int(i0 / 3) + 1  # + 1, 因为根结点才是layer0

        # find stage
        i0 = line.find("*(")
        tmpstage = int(line[i0 + 2: line.find(")")]) if i0 >= 0 else -1

        # 2. Record
        nodename.append(tmpname)
        if "FileScan" in line:
            line = line[:line.find("]") + 1]

        line = line[line.find('-') + 2:]
        nodeLine.append(line)
        nodes_fields_count.append(fields_count)
        layer.append(tmplayer)  # templayer=2
        stage.append(tmpstage)
        # 3. Build adj
        for i in range(id - 1, -1, -1):  # id =2
            if layer[i] < tmplayer:  # layer = [0,1,2]
                if reuse_id < 0:  # reuse_id = -1
                    adj_next[id].append(i)
                else:
                    adj_next[reuse_id].append(i)
                break

    adj_next = adj_next[:len(nodename)]

    nodes_list = []
    top_level_nodes = []

    for i, nexti in enumerate(adj_next):
        current_depth, current_exchanges = dfs_lele_cp(i, 0, [], nodename, adj_next, False)

        canGoOn = True
        if nodename[i] == "BroadcastHashJoin" or nodename[i] == "SortMergeJoin":
            canGoOn = False

        if i > 0:
            # 改了这里
            next_node_id = adj_next[i][0]
            # 改了这里
            node = Node(i, nodename[i], current_depth, current_exchanges, next_node_id, nodes_fields_count[i],
                        nodeLine[i], canGoOn)
            nodes_list.append(node)
            if node.name.find("Scan") >= 0:
                top_level_nodes.append(node)
        else:
            node = Node(i, nodename[i], 0, 0, -1, 0, nodeLine[i], canGoOn)
            nodes_list.append(node)

    draw_adj(nodeLine, adj_next, filename=qname + ".adj")

    return top_level_nodes, nodes_list, adj_next


# return the join_nodes # key:node->id, value:node
def ouput_file_scan_nodes(top_level_nodes, node_list, adj_next):
    ### genereate the sql by top_level_nodes, node_list, and adj_next
    container = []
    # top_level_nodes 开始一个个遍历，不考虑汇集node结果的问题
    # top_level_nodes: list中每一个元素记录着node的详细信息
    # eg.('id:19', 'FileScan parquet tpcds_100_parquet.store_returns', 'type:FileScan', ...)

    # initialization
    # temp_join_table = JoinTable()
    # 未处理 OR，不管重命名问题'ctr_total_return',BHJ合并思路，给每个inputable都加上一个filter，最后在产生sql的时候综合就行

    # result
    join_nodes = []  # the join nodes collection
    output_arary = []

    canProcessHashAggre = False
    first_flag = True
    for t_node in top_level_nodes:

        line = t_node.physical_line
        print(line)
        temp_input_table = InputTable(line=line, with_table_index=get_with_table_index())

        container.append(temp_input_table)

        # output_arary.append(str(t_node.name))
        if first_flag:
            # output_arary.append(str(t_node.operator.value))
            output_arary.append(operator_map["FileScan"])
            first_flag = False
        else:
            output_arary.append("\n"+operator_map["FileScan"])


        next_node = nodes_list[t_node.nextnode_id]

        while True:
            line = next_node.physical_line
            print(line)
            output_arary.append(operator_map[next_node.name])

            if next_node.operator == Operator.Filter:
                # fields_count = next_node.fields_count
                # 通过 "#" 找field对应条件
                # only retrieve the elements in ()
                # eg.((isnotnull(d_year#30) AND (d_year#30 = 2000)) AND isnotnull(d_date_sk#24))
                temp = container.pop()
                temp = handle_filter_with_input_table(temp, line)
                container.append(temp)
            elif next_node.operator == Operator.BroadcastExchange:
                temp_input_table.is_broadcast_exchange = True

            elif next_node.operator == Operator.BroadcastHashJoin:
                first_field, second_field = handle_two_join_fields(line)
                current_join_table=None
                # lele
                if next_node not in join_nodes:
                    # first fill up the first table and let the second table None

                    # --------------lele--------------
                    # new_join_table = JoinTable(t1=temp_input_table.table_name, t2="",
                    #                            with_table_name=get_with_table_index(), t1_table=temp_input_table,
                    #                            t2_table=None)
                    # new_join_table.add_join_constraint(first_field)
                    # new_join_table.add_join_constraint(second_field)
                    #
                    # next_node.join_table = new_join_table
                    # join_nodes.append(next_node)
                    # --------------lele--------------


                    # ------------wenxin----------------
                    temp1 = container.pop()
                    container.append(temp1)
                    # first fill up the first table and let the second table None
                    new_join_table = JoinTable(t1=temp1.with_table_name, t2="",
                                               with_table_name=get_with_table_index(), t1_table=temp1,
                                               t2_table=None)
                    new_join_table.add_join_constraint(first_field)
                    new_join_table.add_join_constraint(second_field)

                    next_node.join_table = new_join_table
                    join_nodes.append(next_node)
                    # ------------wenxin----------------

                else:
                    # --------------lele--------------
                    # current_join_table = next_node.join_table
                    # current_join_table.t2 = temp_input_table.table_name
                    # current_join_table.t2_table = temp_input_table
                    # --------------lele--------------

                    # ------------wenxin----------------
                    temp2 = container.pop()
                    container.append(temp2)
                    current_join_table = next_node.join_table
                    current_join_table.t2 = temp2.with_table_name
                    current_join_table.t2_table = temp2
                    # ------------wenxin----------------

                if len(container) < 2:  # break if the container only has one table
                    break

                temp2 = container.pop()  # tb2
                temp1 = container.pop()  # tb1

                # --------------lele--------------
                # temp_join_table = JoinTable(t1=temp1.with_table_name, t2=temp2.with_table_name,
                #                             with_table_name=get_with_table_index())
                #
                # temp_join_table.add_join_constraint(first_field)
                # temp_join_table.add_join_constraint(second_field)
                # container.append(temp_join_table)
                # --------------lele--------------

                # ------------wenxin----------------
                temp_join_table = current_join_table
                container.append(temp_join_table)
                # ------------wenxin----------------
                break

            elif next_node.operator == Operator.Project:
                temp = container.pop()
                temp = handle_project(temp, line)

                container.append(temp)

            elif next_node.operator == Operator.HashAggregate:

                if not canProcessHashAggre:
                    canProcessHashAggre = True

                else:
                    canProcessHashAggre = False
                    temp = container.pop()
                    # temp.printSQL()
                    temp_hash_aggre_table = handle_hash_aggregate(temp, line, get_with_table_index())
                    container.append(temp_hash_aggre_table)
                    temp_hash_aggre_table.printSQL()

            elif next_node.operator == Operator.SortMergeJoin:
                # eg.BroadcastHashJoin [sr_returned_date_sk#4], [d_date_sk#24], Inner, BuildRight, false
                first_field, second_field = handle_two_join_fields(line)

                if next_node not in join_nodes:
                    # first fill up the first table and let the second table None

                    # --------------lele--------------
                    # new_join_table = JoinTable(t1=temp_input_table.table_name, t2="",
                    #                            with_table_name=get_with_table_index(), t1_table=temp_input_table,
                    #                            t2_table=None)
                    # new_join_table.is_sort_merge_join = True
                    # new_join_table.add_join_constraint(first_field)
                    # new_join_table.add_join_constraint(second_field)
                    #
                    # next_node.join_table = new_join_table
                    # join_nodes.append(next_node)
                    # --------------lele--------------

                    # ------------wenxin----------------
                    temp1 = container.pop()
                    container.append(temp1)
                    # first fill up the first table and let the second table None
                    new_join_table = JoinTable(t1=temp1.with_table_name, t2="",
                                               with_table_name=get_with_table_index(), t1_table=temp1,
                                               t2_table=None)
                    new_join_table.is_sort_merge_join = True
                    new_join_table.add_join_constraint(first_field)
                    new_join_table.add_join_constraint(second_field)

                    next_node.join_table = new_join_table
                    join_nodes.append(next_node)
                    # ------------wenxin----------------

                else:
                    # --------------lele--------------
                    # current_join_table = next_node.join_table
                    # current_join_table.t2 = temp_input_table.table_name
                    # current_join_table.t2_table = temp_input_table
                    # --------------lele--------------

                    # ------------wenxin----------------
                    temp2 = container.pop()
                    container.append(temp2)
                    current_join_table = next_node.join_table
                    current_join_table.t2 = temp2.with_table_name
                    current_join_table.t2_table = temp2
                    # 这个不用放到join_nodes_list里面去了吗？------------------------------------------
                    # ------------wenxin----------------
                break

            elif next_node.operator == Operator.Union:
                # handle the union operation, we regard it as a join table while setting attribute union = true
                if next_node not in join_nodes:

                    # --------------lele--------------
                    # first fill up the first table and let the second table None
                    # new_join_table = JoinTable(t1=temp_input_table.table_name, t2="",
                    #                            with_table_name=get_with_table_index(), t1_table=temp_input_table,
                    #                            t2_table=None)
                    # new_join_table.is_union = True
                    # # particular handling for UNION ALL operation
                    # new_join_table.project_field = temp_input_table.project_field
                    #
                    # next_node.join_table = new_join_table
                    # join_nodes.append(next_node)
                    # --------------lele--------------

                    # ------------wenxin----------------
                    temp1 = container.pop()
                    container.append(temp1)
                    new_join_table = JoinTable(t1=temp1.with_table_name, t2="",
                                               with_table_name=get_with_table_index(), t1_table=temp1,
                                               t2_table=None)
                    new_join_table.is_union = True
                    # particular handling for UNION ALL operation
                    new_join_table.project_field = temp_input_table.project_field

                    next_node.join_table = new_join_table
                    join_nodes.append(next_node)
                    # ------------wenxin----------------
                else:
                    # --------------lele--------------
                    # current_join_table = next_node.join_table
                    # current_join_table.t2 = temp_input_table.table_name
                    # current_join_table.t2_table = temp_input_table
                    # --------------lele--------------

                    # ------------wenxin----------------
                    temp2 = container.pop()
                    container.append(temp2)
                    current_join_table = next_node.join_table
                    current_join_table.t2 = temp2.with_table_name
                    current_join_table.t2_table = temp2
                    # ------------wenxin----------------
                break

            elif next_node.operator == Operator.TakeOrderedAndProject:

                sql = ""

                if "limit" in line:
                    limit_index = line.find("limit")
                    leftIndex = limit_index + 6
                    rightIndex = line.find(",")
                    limit_num = line[leftIndex:rightIndex]
                    sql = "LIMIT " + limit_num

                leftIndex = line.find("orderBy") + 9
                rightIndex = line.find("#", leftIndex)
                orderBy_field = line[leftIndex:rightIndex]
                sql = "ORDER BY " + orderBy_field + " " + sql
                break
            # move to the next node
            if next_node.nextnode_id != 0:
                next_node = nodes_list[next_node.nextnode_id]
            else:
                break

    return join_nodes,output_arary


def output_join_nodes(join_nodes, node_list, adj_next, output_array):
    """

    :node: the final node dealing with TakeOrderedAndProject
    output_array: the final output array
    """

    while join_nodes:
        current_node = join_nodes.pop(0)
        current_join_table = current_node.join_table

        if current_join_table.t1_table is None or current_join_table.t2_table is None:
            join_nodes.append(current_node)
            continue

        # output_array.append("\n"+str(current_node.operator.value))
        output_array.append("\n"+operator_map[current_node.name])

        next_node = nodes_list[current_node.nextnode_id]
        pre_node = current_node
        # while next_node
        while True:
            # output_array.append(str(next_node.operator.value))
            output_array.append(operator_map[next_node.name])
            if next_node.id == 0:
                return current_node, output_array
            line = next_node.physical_line
            if next_node.operator == Operator.Project:
                current_join_table = handle_project(current_join_table, line)
            elif next_node.operator == Operator.Filter:
                current_join_table = handle_filter_with_input_table(current_join_table, line)
            elif next_node.operator == Operator.Sort:
                current_join_table = handle_sort(current_join_table, line)
            elif next_node.operator == Operator.HashAggregate:
                if pre_node.operator == Operator.ExchangeHashpartitioning or Operator.ExchangeSinglePartition:
                    # since hash aggregate show in pair, we ignore the partial_sum and select the sum directly
                    current_join_table = handle_hash_aggregate_joint_table(current_join_table, line)
            elif next_node.operator == Operator.SortMergeJoin or next_node.operator == Operator.BroadcastHashJoin:

                # ----------wenxin-------------
                first_field, second_field = handle_two_join_fields(line)
                if next_node not in join_nodes:
                    next_node.join_table = JoinTable(t1=current_join_table.with_table_name, t2="",
                                               with_table_name=get_with_table_index(), t1_table=current_join_table,
                                               t2_table=None)
                    if next_node.operator == Operator.SortMergeJoin:
                        next_node.join_table.is_sort_merge_join = True

                     # ----------wenxin-------------从下面移上来了（596）
                    next_node.join_table.add_join_constraint(first_field)
                    next_node.join_table.add_join_constraint(second_field)
                    # ----------wenxin-------------

                    join_nodes.append(next_node)
                else:

                    next_node.join_table.t2 = current_join_table.with_table_name
                    next_node.join_table.t2_table = current_join_table


                break

            elif next_node.operator == Operator.TakeOrderedAndProject:
                current_join_table.handle_take_ordered_and_project(line)
                return current_node, output_array

            pre_node = next_node
            next_node = nodes_list[next_node.nextnode_id]



def dfs_lele_cp(node_id, depth, exchanges, nodename, adj_next, flag_hash_aggregate):
    """
    node_ind: int
    depth:int
    exchange:[]
    nodename:string
    adjnext: []
    flag_hash_aggregate: bool
    """
    # if node_id==16:
    #     print("debug")
    if node_id == 0:
        return depth, exchanges
    else:
        depth = depth + 1
        # if str(nodename[node_id]).strip() == "Exchange":
        temp_line = str(nodename[node_id]).strip()
        if temp_line == "Exchange hashpartitioning" or temp_line == "Exchange SinglePartition":
            if flag_hash_aggregate:  # A previous Hash_aggregate operation has shown
                exchanges.append(1)  # since we have seen the Hash_aggregate before, we append 1 here
                flag_hash_aggregate = False
            else:
                exchanges.append(0)
        elif temp_line == "HashAggregate":
            flag_hash_aggregate = True

        next_node_id = adj_next[node_id][0]
        return dfs_lele_cp(next_node_id, depth, exchanges, nodename, adj_next, flag_hash_aggregate)


def draw_adj(nodeLine, adj_next, filename="onegraph"):
    dot = Digraph(comment='The Round Table')
    for i, name in enumerate(nodeLine):
        # if "ReusedExchange" not in name:
        dot.node(str(i), "nodeID(" + str(i) + ") " + name)
    for i, nexti in enumerate(adj_next):
        for j in nexti:
            dot.edge(str(i), str(j))
    # dot.save(save_path)
    dot.render(filename)


# 寻找两个sortJoin表的下标
def searchTwoJoinIndex(index, order):
    oneIndex = index + 1
    sortOrder = order[index + 1]
    otherIndex = -1
    for i in range(index + 2, len(planList)):
        if order[i] == sortOrder:
            otherIndex = i
            break
    return oneIndex, otherIndex


if __name__ == "__main__":
    # ---------swx------------
    try:
        for i in range(61,71):
            print(i)
            if i == 9 or i==28 or i==41 or i==44: # Q9 subquery
                continue

            queryName = "q{}".format(i)
            if i==14 or i==23 or i==24 or i==39:
                queryName = "q{}a".format(i)

            fileName=""
            # if i>=1 and i<=10:
            #     fileName = "../results/q1-10/" + queryName + "_100.plan"
            # elif i>=11 and i<=20:
            #     fileName = "../results/q11-20/" + queryName + "_100.plan"
            # elif i>=21 and i<=30:
            #     fileName = "../results/q21-30/" + queryName + "_100.plan"
            # elif i>=31 and i<=40:
            #     fileName = "../results/q31-40/" + queryName + "_100.plan"
            # elif i>=41 and i<=50:
            #     fileName = "../results/q40-49/" + queryName + "_100.plan"
            # elif i>=51 and i<=60:
            #     fileName = "../results/q50-60/" + queryName + "_100.plan"
            # elif i>=51 and i<=60:
            #     fileName = "../results/q50-60/" + queryName + "_100.plan"
            # elif i>=61 and i<=70:
            #     fileName = "../results/q61-70/" + queryName + "_100.plan"
            # else:
            #     fileName = "../results/q71-99/" + queryName + "_100.plan"

            # --------wenxin--------
            if i>=1 and i<=10:
                fileName = "../../results/q1-10/" + queryName + "_100.plan"
            elif i>=11 and i<=20:
                fileName = "../../results/q11-20/" + queryName + "_100.plan"
            elif i>=21 and i<=30:
                fileName = "../../results/q21-30/" + queryName + "_100.plan"
            elif i>=31 and i<=40:
                fileName = "../../results/q31-40/" + queryName + "_100.plan"
            elif i>=41 and i<=50:
                fileName = "../../results/q41-50/" + queryName + "_100.plan"
            elif i>=51 and i<=60:
                fileName = "../../results/q51-60/" + queryName + "_100.plan"
            elif i>=61 and i<=70:
                fileName = "../../results/q61-70/" + queryName + "_100.plan"
            else:
                fileName = "../../results/q71-99/" + queryName + "_100.plan"
            # --------wenxin--------

            # fileName = "../results/wenxin/" + queryName + "_100GB.txt"
            # fileName = "../results/" + queryName + "_Plan.txt"
            planList = parsePhysicalPlan(fileName)
            # print(planList)
            top_level_nodes, nodes_list, adj_next = generate_adj(planList, qname=fileName + "_operators")
            join_nodes, output_array= ouput_file_scan_nodes(top_level_nodes, nodes_list, adj_next)
            # print(len(join_nodes))
            # print(join_nodes)
            final_node, final_array = output_join_nodes(join_nodes, nodes_list, adj_next, output_array)
            print("final_sql is -----------------------------")

            final_str = queryName+"\n"+' '.join(final_array)+"\n"
            # final_str = ' '.join(final_array)+" ~\n"
            # if not final_node:
            #     print("error")
            # else:
            #     final_sql = final_sql[:-2]  # delete the comma
            #     final_sql += final_node.join_table.printSQL(use_as=False)
            #     final_sql += "LIMIT 100\n"
            print(final_str)

            sql_file_name = '../dag_operators_values.txt'
            with open(sql_file_name, 'a') as file:
                file.write(final_str)


    except Exception as e:
        logging.exception("An error occurred:")
        print(e)
