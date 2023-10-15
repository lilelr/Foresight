import sys, os
import re
from queue import Queue
from operator import attrgetter

from graphviz import Digraph
from node_lele import Node
from table_duration import Table
from stage import FirstStage, BroadcastStage, NormalStage, Stage
from enum_operator import Operator
from first_stage_performance_model import FirstStagePrediction

import numpy as np

operatorList = [
    "ReusedExchange",
    "Scan parquet",
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

resource_config = {"executor_cores": 2, "executor_mem": 4 * 1000, "executor_instances": 14, "number_machines": 3,
                   "parallelism": 200}
executor_cores = 2
executor_mem = 4 * 1000  # 8 GB in MB
executor_instances = 6  # number of instances
number_machines = 3  # number of machines


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
        # if sort in plan
        # if "+- BroadcastExchange" in plan or ":- BroadcastExchange" in plan:
        #     jobNum0 += 1
        # if "+- Subquery subquery" in plan or ":- Subquery subquery" in plan:
        #     jobNum1 += 1
    # print(len(planList))
    # print("BroadcastExchange:  ", jobNum0, "   Subquery", jobNum1)
    jobNum = jobNum0 + jobNum1 + 1
    print("JobNum:  ", jobNum)
    return jobNum
    # return jobNum0 + jobNum1 + 1


def parseStageNum(planList, jobNum):
    shuffleNum = 0
    for plan in planList:
        if "+- Exchange hashpartitioning" in plan or "+- Exchange SinglePartition" in plan:
            shuffleNum += 1
    # print("ShuffleNum:   ", shuffleNum)
    stageNum = jobNum + shuffleNum
    # print("StageNum:   ", stageNum)
    return stageNum


# 提取出物理计划中的具体表
def parseTableInfo(planList):
    tableInfoDict = {}
    for plan in planList:
        if "FileScan parquet" in plan:
            # 对FileScan类型特殊处理，找到数据源的表
            # print(plan)
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
        # print(tableLocation)
        os.system("bash hdfsCMD.sh " + tableLocation)
        f = open("tableInfo.txt", "r")  # 设置文件对象

        str = f.read().strip()  # 将txt文件的所有内容读入到字符串str中
        # print(str)
        infoList = str.split(" ")
        tableInfoList = []
        for info in infoList:
            if info != "":
                tableInfoList.append(info)
        # print(tableInfoList)
        f.close()  # 将文件关闭
    return tableInfoDict


# 从计划中提取出物理操作符
def extractOperatorFromPlan(plan):
    # print(plan) # plan 表示当前行的内容 如:
    # +- FileScan parquet tpcds_100_parquet.date_dim[d_date_sk#24,d_year#30] Batched: true, DataFilter
    for op in operatorList:
        if op in plan:
            if op == "Scan parquet":
                # 对FileScan类型特殊处理，找到数据源的表
                leftIndex = plan.find("Scan parquet")
                rightIndex = plan.find("[")
                newOp = plan[leftIndex: leftIndex + 16] + plan[leftIndex + 16: rightIndex]

                left_square_bracket = rightIndex
                right_square_bracket = plan.find("]")
                table_fields = plan[leftIndex + 13:right_square_bracket + 1]
                # print(table_fields)
                fields_count = table_fields.count("#")
                # print(fields_count)
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


def predict_runtime(planList, qname="q", type="spark3"):
    n = len(planList) - 1
    # 名称; 所在图的层次（最后一个结点是0 或1;    stage号（op前的数字，无就-1）;  nextnode编号
    nodename, layer, stage, adj_next = [""], [0], [-1], [[] for i in range(n)]
    nodeLine = [""]  # 记录该node 对应的physical plan 原信息
    nodes_fileds_count = [0]
    planList = planList[1:] if type == "spark2" else (planList[2:] if type == "spark3" else planList[3:])
    # 1.1. Deal Root name  
    line = planList[0]
    nodename[0] = extractRootOpFromPlan(line)
    i0 = line.find("*(")
    stage[0] = int(line[i0 + 2: line.find(")")]) if i0 > 0 else -1
    # 1.2. Deal nonRoot node
    for id, line in enumerate(planList[1:], start=1):
        if line.strip() == "":
            break
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
            tmpname, fields_count = extractOperatorFromPlan(line)
        i0 = line.find("-")
        if i0 < 0:  # 应该不会有这种情况吧
            # print(line)
            continue
        tmplayer = int(i0 / 3) + 1  # + 1, 因为根结点才是layer0
        i0 = line.find("*(")
        tmpstage = int(line[i0 + 2: line.find(")")]) if i0 >= 0 else -1
        # 2. Record
        nodename.append(tmpname)
        if "FileScan" in line:
            line = line[:line.find("]")]
        line = line[line.find('-') + 2:]
        nodeLine.append(line)
        nodes_fileds_count.append(fields_count)
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
    # print(nodename)
    adj_next = adj_next[:len(nodename)]

    nodes_list = []
    top_level_nodes = []

    operators_dict = {op.name: 0 for op in Operator}  # key: Operator.type value: the numer of operators

    for i, nexti in enumerate(adj_next):
        current_depth, current_exchanges = dfs_lele_cp(i, 0, [], nodename, adj_next, False)
        if i > 0:
            next_node_id = adj_next[i][0]
            node = Node(i, nodename[i], current_depth, current_exchanges, next_node_id, nodes_fileds_count[i],
                        nodeLine[i])
            nodes_list.append(node)
            if node.name.find("Scan") >= 0:
                top_level_nodes.append(node)
        else:
            node = Node(i, nodename[i], 0, 0, -1, 0, nodeLine[i])
            nodes_list.append(node)
        operators_dict[node.operator.name] += 1
        # print("{} ".format(node))

    # for key, value in operators_dict.items():
    #     print("Operator-{}, its count is {}".format(key, value))
        # print("current_depth for {},{} is {} ".format(i,nodename[i],current_depth))
    # display_adj(nodename, adj_next)
    draw_adj(nodeLine, adj_next, filename=qname + ".adj")
    # print(stage)
    # sort the current top_level_nodes based on the number of exchanges first and depth second
    sorted_nodes = sorted(top_level_nodes, key=attrgetter('exchanges', 'depth'), reverse=True)
    # sorted_nodes are also the top level nodes

    endnode_to_first_stage = {}
    first_stage_collection = []  # the array to save the first stages
    broadcast_stage_collection = []  # the array to save the broadcast stages
    second_level_nodes = []  # the nodes after the first stage
    Stage.prediction_init()  # initialization the model parameters for performance model

    for i in range(0, len(sorted_nodes)):
        current_id = sorted_nodes[i].id
        next_node_id = nodes_list[current_id].nextnode_id

        first_stage = FirstStage()
        first_stage.begin_node = sorted_nodes[i]
        flag = True
        table_name = ""
        operators_chain = []
        while flag and current_id != 0:
            current_node = nodes_list[current_id]

            physcial_line = current_node.physical_line
            if current_node.name.find("Scan") >= 0:
                if current_node.name.find("store_sales") >= 0:
                    debug = 1
                first_stage.fileScan_fields = current_node.fields_count
                operators_chain.append(Operator.FileScan)

                table_name = physcial_line[physcial_line.find('.') + 1:physcial_line.find('[')]
                first_stage.fileScan_table = table_name

                first_stage.compute_inpute_size()
            elif current_node.operator == Operator.Filter:
                first_stage.filescan_notNULL = physcial_line.count("isnotnull")
                first_stage.filter_other = 0
                operators_chain.append(Operator.Filter)
            elif current_node.operator == Operator.Project:
                first_stage.projects = max(first_stage.projects, physcial_line.count(',') + 1)
                first_stage.project_depth = first_stage.project_depth + 1
                operators_chain.append(Operator.Project)
            elif current_node.operator == Operator.BroadcastHashJoin:
                first_stage.broadcast_joins = 2
                first_stage.broadcast_join_depth = first_stage.broadcast_join_depth + 1
                operators_chain.append(Operator.BroadcastHashJoin)
            elif current_node.operator == Operator.HashAggregate:
                # keys=physcial_line[physcial_line.find('[')+1:physcial_line.find(']')]
                # first_stage.hash_aggregate = keys.count('#')
                first_stage.hash_aggregate = max(first_stage.hash_aggregate, physcial_line.count('#'))
                first_stage.hash_aggregate_depth = first_stage.hash_aggregate_depth + 1
                operators_chain.append(Operator.HashAggregate)

            if current_node.operator == Operator.BroadcastExchange:
                broadcast_stage = BroadcastStage()
                broadcast_stage.begin_node = sorted_nodes[i]
                broadcast_stage.end_node = current_node
                broadcast_stage.fileScan_table = table_name
                operators_chain.append(Operator.BroadcastExchange)
                broadcast_stage.chain = operators_chain
                # print(broadcast_stage.to_node_str())
                broadcast_stage_collection.append(broadcast_stage)
                flag = False  # broadcast exchange meets an end
                # second_level_nodes.append(nodes_list[current_node.nextnode_id])
            elif current_node.operator == Operator.ExchangeHashpartitioning \
                    or current_node.operator == Operator.ExchangeSinglePartition \
                    or current_node.operator == Operator.ReusedExchange:
                flag = False
                second_level_nodes.append(nodes_list[current_node.nextnode_id])
                first_stage.end_node = current_node
                first_stage.exchange_partition = 1
                operators_chain.append(current_node.operator)
                first_stage.chain = operators_chain
                first_stage_collection.append(first_stage)  # append it to the first_stage_collection
                endnode_to_first_stage[first_stage.end_node] = first_stage
            else:
                current_id = current_node.nextnode_id

    # construct the second stages
    len_second = len(second_level_nodes)
    endnode_to_normalstage = {}  # key:end_node, value:NormalStage
    for i, iterNode in enumerate(second_level_nodes):
        # print("a second level node")
        # print("id:{}, name:{}, line:{}".format(iterNode.id, iterNode.name, iterNode.physical_line))
        current_id = iterNode.id
        new_stage_flag = True
        begin_node = None
        temp_operators_chain = []
        previous_stage = None

        final = False
        while not final:  # 0 denotes the final ending node's id
            current_node = nodes_list[current_id]
            if new_stage_flag:
                begin_node = current_node
                new_stage_flag = False
                temp_operators_chain = []  # reset  temp_operators_chain

            temp_operators_chain.append(current_node.operator)  # adding current node's operator to temp_operators_chain

            if current_node.operator == Operator.ExchangeSinglePartition \
                    or current_node.operator == Operator.ExchangeHashpartitioning \
                    or current_node.operator == Operator.ReusedExchange \
                    or current_node.id == 0:  ## current_node.id == 9 indicate it is the final stage
                # this is the ending symbol of current stage
                if not current_node in endnode_to_normalstage:
                    endnode_to_normalstage[current_node] = NormalStage(current_node)  # initialize a normal stage
                current_stage = endnode_to_normalstage[current_node]
                if previous_stage is not None:
                    previous_stage.next_stage = current_stage  # update the previous Stage's next stage
                current_stage.begin_nodes.append(begin_node)
                # store the operators chain starting with the begin node
                current_stage.beginNodesToChains[begin_node] = temp_operators_chain
                previous_stage = current_stage  # save the current_stage as the previous_stage

                new_stage_flag = True  # reset in order to exploit next stage

            if current_id == 0:
                final = True
            else:
                # get the child node's ID
                current_id = current_node.nextnode_id

    # test to verify that we have gotten the different chains
    # for t_node in endnode_to_normalstage:
    #     print(endnode_to_normalstage[t_node].to_node_str())

    normal_stage_collection = endnode_to_normalstage.values()
    print("len of normal_stage_collection is {}".format(len(normal_stage_collection)))

    # render the DAG of stages
    dot = Digraph(comment='The Round Table')
    dot.attr('node', shape='box')
    # dot.attr('node',shape='star')
    # add nodes of normal_stage_collection to the DAG
    for stage in normal_stage_collection:
        # str_stage= stage.to_node_str()
        # dot.node(str_stage)
        stage.compute_operators()
        dot.node(stage.end_node_id(), stage.to_node_str())

    # add edges between normal stages to the DAG
    print("normal stage collection")
    for stage in normal_stage_collection:
        # print(stage.to_node_str())
        if stage.next_stage is not None:
            dot.edge(stage.end_node_id(), stage.next_stage.end_node_id())

    # add nodes of broadcast_stage_collection to the DAG
    #  traverse the normal stages and add edge between the broadcastStage and it if they have a connection
    for broadcast_stage in broadcast_stage_collection:
        str_broadcat_stage = broadcast_stage.to_node_str()
        dot.node(broadcast_stage.end_node_id(), str_broadcat_stage)

        next_exchange_id = broadcast_stage.end_node.nextnode_id
        temp_node = nodes_list[next_exchange_id]
        while temp_node.operator != Operator.ExchangeSinglePartition and temp_node.operator != Operator.ExchangeHashpartitioning and temp_node.id != 0:
            temp_node = nodes_list[temp_node.nextnode_id]
        # temp_node.operator == Operator.ExchangeSinglePartition or  temp_node.operator == Operator.ExchangeHashpartitioning
        if temp_node in endnode_to_first_stage:
            # traverse the first stages and add edge between the firstStage and it if they have a connection
            dot.edge(broadcast_stage.end_node_id(), str(temp_node.id))
            broadcast_stage.next_stage = endnode_to_first_stage[
                temp_node]  # make the curent first stage as the next stage
        elif temp_node in endnode_to_normalstage:
            dot.edge(broadcast_stage.end_node_id(), str(temp_node.id))
            broadcast_stage.next_stage = endnode_to_normalstage[
                temp_node]  # make the curent first stage as the next stage

        # next_id_broadcast_stage = broadcast_stage.end_node.nextnode_id
        # # traverse the normal stages and add edge between the firstStage and it if they have a connection
        # for stage in normal_stage_collection:
        #     begin_node_id_list = [node.id for node in stage.begin_nodes]
        #     if next_id_broadcast_stage in begin_node_id_list:
        #         dot.edge(broadcast_stage.end_node_id(), stage.end_node_id())
        #         broadcast_stage.next_stage = stage  # make the curent normal stage as the next stage

    # add nodes of first_stage_collection to the DAG
    print("handle first_stage")
    for first_stage in first_stage_collection:
        str_first_stage = first_stage.to_node_str()
        print(first_stage.to_model_str())
        dot.node(first_stage.end_node_id(), str_first_stage)  # construct the description for the first stage
        next_id_first_stage = first_stage.end_node.nextnode_id
        # traverse the normal stages and add edge between the firstStage and it if they have a connection
        for stage in normal_stage_collection:
            begin_node_id_list = [node.id for node in stage.begin_nodes]
            if next_id_first_stage in begin_node_id_list:
                dot.edge(first_stage.end_node_id(), stage.end_node_id())
                first_stage.next_stage = stage  # make the curent normal stage as the next stage 

    stage_dag_file = qname + "_stages"
    dot.render(stage_dag_file)

    # BFS: iterate each stage per level
    stage_count = len(first_stage_collection) + len(broadcast_stage_collection) + len(normal_stage_collection)
    queue_stages = Queue()
    dag_width = 1
    for stage in first_stage_collection:
        queue_stages.put(stage)
    while not queue_stages.empty():
        temp_dag_width = queue_stages.qsize()
        dag_width = max(temp_dag_width, dag_width)
        stage = queue_stages.get()
        predict_output_size, predict_output_runtime = stage.predict_runtime_output_size(resource_config)
        next_normal_stage = stage.next_stage
        if next_normal_stage:  # next_normal_stage is not None, we append it to the queue
            next_normal_stage.compute_input_data_size(predict_output_size)
            queue_stages.put(next_normal_stage)

    # compute the maximum depth of the stage DAG

    max_depth = 0
    max_runtime = 0
    for first_stage in first_stage_collection:
        # predict the runtime and output size in GB of the first stage
        temp_runtime = first_stage.estimate_runtime
        temp_depth = 1
        next_s = first_stage.next_stage  # next_s is the normal stage
        while next_s:
            temp_depth += 1
            temp_runtime += next_s.estimate_runtime
            next_s = next_s.next_stage
        max_depth = max(max_depth, temp_depth)
        max_runtime = max(max_runtime, temp_runtime)

    max_runtime = round(max_runtime)
    return max_depth, dag_width, stage_count, max_runtime, operators_dict

    # print("the maximum depth of the DAG is {}, maximum runtime is {} seconds".format(max_depth, max_runtime))

    # return nodename, adj_next, stage  # nodename == "ReusedExchange" 的可忽视


# def find_broadcast_exchange(sorted_nodes,nodes_list):


# list, tow_dim_list
def display_adj(nodename, adj_next):
    print("The ADJ is:==========")
    print("%10s %38s \t|\t %10s %38s" % ("NodeID", "NodeName", "NextNodeID", "NextNodeName"))
    print(
        "-------------------------------------------------------------------------------------------------------------------")
    for i, nexti in enumerate(adj_next):
        ss = []
        print(nexti)
        for j in nexti:
            ss.append("(%s %s) " % (j, nodename[j]))
        print("%3s %45s \t|\t %s" % (i, nodename[i], ' '.join(ss)))


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
    # fileName = sys.argv[1]
    queryName = "q50"
    # fileName = "results/" + queryName + "_Plan.txt"

    # 1000 GB
    i = 91
    # particular_cases = [17,25,29,64,72]
    # particular_cases = [72]
    configs_str = "-".join([str(key) + str(value) for key, value in resource_config.items()])
    operators_head = ",".join([op.name for op in Operator])
    content = "executor_instances, executor_cores, executor-memory (MB), input data size (GB), query,dag depth, dag width,stage_count, predicted_runtime(seconds), actual " + "," + operators_head
    content += "\n"
    while i <= 99:
        # for i in particular_cases:
        print(i)
        # if i == 44 or i == 46:
        #     i += 1
        #     continue
        # fileName = "../results/q21-30/q"+str(i)+"_100.plan"
        fileName = ""
        if 1 <= i <= 10:
            fileName = "../results/q1-10/q" + str(i) + "_500.plan"
        elif 11 <= i <= 20:

            fileName = "../results/q11-20/q" + str(i) + "_500.plan"
        elif 21 <= i <= 30:
            fileName = "../results/q21-30/q" + str(i) + "_500.plan"
        elif 31 <= i <= 40:
            fileName = "../results/q31-40/q" + str(i) + "_500.plan"
        elif 41 <= i <= 50:
            fileName = "../results/q40-49/q" + str(i) + "_500.plan"
        elif 51 <= i <= 60:
            fileName = "../results/q50-60/q" + str(i) + "_500.plan"
        elif 61 <= i <= 70:
            fileName = "../results/q61-70/q" + str(i) + "_500.plan"
        elif 71 <= i <= 99:
            fileName = "../results/q71-99/q" + str(i) + "_500.plan"
        # fileName = "../results/q1-10/q" + str(i) + "_500.plan"

        # fileName = "../results/q40-49/q"+str(i)+"_500.plan"

        # prediction for tpc-h
        # fileName = "../results/tpch-res/q" + str(i) + "_100GB_plan.txt"

        print(fileName)

        try:
            planList = parsePhysicalPlan(fileName)
            min_prediction_time = 1000000000
            min_executor_instances = 1000
            min_executor_memory = 10000000000
            for instances in range(2,22,4):
                resource_config['executor_instances'] = instances
                depth, dag_width, stage_count, predicted_runtime, operators_dict = predict_runtime(planList,
                                                                                                   qname=fileName + "_operators")  # 检查有无
                predicted_runtime +=10 # 10 seconds for the broadcastExchange overhead
                operators_str = ",".join([str(operators_dict[op.name]) for op in Operator])
                new_line = '{},{},{},{},{},{},{}, {},{},{},'.format(resource_config['executor_instances'],
                                                                   resource_config['executor_cores'],
                                                                   resource_config['executor_mem'], 500, "Q" + str(i),
                                                                   depth, dag_width, stage_count, predicted_runtime, 0)
                new_line += operators_str
                new_line += "\n"
                # print(new_line)

                predicted_runtime_line = '{},{},{},{},{},predicted_runtime: {} seconds'.format(resource_config['executor_instances'],
                                                                    resource_config['executor_cores'],
                                                                    resource_config['executor_mem'], 500, "Q" + str(i),
                                                                    predicted_runtime)
                print(predicted_runtime_line)
                if predicted_runtime < min_prediction_time:
                    min_prediction_time = predicted_runtime
                    min_executor_instances = resource_config['executor_instances']
                    min_executor_memory = resource_config['executor_mem']
                content += new_line

            final_predicted_runtime = 'final prediction: executor instances: {},{},{},{},{},predicted_runtime: {} seconds'.format(
                min_executor_instances,
                resource_config['executor_cores'],
                min_executor_memory, 500, "Q" + str(i),
                min_prediction_time)
            print(final_predicted_runtime)
        except Exception as e:
            print(e)
        break
        i = i + 1

    # prefix = "TPC-DS-500GB-" + configs_str
    # fileName = '../results/{}-runtime_prediction.csv'.format(prefix)
    # with open(fileName, 'w')as file:
    #     file.write(content)
