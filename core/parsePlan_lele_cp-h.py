import sys, os
import re
from operator import attrgetter

from graphviz import Digraph
from node_lele import Node
from table_duration import Table

dot = Digraph(comment='The Round Table')

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
            if op != "Scan parquet":
                return op,0
            else:
                # 对FileScan类型特殊处理，找到数据源的表
                leftIndex = plan.find("Scan parquet")
                rightIndex = plan.find("[")
                newOp = plan[leftIndex: leftIndex + 16] + plan[leftIndex + 16: rightIndex]

                left_square_bracket=rightIndex
                right_square_bracket = plan.find("]")
                table_fields = plan[leftIndex + 13:right_square_bracket+1]
                # print(table_fields)
                fields_count=table_fields.count("#")
                # print(fields_count)
                return newOp,fields_count
    print("Unsupported Operator found in line: {}".format(plan))
    return "error",0


# 从计划中提取出根节点的名字
def extractRootOpFromPlan(plan):
    for op in rootList:
        if op in plan:
            return op
    return "error"


# spark2 : planList[1:]
# spark3 / spark3-formattd :planList[2:]  
# spark3-final: planList[3:]

# def reversed_cmp(node1,node2):
#     if node1>y:
#         return -1
#     if x<y:
#         return 1
#     return 0

def generate_adj(planList, qname="q", type="spark2"):
    n = len(planList) - 1
    # 名称; 所在图的层次（最后一个结点是0 或1;    stage号（op前的数字，无就-1）;  nextnode编号
    nodename, layer, stage, adj_next = [""], [0], [-1], [[] for i in range(n)]
    nodes_fileds_count=[0]
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
            fields_count=0
        else:
            tmpname,fields_count = extractOperatorFromPlan(line)
        i0 = line.find("-")
        if i0 < 0:  # 应该不会有这种情况吧
            # print(line)
            continue
        tmplayer = int(i0 / 3) + 1  # + 1, 因为根结点才是layer0
        i0 = line.find("*(")
        tmpstage = int(line[i0 + 2: line.find(")")]) if i0 >= 0 else -1
        # 2. Record
        nodename.append(tmpname)
        nodes_fileds_count.append(fields_count)
        layer.append(tmplayer) # templayer=2
        stage.append(tmpstage)
        # 3. Build adj
        for i in range(id - 1, -1, -1): # id =2
            if layer[i] < tmplayer: # layer = [0,1,2]
                if reuse_id < 0: # reuse_id = -1
                    adj_next[id].append(i)
                else:
                    adj_next[reuse_id].append(i)
                break
    # print(nodename)
    adj_next = adj_next[:len(nodename)]


    nodes_list = []
    top_level_nodes=[]

    tables={}
    tables['lineitem']=Table("lineitem",58,26,7)
    tables['supplier']=Table("supplier",6,0.4,3)
    tables['part']=Table("part",4,0.6,1)
    tables['partsupp']=Table("partsupp",20,4.9,4)
    tables['inventory']=Table("inventory",34,3,1)

    tables['customer']=Table("customer",39,0.3,1)
    tables['orders']=Table("orders",38,8.6,1)

    # broadcast
    tables['nation']=Table("nation",5,3,1)
    tables['region']=Table("region",5,3,1)



    for i, nexti in enumerate(adj_next):
        current_depth, current_exchanges = dfs_lele_cp(i, 0, [], nodename, adj_next,False)
        if i>0:
            next_node_id = adj_next[i][0]
            node = Node(i, nodename[i], current_depth, current_exchanges, next_node_id,nodes_fileds_count[i])
            nodes_list.append(node)
            if node.name.find("Scan") >= 0:
                top_level_nodes.append(node)
        else:
            node = Node(i, nodename[i], 0, 0, -1,0)
            nodes_list.append(node)
        # print("{} ".format(node))

        # print("current_depth for {},{} is {} ".format(i,nodename[i],current_depth))
    # display_adj(nodename, adj_next)
    draw_adj(nodename,adj_next, filename=qname+".adj")
    # print(stage)
    sorted_nodes = sorted(top_level_nodes, key=attrgetter('exchanges', 'depth'), reverse=True)
    # print(sorted_nodes)
    runtime_prediction_nodes = []
    for i in range(0, len(sorted_nodes)):

        current_id = sorted_nodes[i].id
        next_node_id = nodes_list[current_id].nextnode_id
        flag = True
        while current_id!=0 and flag:
            current_id = nodes_list[next_node_id].id
            if nodes_list[next_node_id].name.find("BroadcastExchange") >=0:
                flag=False
            else:
                next_node_id = nodes_list[current_id].nextnode_id

        if flag:
            # print(sorted_nodes[i])
            for table_name,temp_table in tables.items():
                if sorted_nodes[i].name.find(str(table_name)) >=0:
                    predicted_runtime = sorted_nodes[i].runtime_prediction_half(temp_table.duration)
                    sorted_nodes[i].runtime_prediction_constant(temp_table.duration)
                    runtime_prediction_nodes.append(sorted_nodes[i])

    max_half_runtime=0
    max_constant_runtime=0
    for runtime_node in runtime_prediction_nodes:
            print(runtime_node)
            max_half_runtime=max(runtime_node.estimated_runtime_by_half,max_half_runtime)
            max_constant_runtime=max(runtime_node.estimated_runtime_by_constant,max_constant_runtime)

    max_half_runtime=max_half_runtime+8
    max_constant_runtime = max_constant_runtime+8
    print("The maximum estimated runtime predictions are follows:")
    print("max_half_runtime is {} seconds".format(max_half_runtime))
    print("max_constant_runtime is {} seconds".format(max_constant_runtime))
    return nodename, adj_next, stage  # nodename == "ReusedExchange" 的可忽视


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


def dfs_lele_cp(node_id, depth, exchanges, nodename, adj_next,flag_hash_aggregate):
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
        if temp_line == "Exchange hashpartitioning" or temp_line=="Exchange SinglePartition":
            if flag_hash_aggregate: # A previous Hash_aggregate operation has shown
                exchanges.append(1) # since we have seen the Hash_aggregate before, we append 1 here
                flag_hash_aggregate = False
            else:
                exchanges.append(0)
        elif temp_line == "HashAggregate":
            flag_hash_aggregate = True

        next_node_id = adj_next[node_id][0]
        return dfs_lele_cp(next_node_id, depth, exchanges, nodename, adj_next,flag_hash_aggregate)


def draw_adj(id2name, adj_next, filename="onegraph"):
    dot = Digraph(comment='The Round Table')
    for i, name in enumerate(id2name):
        # if "ReusedExchange" not in name:
        dot.node(str(i), name)
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
    # # fileName = "results/q9_1GB_plan.txt"
    # # fileName = "results/q1_Plan.txt"
    # fileName = "results/q2_Plan.txt"   #error
    # fileName = "results/q6_Plan.txt"
    # fileName = "results/q50_Plan.txt"
    # fileName = "results/q64_Plan.txt"
    # fileName = "results/q14a_Plan.txt"
    # fileName = "results/q23a_Plan.txt"  #error
    # fileName = "results/q14b_Plan.txt"  #error
    # fileFormattedName = "results/q1_Plan_formatted.txt"
    # fileName="../results/q1_Plan.txt"
    # fileName="../results/slave4/100GB/q1_100.plan"
    # fileName="../results/slave4/100GB/q2_100.plan"
    # fileName="../results/slave4/100GB/q3_100.plan"

    # fileName="../results/slave4/100GB/q4_100.plan"

    # 1000 GB
    i = 1
    while i <= 20:
        print(i)
        # fileName="../results/slave4/1000GB/q"+str(i)+"_1000.plan"
        # fileName="../results/slave4/1000GB/q"+str(i)+"_1000.plan"
        # fileName="../results/q"+str(i)+"_100GB_plan.txt"
        # fileName = "../results/q1_Plan_formatted.txt"
        # fileName = "../results/q1_plan_not_format.txt"
        # fileName = "../results/q46_1000.plan.txt"
        # fileName = "../results/q48_500.plan"
        # fileName = "../results/q49_500.plan"
        # fileName = "../results/q50_500.plan"
        # fileName = "../results/q50-60/q"+str(i)+"_500.plan"
        # fileName = "../results/q40-49/q"+str(i)+"_500.plan"
        # fileName = "../results/q61-70/q"+str(i)+"_500.plan"
        fileName = "../results/tpch-res/q"+str(i)+"_100GB_plan.txt"

        # fileName = "../results/q52_500.plan"
        # fileName = "../results/q53_500.plan"
        # fileName = "../results/q54_500.plan"
        # fileName = "../results/q55_500.plan"
        # fileName = "../results/q50_Plan.txt"
        print(fileName)
        # fileName="../results/slave4/1000GB/q1_100.plan"
        # fileName="../results/slave4/1000GB/q2_1000.plan"
        # fileName="../results/slave4/1000GB/q3_1000.plan"
        i = i + 1
        planList = parsePhysicalPlan(fileName)
        # print(planList)
        # jobNum = parseJobNum(planList)
        # stageNum = parseStageNum(planList, jobNum)
        # tableInfoDict = parseTableInfo(planList)
        # generateTree(planList)
        # generate_adj(parsePhysicalPlan(fileFormattedName),"q1","spark3")
        generate_adj(planList, qname=fileName,type="spark3")  # 检查有无 ReusedExchange_-1  error
        # try:
        #     planList = parsePhysicalPlan(fileName)
        #     # print(planList)
        #     # jobNum = parseJobNum(planList)
        #     # stageNum = parseStageNum(planList, jobNum)
        #     # tableInfoDict = parseTableInfo(planList)
        #     # generateTree(planList)
        #     # generate_adj(parsePhysicalPlan(fileFormattedName),"q1","spark3")
        #     generate_adj(planList, qname=fileName)  # 检查有无 ReusedExchange_-1  error
        # except Exception as e:
        #     print(e)
