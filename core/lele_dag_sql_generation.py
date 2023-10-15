import sys, os
import re
from operator import attrgetter

from graphviz import Digraph
from node_lele import Node
from table_duration import Table
from stage import FirstStage, BroadcastStage, NormalStage
from enum_operator import Operator

from tables import InputTable, JoinTable, HashAggreTable
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


# spark2 : planList[1:]
# spark3 / spark3-formattd :planList[2:]  
# spark3-final: planList[3:]

# def reversed_cmp(node1,node2):
#     if node1>y:
#         return -1
#     if x<y:
#         return 1
#     return 0
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
    # print(planList)
    planList = planList[1:] if type == "spark2" else (planList[2:] if type == "spark3" else planList[3:])
    # 1.1. Deal Root name
    # print(planList)
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
            # print()

        # find layer
        # eg.         :        +- BroadcastHashJoin [ctr_store_sk#2], [s_store_sk#52], Inner, BuildRight, false
        i0 = line.find("-")
        if i0 < 0:  # 应该不会有这种情况吧
            # print(line)
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
        # print(nodeLine)
        nodes_fields_count.append(fields_count)
        layer.append(tmplayer)  # templayer=2
        stage.append(tmpstage)
        # 3. Build adj
        # print(adj_next)
        for i in range(id - 1, -1, -1):  # id =2
            if layer[i] < tmplayer:  # layer = [0,1,2]
                if reuse_id < 0:  # reuse_id = -1
                    adj_next[id].append(i)
                else:
                    adj_next[reuse_id].append(i)
                break
        # print(adj_next)
    # print(nodename)
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
                        nodeLine[i],canGoOn)
            nodes_list.append(node)
            if node.name.find("Scan") >= 0:
                top_level_nodes.append(node)
        else:
            node = Node(i, nodename[i], 0, 0, -1, 0, nodeLine[i], canGoOn)
            nodes_list.append(node)
        # print("{} ".format(node))

        # print("current_depth for {},{} is {} ".format(i,nodename[i],current_depth))
    # display_adj(nodename, adj_next)
    input_table_list_unused = []  # 记录了每个top level node 的InputTable类，这个类记录了File Scan读取的表名以及字段名。
    input_table_list_used = []
    container = []
    join_table_list_unused = []
    join_table_list_used = []
    # top_level_nodes 开始一个个遍历，不考虑汇集node结果的问题
    # top_level_nodes: list中每一个元素记录着node的详细信息
    # eg.('id:19', 'FileScan parquet tpcds_100_parquet.store_returns', 'type:FileScan', ...)

    # initialization
    # temp_join_table = JoinTable()
    # 未处理 OR，不管重命名问题'ctr_total_return',BHJ合并思路，给每个inputable都加上一个filter，最后在产生sql的时候综合就行

    canProcessHashAggre = False
    for t_node in top_level_nodes:
        # print(t_node)
        # next_node = t_node
        # the top node must be File Scan
        # if next_node.operator == Operator.FileScan:
        line = t_node.physical_line
        temp_input_table = InputTable(line, get_with_table_index())
        # print(temp_input_table.with_table_name)

        # temp_join_table = JoinTable()

        # store the InputTable instance
        input_table_list_unused.append(temp_input_table)
        input_table_list_used.append(temp_input_table)

        container.append(temp_input_table)

        next_node = nodes_list[t_node.nextnode_id]


        while True:
            # print(next_node.operator)

            # process the HashAggregate Node on the second encounter
            if not next_node.canGoOn:
                next_node.canGoOn = True
                break
            # 如果第一次遇到
            line = next_node.physical_line

            # if line != "Filter isnotnull((avg(ctr_total_return) * 1.2)#103)":
            #
            #     if next_node.nextnode_id != 0:
            #         next_node = nodes_list[next_node.nextnode_id]
            #     else:
            #         break
            #     continue

            if next_node.operator == Operator.Filter:
                # we don't need to care about the Filter(in q1) after the BHJ
                # print("here")
                # if temp_join_table.isInitialized():
                #     continue
                # print("here")
                # fields_count = next_node.fields_count
                # 通过 "#" 找field对应条件
                # only retrieve the elements in ()
                # eg.((isnotnull(d_year#30) AND (d_year#30 = 2000)) AND isnotnull(d_date_sk#24))
                temp = container.pop()
                line = line[6:]

                # eg.line_list = ['((isnotnull(d_year#30)', 'AND', '(d_year#30', '=', '2000))', 'AND', 'isnotnull(d_date_sk#24))']
                line_list = line.split()
                # print(line_list)
                for i, line_cut in enumerate(line_list):
                    # print(line_cut)
                    # OR还未处理，需要用别的query测试。
                    if "AND" in line_cut or "OR" in line_cut:
                        continue
                    # flag_notnull = False this para is used for # Filter isnotnull((avg(ctr_total_return) * 1.2)#103): in this example, leftIndex = -1  未处理
                    if "#" in line_cut:
                        if "isnotnull" in line_cut and "avg" not in line_cut and "sum" not in line_cut:
                            index_null = line_cut.find("isnotnull")
                            leftIndex = line_cut.find("(",index_null)
                            rightIndex = line_cut.find("#")
                            field_name = line_cut[leftIndex + 1:rightIndex]
                            field_filter = " IS NOT NULL"
                            temp.add_field_filters(field_name, field_filter)
                        elif "isnotnull" in line_cut and "avg" in line_cut or "sum" in line_cut:
                            print("meet avg/sum")
                        # flag_notnull = True
                            # index_inn = line_cut.find("isnotnull")
                            # line_cut = line_cut[index_inn + 9:]
                            # print(line_cut)
                        else:
                            leftIndex = line_cut.find("(")

                            if leftIndex != -1:
                                # Filter isnotnull((avg(ctr_total_return) * 1.2)#103): in this example, leftIndex = -1  未处理
                                # ((isnotnull(d_year#30)'
                                # 'AND', '(d_year#30', '=', '2000))'
                                rightIndex = line_cut.find("#")
                                field_name = line_cut[leftIndex + 1:rightIndex]
                                field_filter = ""

                                # if flag_notnull:
                                # 先不管'ctr_total_return'
                                # if temp_input_table.hasFieldName(field_name):
                                #     temp_input_table.addFieldFilters(field_name, "NOT NULL")
                                # continue

                                op_line_cut = line_list[i + 1]
                                num = line_list[i + 2]
                                num = num[:num.find(")")]
                                field_filter = op_line_cut + " " + str(num)
                                # print("a")

                                temp.add_field_filters(field_name, field_filter)

                                # print("b")
                                # temp_input_table.showDetailsForField(field_name)
                                # print("c")

                                # 未处理OR
                                # if not the first one, then it will be an "AND" or "OR" before
                                # if i != 0:
                                #     if line_list[i - 1] == "AND" or line_list[i - 1] == "OR":
                                #         sql = line_list[i - 1] + " " + sql
                                # print(sql)
                            else:
                                print("can't find (")
                container.append(temp)

            elif next_node.operator == Operator.BroadcastHashJoin:
                # print("wrong here? BHJ")
                # temp = container.pop()
                # print("no")
                temp2 = container.pop() # tb2
                temp1 = container.pop() # tb1
                temp1.printSQL()
                temp2.printSQL()
                # print("wrong here? 1")
                # temp_join_table = JoinTable()
                # temp_t1 = input_table_list_used.pop()
                # temp_t2 = input_table_list_used.pop() # now input_table_list_used is empty
                # print("wrong here? 2")
                temp_join_table = JoinTable(t1=temp1.getWithTableName(),t2=temp2.getWithTableName(),with_table_name=get_with_table_index())
                # join_table_list_unused.append(temp_join_table)
                # join_table_list_used.append(temp_join_table)
                # print("wrong here? 3")
                leftIndex = line.find("[") + 1
                rightIndex = line.find("#")
                first_field = line[leftIndex:rightIndex]

                leftIndex = line.find("[", rightIndex) + 1
                rightIndex = line.find("#", leftIndex)
                second_field = line[leftIndex:rightIndex]
                temp_join_table.add_join_constraint(first_field)
                temp_join_table.add_join_constraint(second_field)
                # print("wrong here? 4")
                # if temp_input_table.hasFieldName(first_field):
                #     field_filter = "=" + " " + second_field
                #     temp_input_table.addFieldFilters(field_name, field_filter)
                # else:
                #     field_filter = "=" + " " + first_field
                #     temp_input_table.addFieldFilters(field_name, field_filter)
                # return
                container.append(temp_join_table)
                # print("wrong here? 5")
                # return

            elif next_node.operator == Operator.Project:
                # print("Is project wrong")
                temp = container.pop()

                leftIndex = line.find("[")
                rightIndex = line.find("]")
                # Project [sr_customer_sk#7, sr_store_sk#11, sr_return_amt#15]

                fields_str = str(line[leftIndex + 1:rightIndex])
                # # print(fields_str)
                fields_item = fields_str.split(",")
                # # print((fields_item))
                for word in fields_item:
                    temp_items = word.split("#")
                    field_name = temp_items[0].strip()
                    temp.add_project(field_name)
                # print("yes")
                container.append(temp)
                # print("no project")

            elif next_node.operator == Operator.HashAggregate:
                # print("end")
                # return
                # return
                if not canProcessHashAggre:
                    canProcessHashAggre = True

                else:
                    canProcessHashAggre = False
                    temp = container.pop()
                    # return
                    temp.printSQL()
                    # return

                    temp_hash_aggre_table = HashAggreTable(table_name=temp.getWithTableName(),with_table_name=get_with_table_index())
                    # HashAggregate(keys=[sr_customer_sk#7, sr_store_sk#11], functions=[sum(sr_return_amt#15)])
                    leftIndex = line.find("[")
                    rightIndex = line.find("]")
                    fields_str = str(line[leftIndex + 1:rightIndex])
                    print(fields_str)
                    fields_item = fields_str.split(",")
                    print(fields_item)
                    # return
                    # print(fields_item)
                    for word in fields_item:
                        temp_items = word.split("#")
                        field_name = temp_items[0].strip()
                        # if temp_join_table.hasHashAggConstraint(field_name):
                        #     continue
                        # temp_join_table.addHashAggConstraint(field_name)
                        temp_hash_aggre_table.addKeys(field_name)

                    # get functions
                    functions_index = line.find("functions")
                    leftIndex = line.find("[", functions_index)
                    rightIndex = line.find("#",leftIndex)
                    functions = line[leftIndex+1:rightIndex]+")" # eg.sum(sr_return_amt)
                    # attention: do not consider: line != "Filter isnotnull((avg(ctr_total_return) * 1.2)#103)
                    temp_hash_aggre_table.addFunctions(functions)
                    container.append(temp_hash_aggre_table)
                    temp_hash_aggre_table.printSQL()
                    return

            elif next_node.operator == Operator.SortMergeJoin:
                # eg.BroadcastHashJoin [sr_returned_date_sk#4], [d_date_sk#24], Inner, BuildRight, false
                sql = "WHERE "
                leftIndex = line.find("[") + 1
                rightIndex = line.find("#")
                first_field = line[leftIndex:rightIndex]
                leftIndex = line.find("[", rightIndex) + 1
                rightIndex = line.find("#", leftIndex)
                second_field = line[leftIndex:rightIndex]
                sql = sql + first_field + " = " + second_field
                # print((sql))

            elif next_node.operator == Operator.TakeOrderedAndProject:
                break
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

            # move to the next node
            # print(next_node)
            if next_node.nextnode_id != 0:
                next_node = nodes_list[next_node.nextnode_id]
            else:
                break
            # print(next_node)
            # sql = "select"+ temp_input_table.fields " from "+temp_input_table.table_name



        # break

    join_table_list_used[0].getSQL()

    draw_adj(nodeLine, adj_next, filename=qname + ".adj")
    # print(stage)
    # print(nodeLine)
    # 将物理计划转化为sql语句
    # nodeLine only includes the content part
    # translateToSQL(nodeLine) 这部分转到260行

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
        queryName = "q1"
        # fileName = "./results/" + queryName + "_100GB.txt"
        fileName = "../results/wenxin/" + queryName + "_100GB.txt"
        # fileName = "../results/" + queryName + "_Plan.txt"
        planList = parsePhysicalPlan(fileName)
        # print(planList)
        generate_adj(planList, qname=fileName + "_operators")
    except Exception as e:
        print(e)

    # ----------lele-----------

    # # fileName = sys.argv[1]
    # queryName = "q50"
    # # fileName = "results/" + queryName + "_Plan.txt"
    #
    # # 1000 GB
    # # i = 21
    # i = 4
    # while i <= 4:
    #
    #     print(i)
    #     # fileName = "../results/q21-30/q"+str(i)+"_100.plan"
    #     # fileName = "../results/q1-10/q" + str(i) + "_500.plan"
    #     # fileName = "../results/q11-20/q" + str(i) + "_500.plan"
    #
    #     # prediction for tpc-h
    #     fileName = "../results/tpch-res/q" + str(i) + "_100GB_plan.txt"
    #
    #     print(fileName)
    #
    #     i = i + 1
    #     try:
    #         planList = parsePhysicalPlan(fileName)
    #         # print(planList)
    #         # jobNum = parseJobNum(planList)
    #         # stageNum = parseStageNum(planList, jobNum)
    #         # tableInfoDict = parseTableInfo(planList)
    #         # generateTree(planList)
    #         # generate_adj(parsePhysicalPlan(fileFormattedName),"q1","spark3")
    #         generate_adj(planList, qname=fileName + "_operators")  # 检查有无 ReusedExchange_-1  error
    #     except Exception as e:
    #         print(e)
