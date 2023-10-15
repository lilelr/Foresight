import sys, os

from graphviz import Digraph
from node_lele import Node
from enum_operator import Operator

from edit_distance import edit_dist, best_fit

from tables import InputTable, JoinTable, HashAggreTable, SortMergeJoinTable

from lele_operations_handling import handle_filter_with_input_table, handle_project, handle_hash_aggregate, handle_sort, \
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
    "FileScan": "a",
    "Filter": "b",
    "BroadcastHashJoin": "c",
    "BroadcastExchange": "d",
    "HashAggregate": "e",
    "Project": "f",
    "Sort": "g",
    "SortMergeJoin": "h",
    "Subquery": "i",
    "Union": "j",
    "Expand": "k",
    "Exchange hashpartitioning": "l",
    "Exchange SinglePartition": "m",
    "ReusedExchange": "n",
    "Empty": "o",
    "TakeOrderedAndProject": "p",
    "CollectLimit": "q",
    "error": "r",
    "Error": "r",
    "Exchange": "s"
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


# return tree structure with root node
def extract_tree_structure(top_level_nodes, node_list, adj_next):
    ### genereate the sql by top_level_nodes, node_list, and adj_next

    # result
    root = None
    # save the top_level_nodes which do not have the maximum length
    short_nodes = []
    maximum_node = None
    for t_node in top_level_nodes:

        current_length = 1
        previous_node = t_node
        next_node = nodes_list[t_node.nextnode_id]

        while True:
            if next_node.operator == Operator.BroadcastHashJoin or next_node.operator == Operator.SortMergeJoin or next_node.operator == Operator.Union:
                previous_node.parent_node = next_node
                if previous_node not in next_node.children_nodes:
                    next_node.children_nodes.append(previous_node)
                previous_node = next_node
                current_length = current_length + 1

            elif next_node.id == 0:
                current_length = current_length + 1
                root = next_node
                previous_node.parent_node = next_node
                if previous_node not in next_node.children_nodes:
                    next_node.children_nodes.append(previous_node)
                break

            next_node = nodes_list[next_node.nextnode_id]
        t_node.length_joins = current_length  # update the length of join operations of current node
        if maximum_node:
            maximum_node = t_node if t_node.length_joins > maximum_node.length_joins else maximum_node
        else:
            maximum_node = t_node

    for t_node in top_level_nodes:
        if t_node is not maximum_node:
            short_nodes.append(t_node)

    return root, maximum_node, short_nodes


def code_length_join_with_numbers(maximum_node: Node, short_nodes: list[Node]):

    result = "{}, ".format(maximum_node.length_joins)
    current_node = maximum_node
    index = 1
    while current_node:
        current_node.joins_index = index
        index += 1
        current_node = current_node.parent_node

    for node in short_nodes:
        next_node = node.parent_node
        temp_sub_tree_length = 1
        while next_node.joins_index==1:
            temp_sub_tree_length +=1
            next_node = next_node.parent_node

        next_node.subtree_length = max(1,temp_sub_tree_length)

    current_node = maximum_node.parent_node

    branch_number = 1
    while current_node:
        if current_node.subtree_length>1:
            result += "({}, {}), ".format(current_node.joins_index,current_node.subtree_length)
            branch_number +=1
        current_node = current_node.parent_node
    return result,branch_number

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


letter = 65


def post_order_encoding(root: Node):
    global letter
    if root.children_nodes:
        for i, child in enumerate(root.children_nodes):
            post_order_encoding(child)

        # print("id: {}, parent_id: {}".format(root.id, root.name))
        if root.parent_node:
            root.code = chr(letter)
            letter += 1
            # print("id: {}, parent_id: {}".format(root.id, root.parent_node.id))
        else:
            # root node
            root.code = chr(letter)
            print(0)
            if letter >= 92:
                letter = 65
                return False
            else:
                letter = 65
                return True

    else:
        # print(root.id, root.name)
        root.code = chr(letter)
        letter += 1
        # print("id: {}, parent_id: {}".format(root.id, root.parent_node.id))


def post_order_visit(root: Node, result: str):
    if root.children_nodes:
        for i, child in enumerate(root.children_nodes):
            result = post_order_visit(child, result)

        # print("id: {}, parent_id: {}".format(root.id, root.name))
        if root.parent_node:
            print("code: {}, parent_code: {}".format(root.code, root.parent_node.code))
            # result += "{} {}\n".format(root.code, root.parent_node.code)
            result += "{}\n".format(root.parent_node.code)
            return result
        else:
            # root node
            print(root.code)
            # result += "{} ~\n".format(root.code)
            result += "{}\n~\n".format(root.code)
            return result
    else:
        # print(root.id, root.name)
        # result += "{} {}\n".format(root.code, root.parent_node.code)
        result += "{}\n".format(root.parent_node.code)

        return result
        # print("code: {}, parent_code: {}".format(root.code, root.parent_node.code))


def post_order_visit_integer(root: Node, result: str):
    if root.children_nodes:
        for i, child in enumerate(root.children_nodes):
            result = post_order_visit_integer(child, result)

        # print("id: {}, parent_id: {}".format(root.id, root.name))
        if root.parent_node:
            print("code: {}, parent_code: {}".format(root.code, root.parent_node.code))
            # result += "{} {}\n".format(root.code, root.parent_node.code)
            # result += "{} {} ".format(ord(root.code), ord(root.parent_node.code))
            result += "{} ".format(ord(root.parent_node.code))
            return result
        else:
            # root node
            print(root.code)
            # result += "{} ~\n".format(root.code)
            # result += "{} 1\n".format(ord(root.code))
            result += "{} 1\n".format(ord(root.code))
            return result
    else:
        # print(root.id, root.name)
        # result += "{} {}\n".format(root.code, root.parent_node.code)
        print("code: {}, parent_code: {}".format(root.code, root.parent_node.code))

        result += "{} ".format(ord(root.parent_node.code))

        return result
        # print("code: {}, parent_code: {}".format(root.code, root.parent_node.code))


if __name__ == "__main__":

    try:
        for i in range(1, 100):
            if i == 9 or i == 28 or i == 41 or i == 44:  # Q9 subquery
                continue

            queryName = "q{}".format(i)
            if i == 14 or i == 23 or i == 24 or i == 39:
                queryName = "q{}a".format(i)

            fileName = ""
            if i >= 1 and i <= 10:
                fileName = "../../results/q1-10/" + queryName + "_100.plan"
            elif i >= 11 and i <= 20:
                fileName = "../../results/q11-20/" + queryName + "_100.plan"
            elif i >= 21 and i <= 30:
                fileName = "../../results/q21-30/" + queryName + "_100.plan"
            elif i >= 31 and i <= 40:
                fileName = "../../results/q31-40/" + queryName + "_100.plan"
            elif i >= 41 and i <= 50:
                fileName = "../../results/q40-49/" + queryName + "_100.plan"
            elif i >= 51 and i <= 60:
                fileName = "../../results/q50-60/" + queryName + "_100.plan"
            elif i >= 51 and i <= 60:
                fileName = "../../results/q50-60/" + queryName + "_100.plan"
            elif i >= 61 and i <= 70:
                fileName = "../../results/q61-70/" + queryName + "_100.plan"
            else:
                fileName = "../../results/q71-99/" + queryName + "_100.plan"

            # fileName = "../results/wenxin/" + queryName + "_100GB.txt"
            # fileName = "../results/" + queryName + "_Plan.txt"
            planList = parsePhysicalPlan(fileName)
            # print(planList)
            top_level_nodes, nodes_list, adj_next = generate_adj(planList, qname=fileName + "_operators")
            root, maximum_node, short_nodes = extract_tree_structure(top_level_nodes, nodes_list, adj_next)
            print(queryName)
            code_tree,branch_num = code_length_join_with_numbers(maximum_node,short_nodes)
            code_tree_file_name = './code_tree_TPC_DS_branch_number_{}.txt'.format(branch_num)
            with open(code_tree_file_name, 'a') as file:
                if branch_num==2:
                    code_tree=code_tree.replace(",","")
                    code_tree=code_tree.replace("(","")
                    code_tree=code_tree.replace(")","")
                    file.write(code_tree + "\n")
                else:
                    file.write(queryName+"\n")
                    file.write(code_tree+"\n")

            # result = ""
            # use_letters_only = post_order_encoding(root)
            # if use_letters_only:
            #     print("------------encoding--------------------{}".format(queryName))
            #     # result = post_order_visit(root,result)
            #
            #     result = post_order_visit_integer(root, result)
            #     print(result)
            #     # print(len(join_nodes))
            #     # print(join_nodes)
            #     #
            #     sql_file_name = './dag_operators_post_order_parent_integer.txt'
            #     with open(sql_file_name, 'a') as file:
            #         file.write(result)


    except Exception as e:
        logging.exception("An error occurred:")
        print(e)
