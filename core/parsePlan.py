import sys,os
import re
from graphviz import Digraph

dot = Digraph(comment='The Round Table')


operatorList = [
    "ReusedExchange",
    "Scan parquet",
    "Project",
    #SMJ
    "SortMergeJoin",
    "Sort",
    #BHJ
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
        #去掉回车"\n"
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
        #if sort in plan
        # if "+- BroadcastExchange" in plan or ":- BroadcastExchange" in plan:
        #     jobNum0 += 1
        # if "+- Subquery subquery" in plan or ":- Subquery subquery" in plan:
        #     jobNum1 += 1
    #print(len(planList))
    #print("BroadcastExchange:  ", jobNum0, "   Subquery", jobNum1)
    jobNum = jobNum0 + jobNum1 + 1
    print("JobNum:  ", jobNum)
    return jobNum
    #return jobNum0 + jobNum1 + 1

def parseStageNum(planList, jobNum):
    shuffleNum = 0
    for plan in planList:
        if "+- Exchange hashpartitioning" in plan or "+- Exchange SinglePartition" in plan:
            shuffleNum += 1
    #print("ShuffleNum:   ", shuffleNum)
    stageNum = jobNum + shuffleNum
    print("StageNum:   ", stageNum)
    return stageNum


#提取出物理计划中的具体表
def parseTableInfo(planList):
    tableInfoDict = {}
    for plan in planList:
        if "FileScan parquet" in plan:
            # 对FileScan类型特殊处理，找到数据源的表
            #print(plan)
            leftIndex = plan.find("FileScan parquet")
            rightIndex = plan.find("[")
            tableName = plan[leftIndex + 16: rightIndex].split(".")[1]
            if tableName not in tableInfoDict:
                #找到相应表在hdfs上存储的位置
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
        print(tableLocation)
        os.system("bash hdfsCMD.sh " + tableLocation)
        f = open("tableInfo.txt", "r")  # 设置文件对象

        str = f.read().strip()  # 将txt文件的所有内容读入到字符串str中
        print(str)
        infoList = str.split(" ")
        tableInfoList = []
        for info in infoList:
            if info != "":
                tableInfoList.append(info)
        print(tableInfoList)
        f.close()  # 将文件关闭
    return tableInfoDict




#从计划中提取出物理操作符
def extractOperatorFromPlan(plan):
    for op in operatorList:
        if op in plan:
            if op != "Scan parquet":
                return op
            else:
                #对FileScan类型特殊处理，找到数据源的表
                leftIndex = plan.find("Scan parquet")
                rightIndex = plan.find("[")
                newOp = plan[leftIndex: leftIndex+ 16]  + plan[leftIndex+16: rightIndex]
                return newOp
    return "error"


#从计划中提取出根节点的名字
def extractRootOpFromPlan(plan):
    for op in rootList:
        if op in plan:
            return op
    return "error"




def generateTree(planList):
    planList = planList[1:]
    rootPlan = planList[0]
    rootName = extractRootOpFromPlan(rootPlan)
    dot.node("0", rootName)
    #order数组代表每个operator所在stage的顺序
    order = [0] * (len(planList) + 1)
    for i in range(1,len(planList)):
        order[i] = planList[i].find('-')
    dfs(1, planList, order, "0")
    #print(order)
    dot.view()
    dot.save("test.jpg")

# spark2 : planList[1:]  
# spark3 / spark3-formattd :planList[2:]  
# spark3-final: planList[3:]
def generate_adj(planList, qname="q",type="spark2"):
    n = len(planList) - 1
     # 名称; 所在图的层次（最后一个结点是0 或1;    stage号（op前的数字，无就-1）;  nextnode编号
    nodename, layer, stage, adj_next = [""], [0], [-1], [[] for i in range(n)]
    planList = planList[1:] if type=="spark2" else (planList[2:] if type=="spark3" else planList[3:] )
    # 1.1. Deal Root name  
    line = planList[0]
    nodename[0] = extractRootOpFromPlan(line)
    i0 = line.find("*(")
    stage[0] = int(line[i0+2: line.find(")")]) if i0 > 0 else -1
    # 1.2. Deal nonRoot node
    for id,line in enumerate(planList[1:],start=1):
        if line.strip() == "":
            break
        reuse_id = -1   # 重用哪个node
        # todo 注意q14b [i_item_sk#175, i_brand_id#182, i_class_id#184, i_category_id#186]
        # 注意 q23a 倒数第二个reuse   到底是用哪一个？
        # 注意data_id 有sum的情况
        if "ReusedExchange" in line:
            op = line[line.find("],")+3:].strip()
            data_id = line[line.find("[")+1 : line.find("]")].strip()  # [d_date_sk#71] 这些,叫啥 啥意思？ todo
            # 不能有[]符号，因为q14a 有Project [i_item_sk#26 AS ss_item_sk#18] 
            for ti,tline in enumerate(planList):
                if op in tline and data_id in planList[ti+1]:    # 一定会找到吧?
                    reuse_id = ti
                    break
            tmpname="ReusedExchange_%s"%reuse_id
        else:
            tmpname = extractOperatorFromPlan(line)
        i0 = line.find("-")
        if i0 < 0 :    # 应该不会有这种情况吧
            print(line)
            continue
        tmplayer = int(i0 / 3) + 1    # + 1, 因为根结点才是layer0
        i0 = line.find("*(")
        tmpstage = int(line[i0+2: line.find(")")]) if i0 >= 0 else -1
        # 2. Record
        nodename.append(tmpname)
        layer.append(tmplayer)
        stage.append(tmpstage)     
        # 3. Build adj
        for i in range(id-1,-1,-1):
            if layer[i] < tmplayer:
                if reuse_id < 0:
                    adj_next[id].append(i)
                else:
                    adj_next[reuse_id].append(i)
                break
    adj_next = adj_next[:len(nodename)]
    display_adj(nodename, adj_next)
    draw_adj(nodename,adj_next, filename=qname)
    print(stage)
    return nodename, adj_next, stage    # nodename == "ReusedExchange" 的可忽视

# list, tow_dim_list
def display_adj(nodename, adj_next):
    print("The ADJ is:==========")
    print("%10s %38s \t|\t %10s %38s" %("NodeID","NodeName","NextNodeID","NextNodeName"))
    print("-------------------------------------------------------------------------------------------------------------------")
    for i,nexti in enumerate(adj_next):
        ss = []
        for j in nexti:
            ss.append("(%s %s) "% (j, nodename[j]))
        print("%3s %45s \t|\t %s" %(i, nodename[i], ' '.join(ss)))


def draw_adj(id2name, adj_next,filename="onegraph"):
    dot = Digraph(comment='The Round Table')
    for i,name in enumerate(id2name):
        # if "ReusedExchange" not in name:
        dot.node(str(i), name)
    for i,nexti in enumerate(adj_next):
        for j in nexti:
            dot.edge(str(i), str(j))
    # dot.save(save_path)
    dot.render(filename)

#寻找两个sortJoin表的下标
def searchTwoJoinIndex(index, order):
    oneIndex = index + 1
    sortOrder = order[index + 1]
    otherIndex = -1
    for i in range(index + 2, len(planList)):
        if order[i] == sortOrder:
            otherIndex = i
            break
    return oneIndex, otherIndex

#     dfs(1, planList, order, "0")
def dfs(index, planList, order, curNode):
    #print("index =  ", index)
    if index < len(planList):
        plan = planList[index]
        op = extractOperatorFromPlan(plan)
        nextNode = str(index)
        dot.node(nextNode, op)
        # dot.edge(tail_name=curNode, head_name=nextNode)
        dot.edge(head_name=curNode,tail_name=nextNode)

        curNode = nextNode
        #寻找两个sort的表
        if op == "SortMergeJoin" or op == "BroadcastHashJoin":
            oneIndex, otherIndex = searchTwoJoinIndex(index, order)
            dfs(oneIndex, planList, order, curNode)
            dfs(otherIndex, planList, order, curNode)
        else:
            if order[index] <= order[index + 1]:
                dfs(index+1, planList, order, curNode)








if __name__ == "__main__":
    #fileName = sys.argv[1]
    queryName = "q1"
    fileName = "results/" + queryName + "_Plan.txt"
    # fileName = "results/q9_1GB_plan.txt"
    # fileName = "results/q1_Plan.txt"
    fileName = "../results/q2_Plan.txt"  #error
    fileName = "../results/q6_Plan.txt"
    fileName = "../results/q50_Plan.txt"
    fileName = "../results/q64_Plan.txt"
    fileName = "../results/q14a_Plan.txt"
    # fileName = "results/q23a_Plan.txt"  #error
    # fileName = "results/q14b_Plan.txt"  #error
    # fileFormattedName = "results/q1_Plan_formatted.txt"
    planList = parsePhysicalPlan(fileName)
    #print(planList)
    # jobNum = parseJobNum(planList)
    # stageNum = parseStageNum(planList, jobNum)
    #tableInfoDict = parseTableInfo(planList)
    # generateTree(planList)
    # generate_adj(parsePhysicalPlan(fileFormattedName),"q1","spark3")
    generate_adj(planList)   # 检查有无 ReusedExchange_-1  error

