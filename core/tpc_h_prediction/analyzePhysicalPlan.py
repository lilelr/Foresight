import sys,os

sys.path.append(os.path.abspath("utils/"))
import QueryUtils as Q




if __name__ == "__main__":
    queryName = str(sys.argv[1])
    dataSize = str(sys.argv[2])
    planPath = "results/" + queryName + "_" + dataSize + "GB_plan.txt"
    # 解析并生成物理计划,写入到results文件夹下
    Q.generatePhysicalPlan(queryName, dataSize)
    # 判断物理计划是否存在
    if not os.path.exists(planPath):
        print("corrsponding physical plan not exist! Please check dataSize if exists in database!")
        exit()
    #将每一行的物理计划，写到一个list中（包括第一行的== Physical Plan ==)
    planList = Q.generatePhysicalPlanList(planPath)
    jobNum = Q.getJobNum(planList)
    stageNum = Q.getStageNum(planList, jobNum)
    tableInfoDict = Q.getTableInfo(planList)
    print("JobNum:  ", jobNum)
    print("StageNum:  ",stageNum)
    print(tableInfoDict)


