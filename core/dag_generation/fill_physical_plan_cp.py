import random
from database import Table, Field, Source

store_returns = Table("store_returns", "f")
store_returns.addField(Field("sr_returned_date_sk", "identifier", "store_returns"))
store_returns.addField(Field("sr_return_time_sk", "identifier", "store_returns"))
store_returns.addField(Field("sr_item_sk", "identifier", "store_returns"))
store_returns.addField(Field("sr_customer_sk", "identifier", "store_returns"))
store_returns.addField(Field("sr_cdemo_sk", "identifier", "store_returns"))
store_returns.addField(Field("sr_hdemo_sk", "identifier", "store_returns"))
store_returns.addField(Field("sr_addr_sk", "identifier", "store_returns"))
store_returns.addField(Field("sr_store_sk", "identifier", "store_returns"))
store_returns.addField(Field("sr_reason_sk", "identifier", "store_returns"))
store_returns.addField(Field("sr_ticket_number", "identifier", "store_returns"))
store_returns.addField(Field("sr_return_quantity", "integer", "store_returns"))
store_returns.addField(Field("sr_return_amt", "decimal(7,2)", "store_returns"))
store_returns.addField(Field("sr_return_tax", "decimal(7,2)", "store_returns"))
store_returns.addField(Field("sr_return_amt_inc_tax", "decimal(7,2)", "store_returns"))
store_returns.addField(Field("sr_fee", "decimal(7,2)", "store_returns"))
store_returns.addField(Field("sr_return_ship_cost", "decimal(7,2)", "store_returns"))
store_returns.addField(Field("sr_refunded_cash", "decimal(7,2)", "store_returns"))
store_returns.addField(Field("sr_reversed_charge", "decimal(7,2)", "store_returns"))
store_returns.addField(Field("sr_store_credit", "decimal(7,2)", "store_returns"))
store_returns.addField(Field("sr_net_loss", "decimal(7,2)", "store_returns"))


date_dim = Table("date_dim", "d")
date_dim.addField(Field("d_date_sk", "identifier", "date_dim"))
date_dim.addField(Field("d_date_id", "char(16)", "date_dim"))
date_dim.addField(Field("d_date", "date", "date_dim"))
date_dim.addField(Field("d_month_seq", "integer", "date_dim"))
date_dim.addField(Field("d_week_seq", "integer", "date_dim"))
date_dim.addField(Field("d_quarter_seq", "integer", "date_dim"))
date_dim.addField(Field("d_year", "integer", "date_dim"))
date_dim.addField(Field("d_dow", "integer", "date_dim"))
date_dim.addField(Field("d_moy", "integer", "date_dim"))
date_dim.addField(Field("d_dom", "integer", "date_dim"))
date_dim.addField(Field("d_qoy", "integer", "date_dim"))
date_dim.addField(Field("d_fy_year", "integer", "date_dim"))
date_dim.addField(Field("d_fy_quarter_seq", "integer", "date_dim"))
date_dim.addField(Field("d_fy_week_seq", "integer", "date_dim"))
date_dim.addField(Field("d_day_name ", "char(9)", "date_dim"))
date_dim.addField(Field("d_quarter_name ", "char(1)", "date_dim"))
date_dim.addField(Field("d_holiday ", "char(1)", "date_dim"))
date_dim.addField(Field("d_weekend", "char(1)", "date_dim"))
date_dim.addField(Field("d_following_holiday", "char(1)", "date_dim"))
date_dim.addField(Field("d_first_dom", "integer", "date_dim"))
date_dim.addField(Field("d_last_dom", "integer", "date_dim"))
date_dim.addField(Field("d_same_day_ly", "integer", "date_dim"))
date_dim.addField(Field("d_same_day_lq", "integer", "date_dim"))
date_dim.addField(Field("d_current_day", "char(1)", "date_dim"))
date_dim.addField(Field("d_current_week", "char(1)", "date_dim"))
date_dim.addField(Field("d_current_month ", "char(1)", "date_dim"))
date_dim.addField(Field("d_current_quarter", "char(1)", "date_dim"))
date_dim.addField(Field("d_current_year", "char(1)", "date_dim"))

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

# 还要再统计一下 Ad Ad/Ad Af频率

all_fact = ["store_sales", "store_returns", "catalog_sales",
            "catalog_returns", "web_sales", "web_returns",
            "inventory"]

all_dim = ["store", "call_center", "catalog_page", "web_site",
           "web_page", "warehouse", "customer", "customer_address",
           "customer_demographics", "date_dim", "household_demographics",
           "item", "income_band", "promotion", "reason", "ship_mode",
           "time_dim", "dsdgen_version"]


# if Af, randomly choose one in all_fact. Suppose that it is store_return
# for each sublist, the first one is the key with foreign in ss, the second one is the foreign
# ss_key_with_foreign = [["sr_returned_date_sk","d_date_sk"],["return_time_sk","t_time"],["sr_customer_sk","c_customer_sk"]]
# store_sale_field = ["sr_returned_date_sk","sr_return_time_sk","sr_customer_sk","sr_cdemo_sk","sr_reason_sk","sr_return_amt "]
# date_dim_field = ["d_date_sk","d_date_id","d_date","d_week_seq"]
def get_random_table(indicator):
    if indicator == "d":
        return random.choice(all_fact)
    else:
        return random.choice(all_dim)

def findTableByRelationship(source):
    if source.tb1 is None: return
    # (Table, string, string)
    return random.choice(source.tb1.relationships)

# def __init__(self, name, data_type, source_table):
#     self.name = name # string
#     self.data_type = data_type # string
#     self.source_table = source_table
#     self.target_table = None
#     self.target_field = None

def fill_operator(topological_sorting_list):
    source_list = []
    source = Source()
    physical_file_name = 'physical_plan.txt'
    count = 0
    meet_hash = False
    for sub_list in topological_sorting_list:
        # print("-----------sublist----------")
        # print(sub_list)
        count = count + 1
        # if count == 3: return
        for i in range(len(sub_list)):
            # print((sub_list[i]))
            if (sub_list[i] == 'FileScan parquetf'):
                sub_list[i] = sub_list[i][:-1]
                # if tb1 is None, randomly choose a table from D, suppose it is store_returns
                # for source, initialize tb1 and all_field1
                if source.tb1 is None:
                    # tb = get_random_table("f")
                    print("in f, tb1 is None")
                    tb = "store_returns"
                    source.tb1 = table_dict[tb]
                    source.all_fields1 = source.tb1.fields
                    # FileScan parquet tpcds_100_parquet.date_dim[d_date_sk# 24,d_year#30]
                    physical_plan_line = 'FileScan parquet ' + db_name + "." + tb + "["
                    for field in source.all_fields1:
                        physical_plan_line = physical_plan_line + field.name + ", "
                    physical_plan_line = physical_plan_line[:-2] + "]"
                    # tb1有可能是“”, 这个时候记得all field1可以更新下

                # if tb1 is Not None, then we decide another table from source.relationship,
                # which alse needs to be f
                # for source, initialize tb2 and all_field2
                else:
                    print("in d, tb1 is None")
                    # 同时·确定join field1 和 2
                    # tb, source.join_field1, source.join_field2 = findTableByRelationship(source)
                    tb = "date_dim"
                    source.join_field1 = "sr_returned_date_sk"
                    source.join_field2 = "d_date_sk"
                    source.tb2 = table_dict[tb]
                    source.all_fields2 = source.tb2.fields

                    physical_plan_line = 'FileScan parquet ' + db_name + "." + tb + "["
                    for field in source.all_fields2:
                        physical_plan_line = physical_plan_line + field.name + ", "
                    physical_plan_line = physical_plan_line[:-2] + "]"
                    # # get by relationship of tb1
                    # tb = "store_returns"
                    # print("in f, tb1 is not None")
                    # # 同时·确定join field1 和 2
                    # source.tb2 = table_dict[tb]
                    # source.all_fields2 = source.tb2.fields
                    # # if the second table is filled, then set isComplete to be one.
                    # # and them we ca
                    # source.isComplete = True

            elif sub_list[i] == 'FileScan parquetd':
                sub_list[i] = sub_list[i][:-1]
                # if tb1 is None, randomly choose a table from D, suppose it is store_returns
                # for source, initialize tb1 and all_field1
                if source.tb1 is None:
                    # tb = get_random_table("d")
                    tb = "date_dim"
                    print("in d, tb1 is None")
                    source.tb1 = table_dict[tb]
                    source.all_fields1 = source.tb1.fields
                    physical_plan_line = 'FileScan parquet ' + db_name + "." + tb + "["
                    for field in source.all_fields1:
                        physical_plan_line = physical_plan_line + field.name + ", "
                    physical_plan_line = physical_plan_line[:-2] + "]"
                    # tb1有可能是“”, 这个时候记得all field1可以更新下

                # if tb1 is Not None, then we decide another table from source.relationship,
                # which alse needs to be f
                # for source, initialize tb2 and all_field2
                else:
                    # get by relationship of tb1

                    print("in d, tb1 is None")
                    # 同时·确定join field1 和 2
                    # tb, source.join_field1, source.join_field2 = findTableByRelationship(source)
                    tb = "date_dim"
                    source.join_field1 = "sr_returned_date_sk"
                    source.join_field2 = "d_date_sk"
                    source.tb2 = table_dict[tb]
                    source.all_fields2 = source.tb2.fields
                    physical_plan_line = 'FileScan parquet ' + db_name + "." + tb + "["
                    for field in source.all_fields2:
                        physical_plan_line = physical_plan_line + field.name + ", "
                    physical_plan_line = physical_plan_line[:-2] + "]"
                    # 同时·确定join field1 和 2


                    # if the second table is filled, then set isComplete to be one.
                    # and them we ca
                    source.isComplete = True

            elif sub_list[i] == 'Filter': #------------------field的id还没加
                if source.tb1 is not None and source.tb2 is None:
                    # 对all_field1全部isnotnull
                    physical_plan_line = "Filter"
                    for field in source.tb1.fields:
                        physical_plan_line = physical_plan_line + " isnotnull(" + field.name + ")" + " AND"
                    physical_plan_line = physical_plan_line[:-4]
                elif source.tb2 is not None:
                    # 对all_field2全部isnotnull
                    physical_plan_line = "Filter "
                    for field in source.tb2.fields:
                        physical_plan_line = physical_plan_line + "isnotnull(" + field.name + ")" + " AND"
                    physical_plan_line = physical_plan_line[:-4]

            elif sub_list[i] == 'Project':
                physical_plan_line = 'Project ['
                if source.tb1 is not None and source.tb2 is None:
                    for field in source.all_fields1:
                        physical_plan_line = physical_plan_line + field.name + ", "
                    physical_plan_line = physical_plan_line[:-2] + "]"
                elif source.tb2 is not None:
                    for field in source.all_fields2:
                        physical_plan_line = physical_plan_line + field.name + ", "
                    physical_plan_line = physical_plan_line[:-2] + "]"
                # Project[d_date_sk# 24]

            elif sub_list[i] == 'Sort':
                # randomly pick 1 from all_field
                pass

            elif sub_list[i] == 'BroadcastHashJoin':
                # 如果在list的末尾 直接跳过
                if i == len(sub_list) - 1: continue
                # 如果在开头 先把旧的写到文件里，再存source，在创建一个新的source
                physical_plan_line = 'BroadcastHashJoin'
                # BroadcastHashJoin[sr_returned_date_sk# 4], [d_date_sk#24], Inner, BuildRight, false
                physical_plan_line = physical_plan_line + " [" + source.join_field1 + "], [" + source.join_field2 + "], Inner, BuildRight, false"

                source_list.append(source)
                source_tmp = source
                source = Source()
                source.tb1 = ""
                source.all_fields1 = source_tmp.all_fields1

            elif sub_list[i] == 'SortMergeJoin':
                # SortMergeJoin[d_week_seq1# 0], [(d_week_seq2#8 - 53)], Inner
                if i == len(sub_list) - 1: continue
                # 如果在开头 先把旧的写到文件里，再存source，在创建一个新的source
                physical_plan_line = 'SortMergeJoin'
                # BroadcastHashJoin[sr_returned_date_sk# 4], [d_date_sk#24], Inner, BuildRight, false
                physical_plan_line = physical_plan_line + " [" + source.join_field1 + "], [" + source.join_field2 + "], Inner"

                source_list.append(source)
                source_tmp = source
                source = Source()
                source.tb1 = ""
                source.all_fields1 = source_tmp.all_fields1

            elif sub_list[i] == 'HashAggregate':
                # HashAggregate(keys=[sr_customer_sk# 7, sr_store_sk#11], functions=[partial_sum(sr_return_amt#15)])
                physical_plan_line = "HashAggregate(keys=["
                if source.tb1 is not None and source.tb2 is None:
                    random_number = random.randint(2, 3)
                    tmp_hash_list = random.sample(source.all_fields1, random_number)

                    for i in range(random_number - 1):
                        physical_plan_line = physical_plan_line + tmp_hash_list[i].name + ", "
                    if not meet_hash:
                        physical_plan_line = physical_plan_line[:-2] + "], functions=[partial_sum(" + tmp_hash_list[-1].name + ")])"
                    else:
                        meet_hash = True
                        physical_plan_line = physical_plan_line[:-2] + "], functions=[sum(" + tmp_hash_list[-1].name + ")])"

                elif source.tb2 is not None:
                    random_number = random.randint(2, 3)
                    tmp_hash_list = random.sample(source.all_fields2, random_number)

                    for i in range(random_number - 1):
                        physical_plan_line = physical_plan_line + tmp_hash_list[i].name + ", "
                    if not meet_hash:
                        physical_plan_line = physical_plan_line[:-2] + "], functions=[partial_sum(" + tmp_hash_list[
                            -1].name + ")])"
                    else:
                        meet_hash = True
                        physical_plan_line = physical_plan_line[:-2] + "], functions=[sum(" + tmp_hash_list[
                            -1].name + ")])"


            elif sub_list[i] == 'TakeOrderedAndProject':
                limit = random.randint(50, 100)
                # TakeOrderedAndProject(limit=100, orderBy=[c_customer_id#82 ASC NULLS FIRST], output=[c_customer_id#82])
                physical_plan_line = 'TakeOrderedAndProject(limit=' + str(limit) +", orderBy=["

                orderby_field = random.choice(source.all_fields1).name

                physical_plan_line = physical_plan_line + orderby_field + " ASC NULLS FIRST], output=[" + orderby_field + "])"

            elif sub_list[i] == '': continue
            with open(physical_file_name, 'a') as file:
                file.write(physical_plan_line + '\n')
            print(physical_plan_line)

            # 遇到A: 第一次 随机选一个表
            # 第二次 随机选另一个有交集的表 这个时候join key也能确定
            # 如果是 project 全选
            # filter 全选(isnotnull)
            # sort 选其中一个排一下就行
            # BHJ/SMJ 如果在最后一个那么在第二次遇到的时候打印一下（join field知道了 然后尾巴那里还有一些参数要选一下）
            # 同时还要新建下一个source 把已有的存一下




if __name__ == "__main__":
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
    fill_operator(topological_sorting_list)
