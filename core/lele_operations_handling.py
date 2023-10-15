import re

from core.tables import InputTable, JoinTable, SortMergeJoinTable, HashAggreTable


def handle_filter_with_input_table(input_table, line: str):
    line = line[6:]
    line = re.sub(r'L', '', line)
    line = re.sub(r'#\d+', '', line)

    line_list = line.split("AND")

    for i, line_cut in enumerate(line_list):
        # print("line_cut")
        # print(line_cut)
        field_name = "empty"
        field_filter = "empty"

        if "OR" in line_cut:
            line_cut = line_cut.split("OR")
            line_cut = line_cut[0]

        if "Subquery" in line_cut:
            continue

        if "isnotnull" in line_cut:
            if "avg" not in line_cut and "sum" not in line_cut:
                index_null = line_cut.find("isnotnull")
                leftIndex = line_cut.find("(", index_null)
                rightIndex = line_cut.find(")", leftIndex)
                field_name = line_cut[leftIndex + 1:rightIndex]
                field_filter = " IS NOT NULL"
                input_table.add_field_filters(field_name, field_filter)
            else:
                print("meet avg/sum in isnotnull in handle_filter_with_input_table()")

        elif "=" in line_cut or ">" in line_cut or "<" in line_cut:
            line_cut_pos = line_cut + "=><"  # to make sure that the result of .find != -1
            position = min(line_cut_pos.find("="), line_cut_pos.find(">"), line_cut_pos.find("<"))
            # deal with field name eg. (cast(d_date as date) >= 2000-08-23))
            field_name = line_cut[0:position].strip()
            if "cast" in field_name:
                fn_left_index = field_name.find("cast")
                fn_right_index = field_name.find(")", fn_left_index)
                field_name = field_name[fn_left_index:fn_right_index + 1]
            else:
                # find rightmost "(" and leftmost ")"
                """ eg:
                (((p_channel_email = N)
                     ^                ^
                rightmost_index      leftmost_index
                """
                # (i_manufact_id
                fn_right_index = field_name.rfind("(")  # if not found, = -1
                # if rightmost_index == -1:
                #     rightmost_index = 0
                fn_left_index = field_name.find(")", fn_right_index + 1)

                # fn_plus_filter = line_cut[rightmost_index + 1:leftmost_index]
                # fn_plus_filter_list = fn_plus_filter.split()
                if fn_left_index == -1:
                    field_name = field_name[fn_right_index + 1:]
                else:
                    field_name = field_name[fn_right_index + 1:fn_left_index]

            # deal with field filter
            field_filter = line_cut[position:]
            ff_left_index = 0
            ff_right_index = field_filter.find(")")
            field_filter = " " + field_filter[ff_left_index:ff_right_index]
            input_table.add_field_filters(field_name, field_filter)

        else:
            input_table.special_str_list.append(line_cut)
            print("special line_cut is stored")
        # print("-------------------------")
        # print(field_name)
        # print(field_filter)
    return input_table

 # -----------------lele------------------------
# def handle_filter_with_input_table(input_table, line: str):
    # """
    # :param input_table: the table that should add filter conditions
    # :param line: physical plan of the filter operation
    # :return: table
    # """
    #
    # # Filter (((p_channel_email#95 = N) OR (p_channel_event#100 = N)) AND isnotnull(p_promo_sk#86))
    #
    # #Filter handling methods
    # # 1  delete the "#id", then split by 'AND' , return an array of items
    # #    import re
    # #    line = re.sub(r"""#\d+""", " ", line)
    # #
    # # 2  For each item, judge whether isnotnull
    # #    isnotnull(d_moy),  (d_moy= 11)), (((p_channel_email = N) OR (p_channel_event = N))
    # # 2.1 if isnotnull exists, select the "()"
    # # 2.2 if ' = ' exists, parenthesis; multiple '=' exists, select the first one
    # # 2.3 other items save into the special_str field
    # line = line[6:]
    #
    # # eg.line_list = ['((isnotnull(d_year#30)', 'AND', '(d_year#30', '=', '2000))', 'AND', 'isnotnull(d_date_sk#24))']
    # line_list = line.split()
    # # print(line_list)
    # for i, line_cut in enumerate(line_list):
    #
    #
    #     #------------------------------------------------------------------------------
    #     # print(line_cut)
    #     # OR还未处理，需要用别的query测试。
    #     if "AND" in line_cut or "OR" in line_cut:
    #         continue
    #     # flag_notnull = False this para is used for # Filter isnotnull((avg(ctr_total_return) * 1.2)#103): in this example, leftIndex = -1  未处理
    #     if "#" in line_cut:
    #         if "isnotnull" in line_cut and "avg" not in line_cut and "sum" not in line_cut:
    #             index_null = line_cut.find("isnotnull")
    #             leftIndex = line_cut.find("(", index_null)
    #             rightIndex = line_cut.find("#")
    #             field_name = line_cut[leftIndex + 1:rightIndex]
    #             field_filter = " IS NOT NULL"
    #             input_table.add_field_filters(field_name, field_filter)
    #             # print(input_table.fields)
    #         elif "isnotnull" in line_cut and "avg" in line_cut or "sum" in line_cut:
    #             print("meet avg/sum")
    #
    #         else:
    #             leftIndex = line_cut.find("(")
    #
    #             if leftIndex != -1:
    #                 # Filter isnotnull((avg(ctr_total_return) * 1.2)#103): in this example, leftIndex = -1  未处理
    #                 # ((isnotnull(d_year#30)'
    #                 # 'AND', '(d_year#30', '=', '2000))'
    #                 rightIndex = line_cut.find("#")
    #                 field_name = line_cut[leftIndex + 1:rightIndex]
    #                 field_filter = ""
    #
    #                 # if flag_notnull:
    #                 # 先不管'ctr_total_return'
    #                 # if temp_input_table.hasFieldName(field_name):
    #                 #     temp_input_table.addFieldFilters(field_name, "NOT NULL")
    #                 # continue
    #
    #                 op_line_cut = line_list[i + 1]
    #                 num = line_list[i + 2]
    #                 num = num[:num.find(")")]
    #                 right_part = str(num)
    #                 if right_part.isalpha(): # they are all letters, e.g., s_state#76 = TN
    #                     field_filter = op_line_cut + " '" + str(num)+ "'"
    #                 else:
    #                     field_filter = op_line_cut + " " + str(num) + " "
    #
    #                 # field_filter = op_line_cut + " " + str(num)
    #
    #
    #                 input_table.add_field_filters(field_name, field_filter)
    #
    #                 # 未处理OR
    #                 # if not the first one, then it will be an "AND" or "OR" before
    #                 # if i != 0:
    #                 #     if line_list[i - 1] == "AND" or line_list[i - 1] == "OR":
    #                 #         sql = line_list[i - 1] + " " + sql
    #                 # print(sql)
    #             else:
    #                 print("can't find (")
    #             # ------------------------------------------------------------------------------
    # return input_table


def handle_project(temp, line):
    """
    handling the project operation
    :param temp: InputTable or JoinTable
    :param line: the physical line of the plan
    :rtype: temp
    """
    leftIndex = line.find("[")
    rightIndex = line.find("]")
    # Project [sr_customer_sk#7, sr_store_sk#11, sr_return_amt#15]

    fields_str = str(line[leftIndex + 1:rightIndex])
    # # print(fields_str)
    sentences = fields_str.split(",")
    # # print((fields_item))
    for sentence in sentences:
        if sentence.find("AS") !=-1:
            original_alias = sentence.split("AS")
            field_name = original_alias[0].split("#")[0].strip()
            alia_name = original_alias[1].split("#")[0].strip()
            temp.add_project(alia_name)
            # e.g., ws_sold_date_sk AS sold_date_sk, we will have sold_date_sk -> ws_sold_date_sk
            temp.origin_field_names[alia_name] = field_name
        else:

            temp_items = sentence.split("#")
            field_name = temp_items[0].strip()
            temp.add_project(field_name)

    return temp


def handle_sort(temp, line):
    """
    handling the project operation
    :param temp: InputTable or JoinTable
    :param line: the physical line of the plan
    :rtype: temp
    """
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
        temp.sort.append(field_name)

    return temp


def handle_hash_aggregate_joint_table(temp: JoinTable, line: str) -> JoinTable:
    line = re.sub(r"""#\d+""", " ", line) # delete digits, such as #12
    # line = re.sub(r"""\s+""", "", line)
    # print(line)

    keys_index = line.find("keys=")
    left_index = line.find("[", keys_index)
    right_index = line.find("]", left_index)
    keys_str = line[left_index + 1:right_index]
    keys_str = re.sub(r"""\s+""", "", keys_str) # delete the spaces
    # print(keys_str)  # e.g., d_week_seq,c_first_name,c_last_name
    key_items = keys_str.split(",")
    temp.hash_agg = key_items
    # for i, item in enumerate(key_items):
    #     print(i, item)

    functions_index = line.find("functions=")
    left_index = line.find("[", functions_index)
    right_index = line.find("]", left_index)
    functions_str = line[left_index + 1:right_index]  # e.g., functions=[avg(ctr_total_return), avg(ctr_abc)])
    # print(functions_str)
    function_items = functions_str.split(",")
    temp.hash_agg_functions = function_items
    return temp


def handle_hash_aggregate(temp, line, get_with_table_index):
    """
    handling the project operation
    :param temp: InputTable or JoinTable
    :param line: the physical line of the plan
    :rtype: temp
    """
    temp_hash_aggre_table = HashAggreTable(table_name=temp.with_table_name,
                                           with_table_name=get_with_table_index)
    # HashAggregate(keys=[sr_customer_sk#7, sr_store_sk#11], functions=[sum(sr_return_amt#15)])
    leftIndex = line.find("[")
    rightIndex = line.find("]")
    fields_str = str(line[leftIndex + 1:rightIndex])
    # print(fields_str)
    fields_item = fields_str.split(",")
    # print(fields_item)
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
    rightIndex = line.find("#", leftIndex)
    functions = line[leftIndex + 1:rightIndex] + ")"  # eg.sum(sr_return_amt)
    # attention: do not consider: line != "Filter isnotnull((avgf(ctr_total_return) * 1.2)#103)
    temp_hash_aggre_table.addFunctions(functions)

    return temp


def handle_two_join_fields(line:str):
    """
    :param line:  e.g., BroadcastHashJoin [ctr_store_sk#2], [s_store_sk#52], Inner, BuildRight, false
    :return:   ctr_store_sk,s_store_sk
    """
    leftIndex = line.find("[") + 1
    rightIndex = line.find("#")
    first_field = line[leftIndex:rightIndex]
    leftIndex = line.find("[", rightIndex) + 1
    rightIndex = line.find("#", leftIndex)
    second_field = line[leftIndex:rightIndex]
    return first_field, second_field