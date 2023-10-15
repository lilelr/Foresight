from core.edit_distance import edit_dist, best_fit


# 考虑 Node 对象
# record table name, field name
class InputTable:
    def __init__(self, line, with_table_index):
        self.table_name = ""  # e.g., store_returns
        self.fields = {}  # key-> field_name , value-> field_id, e.g. ctr_store_sk(key) : 12 (value)
        self.with_table_name = "tb" + with_table_index
        self.project_field = []
        self.special_str_list=[]
        self.origin_field_names = {}  # key: alias_name , value: original_name
        # e.g., ws_sold_date_sk AS sold_date_sk, we will have sold_date_sk -> ws_sold_date_sk
        self.is_broadcast_exchange = False
        # eg1
        # FileScan parquet tpcds_500_parquet.promotion[p_promo_sk  # 86,p_channel_email#95,p_channel_event#100]
        # Batched: true, DataFilters: [((p_channel_email#95 = N) OR (p_channel_event#100 = N)), isnotnull(
        # p_promo_sk#86)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[
        # hdfs://p09:8999/BenchmarkData/Tpcds/tpcds_500/tpcds_parquet/promotion], PartitionFilters: [],
        # PushedFilters: [Or(EqualTo(p_channel_email,N),EqualTo(p_channel_event,N)), IsNotNull(p_promo_sk)],
        # ReadSchema: struct<p_promo_sk:int,p_channel_email:string,p_channel_event:string>

        # eg2
        # FileScan parquet tpcds_100_parquet.customer[c_customer_sk#81,
        # c_customer_id#82] Batched: true, DataFilters: [isnotnull(c_customer_sk#81)], Format: Parquet,
        # Location: InMemoryFileIndex(1 paths)[hdfs://p09:8999/BenchmarkData/Tpcds/tpcds_100/tpcds_parquet/customer],
        # PartitionFilters: [], PushedFilters: [IsNotNull(c_customer_sk)], ReadSchema: struct<c_customer_sk:int,
        # c_customer_id:string>

        # extract table name
        leftIndex = line.find(".")
        rightIndex = line.find("[")
        tablename = line[leftIndex + 1:rightIndex]
        self.table_name = tablename

        # extract field
        # 目前只处理了and，未处理or
        # print(line)
        right_bracket = line.find("]")
        fields_str = str(line[rightIndex + 1:right_bracket])
        # print(fields_str)
        fields_item = fields_str.split(",")

        # print((fields_item))
        for word in fields_item:
            temp_items = word.split("#")
            field_name, field_id = temp_items[0], temp_items[1]
            field_id=str(field_id).replace("L","",-1)
            # print(field_name, field_id)
            field_instance = TableField(field_id=int(field_id))
            # print("="+field_name)
            self.fields[field_name] = field_instance
            if field_name not in self.project_field:
                self.project_field.append(field_name)

    def has_field_name(self, field_name):
        if field_name in self.fields:
            return True
        return False

    def add_field_filters(self, field_name, field_filter):
        if field_name not in self.project_field:
            # particularly handling input table with moving all filters to the project fields
            self.project_field.append(field_name)
        self.fields[field_name].add_table_field_filters(field_filter)

    def add_project(self, project_field):
        if project_field not in self.project_field:
            self.project_field.append(project_field)

    def show_details_for_field(self, field_name):
        self.fields[field_name].show_details_for_field()

    # def getWithTableName(self):
    #     return self.with_table_name

    def is_project_field_empty(self):
        return len(self.project_field) == 0

    def handle_take_ordered_and_project(self, line):
        print("final take ordered and project: " + line)

    def printSQL(self, use_as=True):
        """
        input tables do not need with structure
        :return:
         input table sql
        """
        if use_as:
            sql1 = self.with_table_name + " AS ("
        else:
            sql1 = ""
        sql2 = "\nSELECT "
        if self.is_project_field_empty():
            for key in self.fields:
                sql2 = sql2 + key + ", "
        else:
            for pf in self.project_field:
                if pf in self.origin_field_names:  # we have AS semantic, such as d_week_seq AS d_week_seq1
                    new_pf = self.origin_field_names[pf] + " AS " + pf
                    sql2 = sql2 + new_pf + ", "
                else:
                    sql2 = sql2 + pf + " , "
        sql2 = sql2[:-2]

        sql3 = "\nFROM " + self.table_name

        # if
        hasConstraints = False
        sql4 = "\nWHERE "
        # t1
        for fn, tf in self.fields.items():
            # print(fn,tf.field_filters)
            if len(tf.field_filters):
                for v in tf.field_filters:
                    # print()
                    # AND tb1.a > 100 AND tb1.b < 30
                    sql4 = sql4 + fn + v + " AND "
                    hasConstraints = True
        if hasConstraints:
            sql4 = sql4[:-5]
        else:
            sql4 = ""
        result_sql = sql1 + sql2 + sql3 + sql4 + "\n"
        if use_as:
            result_sql += ")"
        print(result_sql)
        return result_sql

    def __repr__(self):
        return repr(
            (self.name, "actual_size:" + str(self.actual_size), "cost_per_column (sec):" + str(self.cost_per_column)))


# record the detailed
class TableField:
    def __init__(self, field_id=-2):
        self.field_id = field_id
        self.field_filters = []

    def add_table_field_filters(self, str_filter):
        self.field_filters.append(str_filter)

    def show_details_for_field(self):
        return str("field_id:" + str(self.field_id) + " " + "field_filters:" + str(self.field_filters))


class JoinTable:
    # class static members
    alias_fields = {}
    # the match between the renamed fields. renamed name  (key) ->  original_name (value)
    # e.g., ctr_store_sk -> sr_store_sk
    alias_fields_no_tbname = {}

    def __init__(self, t1="", t2="", with_table_name="", t1_table=None, t2_table=None):
        self.t1 = t1
        self.t2 = t2
        self.t1_table = t1_table
        self.t2_table = t2_table
        # self.table = [] # FROM
        # self.constraint = {} # WHERE:key->field_name (string); constraint (string)
        self.join = []
        # self.hash_agg = []  # WHERE
        self.fields = {}
        self.project_field = []  # SELECT
        self.origin_field_names = {}  # key: alias_name , value: original_name
        # e.g., ws_sold_date_sk AS sold_date_sk, we will have sold_date_sk -> ws_sold_date_sk
        self.sort = []
        self.with_table_name = "tb" + with_table_name
        self.hash_agg = []
        self.hash_agg_functions = []
        self.is_sort_merge_join = False
        self.is_broadcast_exchange = False
        self.is_union = False

        self.special_str_list = []

    def is_project_field_empty(self):
        return len(self.project_field) == 0

    def add_field_filters(self, field_name, field_filter):
        if field_name not in self.fields:
            self.fields[field_name] = TableField()
        self.fields[field_name].add_table_field_filters(field_filter)

    def add_join_constraint(self, join_constraint):
        self.join.append(join_constraint)  # field_a,field_b

    def add_hash_agg_constraint(self, hash_agg):
        self.hash_agg.append(hash_agg)

    def add_project(self, project_field):
        if project_field not in self.project_field:
            self.project_field.append(project_field)

    def has_hash_agg_constraint(self, field_name):
        if field_name in self.hash_agg:
            return True
        return False

    def handle_take_ordered_and_project(self, line):
        print("final take ordered and project: " + line)

    def printSQL(self, use_as=True):
        if use_as:
            sql1 = self.with_table_name + " AS ("
        else:
            sql1 = ""

        if self.is_union:  # cope with the particularly union join node

            # sql2 = "\n" + self.t1_table.with_table_name + "  \n UNION ALL \n " + self.t2_table.with_table_name
            sql2 = "\n" + self.t1_table.printSQL(use_as=False) + "  \n UNION ALL \n " + self.t2_table.printSQL(
                use_as=False)

            result_sql = sql1 + sql2 + "\n"
            if use_as:
                result_sql += ")"
            return result_sql

        # SELECT
        sql2 = "\nSELECT "
        if self.is_project_field_empty():
            sql2 = sql2 + "*"
        else:

            for i, pf in enumerate(self.project_field):
                if pf in self.t1_table.project_field or pf in self.t1_table.fields:
                    table_name_pf = self.t1_table.with_table_name + "." + pf
                elif pf in self.t2_table.project_field or pf in self.t2_table.fields:
                    table_name_pf = self.t2_table.with_table_name + "." + pf
                else:
                    # table_name_pf = self.t2_table.with_table_name + ".*"
                    # if pf in self.alias_fields:
                    #     table_name_pf = self.alias_fields[pf]
                    # else:
                    result_tb_name, min_item = best_fit(current_filter_name=pf,
                                                        tb1_name=self.t1_table.with_table_name,
                                                        tb1_fields=self.t1_table.project_field,
                                                        tb2_name=self.t2_table.with_table_name,
                                                        tb2_fields=self.t2_table.project_field)
                    table_name_pf = result_tb_name + "." + min_item
                    self.project_field[i] = min_item
                    self.alias_fields_no_tbname[pf] = min_item

                sql2 = sql2 + table_name_pf + ", "
        sql2 = sql2[:-2]  # delete ", "

        if self.hash_agg_functions:
            aggregate_items = ",".join(self.hash_agg_functions)
            sql2 += ", "
            sql2 += aggregate_items

        # FROM
        sql3 = "\nFROM " + self.t1_table.with_table_name + " , " + self.t2_table.with_table_name

        # WHERE
        # constraints in BHJ:
        sql4 = "\nWHERE "
        first_field_join = self.join[0]
        second_field_join = self.join[1]

        first_field_join = self.alias_fields_no_tbname[
            first_field_join] if first_field_join in self.alias_fields_no_tbname else first_field_join

        second_field_join = self.alias_fields_no_tbname[
            second_field_join] if second_field_join in self.alias_fields_no_tbname else second_field_join

        if first_field_join in self.t1_table.project_field:
            sql4 += self.t1_table.with_table_name + "." + first_field_join
        elif first_field_join in self.t2_table.project_field:
            sql4 += self.t2_table.with_table_name + "." + first_field_join
        else:
            # if first_field_join not in self.alias_fields:
            if first_field_join in self.alias_fields_no_tbname:
                sql4 += self.alias_fields_no_tbname[first_field_join]
            else:
                result_tb_name, min_item = best_fit(current_filter_name=first_field_join,
                                                    tb1_name=self.t1_table.with_table_name,
                                                    tb1_fields=self.t1_table.project_field,
                                                    tb2_name=self.t2_table.with_table_name,
                                                    tb2_fields=self.t2_table.project_field)
                # self.alias_fields[first_field_join] = result_tb_name + "." + min_item
                sql4 += result_tb_name + "." + min_item

        sql4 += " = "

        if second_field_join in self.t2_table.fields:
            sql4 += self.t2_table.with_table_name + "." + second_field_join
        elif second_field_join in self.t1_table.fields:
            sql4 += self.t1_table.with_table_name + "." + second_field_join
        else:
            result_tb_name, min_item = best_fit(current_filter_name=second_field_join,
                                                tb1_name=self.t2_table.with_table_name,
                                                tb1_fields=self.t2_table.project_field,
                                                tb2_name=self.t1_table.with_table_name,
                                                tb2_fields=self.t1_table.project_field)
            # self.alias_fields[second_field_join] = result_tb_name + "." + min_item

            sql4 += result_tb_name + "." + min_item

        # sql4 = sql4 + first_field_join + " = " + second_field_join

        hasConstraints = False
        sql5 = "\nAND "
        # t1
        for fn, tf in self.fields.items():
            # print(fn,tf.field_filters) # ctr_total_return, is not null
            table_filter_name = ""
            if fn in self.t1_table.project_field:
                table_filter_name += self.t1_table.with_table_name + "." + fn
            elif fn in self.t2_table.project_field:
                table_filter_name += self.t2_table.with_table_name + "." + fn
            else:
                print("cannot find the fn, using the edit distance to help")
                # if fn not in self.alias_fields:
                result_tb_name, min_item = best_fit(current_filter_name=fn,
                                                    tb1_name=self.t1_table.with_table_name,
                                                    tb1_fields=self.t1_table.project_field,
                                                    tb2_name=self.t2_table.with_table_name,
                                                    tb2_fields=self.t2_table.project_field)
                # self.alias_fields[fn] = result_tb_name + "." + min_item
                table_filter_name = result_tb_name + "." + min_item
            if len(tf.field_filters):
                for v in tf.field_filters:
                    # print()
                    # AND tb1.a > 100 AND tb1.b < 30
                    sql5 = sql5 + table_filter_name + v + " AND "
                    hasConstraints = True
        if hasConstraints:  # delete the last " AND " (the length is 5)
            sql5 = sql5[:-5]
        else:
            sql5 = ""
        # print(sql1 + sql2 + sql3 + sql4 + sql5 + "\n" + ")")
        result_sql = sql1 + sql2 + sql3 + sql4 + sql5 + "\n"
        if self.hash_agg:
            sql6_group_by = "GROUP BY " + ",".join(self.hash_agg)+"\n"

        else:
            sql6_group_by = ""
        result_sql += sql6_group_by

        if use_as:
            result_sql += ")"
        print(result_sql)
        return result_sql


class SortMergeJoinTable(JoinTable):
    def __init__(self):
        self.name = "SortMergeJoin"
        self.special_str_list = []


# HashAggregate(keys=[sr_customer_sk#7, sr_store_sk#11], functions=[sum(sr_return_amt#15)])


class HashAggreTable:
    def __init__(self, table_name="", with_table_name=""):
        self.keys = []
        self.table_name = table_name
        self.functions = ""
        self.with_table_name = "tb"+with_table_name
        self.project_field = []
        self.fields = {}

        self._list = []

    def add_field_filters(self, field_name, field_filter):
        if field_name not in self.fields:
            self.fields[field_name] = TableField()
        self.fields[field_name].add_table_field_filters(field_filter)

    def add_project(self, project_field):
        if project_field not in self.project_field:
            self.project_field.append(project_field)

    def addKeys(self, key):
        self.keys.append(key)

    def addFunctions(self, functions):
        self.functions = functions

    # def getWithTableName(self):
    #     return self.with_table_name

    def getFunctions(self):
        return self.functions

    def printSQL(self, use_as=True):
        sql1 = "WITH " + self.with_table_name + " AS ("

        sql2 = "\nSELECT "
        for key in self.keys:
            sql2 = sql2 + key + ", "
        sql2 = sql2 + self.getFunctions()
        # sql2 = sql2[:-2]

        sql3 = "\nFROM " + self.table_name

        sql4 = "\nGROUP BY "
        for key in self.keys:
            sql4 = sql4 + key + ", "
        sql4 = sql4[:-2]
        print(sql1 + sql2 + sql3 + sql4 + "\n" + ")")
