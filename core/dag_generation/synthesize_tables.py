from core.edit_distance import edit_dist, best_fit


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
        self.join_fields = []  # join[0] = tb1.field_name, join[1]=tb2.field_name
        # self.hash_agg = []  # WHERE
        self.fields = []
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
        self.join_field_str = None #连接的字段名
        self.relationships = {}  # (table, string, string),(),...

        self.special_str_list = []

    def is_project_field_empty(self):
        return len(self.project_field) == 0

    def add_field_filters(self, field_name, field_filter):
        if field_name not in self.fields:
            self.fields[field_name] = TableField()
        self.fields[field_name].add_table_field_filters(field_filter)

    def print_sql(self, use_as=True):
        as_sql=""
        if use_as:
            as_sql = self.with_table_name + " AS ("


        # if self.is_union:  # cope with the particularly union join node
        #
        #     # sql2 = "\n" + self.t1_table.with_table_name + "  \n UNION ALL \n " + self.t2_table.with_table_name
        #     # sql2 = "\n" + self.t1_table.printSQL(use_as=False) + "  \n UNION ALL \n " + self.t2_table.printSQL(
        #     #     use_as=False)
        #     #
        #     # result_sql = sql1 + sql2 + "\n"
        #     # if use_as:
        #     #     result_sql += ")"
        #     # return result_sql

        # SELECT
        select_str = "\nSELECT "
        temp_str = "  "+self.t1_table.with_table_name + ".* "
        if self.hash_agg:
            temp_str = " "
            for index, field in enumerate(self.hash_agg):
                temp_str += self.t1_table.with_table_name + "." + field.name
            temp_str = ",".join([self.t1_table.with_table_name + "." + field.name for field in self.hash_agg])

        select_str += temp_str
        # FROM
        from_sql = "\nFROM " + self.t1_table.with_table_name + " , " + self.t2_table.with_table_name
        where_sql = "\nWHERE "
        # first_field_join = self.t1_table.with_table_name + "." + self.t1_table.join_field_str
        # self.t1_table=store_sales
        # first_field_join = self.t1_table.with_table_name + "." + self.join_fields[0]
        # second_field_join = self.t2_table.with_table_name + "." + self.join_fields[1]
        # second_field_join = self.t2_table.with_table_name + "." + self.t2_table.join_field_str
        (field1, filed2)  = self.t1_table.relationships[self.t2_table]
        first_field_join = self.t1_table.with_table_name + "." + field1
        second_field_join = self.t2_table.with_table_name + "." + filed2
        print("self.t1_table.name is {}, first_field_join is {}, self.t2_table.name is {}, second_field_join is {}".format(self.t1_table.with_table_name, first_field_join, self.t2_table.with_table_name, second_field_join))
        where_sql += (first_field_join + "=" + second_field_join)

        group_sql = "\n "
        if self.hash_agg:
            group_sql = "\n GROUP BY "

            group_str = ",".join([self.t1_table.with_table_name + "." + field.name for field in self.hash_agg])
            group_sql += group_str


        result_sql = as_sql+ select_str + from_sql + where_sql + group_sql
        if use_as:
            result_sql += ")"
        return result_sql
