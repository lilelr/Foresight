class Table:
    def __init__(self, name="", type=""):
        self.name = name # string
        self.type = type # string: f/d
        self.fields = [] # [Field]
        self.relationships = {} # (table, string, string),(),...
        self.join_field_str=None # connected field name
        self.with_table_name=None


    def addRelationship(self, other_table, field1, field2):
        # other_table : table instance
        # e.g., store_returns.addRelationship(date_dim, "sr_returned_date_sk", "d_date_sk")
        self.relationships[other_table] = (field1, field2) # field2 is the join field of table2

    def addField(self, field):
        self.fields.append(field)

    def print_sql(self,use_as=True):
        if use_as:
            sql1 = self.with_table_name + " AS ("
        else:
            sql1 = ""
        sql2 = "\nSELECT * "


        sql3 = "\nFROM " + self.name

        # if
        hasConstraints = False
        # sql4 = "\nWHERE "
        # t1

        result_sql = sql1 + sql2 + sql3 + "\n"
        if use_as:
            result_sql += ")"
        # print(result_sql)
        return result_sql


class Field:
    def __init__(self, name, data_type, source_table):
        self.name = name # string
        self.data_type = data_type # string
        self.source_table = source_table
        self.target_table = None
        self.target_field = None

    def setRelationship(self, source_table, target_table, target_field):
        self.source_table = source_table
        self.target_table = target_table
        self.target_field = target_field


class Source:
    def __init__(self):
        self.tb1 = None # Table
        self.tb2 = None # Table
        self.join_field1 = [] # [Field]
        self.join_field2 = [] # [Field]
        self.all_fields1 = [] # [Field]
        self.all_fields2 = [] # [Field]
        self.isComplete = False


