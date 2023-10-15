# -*- coding: utf-8 -*-
# 在排序时, 可以用lambda表达式将对象map成keys
# 亦可以使用operator包中的attrgetter和itemgetter函数以提高效率
# 参考 http://wiki.python.org/moin/HowTo/Sorting

from core.enum_operator import Operator
from database import Table
from synthesize_tables import JoinTable

# 考虑 Node 对象
class SynthesizedNode:
    def __init__(self, id, name):
        self.id = id
        self.name = name

        self.operator = Operator.Empty
        self.parent_node = []  # parent node, 1<=n<=2
        self.code = None
        self.child_node = None

        self.input_table= Table() # if the node is FileScan node, we associate it with the input table
        # if the node is sort merge join or broadcastHashJoin , we would associate it with the join table.
        self.join_table = JoinTable()

        if self.name.find("Scan") >= 0:
            self.operator = Operator.FileScan
        elif self.name.find("Filter") >= 0:
            self.operator = Operator.Filter
        elif self.name.find("TakeOrderedAndProject") >= 0:
            self.operator = Operator.TakeOrderedAndProject
        elif self.name.find("Project") >= 0:
            self.operator = Operator.Project
        elif self.name.find("SortMergeJoin") >= 0:
            self.operator = Operator.SortMergeJoin
        elif self.name.find("Sort") >= 0:
            self.operator = Operator.Sort
        elif self.name.find("BroadcastHashJoin") >= 0:
            self.operator = Operator.BroadcastHashJoin
        elif self.name.find("BroadcastExchange") >= 0:
            self.operator = Operator.BroadcastExchange
        elif self.name.find("HashAggregate") >= 0:
            self.operator = Operator.HashAggregate
        elif self.name.find("Union") >= 0:
            self.operator = Operator.Union
        elif self.name.find("hashpartition") >= 0:
            self.operator = Operator.ExchangeHashpartitioning
        elif self.name.find("SinglePartition") >= 0:
            self.operator = Operator.ExchangeSinglePartition
        elif self.name.find("ReusedExchange") >= 0:
            self.operator = Operator.ReusedExchange
        elif self.name.find("Expand") >= 0:
            self.operator = Operator.Expand
        elif self.name.find("Subquery") >= 0:
            self.operator = Operator.Subquery
        elif self.name.find("Error") >= 0:
            self.operator = Operator.Error
        elif self.name.find("CollectLimit") >= 0:
            self.operator = Operator.CollectLimit


    def set_child_node(self,node):
        self.child_node = node

    def __repr__(self):
        return repr(("id:" + str(self.id), self.name, "type:" + str(self.operator.name) ))



