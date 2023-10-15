# -*- coding: utf-8 -*-
# 在排序时, 可以用lambda表达式将对象map成keys
# 亦可以使用operator包中的attrgetter和itemgetter函数以提高效率
# 参考 http://wiki.python.org/moin/HowTo/Sorting

from core.enum_operator import Operator


# 考虑 Node 对象
class Node:
    def __init__(self, id, name, depth, exchanges, nextnode_id, fields_count, line="", canGoOn=False):
        self.id = id
        self.name = name
        self.depth = depth
        self.exchanges = exchanges
        self.nextnode_id = nextnode_id  # parent nodes id
        self.fields_count = fields_count  # the number of "#" in this line, For example, "Filter (isnotnull(
        # d_date_sk#95) AND isnotnull(d_week_seq#99)) " has 2 fields
        self.estimated_runtime_by_half = 0
        self.estimated_runtime_by_constant = 0
        self.physical_line = line
        self.operator = Operator.Empty
        self.canGoOn = canGoOn
        self.parent_node = None  # parent node, only have one
        self.children_nodes = []  # children nodes, two nodes for most cases
        self.code = None
        self.length_joins = 1
        self.joins_index = 1
        self.subtree_length = 1
        self.child_node=None

        self.input_table= None # if the node is FileScan node, we associate it with the input table
        # if the node is sort merge join or broadcastHashJoin , we would associate it with the join table.
        self.join_table = None

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
        return repr(("id:" + str(self.id), self.name, "type:" + str(self.operator.name), "depth:" + str(self.depth),
                     "exchanges:" + str(self.exchanges),
                     "nextnode_id:" + str(self.nextnode_id), "fields_count:" + str(self.fields_count),
                     "estimated_runtime_by_half:" + str(self.estimated_runtime_by_half),
                     "estimated_runtime_by_constant:" + str(self.estimated_runtime_by_constant)))

    def runtime_prediction_half(self, table_duration):
        predicted_runtime = 0
        for index, val in enumerate(self.exchanges):
            predicted_runtime = predicted_runtime + table_duration
            previous_table_duration = table_duration  # save current table_duration as previous one
            table_duration = table_duration / 2
            if val == 0 and self.exchanges[0] == 0:  # no hash aggregate before, we add the previous_table_duration
                predicted_runtime = predicted_runtime + previous_table_duration

        self.estimated_runtime_by_half = predicted_runtime
        return predicted_runtime

    def runtime_prediction_constant(self, table_duration):
        temp_exchange = self.exchanges
        predicted_runtime = 0
        for index, val in enumerate(self.exchanges):
            predicted_runtime = predicted_runtime + table_duration
            previous_table_duration = table_duration
            if val == 0 and self.exchanges[0] == 0:  # no hash aggregate berfore, we add the previous_table_duration
                predicted_runtime = predicted_runtime + previous_table_duration
            # if val == 0:  # if we have seen hash_aggregate before, we reduce the table_duration by half
            #     table_duration = table_duration / 2
        self.estimated_runtime_by_constant = predicted_runtime
        return predicted_runtime
