# -*- coding: utf-8 -*-
# 在排序时, 可以用lambda表达式将对象map成keys
# 亦可以使用operator包中的attrgetter和itemgetter函数以提高效率
# 参考 http://wiki.python.org/moin/HowTo/Sorting
from node_lele import Node
from enum_operator import Operator
from tpch_normal_stage_performance_model import NormalStagePrediction
from tpch_first_stage_performance_model import FirstStagePrediction
from tpch_first_stage_no_aggregate_performance_model import NoAggregatePrediction
import numpy as np
import warnings

warnings.filterwarnings("ignore")


# stage 对象 separated by two consecutive exchange nodes

class Stage:

    @staticmethod
    def prediction_init():
        FirstStagePrediction.model_init()
        NormalStagePrediction.model_init()
        NoAggregatePrediction.model_init()

    def __init__(self):
        self.begin_node = None
        self.end_node = None
        self.next_stage = None
        self.chain = []
        self.estimate_output_size = 0
        self.estimate_runtime = 0

    def end_node_id(self):
        return str(self.end_node.id)


class FirstStage(Stage):
    def __init__(self):
        super().__init__()
        self.fileScan_table = ""
        self.input_data_size = 0  # MB
        self.fileScan_fields = 0
        self.filescan_notNULL = 0
        self.filter_other = 0
        self.broadcast_joins = 0  # the number of "#" in this line, For example, "Filter (isnotnull(
        # d_date_sk#95) AND isnotnull(d_week_seq#99)) " has 2 fields
        self.projects = 0
        self.hash_aggregate = 0
        self.exchange_partition = 0
        self.broadcast_join_depth = 0
        self.project_depth = 0
        self.hash_aggregate_depth = 0

        # table_name : size (GB)
        self.table_sizes_500G = {"web_sales": 26.5, "store_returns": 8.3, "catalog_sales": 56.3, "store_sales": 70,
                                 # TPC-H
                                 "supplier":0.384, "customer":5.9,"lineitem":117.4,"nation":0.003, "orders":32, "part":3.2,
                                 "partsupp":21.4, "region":0.0015
                                 }

        self.table_columns = {"web_sales": 34, "store_returns": 20, "catalog_sales": 34, "store_sales": 23,
                              # TPC-H
                              "supplier":7, "customer":8,"lineitem":16,"nation":4, "orders":9, "part":9,
                                 "partsupp":5, "region":3
                              }

    def compute_inpute_size(self):
        # # initializing list
        # lst = [1, 7, 0, 6, 2, 5, 6]
        #
        # # converting list to array
        # arr = numpy.array(lst)
        if self.fileScan_fields != 0 and self.fileScan_table != "":

            # particular cases for Table "orders"
            # if self.fileScan_table=="orders":
            #     self.input_data_size =21*1000 # MB
            #     return

            if self.fileScan_table in self.table_sizes_500G:
                self.input_data_size = self.table_sizes_500G[self.fileScan_table] * 1000 * (
                        self.fileScan_fields * 1.0) / \
                                       self.table_columns[self.fileScan_table]
                self.input_data_size = round(self.input_data_size, 1) #MB

    def __repr__(self):
        return repr(("fileScan_table:" + str(self.fileScan_table), "input_data_size:" + str(self.input_data_size),
                     "fileScan_fields:" + str(self.fileScan_fields),
                     "filescan_notNULL:" + str(self.filescan_notNULL), "filter_other:" + str(self.filter_other),
                     "broadcast_joins:" + str(self.broadcast_joins),
                     "broadcast_join_depth:" + str(self.broadcast_join_depth),
                     "projects:" + str(self.projects), "project_depth_depth:" + str(self.project_depth),
                     "hash_aggregate:" + str(self.hash_aggregate), "aggregate_depth:" + str(self.hash_aggregate_depth)))

    def to_model_str(self):
        # [orders,25000,3,2,0,0,2,0,1,0,1] input_data_size in MB
        arrayStr = "[{},{},{},{},{},{},{},{},{},{},{}]".format(self.fileScan_table, self.input_data_size,
                                                               self.fileScan_fields,
                                                               self.filescan_notNULL, self.filter_other,
                                                               self.broadcast_joins, self.projects, self.hash_aggregate,
                                                               self.exchange_partition, self.broadcast_join_depth,
                                                               self.project_depth)
        return arrayStr

    def predict_runtime_output_size(self, resource_config):

        test_x_datasize = np.array([[
                                    self.input_data_size, self.fileScan_fields, self.filescan_notNULL,
                                    self.filter_other,
                                    self.broadcast_joins, self.projects, self.hash_aggregate, self.exchange_partition,
                                    self.broadcast_join_depth, self.project_depth]])

        test_x_runtime = np.array([[resource_config["executor_cores"], resource_config["executor_mem"],
                            resource_config["executor_instances"], resource_config["number_machines"],
                            self.input_data_size, self.fileScan_fields, self.filescan_notNULL, self.filter_other,
                            self.broadcast_joins, self.projects, self.hash_aggregate, self.exchange_partition,
                            self.broadcast_join_depth, self.project_depth]])
        # if self.hash_aggregate == 0 and self.broadcast_join_depth == 0 and self.project_depth == 0:
        # TPC-H handling
        if self.hash_aggregate == 0:

            # we should perform the prediction model with no aggregation operations
            self.estimate_output_size, self.estimate_runtime = NoAggregatePrediction.predict_stage(test_x_datasize,test_x_runtime)
        else:
            self.estimate_output_size, self.estimate_runtime = FirstStagePrediction.predict_stage(test_x_datasize,test_x_runtime)
        self.estimate_runtime = round(self.estimate_runtime, 1)
        return self.estimate_output_size, self.estimate_runtime

    def to_node_str(self):
        # input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#,
        # project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#
        arrayStr = "First stage (end_node_id {}): File Scan {},{}".format(self.end_node.id, self.fileScan_table,
                                                                          self.input_data_size)
        operators_str = [str(a.name) for a in self.chain]

        return arrayStr + str("->".join(operators_str))


class NormalStage(Stage):
    def __init__(self, end_node):
        super().__init__()
        self.input_data_size = 0  # MB
        self.begin_nodes = []
        self.end_node = end_node
        self.beginNodesToChains = {}  # key:beginNode, value: [filter,sort, join]
        self.broadcastjoin_depth = 0
        self.project_depth = 0
        self.aggregate_depth = 0
        self.sort_depth = 0
        self.sort_merge_depth = 0
        self.filter_depth = 0
        self.union_depth = 0
        self.expand_depth = 0
        self.begin_node_length = 0

    def to_node_str(self):
        # # input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#,
        # project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#
        arrayStr = "NormalStage (end_node_id {}): length of beginNodesToChains is {}\n".format(self.end_node.id, len(
            self.beginNodesToChains))

        res = "[{},{},{},{},{},{},{},{}]\n".format(self.begin_node_length, self.broadcastjoin_depth, self.project_depth,
                                                   self.aggregate_depth, self.sort_depth, self.sort_merge_depth,
                                                   self.filter_depth, self.union_depth)
        arrayStr += res
        for i, node in enumerate(self.beginNodesToChains):
            chain_str = "Chain " + str(i) + " is : " + str(node.physical_line) + "\n"

            temp_chain = self.beginNodesToChains[node]
            operators_str = [str(a.name) for a in temp_chain]
            chain_str = chain_str + str("->".join(operators_str)) + "\n"
            arrayStr = arrayStr + chain_str

        return arrayStr

    def compute_operators(self):
        for i, node in enumerate(self.beginNodesToChains):
            temp_chain = self.beginNodesToChains[node]
            for op in temp_chain:
                if op == Operator.BroadcastHashJoin:
                    self.broadcastjoin_depth += 1
                elif op == Operator.Project:
                    self.project_depth += 1
                elif op == Operator.HashAggregate:
                    self.aggregate_depth += 1
                elif op == Operator.Sort:
                    self.sort_depth += 1
                elif op == Operator.SortMergeJoin:
                    self.sort_merge_depth += 1
                elif op == Operator.Filter:
                    self.filter_depth += 1
                elif op == Operator.Union:
                    self.union_depth += 1
                elif op == Operator.Expand:
                    self.expand_depth += 1
        self.begin_node_length = len(self.beginNodesToChains)

    def compute_input_data_size(self, add_size):
        self.input_data_size += add_size

    def predict_runtime_output_size(self,resource_config):
        normal_predictor = NormalStagePrediction()

        test_X_datasize = np.array([[
                                    self.input_data_size, self.begin_node_length, self.broadcastjoin_depth,
                                    self.project_depth,
                                    self.aggregate_depth, self.sort_depth, self.sort_merge_depth, self.filter_depth,
                                    self.union_depth]])

        test_X_runtime = np.array([[resource_config["executor_cores"], resource_config["executor_mem"],
                            resource_config["executor_instances"], resource_config["number_machines"],self.input_data_size, self.begin_node_length, self.broadcastjoin_depth, self.project_depth,
                            self.aggregate_depth, self.sort_depth, self.sort_merge_depth, self.filter_depth,
                            self.union_depth]])
        self.estimate_output_size, self.estimate_runtime = normal_predictor.predict_stage(test_X_datasize,test_X_runtime)
        self.estimate_runtime = round(self.estimate_runtime, 1)
        return self.estimate_output_size, self.estimate_runtime


class BroadcastStage(Stage):
    def __init__(self):
        super().__init__()
        self.fileScan_table = ""
        self.input_data_size = 10 * 1000

    def to_node_str(self):
        # # input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#, project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#
        arrayStr = "Broadcast stage(end_node_id {}): {},{}".format(self.end_node.id, self.fileScan_table,
                                                                   self.input_data_size)
        operators_str = [str(a.name) for a in self.chain]

        return arrayStr + str("->".join(operators_str))
