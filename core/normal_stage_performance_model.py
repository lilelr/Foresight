#!/usr/bin/env python
# coding: utf-8

import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import LinearSVR
from sklearn.ensemble import RandomForestRegressor

from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_classification
import pickle




class NormalStagePrediction:

    linear_reg_datasize=None
    random_forest_reg_datasize = None
    svm_reg_datasize = None
    tree_reg_datasize=None
    large_tree_reg_datasize=None
    large_random_forest_datasize=None
    tree_reg_runtimes = None
    svm_reg_runtimes = None
    large_svm_reg_runtimes= None
    large_tree_reg_runtimes=None

    svm_reg_outputsize = None

    @staticmethod
    def model_init():
        input_x_stage = np.array([
            #  input_data_size(MB),
            # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth
            # TPCDS 500GB
            [421.9, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 500GB tpcds 5
            [423.0, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 500GB tpcds 6
            [377.7, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 500GB tpcds 7
            [5.5, 1, 1, 1, 1, 0, 0, 0, 0],  # Q2 500GB tpcds
            [5.6, 2, 1, 3, 1, 4, 2, 0, 0],  # Q2 500GB tpcds
            [15.3, 1, 0, 1, 1, 0, 0, 0, 0],  # Q3 500GB  tpcds
            [30.1 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 10
            [532.3, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 11
            [9.5 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 16
            [325.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 17

            [3.8 * 1000, 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 500 tpcds
            [11.3 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 500 tpcds 19
            [123.9, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 500 tpcds 20
            [202.2, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 500 tpcds 7


            # TPCDS 100GB
            [81.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 100GB tpcds 5
            [81.5, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 100GB tpcds 6
            [75.6, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 100GB tpcds 7
            [1992.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4   100 tpcds 10
            [79.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 11
            [6.0 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 100 tpcds 14
            [152.7, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 15
            [1058.6, 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 100 tpcds 7
            [2.2 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 100 tpcds 19
            [31.3, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 100 tpcds 20
            [48.8, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 100 tpcds 7
            [1271.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q11 100GB tpcds stage 4
            [79.5, 1, 0, 0, 1, 0, 0, 0, 0]  # Q11 100GB tpcds stage 5
        ])

        output_y_sizes = np.array([
            # TPCDS 500GB
            374.3,  # Q1 500GB tpcds
            3.4,  # Q1 500GB tpcds
            2.3,  # Q1 500GB tpcds
            0.01,  # Q2 500GB tpcds
            0.1,  # Q2 500GB tpcds
            2,  # Q3 500GB
            532.3,  # Q4 500 tpcds
            115.2,  # Q4 500 tpcds 11
            325.6,  # Q4 500 tpcds 16
            70.9,  # Q4 500 tpcds 17
            1,  # Q5 500GB tpcds
            123.7,  # Q6 500 tpcds 11.3GiB
            0.7,  # Q6 500 tpcds 20
            1.7,  # Q10 500 tpcds 7

            # TPCDS 100GB
            2.0,  # Q1 100GB tpcds 5
            73.6,  # Q1 100GB tpcds 6
            1.2,  # Q1 100GB tpcds 7
            79.6,  # Q4   100 tpcds 10
            18.3,  # Q4   100 tpcds 11
            152.7,  # Q4   100 tpcds 14
            33.6,  # Q4   100 tpcds 15
            0.3,  # Q5 100 tpcds 7
            31.2,  # Q6 100 tpcds 19
            0.6,  # Q6 100 tpcds 20
            0.3,  # Q10 100 tpcds 7
            79.5,  # Q11 100GB tpcds stage 4
            18.2  # Q11 100GB tpcds stage 5
        ])

        input_x__large_stage = np.array([
            # particular cases for Q25, , Q29, Q17, Q72
            #  input_data_size(MB),
            # begin_node_length, broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth
            [40 * 1000, 2, 0, 2, 0, 2, 2, 0, 0],
            # Q25 500 tpcds 6 http://slave4:18080/history/application_1683354485229_0015/stages/
            [39 * 1000, 2, 0, 2, 0, 2, 2, 0, 0],
            # Q29 500 tpcds stage 8 http://slave4:18080/history/application_1683354485229_0019/jobs/job/?id=5
            [39 * 1000, 2, 0, 2, 0, 2, 2, 0, 0],
            # Q17 500 tpcds stage 7 http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
            [28 * 1000, 2, 16, 18, 0, 2, 2, 0, 0]
            # Q72 500 tpcds 10 http://slave4:18080/history/application_1683354485229_0042/jobs/job/?id=8
        ])

        output_y_large_sizes = np.array([
            # particular cases for Q25,Q72, Q29
            3.4 * 1000,  # Q25 500GB tpcds stage 6
            3 * 1000,  # Q29 500GB tpcds stage 8
            3 * 1000,  # Q17 500GB tpcds stage 7
            279  # Q72 500GB tpcds stage 10

        ])


        input_x_stage_runtime = np.array([
            # executor_cores, executor_memory,executor_instances, number_of_machines, input_data_size(MB),
            # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth  2,0,2,0,2,2,0,0
            [2, 8, 2, 2, 421.9, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 500GB tpcds 5
            [2, 8, 2, 2, 423.0, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 500GB tpcds 6
            [2, 8, 2, 2, 377.7, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 500GB tpcds 7
            [2, 8, 2, 2, 5.5, 1, 1, 1, 1, 0, 0, 0, 0],  # Q2 500GB tpcds
            [2, 8, 2, 2, 5.6, 2, 1, 3, 1, 4, 2, 0, 0],  # Q2 500GB tpcds
            [2, 8, 2, 2, 15.3, 1, 0, 1, 1, 0, 0, 0, 0],  # Q3 500GB  tpcds
            [2, 8, 2, 2, 30.1 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 10 30.1 GiB
            [2, 8, 2, 2, 532.3, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 11 	532.3 MiB
            [2, 8, 2, 2, 9.5 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 16
            [2, 8, 2, 2, 325.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 17

            [2, 8, 2, 2, 3.8 * 1000, 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 500 tpcds
            [2, 8, 2, 2, 11.3 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 500 tpcds 19
            [2, 8, 2, 2, 123.9, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 500 tpcds 20
            [2, 8, 2, 2, 202.2, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 500 tpcds 7

            # TPCDS 100GB
            [2, 8, 2, 2, 81.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 100GB tpcds 5
            [2, 8, 2, 2, 81.5, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 100GB tpcds 6
            [2, 8, 2, 2, 75.6, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 100GB tpcds 7
            [2, 8, 2, 2, 1992.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4   100 tpcds 10
            [2, 8, 2, 2, 79.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 11
            [2, 8, 2, 2, 6.0 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 100 tpcds 14
            [2, 8, 2, 2, 152.7, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 15
            [2, 8, 2, 2, 1058.6 , 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 100 tpcds 7
            [2, 8, 2, 2, 2.2 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 100 tpcds 19 2.2GB
            [2, 8, 2, 2, 31.3, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 100 tpcds 20 31.3 MB
            [2, 8, 2, 2, 48.8, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 100 tpcds 7
            [2, 8, 2, 2, 1271.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q11 100GB tpcds stage 4 1271.9MiB
            [2, 8, 2, 2, 79.5, 1, 0, 0, 1, 0, 0, 0, 0],  # Q11 100GB tpcds stage 5 79.2 MiB

            # 2,8,4,4

            # executor_cores, executor_memory,executor_instances, number_of_machines, input_data_size(MB),
            # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth
            [2, 8, 4, 4, 421.9, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 500GB tpcds 5
            [2, 8, 4, 4, 423.0, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 500GB tpcds 6
            [2, 8, 4, 4, 377.7, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 500GB tpcds 7
            [2, 8, 4, 4, 5.5, 1, 1, 1, 1, 0, 0, 0, 0],  # Q2 500GB tpcds
            [2, 8, 4, 4, 5.6, 2, 1, 3, 1, 4, 2, 0, 0],  # Q2 500GB tpcds
            [2, 8, 4, 4, 15.3, 1, 0, 1, 1, 0, 0, 0, 0],  # Q3 500GB  tpcds
            [2, 8, 4, 4, 30.1 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 10 30.1 GiB
            [2, 8, 4, 4, 532.3, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 11 	532.3 MiB
            [2, 8, 4, 4, 9.5 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 16
            [2, 8, 4, 4, 325.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 17

            [2, 8, 4, 4, 3.8 * 1000, 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 500 tpcds
            [2, 8, 4, 4, 11.3 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 500 tpcds 19
            [2, 8, 4, 4, 123.9, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 500 tpcds 20
            [2, 8, 4, 4, 202.2, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 500 tpcds 7

            # TPCDS 100GB
            [2, 8, 4, 4, 81.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 100GB tpcds 5
            [2, 8, 4, 4, 81.5, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 100GB tpcds 6
            [2, 8, 4, 4, 75.6, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 100GB tpcds 7
            [2, 8, 4, 4, 1992.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4   100 tpcds 10 1992.9MB
            [2, 8, 4, 4, 79.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 11  79.3MB
            [2, 8, 4, 4, 6.0 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 100 tpcds 14 6GB
            [2, 8, 4, 4, 152.7, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 15 153MB
            [2, 8, 4, 4, 1058.6, 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 100 tpcds 7
            [2, 8, 4, 4, 2.2 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 100 tpcds 19 2.2GB
            [2, 8, 4, 4, 31.3, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 100 tpcds 20 31.3 MB
            [2, 8, 4, 4, 48.8, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 100 tpcds 7
            [2, 8, 4, 4, 1271.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q11 100GB tpcds stage 4 1271.9MB
            [2, 8, 4, 4, 79.5, 1, 0, 0, 1, 0, 0, 0, 0],  # Q11 100GB tpcds stage 5 79.5 MB

            # 2,8,5,5

            # executor_cores, executor_memory,executor_instances, number_of_machines, input_data_size(MB),
            # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth
            [2, 8, 5, 5, 421.9, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 500GB tpcds 5
            [2, 8, 5, 5, 423.0, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 500GB tpcds 6
            [2, 8, 5, 5, 377.7, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 500GB tpcds 7
            [2, 8, 5, 5, 5.5, 1, 1, 1, 1, 0, 0, 0, 0],  # Q2 500GB tpcds 5.5 MB
            [2, 8, 5, 5, 5.6, 2, 1, 3, 1, 4, 2, 0, 0],  # Q2 500GB tpcds  5.6 MB
            [2, 8, 5, 5, 15.3, 1, 0, 1, 1, 0, 0, 0, 0],  # Q3 500GB  tpcds
            [2, 8, 5, 5, 30.1 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 10 30.1 GiB
            [2, 8, 5, 5, 532.3, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 11 	532.3 MiB
            [2, 8, 5, 5, 9.5 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 16
            [2, 8, 5, 5, 325.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 17

            [2, 8, 5, 5, 3.8 * 1000, 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 500 tpcds
            [2, 8, 5, 5, 11.3 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 500 tpcds 19
            [2, 8, 5, 5, 123.9, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 500 tpcds 20
            [2, 8, 5, 5, 202.2, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 500 tpcds 7

            # # particular cases for Q25, Q29, Q17, Q72
            # [2, 8, 5, 5, 40 * 1000, 2, 0, 2, 0, 2, 2, 0, 0],
            # # Q25 500 tpcds 6 http://slave4:18080/history/application_1683354485229_0015/stages/
            # [2, 8, 5, 5, 39 * 1000, 2, 0, 2, 0, 2, 2, 0, 0],
            # # Q29 500 tpcds stage 8 http://slave4:18080/history/application_1683354485229_0019/jobs/job/?id=5
            # [2, 8, 5, 5, 39 * 1000, 2, 0, 2, 0, 2, 2, 0, 0],
            # # Q17 500 tpcds stage 7 http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
            # [2, 8, 5, 5, 28 * 1000, 2, 16, 18, 0, 2, 2, 0, 0],
            # # Q72 500 tpcds 10 http://slave4:18080/history/application_1683354485229_0042/jobs/job/?id=8

            # TPCDS 100GB
            [2, 8, 5, 5, 81.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 100GB tpcds 5
            [2, 8, 5, 5, 81.5, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 100GB tpcds 6
            [2, 8, 5, 5, 75.6, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 100GB tpcds 7
            [2, 8, 5, 5, 1992.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4   100 tpcds 10 1992.9MB
            [2, 8, 5, 5, 79.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 11  79.3MB
            [2, 8, 5, 5, 6.0 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 100 tpcds 14 6GB
            [2, 8, 5, 5, 152.7, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 15 153MB
            [2, 8, 5, 5, 1058.6, 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 100 tpcds 7
            [2, 8, 5, 5, 2.2 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 100 tpcds 19 2.2GB
            [2, 8, 5, 5, 31.3, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 100 tpcds 20 31.3 MB
            [2, 8, 5, 5, 48.8, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 100 tpcds 7
            [2, 8, 5, 5, 1271.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q11 100GB tpcds stage 4 1271.9MB
            [2, 8, 5, 5, 79.5, 1, 0, 0, 1, 0, 0, 0, 0],  # Q11 100GB tpcds stage 5 79.5 MB
            

            # 2,8,6,6

            # executor_cores, executor_memory,executor_instances, number_of_machines, input_data_size(MB),
            # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth
            [2, 8, 6, 6, 421.9, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 500GB tpcds 5 421.9
            [2, 8, 6, 6, 423.0, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 500GB tpcds 6 423.0
            [2, 8, 6, 6, 377.7, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 500GB tpcds 7 377
            [2, 8, 6, 6, 5.5, 1, 1, 1, 1, 0, 0, 0, 0],  # Q2 500GB tpcds
            [2, 8, 6, 6, 5.6, 2, 1, 3, 1, 4, 2, 0, 0],  # Q2 500GB tpcds
            [2, 8, 6, 6, 15.3, 1, 0, 1, 1, 0, 0, 0, 0],  # Q3 500GB  tpcds
            [2, 8, 6, 6, 30.1 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 10 30.1 GiB
            [2, 8, 6, 6, 532.3, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 11 	532.3 MiB
            [2, 8, 6, 6, 9.5 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 500 tpcds 16 9.5 GB
            [2, 8, 6, 6, 325.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 500 tpcds 17 325 MB

            [2, 8, 6, 6, 3.8 * 1000, 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 500 tpcds
            [2, 8, 6, 6, 11.3 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 500 tpcds 11.3GB
            [2, 8, 6, 6, 123.9, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 500 tpcds 20 123.9 MB
            [2, 8, 6, 6, 202.2, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 500 tpcds 7

            # # particular cases for Q25, Q29, Q17, Q72
            # [2, 8, 6, 6, 40*1000, 2, 0, 2, 0, 2, 2, 0, 0],
            # # Q25 500 tpcds 6 http://slave4:18080/history/application_1683354485229_0015/stages/
            # [2, 8, 6, 6, 39*1000, 2, 0, 2, 0, 2, 2, 0, 0],
            # # Q29 500 tpcds stage 8 http://slave4:18080/history/application_1683354485229_0019/jobs/job/?id=5
            # [2, 8, 6, 6, 39*1000, 2, 0, 2, 0, 2, 2, 0, 0],
            # # Q17 500 tpcds stage 7 http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
            # [2, 8, 6, 6, 28*1000, 2, 16, 18, 0, 2, 2, 0, 0],
            # # Q72 500 tpcds 10 http://slave4:18080/history/application_1683354485229_0042/jobs/job/?id=8

            # TPCDS 100GB
            [2, 8, 6, 6, 81.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q1 100GB tpcds 5
            [2, 8, 6, 6, 81.5, 1, 0, 0, 2, 0, 0, 0, 0],  # Q1 100GB tpcds 6
            [2, 8, 6, 6, 75.6, 2, 2, 4, 1, 2, 2, 1, 0],  # Q1 100GB tpcds 7
            [2, 8, 6, 6, 1992.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4   100 tpcds 10 1992.9MB
            [2, 8, 6, 6, 79.6, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 11  79.3MB
            [2, 8, 6, 6, 6.0 * 1000, 2, 2, 4, 2, 2, 2, 0, 0],  # Q4 100 tpcds 14 6GB
            [2, 8, 6, 6, 152.7, 1, 0, 0, 1, 0, 0, 1, 0],  # Q4 100 tpcds 15 153MB
            [2, 8, 6, 6, 1058.6, 3, 0, 0, 6, 0, 0, 0, 3],  # Q5 100 tpcds 7
            [2, 8, 6, 6, 2.2 * 1000, 3, 3, 6, 1, 2, 2, 1, 0],  # Q6 100 tpcds 19 2.2GB
            [2, 8, 6, 6, 31.3, 2, 0, 2, 2, 2, 2, 0, 0],  # Q6 100 tpcds 20 31.3 MB
            [2, 8, 6, 6, 48.8, 4, 8, 12, 4, 4, 9, 4, 0],  # Q10 100 tpcds 7
            [2, 8, 6, 6, 1271.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q11 100GB tpcds stage 4 1271.9MB
            [2, 8, 6, 6, 79.5, 1, 0, 0, 1, 0, 0, 0, 0]  # Q11 100GB tpcds stage 5 79.5 MB
        ])



        output_y_runtimes = np.array([
            13,  # Q1 500GB tpcds 5 http://slave4:18080/history/application_1679295911706_0003/jobs/job/?id=2
            5,  # Q1 500GB tpcds
            3,  # Q1 500GB tpcds
            1,  # # Q2 500GB
            1,  # # Q2 500GB http://slave4:18080/history/application_1679295911706_0004/jobs/job/?id=3
            2,  # # Q3 500GB  15.3MiB http://slave4:18080/history/application_1679295911706_0005/jobs/job/?id=2
            3.1*60,  # Q4 500GB 10 30.1GB http://slave4:18080/history/application_1679295911706_0006/jobs/job/?id=2
            4,  # Q4 500GB 11  532.3 MiB	http://slave4:18080/history/application_1679295911706_0006/jobs/job/?id=2
            3.5*60,  # Q4 500 tpcds 16  9.5 GiB	http://slave4:18080/history/application_1679295911706_0006/jobs/job/?id=2
            3,  # Q4 500 tpcds 17
            2.2*60,  # Q5 500GB 3.8GiB  http://slave4:18080/history/application_1679295911706_0007/jobs/job/?id=4
            1.4*60,  # Q6 500GB 11.3GiB http://slave4:18080/history/application_1679295911706_0008/jobs/job/?id=9
            2,  # Q6 500 tpcds 123.9 MiB
            19,  # Q10 500 tpcds http://slave4:18080/history/application_1679295911706_0012/jobs/job/?id=3

            # tpcds 100GB
            5,  # Q1 100GB tpcds 5 81.6MB http://slave4:18080/history/application_1679295911706_0016/jobs/job/?id=2
            3,  # Q1 100GB tpcds 6 81.5MB
            2,  # Q1 100GB tpcds 7 75.6MB
            46,  # Q4   100 tpcds 10 1992.4MB http://slave4:18080/history/application_1679295911706_0019/jobs/job/?id=2
            2,  # Q4   100 tpcds 11
            32,  # Q4   100 tpcds 14
            1,  # Q4   100 tpcds 15
            24,  # Q5 100 tpcds 7 1058.6MB http://slave4:18080/history/application_1679295911706_0020/jobs/job/?id=4
            48,  # Q6 100 tpcds 19 2.2GB http://slave4:18080/history/application_1679295911706_0021/jobs/job/?id=9
            4,  # Q6 100 tpcds 20  31.3 MB http://slave4:18080/history/application_1679304940725_0019/jobs/job/?id=9
            5,  # Q10 100 tpcds 7 48.7MB http://slave4:18080/history/application_1679295911706_0025/jobs/job/?id=3
            12,  # Q11 100GB tpcds stage 4 1271.5 MiB http://slave4:18080/history/application_1679295911706_0026/jobs/job/?id=2
            2,  # Q11 100GB tpcds stage 5 79.2MB


            # 2,8,4,4
            2,  # Q1 500GB tpcds 5 http://slave4:18080/history/application_1679304940725_0014/jobs/job/?id=2
            5,  # Q1 500GB tpcds
            2,  # Q1 500GB tpcds
            1,  # # Q2 500GB 5.5MB
            1,  # # Q2 500GB 5.6 MB http://slave4:18080/history/application_1679295911706_0004/jobs/job/?id=3
            2,  # # Q3 500GB  15.3MiB http://slave4:18080/history/application_1679295911706_0005/jobs/job/?id=2
            1.8 * 60,  # Q4 500GB 10 30.1GB http://slave4:18080/history/application_1679295911706_0006/jobs/job/?id=2
            5,  # Q4 500GB 11  532.3 MiB	http://slave4:18080/history/application_1679295911706_0006/jobs/job/?id=2
            2.8 * 60,
            # Q4 500 tpcds 16  9.5 GiB	http://slave4:18080/history/application_1679304940725_0004/jobs/job/?id=2
            4,  # Q4 500 tpcds 17  325.6MB
            1.4 * 60,  # Q5 500GB 3.8GiB  http://slave4:18080/history/application_1679304940725_0005/jobs/job/?id=4
            1.2 * 60,  # Q6 500GB 11.3GiB http://slave4:18080/history/application_1679366052438_0007/jobs/job/?id=9
            3,  # Q6 500 tpcds 123.9 MiB
            23,  # Q10 500 tpcds 202MB http://slave4:18080/history/application_1679304940725_0010/jobs/job/?id=3

            # tpcds 100GB
            5,  # Q1 100GB tpcds 5 81.6MB http://slave4:18080/history/application_1679295911706_0016/jobs/job/?id=2
            3,  # Q1 100GB tpcds 6 81.5MB
            2,  # Q1 100GB tpcds 7 75.6MB
            9,  # Q4   100 tpcds 10 1992.4MB http://slave4:18080/history/application_1679304940725_0017/jobs/job/?id=2
            3,  # Q4   100 tpcds 11 152.7 MB http://slave4:18080/history/application_1679304940725_0017/jobs/job/?id=2
            24,  # Q4   100 tpcds 14 6GB
            3,  # Q4   100 tpcds 15  153MB
            18,  # Q5 100 tpcds 7 1058.6MB http://slave4:18080/history/application_1679304940725_0018/jobs/job/?id=4
            25,  # Q6 100 tpcds 19 2.2GB http://slave4:18080/history/application_1679295911706_0021/jobs/job/?id=9
            2,  # Q6 100 tpcds 20
            7,  # Q10 100 tpcds 7 48.7MB http://slave4:18080/history/application_1679304940725_0023/jobs/job/?id=3
            8,
            # Q11 100GB tpcds stage 4 1271.5 MiB http://slave4:18080/history/application_1679295911706_0026/jobs/job/?id=2
            2,  # Q11 100GB tpcds stage 5 79.2MB

            # 2,8,5,5 500GB
            5,  # Q1 500GB tpcds 421.9 http://slave4:18080/history/application_1679304940725_0014/jobs/job/?id=2
            10,  # Q1 500GB tpcds 423.0
            3,  # Q1 500GB tpcds 377.7
            1,  # # Q2 500GB 5.5 MB
            1,  # # Q2 500GB 5.6 MB http://slave4:18080/history/application_1679295911706_0004/jobs/job/?id=3
            2,  # # Q3 500GB  15.3MiB http://slave4:18080/history/application_1679295911706_0005/jobs/job/?id=2
            1.7 * 60,  # Q4 500GB 10 30.1GB http://slave4:18080/history/application_1679295911706_0006/jobs/job/?id=2
            4,  # Q4 500GB 11  532.3 MiB	http://slave4:18080/history/application_1682218828348_0009/jobs/job/?id=2
            28,
            # Q4 500 tpcds 16  9.5 GiB	http://slave4:18080/history/application_1679304940725_0004/jobs/job/?id=2
            3,  # Q4 500 tpcds 17  325.6MB
            1.3 * 60,  # Q5 500GB 3.8GiB  http://slave4:18080/history/application_1679304940725_0005/jobs/job/?id=4
            1.6 * 60,  # Q6 500GB 11.3GiB http://slave4:18080/history/application_1682218828348_0011/jobs/job/?id=9
            5,  # Q6 500 tpcds 123.9 MiB http://slave4:18080/history/application_1682218828348_0011/jobs/job/?id=9
            21,  # Q10 500 tpcds 202MB http://slave4:18080/history/application_1682218828348_0015/jobs/job/?id=3

            # # particular cases for Q25, Q29, Q17, Q72 500GB tpcds
            # 4.2 * 60,  # Q25 stage 6 http://slave4:18080/history/application_1683354485229_0015/jobs/job/?id=4
            # 4.4 * 60,  # Q29 http://slave4:18080/history/application_1683354485229_0019/jobs/job/?id=5
            # 4.6 * 60,  # Q17 stage 7 http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
            # 12 * 60,  # Q72 stage 10 http://slave4:18080/history/application_1683354485229_0042/stages/

            # tpcds 100GB
            5,  # Q1 100GB tpcds 5 81.6MB http://slave4:18080/history/application_1679295911706_0016/jobs/job/?id=2
            3,  # Q1 100GB tpcds 6 81.5MB
            2,  # Q1 100GB tpcds 7 75.6MB
            9,  # Q4   100 tpcds 10 1992.4MB http://slave4:18080/history/application_1679304940725_0017/jobs/job/?id=2
            3,  # Q4   100 tpcds 11 152.7 MB http://slave4:18080/history/application_1679304940725_0017/jobs/job/?id=2
            30,  # Q4   100 tpcds 14 6GB http://slave4:18080/history/application_1682218828348_0039/jobs/job/?id=2
            3,  # Q4   100 tpcds 15  153MB
            20,  # Q5 100 tpcds 7 1058.6MB http://slave4:18080/history/application_1682218828348_0040/jobs/job/?id=4
            38,  # Q6 100 tpcds 19 2.2GB http://slave4:18080/history/application_1679295911706_0021/jobs/job/?id=9
            4,  # Q6 100 tpcds 20 http://slave4:18080/history/application_1682218828348_0041/jobs/job/?id=9
            7,  # Q10 100 tpcds 7 48.7MB http://slave4:18080/history/application_1682218828348_0045/jobs/job/?id=3
            11,
            # Q11 100GB tpcds stage 4 1271.5 MiB http://slave4:18080/history/application_1682218828348_0046/jobs/job/?id=2
            2,  # Q11 100GB tpcds stage 5 79.2MB

            # 2,8,6,6
            6,  # Q1 500GB 421.9MB http://slave4:18080/history/application_1682413175115_0002/jobs/job/?id=2
            7,  # Q1 500GB 423.0 MiB
            3,  # Q1 500GB 377.7 MiB
            1,  # # Q2 500GB
            1,  # # Q2 500GB http://slave4:18080/history/application_1682413175115_0003/stages/
            5,  # # Q3 500GB  15.3MiB http://slave4:18080/history/application_1679295911706_0005/jobs/job/?id=2
            1.4 * 60,  # Q4 500GB 10 30.1GB http://slave4:18080/history/application_1679295911706_0006/jobs/job/?id=2
            4,  # Q4 500GB 11  532.3 MiB	http://slave4:18080/history/application_1679366052438_0005/jobs/job/?id=2
            24,
            # Q4 500 tpcds 16  9.5 GiB	http://slave4:18080/history/application_1679366052438_0005/jobs/job/?id=2
            2,  # Q4 500 tpcds 17  325.6MB
            1.1*60,  # Q5 500GB 3.8GiB  http://slave4:18080/history/application_1679366052438_0006/jobs/job/?id=4
            1.5 * 60,  # Q6 500GB 11.3GiB http://slave4:18080/history/application_1682413175115_0007/jobs/job/?id=9
            5,  # Q6 500 tpcds 123.9 MiB http://slave4:18080/history/application_1682413175115_0007/jobs/job/?id=9
            10,  # Q10 500 tpcds 202MB http://slave4:18080/history/application_1682413175115_0011/jobs/job/?id=3

            # # particular cases for Q25, Q29, Q17, Q72 500GB tpcds
            # 4*60, # Q25 stage 6 http://slave4:18080/history/application_1683354485229_0015/jobs/job/?id=4
            # 4.1*60, # Q29 http://slave4:18080/history/application_1683354485229_0019/jobs/job/?id=5
            # 4.4 * 60,  # Q17 stage 7 http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
            # 11*60,  # Q72 stage 10 http://slave4:18080/history/application_1683354485229_0042/stages/

            # tpcds 100GB
            4,  # Q1 100GB tpcds 5 81.6MB http://slave4:18080/history/application_1682413175115_0014/jobs/job/?id=2
            8,  # Q1 100GB tpcds 6 81.5MB
            3,  # Q1 100GB tpcds 7 75.6MB
            7,  # Q4   100 tpcds 10 1992.4MB http://slave4:18080/history/application_1682413175115_0017/jobs/job/?id=2
            2,  # Q4   100 tpcds 11 152.7 MB http://slave4:18080/history/application_1682413175115_0017/jobs/job/?id=2
            37,  # Q4   100 tpcds 14 6GB http://slave4:18080/history/application_1682413175115_0017/jobs/job/?id=2
            3,  # Q4   100 tpcds 15  153MB
            15,  # Q5 100 tpcds 7 1058.6MB http://slave4:18080/history/application_1682413175115_0018/jobs/job/?id=4
            33,  # Q6 100 tpcds 19 2.2GB http://slave4:18080/history/application_1682413175115_0019/jobs/job/?id=9
            3,  # Q6 100 tpcds 20 31.3MB http://slave4:18080/history/application_1682413175115_0019/jobs/job/?id=9
            11,  # Q10 100 tpcds 7 48.7MB http://slave4:18080/history/application_1682413175115_0023/jobs/job/?id=3
            17,
            # Q11 100GB tpcds stage 4 1271.5 MiB http://slave4:18080/history/application_1682413175115_0024/jobs/job/?id=2
            2  # Q11 100GB tpcds stage 5 79.2MB
        ])

        # print(len(output_y_sizes), len(output_y_runtimes))

        # predict output data size
        # NormalStagePrediction.svm_reg_datasize = LinearSVR(epsilon=10)
        # NormalStagePrediction.svm_reg_datasize.fit(input_x_stage, output_y_sizes)
        #
        # NormalStagePrediction.tree_reg_datasize = DecisionTreeRegressor(max_depth=6)
        # NormalStagePrediction.tree_reg_datasize.fit(input_x_stage, output_y_sizes)
        #
        # NormalStagePrediction.random_forest_reg_datasize=RandomForestRegressor(max_depth=6, random_state=0)
        # NormalStagePrediction.random_forest_reg_datasize.fit(input_x_stage, output_y_sizes)
        #
        # # NormalStagePrediction.linear_reg_datasize = LinearRegression()
        # NormalStagePrediction.linear_reg_datasize = LinearRegression(positive=True)
        # NormalStagePrediction.linear_reg_datasize.fit(input_x_stage,output_y_sizes)
        #
        # ## predict output data size with large datasets (>=20GB)
        # NormalStagePrediction.large_tree_reg_datasize = DecisionTreeRegressor(max_depth=4)
        # NormalStagePrediction.large_tree_reg_datasize.fit(input_x__large_stage, output_y_large_sizes)
        #
        # NormalStagePrediction.large_random_forest_datasize = RandomForestRegressor(max_depth=6, random_state=0)
        # NormalStagePrediction.large_random_forest_datasize.fit(input_x__large_stage, output_y_large_sizes)
        #
        # # predict stage's runtime
        #
        # NormalStagePrediction.tree_reg_runtimes = DecisionTreeRegressor(max_depth=3)
        # NormalStagePrediction.tree_reg_runtimes.fit(input_x_stage_runtime, output_y_runtimes)
        #
        # NormalStagePrediction.svm_reg_runtimes = LinearSVR(epsilon=0.5)
        # NormalStagePrediction.svm_reg_runtimes.fit(input_x_stage_runtime, output_y_runtimes)
        #
        #
        # large_input_stage_runtime = np.array([
        #     # executor_cores, executor_memory,executor_instances, number_of_machines, input_data_size(MB),
        #     # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
        #     # filter_depth, union_depth  2,0,2,0,2,2,0,0
        #     [2, 8, 5, 5, 39*1000, 2, 0, 2, 0, 2, 2, 0, 0],  # Q17 500GB tpcds stage 7
        #     # http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
        #     [2, 8, 5, 5, 14.2*1000, 2, 10, 12, 2, 2, 2, 0, 0],  # Q17 500GB tpcds stage 8
        #
        #     # http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
        #
        #     [2, 8, 5, 5, 39 * 1000, 2, 0, 2, 0, 2, 2, 0, 0],  # Q29 500GB tpcds stage 8
        #     # http://slave4:18080/history/application_1683354485229_0019/jobs/job/?id=5
        #     [2, 8, 5, 5, 14.2 * 1000, 2, 10, 12, 2, 2, 2, 0, 0],  # Q29 500GB tpcds stage 9
        #
        #     # http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
        #     [2, 8, 5, 5, 28*1000,     2, 0, 2, 2, 2, 2, 0, 0] # Q72 500GB tpcds stage 10
        # ])
        #
        # large_input_runtimes = np.array([
        #                                 4.4*60, # Q17 500GB tpcds stage 7
        #                                  2*60, # # Q17 500GB tpcds stage 8
        #                                 4.1 * 60,  # Q29 500GB tpcds stage 8
        #                                 2 * 60, # Q29 500GB tpcds stage 9
        #                                  11*60 # Q72 500GB tpcds stage 10
        #                                  ])
        # NormalStagePrediction.large_svm_reg_runtimes  = LinearSVR(epsilon=0.5)
        # NormalStagePrediction.large_svm_reg_runtimes.fit(large_input_stage_runtime, large_input_runtimes)
        #
        # NormalStagePrediction.large_tree_reg_runtimes = DecisionTreeRegressor(max_depth=6)
        # NormalStagePrediction.large_tree_reg_runtimes.fit(large_input_stage_runtime, large_input_runtimes)

        prefix = "../jupyter-notebook/"
        filename = prefix + "model-foresight_normal_stage_prediction_TPC-DS-500GB-svm_reg_outputsize.model"
        NormalStagePrediction.svm_reg_outputsize = pickle.load(open(filename, 'rb'))
        filename = prefix + "model-foresight_normal_stage_prediction_TPC-DS-500GB-tree_reg_runtimes.model"
        NormalStagePrediction.tree_reg_runtimes = pickle.load(open(filename, 'rb'))
        filename = prefix + "model-foresight_normal_stage_prediction_TPC-DS-500GB-svm_reg_runtimes.model"
        NormalStagePrediction.svm_reg_runtimes = pickle.load(open(filename, 'rb'))

    @staticmethod
    def test_prediction():

        # input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#, project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#

        test_x = np.array([
            [2, 6069, 8, 3, 9216, 3, 0, 0, 6, 0, 0, 0, 3, 200]  # Q4,500,9,web_sales,9216

        ])

        # actual [0.1 ] MB
        # predict_output_size = svm_reg.predict(test_x)
        # print("predict_output_size is {} MB".format(predict_output_size) )

        predict_output_size = NormalStagePrediction.svm_reg_outputsize.predict(test_x)

        actual_oput_size = np.array([
            1
        ])

        print("actual_oputput_size is {} MiB".format(actual_oput_size))

        print("svm_reg： predict_output_size is {} MiB".format(predict_output_size))

        # actual runtime [13] seconds

        actual_oputput_runtime = np.array([
            47
        ])

        print("actual_oputput_runtime is {} seconds".format(actual_oputput_runtime))
        predict_output_runtime = NormalStagePrediction.tree_reg_runtimes.predict(test_x)
        print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

        predict_output_runtime = NormalStagePrediction.svm_reg_runtimes.predict(test_x)
        print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

    def __init__(self):
        return

    def predict_stage(self, test_x_datasize, test_x_runtime):
        # print("in normal stage performance model.py")
        # print("test_X_datasize is {}".format(test_x_datasize[0]))
        # print("test_X_runtime is {}".format(test_x_runtime[0]))

        input_data_size = test_x_datasize[0][0]
        # if input_data_size>=20*1000: # large input dataset (>=20GB)
        #     print("large input data size (>=20GB) for prediction of output size")
        #     predict_output_size = NormalStagePrediction.large_tree_reg_datasize.predict(test_X_datasize)[0]
        #     # predict_output_size = NormalStagePrediction.large_random_forest_datasize.predict(test_X_datasize)[0]
        # else:
        #     # predict_output_size = NormalStagePrediction.svm_reg_datasize.predict(test_X_datasize)[0]
        #     predict_output_size = NormalStagePrediction.tree_reg_datasize.predict(test_X_datasize)[0]
        #
        # # print("svm_reg： predict_output_size is {} MB".format(predict_output_size))
        # # [2, 8, 2, 2, 40*1000, 2, 0, 2, 0, 2, 2, 0, 0]
        # test_X=test_X_runtime[0]
        # # if test_X[4] >2000 and test_X[5] ==2 and test_X[6]==0 and test_X[7]==2 and test_X[8]==0 and test_X[9]==2 and test_X[10]==2 and test_X[11]==0 and test_X[12] ==0:
        # if test_X[4] > 5*2000: # large data set
        #     # call the predction model of large dataset
        #     print("large input data size (>=10GB) for prediction of runtime")
        #     # large_svm_output_runtime=NormalStagePrediction.large_svm_reg_runtimes.predict(test_X_runtime)[0]
        #     large_tree_output_runtime=NormalStagePrediction.large_tree_reg_runtimes.predict(test_X_runtime)[0]
        #     print("predict_output_size is {}".format(predict_output_size))
        #     print("large_tree_reg_output_runtime is {}".format(large_tree_output_runtime))
        #
        #     # return predict_output_size,large_svm_output_runtime
        #     return predict_output_size,large_tree_output_runtime
        # # print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(tree_predict_output_runtime))
        # else:
        #     tree_predict_output_runtime = NormalStagePrediction.tree_reg_runtimes.predict(test_X_runtime)[0]
        #     # svm_predict_output_runtime = NormalStagePrediction.svm_reg_runtimes.predict(test_X_runtime)[0]
        #     # print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(svm_predict_output_runtime))
        #     print(" predict_output_size is {}".format( predict_output_size))
        #     print("tree_predict_output_runtime is {}".format( tree_predict_output_runtime))
        #     return predict_output_size, tree_predict_output_runtime

        predict_output_size = NormalStagePrediction.svm_reg_outputsize.predict(test_x_datasize)[0]
        tree_predict_output_runtime = NormalStagePrediction.tree_reg_runtimes.predict(test_x_runtime)[0]
        #
        # print("test_X_datasize is {}, predict_output_size is {}".format(test_x_datasize[0], predict_output_size))
        # print("test_X_runtime is {} tree_predict_output_runtime is {}".format(test_x_runtime[0],
        #                                                                       tree_predict_output_runtime))
        if predict_output_size <=1:
            predict_output_size = 1
        return predict_output_size, tree_predict_output_runtime



if __name__ == '__main__':
    test_stage_predict = NormalStagePrediction()
    test_stage_predict.model_init()
    NormalStagePrediction.test_prediction()
