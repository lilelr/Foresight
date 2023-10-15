#!/usr/bin/env python
# coding: utf-8

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import LinearSVR

import pickle


class NoAggregatePrediction:
    reg_nnls = None
    svm_reg = None
    svm_reg_outputsize=None
    tree_reg_datasize=None
    tree_reg_runtimes = None
    svm_reg_runtimes = None

    @staticmethod
    def model_init():
        # using linear regression to estimate output datasize since the training dataset is small-scale # focus on
        # the table called catalog_sales with two types: 1: filescan->exchange 2: filescan-project-aggregate-join
        # input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#,
        # project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#

        input_x_no_join_project = np.array([
            # catalog_sales
            [3.0 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 6
            [15.1 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6
            # store_sales
            [1416.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16
            [6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16
            # web_sales
            [1502.5, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 9
            [7.3 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 2
            # customer
            [18.0, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 100GB tpcds stage 2
            [60.9, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 500GB tpcds stage 2
            [1416.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16
            [6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16
            [23, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 100GB tpcds stage 4
            [80.7, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 500GB tpcds stage 4
            # store_sales
            [10.6 * 1000, 4, 2, 0, 0, 0, 0, 1, 0, 0]  # Q11 500GB tpcds stage 8 10.6GiB

        ])

        output_y_sizes = np.array([
            # catalog_sales
            3.6 * 1000,  # Q4 100GB tpcds stage 6
            18.2 * 1000,  # Q4 500GB tpcds stage 6

            # store_sales
            2.2 * 1000,  # Q6 100GB tpcds stage 16
            11.2 * 1000,  # Q6 500GB tpcds stage 16
            # web_sales
            1847.4,  # Q4 100GB tpcds stage 9
            9.0 * 1000,  # Q4 500GB tpcds stage 2
            # customer
            21,  # Q1 100GB tpcds stage 2
            75.2,  # Q1 500GB tpcds stage 2
            2.2 * 1000,  # Q6 100GB tpcds stage 16
            11.2 * 1000,  # Q6 500GB tpcds stage 16
            29,  # Q10 100GB tpcds stage 4
            104.1,  # Q10 500GB tpcds stage 4
            16.3 * 1000
            # Q11 500GB tpcds stage 8 10.6GiB http://slave4:18080/history/application_1679295911706_0013/jobs/job/?id=2
        ])

        # # executor_cores, executor_memory,executor_instances, number_of_machines, input data sizes (MB)1,
        # filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#, # project 6#, hashAggregate 7
        # #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#

        input_x_no_join_project_runtime = np.array([
            # 2,8,2,2
            # catalog_sales
            [2, 8, 2, 2, 3.0 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 6
            [2, 8, 2, 2, 15.1 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6
            # store_sales
            [2, 8, 2, 2, 1416.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16
            [2, 8, 2, 2, 6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16 6.9GiB
            # web_sales
            [2, 8, 2, 2, 1502.5, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 9
            [2, 8, 2, 2, 7.3 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6 7.3GB
            # customer
            [2, 8, 2, 2, 18.0, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 100GB tpcds stage 2
            [2, 8, 2, 2, 60.9, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 500GB tpcds stage 2
            [2, 8, 2, 2, 16.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16
            [2, 8, 2, 2, 6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16
            [2, 8, 2, 2, 23, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 100GB tpcds stage 4
            [2, 8, 2, 2, 80.7, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 500GB tpcds stage 4
            # store_sales
            [2, 8, 4, 4, 10.6 * 1000, 4, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 500GB tpcds stage 8 10.6GiB

            # 2,8,4,4
        
            # catalog_sales
            [2, 8, 4, 4, 3.0 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 6 3.0GB
            [2, 8, 4, 4, 15.1 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6 15.1GB
            # store_sales
            [2, 8, 4, 4, 1416.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16
            [2, 8, 4, 4, 6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16 6.9GiB
            # web_sales
            [2, 8, 4, 4, 1502.5, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 9 1502.5 MiB
            [2, 8, 4, 4, 7.3 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6 7.3GB
            # customer
            [2, 8, 4, 4, 18.0, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 100GB tpcds stage 2
            [2, 8, 4, 4, 60.9, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 500GB tpcds stage 2
            [2, 8, 4, 4, 16.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16 16.2MB
            [2, 8, 4, 4, 6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16
            [2, 8, 4, 4, 23, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 100GB tpcds stage 4
            [2, 8, 4, 4, 80.7, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 500GB tpcds stage 4

            # store_sales
            [2, 8, 4, 4, 10.6 * 1000, 4, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 500GB tpcds stage 8 10.6GiB

            # 2,8,5,5

            # catalog_sales
            [2, 8, 5, 5, 3.0 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 6 3.0GB
            [2, 8, 5, 5, 15.1 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6 15.1GB
            # store_sales
            [2, 8, 5, 5, 1416.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16
            [2, 8, 5, 5, 6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16 6.9GiB
            # web_sales
            [2, 8, 5, 5, 1502.5, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 9 1502.5 MiB
            [2, 8, 5, 5, 7.3 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6 7.3GB
            # customer
            [2, 8, 5, 5, 18.0, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 100GB tpcds stage 2
            [2, 8, 5, 5, 60.9, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 500GB tpcds stage 2
            [2, 8, 5, 5, 16.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16 16.2MB
            [2, 8, 5, 5, 6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16 6.9GiB
            [2, 8, 5, 5, 23, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 100GB tpcds stage 4
            [2, 8, 5, 5, 80.7, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 500GB tpcds stage 4

            # store_sales
            [2, 8, 4, 4, 10.6 * 1000, 4, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 500GB tpcds stage 8 10.6GiB


            # 2,8,6,6
            # store_sales
            [2, 8, 6, 6, 10.6 * 1000, 4, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 500GB tpcds stage 8 10.6GiB
            # catalog_sales
            [2, 8, 6, 6, 3.0 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 6 3.0GB
            [2, 8, 6, 6, 15.1 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6 15.1GB
            # store_sales
            [2, 8, 6, 6, 1416.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16
            [2, 8, 6, 6, 6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16 6.9GiB
            # web_sales
            [2, 8, 6, 6, 1502.5, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 100GB tpcds stage 9 1502.5 MiB
            [2, 8, 6, 6, 7.3 * 1000, 6, 2, 0, 0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6 7.3GB
            # customer
            [2, 8, 6, 6, 18.0, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 100GB tpcds stage 2 18MB
            [2, 8, 6, 6, 60.9, 2, 2, 0, 0, 0, 0, 1, 0, 0],  # Q1 500GB tpcds stage 2 60MB
            [2, 8, 6, 6, 16.2, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16 16.2MB
            [2, 8, 6, 6, 6.9 * 1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16
            [2, 8, 6, 6, 23, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 100GB tpcds stage 4
            [2, 8, 6, 6, 80.7, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q10 500GB tpcds stage 4

            # store_sales
            [2, 8, 6, 6, 10.6 * 1000, 4, 2, 0, 0, 0, 0, 1, 0, 0]  # Q11 500GB tpcds stage 8 10.6GiB

        ])

        output_y_runtimes = np.array([
            # 2, 8, 2, 2
            # catalog_sales
            26,  # Q4 100GB tpcds stage 3.0GiB http://slave4:18080/history/application_1679295911706_0019/jobs/job/?id=2
            2.4 * 60,
            # Q4 500GB tpcds stage 11 http://slave4:18080/history/application_1679295911706_0006/jobs/job/?id=2

            # store_sales
            23,  # Q6 100GB tpcds stage 16 1416MB
            29,  # Q6 500GB tpcds stage 16
            # web_sales
            15,
            # Q4 100GB tpcds stage 9 1502.5 MiB http://slave4:18080/history/application_1679295911706_0019/jobs/job/?id=2
            3.6 * 60,  # Q4 500GB tpcds stage 6 7.3GB
            # customer
            3,  # Q1 100GB tpcds stage 2 http://slave4:18080/history/application_1679295911706_0016/jobs/job/?id=2
            4,  # Q1 500GB tpcds stage 2 60.9MB(input) ,
            # http://slave4:18080/history/application_1679295911706_0003/jobs/job/?id=2
            6,  # Q6 100GB tpcds stage 16 16.2MB http://slave4:18080/history/application_1679295911706_0021/jobs/job/?id=9
            1.7 * 60,
            # Q6 500GB tpcds stage 16 6.9GiB http://slave4:18080/history/application_1679295911706_0008/jobs/job/?id=9
            1,  # Q10 100GB tpcds stage 4 23.7MB http://slave4:18080/history/application_1679295911706_0025/jobs/job/?id=3
            2,
            # Q10 500GB tpcds stage 4 80.7MB http://slave4:18080/history/application_1679295911706_0012/jobs/job/?id=3
            3.9 * 60,
            # Q11 500GB tpcds stage 8 10.6GiB http://slave4:18080/history/application_1679295911706_0013/jobs/job/?id=2

            # 2,8,4,4

            # catalog_sales
            19,  # Q4 100GB tpcds stage 3.0GiB http://slave4:18080/history/application_1679295911706_0019/jobs/job/?id=2
            1.8 * 60,
            # Q4 500GB tpcds stage 11 15.1GB http://slave4:18080/history/application_1679304940725_0004/jobs/job/?id=2
            # store_sales
            16,  # Q6 100GB tpcds stage 16 1416MB
            29,  # Q6 500GB tpcds stage 16
            # web_sales
            30,
            # Q4 100GB tpcds stage 9 1502.5 MiB http://slave4:18080/history/application_1679295911706_0019/jobs/job/?id=2
            39,  # Q4 500GB tpcds stage 6 7.3GB http://slave4:18080/history/application_1679304940725_0004/jobs/job/?id=2

            # customer
            2,  # Q1 100GB tpcds stage 18MiB http://slave4:18080/history/application_1679304940725_0014/jobs/job/?id=2
            4,  # Q1 500GB tpcds stage 2 60.9MB(input) ,
            # http://slave4:18080/history/application_1679295911706_0003/jobs/job/?id=2
            6,
            # Q6 100GB tpcds stage 16 16.2MB http://slave4:18080/history/application_1679304940725_0019/jobs/job/?id=9
            1.3 * 60,
            # Q6 500GB tpcds stage 16 6.9GiB http://slave4:18080/history/application_1679304940725_0006/jobs/job/?id=9
            2,
            # Q10 100GB tpcds stage 4 23.7MB http://slave4:18080/history/application_1679304940725_0023/jobs/job/?id=3
            3,
            # Q10 500GB tpcds stage 4 80.7MB http://slave4:18080/history/application_1679304940725_0010/jobs/job/?id=3

            # store_sales
            2.1 * 60,
            # Q11 500GB tpcds stage 8 10.6GiB http://slave4:18080/history/application_1679295911706_0013/jobs/job/?id=2

            # 2,8,5,5

            # catalog_sales
            46,  # Q4 100GB tpcds stage 3.0GiB http://slave4:18080/history/application_1682218828348_0039/jobs/job/?id=2
            2.8 * 60,
            # Q4 500GB tpcds stage 11 15.1GB http://slave4:18080/history/application_1679304940725_0004/jobs/job/?id=2
            # store_sales
            18,  # Q6 100GB tpcds stage 16 1416MB http://slave4:18080/history/application_1682218828348_0041/jobs/job/?id=9
            60,  # Q6 500GB tpcds stage 16 6.9 GiB http://slave4:18080/history/application_1682218828348_0011/jobs/job/?id=9
            # web_sales
            35,
            # Q4 100GB tpcds stage 9 1502.5 MiB http://slave4:18080/history/application_1682218828348_0039/jobs/job/?id=2
            1.4*60,
            # Q4 500GB tpcds stage 6 7.3GB http://slave4:18080/history/application_1679304940725_0004/jobs/job/?id=2

            # customer
            2,  # Q1 100GB tpcds stage 18MiB http://slave4:18080/history/application_1679304940725_0014/jobs/job/?id=2
            4,  # Q1 500GB tpcds stage 2 60.9MB(input) ,
            # http://slave4:18080/history/application_1679295911706_0003/jobs/job/?id=2
            13,
            # Q6 100GB tpcds stage 16 16.2MB http://slave4:18080/history/application_1679304940725_0019/jobs/job/?id=9
            1.3 * 60,
            # Q6 500GB tpcds stage 16 6.9GiB http://slave4:18080/history/application_1679304940725_0006/jobs/job/?id=9
            2,
            # Q10 100GB tpcds stage 4 23.7MB http://slave4:18080/history/application_1682218828348_0045/jobs/job/?id=3
            12,
            # Q10 500GB tpcds stage 4 80.7MB http://slave4:18080/history/application_1682218828348_0015/jobs/job/?id=3

            # store_sales
            1.8 * 60,
            # Q11 500GB tpcds stage 8 10.6GiB http://slave4:18080/history/application_1682218828348_0049/jobs/job/?id=2

            # 2,8,6,6

            # store sales
            1.6*60,  # Q11 500GB tpcds stage 8 10.6GiB http://slave4:18080/history/application_1682413175115_0012/jobs/job/?id=2
            # catalog_sales
            23,  # Q4 100GB tpcds stage 3.0GiB http://slave4:18080/history/application_1682413175115_0017/jobs/job/?id=2
            1.2*60,
            # Q4 500GB tpcds  15.1GB http://slave4:18080/history/application_1679304940725_0004/jobs/job/?id=2

            # store_sales
            24,  # Q6 100GB tpcds stage 16 1416MB http://slave4:18080/history/application_1682413175115_0019/jobs/job/?id=9
            1.6*60,  # Q6 500GB tpcds stage 16 6.9GB
            # web_sales
            36,
            # Q4 100GB tpcds stage 9 1502.5 MiB http://slave4:18080/history/application_1682413175115_0017/jobs/job/?id=2
            2.9*60,
            # Q4 500GB tpcds stage 6 7.3GB http://slave4:18080/history/application_1679304940725_0004/jobs/job/?id=2
            # customer
            6,  # Q1 100GB tpcds stage 18MiB http://slave4:18080/history/application_1682413175115_0014/jobs/job/?id=2
            14,  # Q1 500GB tpcds stage 2 60.9MB(input) ,
            # http://slave4:18080/history/application_1682413175115_0002/jobs/job/?id=2
            3,
            # Q6 100GB tpcds stage 16 16.2MB http://slave4:18080/history/application_1682413175115_0019/jobs/job/?id=9
            1.6*60,
            # Q6 500GB tpcds stage 16 6.9GiB http://slave4:18080/history/application_1682413175115_0007/jobs/job/?id=9
            1,
            # Q10 100GB tpcds stage 4 23.7MB http://slave4:18080/history/application_1682413175115_0023/jobs/job/?id=3
            2,
            # Q10 500GB tpcds stage 4 80.7MB http://slave4:18080/history/application_1682413175115_0011/jobs/job/?id=3
            # store_sales
            1.6 * 60
            # Q11 500GB tpcds stage 8 10.6GiB http://slave4:18080/history/application_1682413175115_0012/jobs/job/?id=2

        ])

        # print(len(output_y_sizes), len(output_y_runtimes))
        # predict output datasize
        # NoAggregatePrediction.reg_nnls = LinearRegression(positive=True)
        # NoAggregatePrediction.reg_nnls.fit(input_x_no_join_project, output_y_sizes)
        #
        # NoAggregatePrediction.svm_reg = LinearSVR(epsilon=4)
        # NoAggregatePrediction.svm_reg.fit(input_x_no_join_project, output_y_sizes)
        #
        # NoAggregatePrediction.tree_reg_datasize = DecisionTreeRegressor(max_depth=6)
        # NoAggregatePrediction.tree_reg_datasize.fit(input_x_no_join_project, output_y_sizes)
        #
        # # predict stage's runtime
        # NoAggregatePrediction.tree_reg_runtimes = DecisionTreeRegressor(max_depth=5)
        # NoAggregatePrediction.tree_reg_runtimes.fit(input_x_no_join_project_runtime, output_y_runtimes)
        #
        # NoAggregatePrediction.svm_reg_runtimes = LinearSVR(epsilon=0.5)
        # NoAggregatePrediction.svm_reg_runtimes.fit(input_x_no_join_project_runtime, output_y_runtimes)

        prefix = "../jupyter-notebook/"
        filename = prefix + "model-foresight_first_stage_no_aggregation_prediction_TPC-DS-500GB-svm_reg_outputsize.model"
        NoAggregatePrediction.svm_reg_outputsize = pickle.load(open(filename, 'rb'))
        filename = prefix + "model-foresight_first_stage_no_aggregation_prediction_TPC-DS-500GB-tree_reg_runtimes.model"
        NoAggregatePrediction.tree_reg_runtimes = pickle.load(open(filename, 'rb'))
        filename = prefix + "model-foresight_first_stage_no_aggregation_prediction_TPC-DS-500GB-svm_reg_runtimes.model"
        NoAggregatePrediction.svm_reg_runtimes = pickle.load(open(filename, 'rb'))


    @staticmethod
    def test_prediction():
        # test_x = np.array([
        #
        #     [2.3 * 1000, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q15 500GB tpcds stage 1
        #     [1.3 * 1000, 2, 0, 0, 0, 0, 0, 1, 0, 0],
        #     [9.9 * 1000, 7, 3, 0, 0, 0, 0, 1, 0, 0],
        #     # customer
        #     [75.6, 8, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 100GB tpcds stage 2
        #     [252.5, 8, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 500GB tpcds stage 3
        #
        #     # tpch q3 500G
        #     [34 * 1000, 4, 2, 0, 0, 3, 0, 1, 0, 1]
        #
        # ])
        #
        # test_x_runtime = np.array([
        #
        #     [2, 8, 2, 2, 2.3 * 1000, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q15 500GB tpcds stage 1
        #     [2, 8, 2, 2, 1.3 * 1000, 2, 0, 0, 0, 0, 0, 1, 0, 0],
        #     [2, 8, 2, 2, 9.9 * 1000, 7, 3, 0, 0, 0, 0, 1, 0, 0],
        #     # customer
        #     [2, 8, 2, 2, 75.6, 8, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 100GB tpcds stage 2
        #     [2, 8, 2, 2, 252.5, 8, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 500GB tpcds stage 3
        #
        #     # tpch q3 500G
        #     [2, 8, 2, 2, 34 * 1000, 4, 2, 0, 0, 3, 0, 1, 0, 1]
        #
        # ])
        #
        # # actual [0.1 ] MB
        # predict_output_size = NoAggregatePrediction.reg_nnls.predict(test_x)
        # print("reg_nnls： predict_output_size is {} MB".format(predict_output_size))
        #
        # predict_output_size = NoAggregatePrediction.svm_reg.predict(test_x)
        # print("svm_reg： predict_output_size is {} MB".format(predict_output_size))
        #
        # actual_oput_size = np.array([
        #     7.3 * 1000,  # Q15 500GB tpcds stage 1
        #     0,
        #     0,
        #     145,  # Q11 100GB tpcds stage 2
        #     505.4  # Q11 500GB tpcds stage 3
        # ])
        #
        # # actual runtime [13] seconds
        #
        # predict_output_runtime = NoAggregatePrediction.tree_reg_runtimes.predict(test_x_runtime)
        # print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))
        #
        # predict_output_runtime = NoAggregatePrediction.svm_reg_runtimes.predict(test_x_runtime)
        # print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))
        #
        # actual_oputput_runtime = np.array([
        #     52,  # Q15 500GB tpcds stage 1
        #     0,
        #     0,
        #     7,  # Q11 100GB tpcds stage 2
        #     12  # Q11 500GB tpcds stage 3
        #
        # ])
        #
        # print(NoAggregatePrediction.reg_nnls.intercept_, NoAggregatePrediction.reg_nnls.coef_)

        test_x = np.array([
            [2, 7136, 10, 3, 7475.2, 6, 2, 0, 0, 0, 0, 1, 0, 0, 0, 240]  # Q4,500,9,web_sales,9216

        ])

        # actual [0.1 ] MB
        # predict_output_size = svm_reg.predict(test_x)
        # print("predict_output_size is {} MB".format(predict_output_size) )

        predict_output_size = NoAggregatePrediction.svm_reg_outputsize.predict(test_x)

        actual_oput_size = np.array([
            9216
        ])

        print("actual_oputput_size is {} MiB".format(actual_oput_size))

        print("svm_reg： predict_output_size is {} MiB".format(predict_output_size))

        # actual runtime [13] seconds

        actual_oputput_runtime = np.array([
            16
        ])

        print("actual_oputput_runtime is {} seconds".format(actual_oputput_runtime))
        predict_output_runtime = NoAggregatePrediction.tree_reg_runtimes.predict(test_x)
        print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

        predict_output_runtime = NoAggregatePrediction.svm_reg_runtimes.predict(test_x)
        print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

    def __init__(self):
        return

    @staticmethod
    def predict_stage(test_x_datasize,test_x_runtime):
        # print("in first stage no aggregate performance model.py")

        # predict_output_size = NoAggregatePrediction.svm_reg.predict(test_x_datasize)[0]
        # predict_output_size = NoAggregatePrediction.tree_reg_datasize.predict(test_x_datasize)[0]
        # # print("svm_reg： predict_output_size is {} MB".format(predict_output_size))
        # tree_predict_output_runtime = NoAggregatePrediction.tree_reg_runtimes.predict(test_x_runtime)[0]
        # # print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(tree_predict_output_runtime))
        #
        # # svm_predict_output_runtime = NoAggregatePrediction.svm_reg_runtimes.predict(test_x_runtime)[0]
        # # print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(svm_predict_output_runtime))
        # print("test_X_datasize is {}, predict_output_size is {}".format(test_x_datasize[0], predict_output_size))
        # print("test_X_runtime is {} tree_predict_output_runtime is {}".format(test_x_runtime[0],
        #                                                                       tree_predict_output_runtime))
        # return predict_output_size, tree_predict_output_runtime

        predict_output_size = NoAggregatePrediction.svm_reg_outputsize.predict(test_x_datasize)[0]
        tree_predict_output_runtime = NoAggregatePrediction.tree_reg_runtimes.predict(test_x_runtime)[0]

        # print("test_X_datasize is {}, predict_output_size is {}".format(test_x_datasize[0], predict_output_size))
        # print("test_X_runtime is {} tree_predict_output_runtime is {}".format(test_x_runtime[0],
        #                                                                       tree_predict_output_runtime))
        if predict_output_size <=1:
            predict_output_size = 1
        return predict_output_size, tree_predict_output_runtime


if __name__ == '__main__':
    NoAggregatePrediction.model_init()
    NoAggregatePrediction.test_prediction()
