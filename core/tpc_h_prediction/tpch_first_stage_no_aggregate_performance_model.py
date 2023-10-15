#!/usr/bin/env python
# coding: utf-8

import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import LinearSVR


class NoAggregatePrediction:
    reg_nnls = None
    svm_reg = None
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
            # TPC-H
            [4.9*1000, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q2 500GB tpc-h stage 3
            # http://slave4:18080/history/application_1684287635805_0017/jobs/job/?id=3
            [632.1, 4, 3, 0, 0, 2, 0, 1, 0, 1],  # Q2 500GB tpc-h stage 4
            [4.9*1000, 3, 3, 0, 0, 0, 0, 1, 0, 0], # Q2 500GB tpc-h stage 5
            [24.8,     7, 2, 2, 0, 0, 0,  1, 0,0], # Q2 500GB tpc-h stage 6
            [384.8,    7, 2, 0, 0, 0, 0,  1, 0,0], # Q2 500GB tpc-h stage 11
            [8.6*1000, 4, 3, 0, 0, 0, 0,  1, 0,0], # Q3 500GB stage 1
            # http://slave4:18080/history/application_1684287635805_0018/jobs/job/?id=0
            [34.1*1000,     4, 2, 0, 0, 3, 0,  1, 0,1],  # Q3 500GB stage 3
            [4.8*1000,  3, 1, 0, 0, 2, 0, 1, 0, 1],  # Q4 500GB stage 0
            # http://slave4:18080/history/application_1684287635805_0019/jobs/job/?id=0
            [20.8*1000, 3, 2, 0, 0, 1, 0, 1, 0, 1] # Q4 stage 1 500GB
        ])

        output_y_sizes = np.array([
            # TPC-H http://slave4:18080/history/application_1684287635805_0017/jobs/job/?id=3
            6.4* 1000,   # Q2 500GB tpc-h stage 3
            4.4, # Q2 500GB tpc-h stage 4
            6.4 * 1000, # Q2 500GB tpc-h stage 5
            47.7, # Q2 500GB tpc-h stage 6
            559.6, # Q2 500 GB tpch-h stage 11
            6.8*1000, # Q3 500GB stage 1
            23.8*1000, # Q3 500GB stage 3
            301.9, # Q4 500GB stage 0
            9.8*1000, # Q4 500Gb stage 1
        ])

        # # executor_cores, executor_memory,executor_instances, number_of_machines, nput data sizes (MB)1,
        # filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#, # project 6#, hashAggregate 7
        # #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#

        input_x_no_join_project_runtime = np.array([

            # TPC-H
            [2, 8, 4, 4, 4.9*1000, 3, 2, 0, 0, 0, 0, 1, 0, 0],   # Q2 500GB tpc-h stage 3
            # http://slave4:18080/history/application_1684287635805_0017/jobs/job/?id=3
            [2, 8, 4, 4, 632.1, 4, 3, 0, 0, 2, 0, 1, 0, 1],  # Q2 500GB tpc-h stage 4
            # http://slave4:18080/history/application_1684287635805_0017/jobs/job/?id=3
            [2, 8, 4, 4, 4.9*1000, 3, 3, 0, 0, 0, 0, 1, 0, 0],
            [2, 8, 4, 4, 24.8,     7, 2, 2, 0, 0, 0,  1, 0,0], # Q2 500GB tpc-h stage 6
            [2, 8, 4, 4, 559.6,    7, 2, 0, 0, 0, 0, 1, 0, 0], # Q2 500GB tpc-h stage 11
            [2, 8, 4, 4, 8.6*1000, 4, 3, 0, 0, 0, 0,  1, 0,0], # Q3 500GB stage 1
            [2, 8, 4, 4, 34.1*1000, 4, 2, 0, 0, 3, 0,  1, 0,1], # Q3 500GB stage 1
            [2, 8, 4, 4, 4.8*1000,  3, 1, 0, 0, 2, 0, 1, 0, 1],  # Q4 500GB stage 0
            [2, 8, 4, 4, 20.8*1000, 3, 2, 0, 0, 1, 0, 1, 0, 1],  # Q4 500GB stage 1

        ])

        output_y_runtimes = np.array([
            # 2, 8, 4, 4
            # TPC-H
            34,  # Q2 500GB tpc-h stage 3 http://slave4:18080/history/application_1684287635805_0017/jobs/job/?id=3
            9, # Q2 500GB tpc-h stage 4
            1.1*60, # Q2 500GB tpc-h stage 5
            7, # Q2 500GB tpc-h stage 6
            1.2*60, # Q2 500GB tpc-h stage 11
            1.1*60, # Q3 500GB stage 1 http://slave4:18080/history/application_1684287635805_0018/jobs/job/?id=0
            5.1*60,
            46,  # Q4 500GB stage 0
            3.7*60
        ])

        # print(len(output_y_sizes), len(output_y_runtimes))
        # predict output datasize
        NoAggregatePrediction.reg_nnls = LinearRegression(positive=True)
        NoAggregatePrediction.reg_nnls.fit(input_x_no_join_project, output_y_sizes)

        NoAggregatePrediction.svm_reg = LinearSVR(epsilon=4)
        NoAggregatePrediction.svm_reg.fit(input_x_no_join_project, output_y_sizes)

        NoAggregatePrediction.tree_reg_datasize = DecisionTreeRegressor(max_depth=6)
        NoAggregatePrediction.tree_reg_datasize.fit(input_x_no_join_project, output_y_sizes)

        # predict stage's runtime
        NoAggregatePrediction.tree_reg_runtimes = DecisionTreeRegressor(max_depth=6)
        NoAggregatePrediction.tree_reg_runtimes.fit(input_x_no_join_project_runtime, output_y_runtimes)

        NoAggregatePrediction.svm_reg_runtimes = LinearSVR(epsilon=0.5)
        NoAggregatePrediction.svm_reg_runtimes.fit(input_x_no_join_project_runtime, output_y_runtimes)

        # NoAggregatePrediction.svm_reg = LinearSVR(epsilon=0.5)
        # NoAggregatePrediction.svm_reg.fit(input_x_no_join_project, output_y_sizes)
        #
        # NoAggregatePrediction.tree_reg_runtimes = DecisionTreeRegressor(max_depth=3)
        # NoAggregatePrediction.tree_reg_runtimes.fit(input_x_no_join_project, output_y_runtimes)
        #
        # NoAggregatePrediction.svm_reg_runtimes = LinearSVR(epsilon=0.5)
        # NoAggregatePrediction.svm_reg_runtimes.fit(input_x_no_join_project, output_y_runtimes)

    @staticmethod
    def test_prediction():
        test_x = np.array([

            [21.4 * 1000, 3,2,0,0,2, 0, 1, 0, 1],  # Q13 500GB tpcds stage 0
            [10 * 1000, 3, 2, 0, 0, 2, 0, 1, 0, 1],  # Q13 500GB tpcds stage 0

        ])

        test_x_runtime = np.array([

            [2, 8, 4, 4, 21.4 * 1000, 3,2,0,0,2, 0, 1, 0, 1],  # Q13 500GB tpcds stage 0
            [2, 8, 4, 4, 10 * 1000, 3,2,0,0,2, 0, 1, 0, 1],  # Q13 500GB tpcds stage 0


        ])

        # actual [0.1 ] MB
        predict_output_size = NoAggregatePrediction.reg_nnls.predict(test_x)
        print("reg_nnls： predict_output_size is {} MB".format(predict_output_size))

        predict_output_size = NoAggregatePrediction.svm_reg.predict(test_x)
        print("svm_reg： predict_output_size is {} MB".format(predict_output_size))

        predict_output_size = NoAggregatePrediction.tree_reg_datasize.predict(test_x)
        print("tree_reg_datasize： predict_output_size is {} MB".format(predict_output_size))

        actual_oput_size = np.array([
            7.3 * 1000,  # Q15 500GB tpcds stage 1
            0,
            0,
            145,  # Q11 100GB tpcds stage 2
            505.4  # Q11 500GB tpcds stage 3
        ])

        # actual runtime [13] seconds

        predict_output_runtime = NoAggregatePrediction.tree_reg_runtimes.predict(test_x_runtime)
        print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

        predict_output_runtime = NoAggregatePrediction.svm_reg_runtimes.predict(test_x_runtime)
        print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

        actual_oputput_runtime = np.array([
            52,  # Q15 500GB tpcds stage 1
            0,
            0,
            7,  # Q11 100GB tpcds stage 2
            12  # Q11 500GB tpcds stage 3

        ])

        print(NoAggregatePrediction.reg_nnls.intercept_, NoAggregatePrediction.reg_nnls.coef_)

    def __init__(self):
        return

    @staticmethod
    def predict_stage(test_x_datasize,test_x_runtime):
        print("in first stage no aggregate performance model.py")

        # predict_output_size = NoAggregatePrediction.svm_reg.predict(test_x_datasize)[0]
        predict_output_size = NoAggregatePrediction.tree_reg_datasize.predict(test_x_datasize)[0]
        # print("svm_reg： predict_output_size is {} MB".format(predict_output_size))
        tree_predict_output_runtime = NoAggregatePrediction.tree_reg_runtimes.predict(test_x_runtime)[0]
        # print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(tree_predict_output_runtime))

        # svm_predict_output_runtime = NoAggregatePrediction.svm_reg_runtimes.predict(test_x_runtime)[0]
        # print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(svm_predict_output_runtime))
        print("test_X_datasize is {}, predict_output_size is {}".format(test_x_datasize[0], predict_output_size))
        print("test_X_runtime is {} tree_predict_output_runtime is {}".format(test_x_runtime[0],
                                                                              tree_predict_output_runtime))
        return predict_output_size, tree_predict_output_runtime


if __name__ == '__main__':
    NoAggregatePrediction.model_init()
    NoAggregatePrediction.test_prediction()
