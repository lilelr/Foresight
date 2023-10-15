#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import LinearSVR

from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_classification


class FirstStagePrediction:

    svm_reg=None
    tree_reg_datasets=None
    tree_reg_runtimes=None
    svm_reg_runtimes=None

    @staticmethod
    def model_init():
        # focus on the table called catalog_sales input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,
        # filter other  4#,  BroadcastHashJoin  5#, project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #,
        # broadcasthashJoin depth 9#, project depth 10#

        input_x_first_stage = np.array([


            [26.4*1000, 7, 1, 0, 0, 6, 12, 1, 0, 1],  # Q1 500 GB stage 0
            [348.1,     2, 2, 0, 0, 1,  0, 1, 0, 1]  # Q1 500 GB stage 0

        ])

        # for i, elements in enumerate(input_x_first_stage):
        #     if i % 2 == 1:  # odd
        #         print("500GB input data size is {}".format(input_x_first_stage[i][0]))
        #         relation = input_x_first_stage[i][0] / input_x_first_stage[i - 1][0]
        #         print("relation is {}".format(relation))

        output_y_sizes = np.array([


            0.5,  # Q1 500 GB stage 0
            82.4, # Q3 500 GB stage 0

        ])

        # focus on the first stage with complex operators like hashjoin, project, hashaggregate
        #  # executor_cores, executor_memory,executor_instances, number_of_machines,
        #  focus on the table called catalog_sales input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,
        # filter other  4#,  BroadcastHashJoin  5#, project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #,
        # broadcasthashJoin depth 9#, project depth 10#

        input_x_first_stage_runtime = np.array([
            # TPC-H 500GB
            [2, 8, 4, 4, 26.4*1000, 7, 1, 0, 0, 6, 12, 1, 0, 1],  # Q1 500GB stage 0
            [2, 8, 4, 4, 348.1,     2, 2, 0, 0, 1,  0, 1, 0, 1],  # Q3 500GB stage 0
        ])


        output_y_runtimes = np.array([
            # catalog_sales
            4.7*60,  # Q1 500GB stage 0 http://slave4:18080/history/application_1684287635805_0016/jobs/job/?id=0
            8,  # Q3 500GB stage 0 http://slave4:18080/history/application_1684287635805_0018/jobs/job/?id=0
        ])

        FirstStagePrediction.svm_reg = LinearSVR(epsilon=0.5)
        FirstStagePrediction.svm_reg.fit(input_x_first_stage, output_y_sizes)

        FirstStagePrediction.tree_reg_datasets = DecisionTreeRegressor(max_depth=3)
        FirstStagePrediction.tree_reg_datasets.fit(input_x_first_stage, output_y_sizes)

        FirstStagePrediction.tree_reg_runtimes = DecisionTreeRegressor(max_depth=3)
        FirstStagePrediction.tree_reg_runtimes.fit(input_x_first_stage_runtime, output_y_runtimes)

        FirstStagePrediction.svm_reg_runtimes = LinearSVR(epsilon=0.5)
        FirstStagePrediction.svm_reg_runtimes.fit(input_x_first_stage_runtime, output_y_runtimes)

    def __init__(self):
        return

    @staticmethod
    def predict_stage( test_x_datasize,test_x_runtime):
        print("in first stage performance model.py")

        # predict_output_size = FirstStagePrediction.svm_reg.predict(test_x_datasize)[0]
        predict_output_size = FirstStagePrediction.tree_reg_datasets.predict(test_x_datasize)[0]
        # print("svm_reg： predict_output_size is {} MB".format(predict_output_size))
        tree_predict_output_runtime = FirstStagePrediction.tree_reg_runtimes.predict(test_x_runtime)[0]
        # print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(tree_predict_output_runtime))

        # svm_predict_output_runtime = FirstStagePrediction.svm_reg_runtimes.predict(test_x_runtime)[0]
        # print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(svm_predict_output_runtime))
        print("test_X_datasize is {}, predict_output_size is {}".format(test_x_datasize[0],predict_output_size))
        print("test_X_runtime is {} tree_predict_output_runtime is {}".format(test_x_runtime[0],tree_predict_output_runtime))
        return predict_output_size, tree_predict_output_runtime

    @staticmethod
    def test_prediction():

        # input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#,
        # project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#

        test_x = np.array([
            [465.8, 3, 2, 0, 2, 3, 1, 1, 3, 3],  # catalog_sales, Q15 100GB tpcds stage 3

            #     [2.3 *1000,   3, 2, 0, 2,  3,  1,1, 3, 3]# Q15 500GB tpcds stage 3

            [9.9 * 1000, 7, 3, 0, 0, 0, 0, 1, 0, 0],

            # tpch-q1
            [26 * 1000, 7, 1, 0, 0, 6, 12, 1, 0, 1],
            # tpch q3 500G
            [34 * 1000, 4, 2, 0, 0, 3, 0, 1, 0, 1]

        ])

        test_x_runtime = np.array([
            [2, 8, 2, 2,465.8, 3, 2, 0, 2, 3, 1, 1, 3, 3],  # catalog_sales, Q15 100GB tpcds stage 3

            #     [2.3 *1000,   3, 2, 0, 2,  3,  1,1, 3, 3]# Q15 500GB tpcds stage 3

            [2, 8, 2, 2,9.9 * 1000, 7, 3, 0, 0, 0, 0, 1, 0, 0],

            # tpch-q1
            [2, 8, 2, 2,26 * 1000, 7, 1, 0, 0, 6, 12, 1, 0, 1],
            # tpch q3 500G
            [2, 8, 2, 2,34 * 1000, 4, 2, 0, 0, 3, 0, 1, 0, 1]

        ])



        # actual [0.1 ] MB
        # predict_output_size = svm_reg.predict(test_x)
        # print("predict_output_size is {} MB".format(predict_output_size) )

        predict_output_size = FirstStagePrediction.svm_reg.predict(test_x)
        print("svm_reg： predict_output_size is {} MB".format(predict_output_size))

        # actual runtime [13] seconds
        actual_oputput_runtime = np.array([
            13,  # catalog_sales, Q15 100GB tpcds stage 3
            58,  # tpch q1 500G
            51  # tpch q3 500G
        ])

        print("actual_oputput_runtime is {} seconds".format(actual_oputput_runtime))
        predict_output_runtime = FirstStagePrediction.tree_reg_runtimes.predict(test_x_runtime)
        print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

        predict_output_runtime = FirstStagePrediction.svm_reg_runtimes.predict(test_x_runtime)
        print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))


if __name__ == '__main__':
    FirstStagePrediction.model_init()
    FirstStagePrediction.test_prediction()
