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

    @staticmethod
    def model_init():
        input_x_stage = np.array([
            #  input_data_size(MB),
            # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth
            # TPC-H
            [6.5*1000, 2, 4, 6, 2, 2, 2, 0, 0],  # Q2 500GB stage 7
            [6.4*1000, 2, 0, 2, 0, 2, 2, 0, 0],  # Q2 500GB stage 9
            [981.4,    1, 0, 0, 1, 0, 0, 1, 0], # Q2 500GB stage 8
            [735.8,    2, 0, 2, 0, 2, 2, 0, 0], # Q2 500GB stage 10
            [6.8*1000, 2, 0, 2, 0, 2, 2, 0, 0], # # Q3 500GB stage 2
            # 10http://slave4:18080/history/application_1684287635805_0018/jobs/job/?id=0
            [24.8*1000, 2,0,2,4,2,2,0,0      ], # Q3 500GB stage 4
            [10*1000,  2, 0, 2, 4, 2,  2, 0, 0] # Q4 500GB stage 2
            # http://slave4:18080/history/application_1684287635805_0019/jobs/job/?id=0
        ])

        output_y_sizes = np.array([
            # TPC-H 500GB
            981.4,  # Q2 500GB stage 7
            40.3, # Q2 500GB stage 9
            695,  # Q2 500GB stage 8
            7.6, # Q2 500GB stage 10
            1032.4, # Q3 500GB stage 2
            1000, # Q3 500GB stage 4
            0.1, # Q4 500GB stage 2
        ])

        input_x__large_stage = np.array([
            # particular cases for Q25, , Q29, Q17, Q72
            #  input_data_size(MB),
            # begin_node_length, broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth
            [40 * 1000, 2, 0, 2, 0, 2, 2, 0, 0]

        ])

        output_y_large_sizes = np.array([
            # particular cases for Q25,Q72, Q29
            3.4 * 1000  # Q25 500GB tpcds stage 6


        ])


        input_x_stage_runtime = np.array([
            # executor_cores, executor_memory,executor_instances, number_of_machines, input_data_size(MB),
            # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth  2,0,2,0,2,2,0,0

            # 2,8,4,4

            # executor_cores, executor_memory,executor_instances, number_of_machines, input_data_size(MB),
            # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth
            [2, 8, 4, 4, 6.5*1000, 2, 4, 6, 2, 2, 2, 0, 0],  # Q2 500GB stage 7
            [2, 8, 4, 4, 6.9*1000, 2, 0, 2, 0, 2, 2, 0, 0], # Q2 500GB stage 9
            [2, 8, 4, 4, 981.4,    1, 0, 0, 1, 0, 0, 1, 0], # Q2 500GB stage 8
            [2, 8, 4, 4, 735.8,    2, 0, 2, 0, 2, 2, 0, 0], # Q2 500GB stage 10
            [2, 8, 4, 4, 6.8*1000, 2, 0, 2, 0, 2, 2, 0, 0], # Q3 500GB stage 2
            [2, 8, 4, 4, 24.8*1000,2, 0, 2, 4, 2, 2, 0, 0], # Q3 500GB stage 4
            [2, 8, 4, 4, 10*1000,  2, 0, 2, 4, 2,  2, 0, 0], # Q4 500GB stage 2
        ])



        output_y_runtimes = np.array([
            37,  # Q2 500GB stage 7 http://slave4:18080/history/application_1684287635805_0017/jobs/job/?id=3
            1.2*60, # Q2 500GB stage 9
            11, # Q2 500GB stage 8
            11, # Q2 500GB stage 10
            41, # Q3 500GB stage 2
            2.2*60,
            1.7*60, # Q4 500GB stage 2
            # http://slave4:18080/history/application_1684287635805_0019/jobs/job/?id=0
        ])

        # print(len(output_y_sizes), len(output_y_runtimes))

        # predict output data size
        NormalStagePrediction.svm_reg_datasize = LinearSVR(epsilon=10)
        NormalStagePrediction.svm_reg_datasize.fit(input_x_stage, output_y_sizes)

        NormalStagePrediction.tree_reg_datasize = DecisionTreeRegressor(max_depth=6)
        NormalStagePrediction.tree_reg_datasize.fit(input_x_stage, output_y_sizes)

        NormalStagePrediction.random_forest_reg_datasize=RandomForestRegressor(max_depth=6, random_state=0)
        NormalStagePrediction.random_forest_reg_datasize.fit(input_x_stage, output_y_sizes)

        # NormalStagePrediction.linear_reg_datasize = LinearRegression()
        NormalStagePrediction.linear_reg_datasize = LinearRegression(positive=True)
        NormalStagePrediction.linear_reg_datasize.fit(input_x_stage,output_y_sizes)

        ## predict output data size with large datasets (>=20GB)
        NormalStagePrediction.large_tree_reg_datasize = DecisionTreeRegressor(max_depth=4)
        NormalStagePrediction.large_tree_reg_datasize.fit(input_x__large_stage, output_y_large_sizes)

        NormalStagePrediction.large_random_forest_datasize = RandomForestRegressor(max_depth=6, random_state=0)
        NormalStagePrediction.large_random_forest_datasize.fit(input_x__large_stage, output_y_large_sizes)

        # predict stage's runtime

        NormalStagePrediction.tree_reg_runtimes = DecisionTreeRegressor(max_depth=3)
        NormalStagePrediction.tree_reg_runtimes.fit(input_x_stage_runtime, output_y_runtimes)

        NormalStagePrediction.svm_reg_runtimes = LinearSVR(epsilon=0.5)
        NormalStagePrediction.svm_reg_runtimes.fit(input_x_stage_runtime, output_y_runtimes)


        large_input_stage_runtime = np.array([
            # executor_cores, executor_memory,executor_instances, number_of_machines, input_data_size(MB),
            # begin_node_length,broadcastjoin_depth, project_depth,aggregate_depth, sort_depth, sort_merge_depth,
            # filter_depth, union_depth  2,0,2,0,2,2,0,0
            [2, 8, 5, 5, 39*1000, 2, 0, 2, 0, 2, 2, 0, 0],  # Q17 500GB tpcds stage 7
            # http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
            [2, 8, 5, 5, 14.2*1000, 2, 10, 12, 2, 2, 2, 0, 0],  # Q17 500GB tpcds stage 8

            # http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4

            [2, 8, 5, 5, 39 * 1000, 2, 0, 2, 0, 2, 2, 0, 0],  # Q29 500GB tpcds stage 8
            # http://slave4:18080/history/application_1683354485229_0019/jobs/job/?id=5
            [2, 8, 5, 5, 14.2 * 1000, 2, 10, 12, 2, 2, 2, 0, 0],  # Q29 500GB tpcds stage 9

            # http://slave4:18080/history/application_1683354485229_0007/jobs/job/?id=4
            [2, 8, 5, 5, 28*1000,     2, 0, 2, 2, 2, 2, 0, 0] # Q72 500GB tpcds stage 10
        ])

        large_input_runtimes = np.array([
                                        4.4*60, # Q17 500GB tpcds stage 7
                                         2*60, # # Q17 500GB tpcds stage 8
                                        4.1 * 60,  # Q29 500GB tpcds stage 8
                                        2 * 60, # Q29 500GB tpcds stage 9
                                         11*60 # Q72 500GB tpcds stage 10
                                         ])
        NormalStagePrediction.large_svm_reg_runtimes  = LinearSVR(epsilon=0.5)
        NormalStagePrediction.large_svm_reg_runtimes.fit(large_input_stage_runtime, large_input_runtimes)

        NormalStagePrediction.large_tree_reg_runtimes = DecisionTreeRegressor(max_depth=6)
        NormalStagePrediction.large_tree_reg_runtimes.fit(large_input_stage_runtime, large_input_runtimes)

    @staticmethod
    def test_prediction():
        test_x = np.array([
            [1271.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q11 100GB tpcds stage 4
            [79.5, 1, 0, 0, 1, 0, 0, 0, 0],  # Q11 100GB tpcds stage 5

        ])

        # [31254, 2, 0, 2, 0, 2, 2, 0, 0]

        large_test_x = np.array([
            [20000, 2, 0, 2, 0, 2, 2, 0, 0],
            [31254, 2, 0, 2, 0, 2, 2, 0, 0]
        ])

        test_x_runtime = np.array([
            [2, 8, 2, 2,1271.9, 2, 2, 4, 2, 2, 2, 0, 0],  # Q11 100GB tpcds stage 4
            [2, 8, 2, 2,79.5, 1, 0, 0, 1, 0, 0, 0, 0]  # Q11 100GB tpcds stage 5

        ])

        large_test_x_runtime = np.array([
            [2,2, 8, 5, 5, 28*1000,     2, 0, 2, 2, 2, 2, 0, 0]  # Q11 100GB tpcds stage 4

        ])

        predict_output_size = NormalStagePrediction.svm_reg_datasize.predict(test_x)
        print("svm_reg： predict_output_size is {} MB".format(predict_output_size))

        tree_predict_output_size = NormalStagePrediction.tree_reg_datasize.predict(test_x)
        print("tree_reg_datasize： tree_predict_output_size is {} MB".format(tree_predict_output_size))

        print("predict output datasize with large datasets (>=20GB)")

        large_predict_datasize = NormalStagePrediction.large_tree_reg_datasize.predict(large_test_x)
        print("tree_predict_output_size： large_tree_reg_datasize is {} MB".format(large_predict_datasize))

        large_predict_datasize = NormalStagePrediction.large_random_forest_datasize.predict(large_test_x)
        print("tree_predict_output_size： large_random_forest_datasize is {} MB".format(large_predict_datasize))

        random_forest_predict_output_size = NormalStagePrediction.random_forest_reg_datasize.predict(test_x)
        print("random_forest_predict_output_size： random_forest_predict_output_size is {} MB".format(random_forest_predict_output_size))

        linear_predict_output_size = NormalStagePrediction.linear_reg_datasize.predict(test_x)
        print("linear_predict_output_size： linear_predict_output_size is {} MB".format(
            linear_predict_output_size))

        actual_oput_size = np.array([
            79.5,  # Q11 100GB tpcds stage 4
            18.2  # Q11 100GB tpcds stage 5
        ])

        actual_oputput_runtime = np.array([
            12,  # Q11 100GB tpcds stage 4
            2,  # Q11 100GB tpcds stage 5
        ])

        print("actual_oputput_runtime is {} seconds".format(actual_oputput_runtime))
        predict_output_runtime = NormalStagePrediction.tree_reg_runtimes.predict(test_x_runtime)
        print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

        predict_output_runtime = NormalStagePrediction.svm_reg_runtimes.predict(test_x_runtime)
        print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))


        large_test_x_runtime = np.array([
            [2, 8, 5, 5, 40*1000, 2, 0, 2, 0, 2, 2, 0, 0] , #
            [2, 8, 5, 5, 28 * 1000, 2, 0, 2, 2, 2, 2, 0, 0]  # Q11 100GB tpcds stage 4
            # [2, 8, 5, 5,79.5, 1, 0, 0, 1, 0, 0, 0, 0]  # Q11 100GB tpcds stage 5

        ])

        predict_output_runtime = NormalStagePrediction.large_svm_reg_runtimes.predict(large_test_x_runtime)
        print("large_svm_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

        predict_output_runtime = NormalStagePrediction.large_tree_reg_runtimes.predict(large_test_x_runtime)
        print("large_tree_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

    def __init__(self):
        return

    def predict_stage(self, test_X_datasize, test_X_runtime):
        print("in normal stage performance model.py")
        print("test_X_datasize is {}".format(test_X_datasize[0]))
        print("test_X_runtime is {}".format(test_X_runtime[0]))

        input_data_size = test_X_datasize[0][0]
        if input_data_size>=20*1000: # large input dataset (>=20GB)
            print("large input data size (>=20GB) for prediction of output size")
            predict_output_size = NormalStagePrediction.large_tree_reg_datasize.predict(test_X_datasize)[0]
            # predict_output_size = NormalStagePrediction.large_random_forest_datasize.predict(test_X_datasize)[0]
        else:
            # predict_output_size = NormalStagePrediction.svm_reg_datasize.predict(test_X_datasize)[0]
            predict_output_size = NormalStagePrediction.tree_reg_datasize.predict(test_X_datasize)[0]

        # print("svm_reg： predict_output_size is {} MB".format(predict_output_size))
        # [2, 8, 2, 2, 40*1000, 2, 0, 2, 0, 2, 2, 0, 0]
        test_X=test_X_runtime[0]
        # if test_X[4] >2000 and test_X[5] ==2 and test_X[6]==0 and test_X[7]==2 and test_X[8]==0 and test_X[9]==2 and test_X[10]==2 and test_X[11]==0 and test_X[12] ==0:
        if test_X[4] > 5*2000: # large data set
            # call the predction model of large dataset
            print("large input data size (>=10GB) for prediction of runtime")
            # large_svm_output_runtime=NormalStagePrediction.large_svm_reg_runtimes.predict(test_X_runtime)[0]
            large_tree_output_runtime=NormalStagePrediction.large_tree_reg_runtimes.predict(test_X_runtime)[0]
            print("predict_output_size is {}".format(predict_output_size))
            print("large_tree_reg_output_runtime is {}".format(large_tree_output_runtime))

            # return predict_output_size,large_svm_output_runtime
            return predict_output_size,large_tree_output_runtime
        # print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(tree_predict_output_runtime))
        else:
            tree_predict_output_runtime = NormalStagePrediction.tree_reg_runtimes.predict(test_X_runtime)[0]
            # svm_predict_output_runtime = NormalStagePrediction.svm_reg_runtimes.predict(test_X_runtime)[0]
            # print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(svm_predict_output_runtime))
            print(" predict_output_size is {}".format( predict_output_size))
            print("tree_predict_output_runtime is {}".format( tree_predict_output_runtime))
            return predict_output_size, tree_predict_output_runtime



if __name__ == '__main__':
    test_stage_predict = NormalStagePrediction()
    test_stage_predict.model_init()
    NormalStagePrediction.test_prediction()
