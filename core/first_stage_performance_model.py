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
import pickle


class FirstStagePrediction:

    svm_reg=None
    svm_reg_outputsize=None
    tree_reg_datasets=None
    tree_reg_runtimes=None
    svm_reg_runtimes=None

    @staticmethod
    def model_init():
        # focus on the table called catalog_sales input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,
        # filter other  4#,  BroadcastHashJoin  5#, project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #,
        # broadcasthashJoin depth 9#, project depth 10#

        input_x_first_stage = np.array([
            # catalog_sales
            #     [3.0*1000,  6, 2, 0,  0, 0, 0, 1, 0, 0], # Q4 100GB tpcds stage 6
            #     [15.1*1000, 6, 2, 0,  0, 0, 0, 1, 0, 0],  # Q4 500GB tpcds stage 6
            [1770.2, 4, 2, 0, 2, 6, 1, 1, 2, 2],  # Q5 100GB tpcds stage 6 # union is an expensive operation
            [8.6 * 1000, 4, 2, 0, 2, 6, 1, 1, 2, 2],  # Q5 500GB tpcds stage 6
            [1093.9, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 100GB tpcds stage 3
            [5.3 * 1000, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 500GB tpcds stage 3
            [193.3, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds stage 6
            [1017.1, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 500GB tpcds stage 6

            # store_sales
            [2.5 * 1000, 3, 2, 0, 2, 4, 3, 3, 2, 2],  # Q3 100GB tpcds stage 2
            [12.6 * 1000, 3, 2, 0, 2, 3, 3, 3, 2, 2],  # Q3 500GB tpcds stage 2
            #     [1416.2,    3, 3, 0,  0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16
            #     [6.9*1000,  3, 3, 0,  0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16
            [3.4 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 100GB tpcds stage 4
            [17.3 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 500GB tpcds stage 4
            [1553.9, 3, 2, 0, 2, 2, 1, 1, 3, 3],  # Q8 100GB tpcds stage 8
            [7.7 * 1000, 3, 2, 0, 2, 1, 0, 1, 3, 3],  # Q8 500GB tpcds stage 8

            # web_sales
            [1093.9, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 500GB tpcds stage3
            [5.3 * 1000, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 100GB tpcds stage3
            [60.3, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds stage 4
            [309.8, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds stage 5

            # store_returns

            [308.3, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 100GB tpcds
            [1551.6, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 500GB tpcds
            [3.2 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3],  # Q5 100GB tpcds stage 8
            [16.0 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3]  # Q5 500GB tpcds stage 8

        ])

        # for i, elements in enumerate(input_x_first_stage):
        #     if i % 2 == 1:  # odd
        #         print("500GB input data size is {}".format(input_x_first_stage[i][0]))
        #         relation = input_x_first_stage[i][0] / input_x_first_stage[i - 1][0]
        #         print("relation is {}".format(relation))

        output_y_sizes = np.array([

            # catalog_sales
            #                    3.6*1000, # Q4 100GB tpcds stage 6
            #                    18.2*1000 , # Q4 500GB tpcds stage 6
            0.3,  # Q5 100GB tpcds stage 6
            0.4,  # Q5 500GB tpcds stage 6
            1.4,  # Q2 100GB tpcds stage 3
            5.5,  # Q2 500GB tpcds stage 3
            5.9,  # Q10 100GB tpcds stage 6
            31,  # Q10 500GB tpcds stage 6

            # store_sales
            3.0,  # Q3 100GB tpcds stage 2
            15.3,  # # Q3 500GB tpcds stage 2
            #                    2.2*1000 , # Q6 100GB tpcds stage 16
            #                    11.2*1000,  # Q6 500GB tpcds stage 16
            29,  # Q7 100GB tpcds stage 4
            144.0,  # Q7 500GB tpcds stage 4
            0.1,  # Q8 100GB tpcds stage 8
            0.5,  # Q8 500GB tpcds stage 8

            # web_sales
            1.5,  # Q2 100GB tpcds
            5.5,  # Q2 500GB tpcds
            3,  # Q10 100GB tpcds stage 4
            14,  # Q10 100GB tpcds stage 5

            # store_returns
            81.5,  # q1 100GB tpcds
            423.0,  # q1 500GB tpcds
            1.9,  # Q5 100GB tpcds stage 8
            16.5  # Q5 500GB tpcds stage 8

        ])

        # focus on the first stage with complex operators like hashjoin, project, hashaggregate
        #  # executor_cores, executor_memory,executor_instances, number_of_machines,
        #  focus on the table called catalog_sales input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,
        # filter other  4#,  BroadcastHashJoin  5#, project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #,
        # broadcasthashJoin depth 9#, project depth 10#

        input_x_first_stage_runtime = np.array([
            # catalog_sales
            [2, 8, 2, 2, 1770.2, 4, 2, 0, 2, 6, 1, 1, 2, 2],  # Q5 100GB tpcds stage 6 # union is an expensive operation
            [2, 8, 2, 2, 8.6 * 1000, 4, 2, 0, 2, 6, 1, 1, 2, 2],  # Q5 500GB tpcds stage 6
            [2, 8, 2, 2, 1093.9, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 100GB tpcds stage 3
            [2, 8, 2, 2, 5.3 * 1000, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 500GB tpcds stage 3
            [2, 8, 2, 2, 193.3, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds stage 6
            [2, 8, 2, 2, 1017.1, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 500GB tpcds stage 6

            # store_sales
            [2, 8, 2, 2, 2.5 * 1000, 3, 2, 0, 2, 4, 3, 3, 2, 2],  # Q3 100GB tpcds stage 2
            [2, 8, 2, 2, 12.6 * 1000, 3, 2, 0, 2, 3, 3, 3, 2, 2],  # Q3 500GB tpcds stage 2
            #     [1416.2,    3, 3, 0,  0, 0, 0, 1, 0, 0],  # Q6 100GB tpcds stage 16
            #     [6.9*1000,  3, 3, 0,  0, 0, 0, 1, 0, 0],  # Q6 500GB tpcds stage 16
            [2, 8, 2, 2, 3.4 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 100GB tpcds stage 4
            [2, 8, 2, 2, 17.3 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 500GB tpcds stage 4
            [2, 8, 2, 2, 1553.9, 3, 2, 0, 2, 2, 1, 1, 3, 3],  # Q8 100GB tpcds stage 8
            [2, 8, 2, 2, 7.7 * 1000, 3, 2, 0, 2, 1, 0, 1, 3, 3],  # Q8 500GB tpcds stage 8

            # web_sales
            [2, 8, 2, 2, 1093.9, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 500GB tpcds stage3
            [2, 8, 2, 2, 5.3 * 1000, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 100GB tpcds stage3
            [2, 8, 2, 2, 60.3, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds stage 4
            [2, 8, 2, 2, 316.4, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds stage 5

            # store_returns

            [2, 8, 2, 2, 308.3, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 100GB tpcds
            [2, 8, 2, 2, 1551.6, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 500GB tpcds stage 4
            [2, 8, 2, 2, 3.2 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3],  # Q5 100GB tpcds stage 8
            [2, 8, 2, 2, 16.0 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3],  # Q5 500GB tpcds stage 8
            
            # 2,8,4,4

                # catalog_sales
            [2, 8, 4, 4, 1770.2, 4, 2, 0, 2, 6, 1, 1, 2, 2],  # Q5 100GB tpcds stage 6  1770 MB # union is an expensive operation
            [2, 8, 4, 4, 8.6 * 1000, 4, 2, 0, 2, 6, 1, 1, 2, 2],  # Q5 500GB tpcds stage 6
            [2, 8, 4, 4, 1093.9, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 100GB tpcds stage 3
            [2, 8, 4, 4, 5.3 * 1000, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 500GB tpcds stage 3
            [2, 8, 4, 4, 193.3, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds stage 6
            [2, 8, 4, 4, 1017.1, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 500GB tpcds stage 6

            # store_sales
            [2, 8, 4, 4, 2.5 * 1000, 3, 2, 0, 2, 4, 3, 3, 2, 2],  # Q3 100GB tpcds stage 2
            [2, 8, 4, 4, 12.6 * 1000, 3, 2, 0, 2, 3, 3, 3, 2, 2],  # Q3 500GB tpcds stage 2
            [2, 8, 4, 4, 3.4 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 100GB tpcds 3.4GB
            [2, 8, 4, 4, 17.3 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 500GB tpcds stage 4
            [2, 8, 4, 4, 1553.9, 3, 2, 0, 2, 2, 1, 1, 3, 3],  # Q8 100GB tpcds stage 8
            [2, 8, 4, 4, 7.7 * 1000, 3, 2, 0, 2, 1, 0, 1, 3, 3],  # Q8 500GB tpcds stage 8

            # web_sales
            [2, 8, 4, 4, 1093.9, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 500GB tpcds stage3
            [2, 8, 4, 4, 5.3 * 1000, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 100GB tpcds stage3
            [2, 8, 4, 4, 60.3, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds 60.3
            [2, 8, 4, 4, 316.4, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds 316.4MB

            # store_returns

            [2, 8, 4, 4, 308.3, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 100GB tpcds
            [2, 8, 4, 4, 1551.6, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 500GB tpcds
            [2, 8, 4, 4, 3.2 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3],  # Q5 100GB tpcds stage 8 3.2GB
            [2, 8, 4, 4, 16.0 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3],  # Q5 500GB tpcds stage 8 16GB

            # 2,8,5,5

            # catalog_sales
            [2, 8, 5, 5, 1770.2, 4, 2, 0, 2, 6, 1, 1, 2, 2],
            # Q5 100GB tpcds stage 6  1770 MB # union is an expensive operation
            [2, 8, 5, 5, 8.6 * 1000, 4, 2, 0, 2, 6, 1, 1, 2, 2],  # Q5 500GB tpcds stage 6
            [2, 8, 5, 5, 1093.9, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 100GB tpcds stage 3
            [2, 8, 5, 5, 5.3 * 1000, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 500GB tpcds stage 3
            [2, 8, 5, 5, 193.3, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds stage 6
            [2, 8, 5, 5, 1017.1, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 500GB tpcds stage 6

            # store_sales
            [2, 8, 5, 5, 2.5 * 1000, 3, 2, 0, 2, 4, 3, 3, 2, 2],  # Q3 100GB tpcds stage 2
            [2, 8, 5, 5, 12.6 * 1000, 3, 2, 0, 2, 3, 3, 3, 2, 2],  # Q3 500GB tpcds stage 2
            [2, 8, 5, 5, 3.4 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 100GB tpcds 3.4GB
            [2, 8, 5, 5, 17.3 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 500GB tpcds stage 4
            [2, 8, 5, 5, 1553.9, 3, 2, 0, 2, 2, 1, 1, 3, 3],  # Q8 100GB tpcds stage 8
            [2, 8, 5, 5, 7.7 * 1000, 3, 2, 0, 2, 1, 0, 1, 3, 3],  # Q8 500GB tpcds stage 8

            # web_sales
            [2, 8, 5, 5, 1093.9, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 500GB tpcds stage3
            [2, 8, 5, 5, 5.3 * 1000, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 100GB tpcds stage3
            [2, 8, 5, 5, 60.3, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds 60.3
            [2, 8, 5, 5, 316.4, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds 316.4MB

            # store_returns

            [2, 8, 5, 5, 308.3, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 100GB tpcds
            [2, 8, 5, 5, 1551.6, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 500GB tpcds
            [2, 8, 5, 5, 3.2 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3],  # Q5 100GB tpcds stage 8 3.2GB
            [2, 8, 5, 5, 16.0 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3],  # Q5 500GB tpcds stage 8 16GB


            # 2,8,6,6

                # catalog_sales
            [2, 8, 6, 6, 1770.2, 4, 2, 0, 2, 6, 1, 1, 2, 2],
            # Q5 100GB tpcds stage 6  1770 MB # union is an expensive operation
            [2, 8, 6, 6, 8.6 * 1000, 4, 2, 0, 2, 6, 1, 1, 2, 2],  # Q5 500GB tpcds stage 6 8.6GB
            [2, 8, 6, 6, 1093.9, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 100GB tpcds stage 3
            [2, 8, 6, 6, 5.3 * 1000, 2, 2, 0, 2, 3, 1, 1, 1, 2],  # Q2 500GB tpcds stage 3
            [2, 8, 6, 6, 193.3, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds stage 6 193.3 MiB
            [2, 8, 6, 6, 1017.1, 2, 2, 0, 2, 1, 0, 1, 1, 1],  # Q10 500GB tpcds stage 6

            # store_sales
            [2, 8, 6, 6, 2.5 * 1000, 3, 2, 0, 2, 4, 3, 3, 2, 2],  # Q3 100GB tpcds stage 2
            [2, 8, 6, 6, 12.6 * 1000, 3, 2, 0, 2, 3, 3, 3, 2, 2],  # Q3 500GB tpcds stage 2
            [2, 8, 6, 6, 3.4 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 100GB tpcds 3.4GB
            [2, 8, 6, 6, 17.3 * 1000, 8, 4, 0, 2, 5, 0, 1, 3, 3],  # Q7 500GB tpcds stage 4
            [2, 8, 6, 6, 1553.9, 3, 2, 0, 2, 2, 1, 1, 3, 3],  # Q8 100GB tpcds stage 8
            [2, 8, 6, 6, 7.7 * 1000, 3, 2, 0, 2, 1, 0, 1, 3, 3],  # Q8 500GB tpcds stage 8

            # web_sales
            [2, 8, 6, 6, 1093.9, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 100GB tpcds stage3
            [2, 8, 6, 6, 5.3 * 1000, 2, 1, 0, 2, 2, 1, 1, 1, 2],  # Q2 500GB tpcds stage3
            [2, 8, 6, 6, 60.3, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds 60.3
            [2, 8, 6, 6, 316.4, 2, 1, 0, 2, 1, 0, 1, 1, 1],  # Q10 100GB tpcds 316.4MB

            # store_returns

            [2, 8, 6, 6, 308.3, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 100GB tpcds
            [2, 8, 6, 6, 1551.6, 4, 3, 0, 2, 3, 3, 1, 1, 1],  # Q1 500GB tpcds
            [2, 8, 6, 6, 3.2 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3],  # Q5 100GB tpcds stage 8 3.2GB
            [2, 8, 6, 6, 16.0 * 1000, 4, 2, 0, 2, 6, 5, 1, 2, 3]  # Q5 500GB tpcds stage 8 16GB

        ])


        output_y_runtimes = np.array([
            # catalog_sales
            24,  # Q5 100GB tpcds stage 6
            49,  # Q5 500GB tpcds stage 6 http://slave4:18080/history/application_1679295911706_0007/jobs/job/?id=4
            20,  # Q2 100GB tpcds stage 3  1093MB http://slave4:18080/history/application_1679295911706_0017/jobs/job
            # /?id=3
            70,  # # Q2 500GB tpcds stage 3 # http://slave4:18080/history/application_1679295911706_0004/jobs/job/?id=3
            7,  # Q10 100GB tpcds stage 6 193.3 MiB
            10,  # Q10 500GB tpcds stage 1017MB http://slave4:18080/history/application_1679295911706_0012/jobs/job/?id=3

            # store_sales
            18,  # Q3 100GB tpcds stage 2 2.5GiB http://slave4:18080/history/application_1679295911706_0018/jobs/job/?id=2
            70,  # # Q3 500GB tpcds stage 2 # http://slave4:18080/history/application_1679295911706_0005/jobs/job/?id=2
            #                    13,  # Q6 100GB tpcds stage 16
            #                    29,  # Q6 500GB tpcds stage 16
            20,  # Q7 100GB tpcds stage 4 http://slave4:18080/history/application_1679295911706_0022/jobs/job/?id=4
            1.5*60,  # Q7 500GB tpcds stage 4 http://slave4:18080/history/application_1679295911706_0009/jobs/job/?id=4
            5,  # Q8 100GB tpcds 1553.9MiB http://slave4:18080/history/application_1679295911706_0023/jobs/job/?id=4
            34,  # Q8 500GB tpcds stage 8 http://slave4:18080/history/application_1679295911706_0010/jobs/job/?id=4

            # web_sales
            20,  # Q2 100GB tpcds stage3 1093MB http://slave4:18080/history/application_1679295911706_0017/jobs/job
            # /?id=3
            70,  # Q2 500GB tpcds stage3 # http://slave4:18080/history/application_1679295911706_0004/jobs/job/?id=3
            2,  # Q10 100GB tpcds 60.3 http://slave4:18080/history/application_1679295911706_0025/jobs/job/?id=3
            5,  # Q10 100GB tpcds stage 5 316.4MB http://slave4:18080/history/application_1679295911706_0025/jobs/job/?id=3

            # store_returns
            6,  # q1 100GB tpcds http://slave4:18080/history/application_1679295911706_0016/jobs/job/?id=2
            12,  # q1 500GB tpcds 1551.6 MB http://slave4:18080/history/application_1679295911706_0003/jobs/job/?id=2
            35,  # Q5 100GB tpcds stage 8 3.2G http://slave4:18080/history/application_1679295911706_0020/jobs/job/?id=4
            2.3*60,  # Q5 500GB tpcds stage 8 16GB http://slave4:18080/history/application_1679295911706_0007/jobs/job/?id=4

            # 2,8,4,4
            # catalog_sales
            5,  # Q5 100GB tpcds stage 6 1770.2MB
            1.7*60,  # Q5 500GB tpcds stage 6 8.6GB http://slave4:18080/history/application_1679304940725_0005/jobs/job/?id=4
            13,  # Q2 100GB tpcds stage 3  1093MB http://slave4:18080/history/application_1679304940725_0015/jobs/job/?id=3
            # /?id=3
            70,  # # Q2 500GB tpcds stage 3 # http://slave4:18080/history/application_1679304940725_0002/jobs/job/?id=3
            7,  # Q10 100GB tpcds stage 6
            10,
            # Q10 500GB tpcds stage 1017MB http://slave4:18080/history/application_1679295911706_0012/jobs/job/?id=3

            # store_sales
            11,
            # Q3 100GB tpcds stage 2 2.5GiB http://slave4:18080/history/application_1679304940725_0016/jobs/job/?id=2
            30,  # # Q3 500GB tpcds stage 2 # http://slave4:18080/history/application_1679304940725_0003/jobs/job/?id=2
            #                    13,  # Q6 100GB tpcds stage 16
            #                    29,  # Q6 500GB tpcds stage 16
            13,  # Q7 100GB tpcds 3.4GB http://slave4:18080/history/application_1679304940725_0020/jobs/job/?id=4
            52,
            # Q7 500GB tpcds 17.3 GB stage 4 http://slave4:18080/history/application_1679304940725_0007/jobs/job/?id=4
            7,  # Q8 100GB tpcds stage 8 http://slave4:18080/history/application_1679295911706_0023/jobs/job/?id=4
            24,  # Q8 500GB tpcds stage 8 7.7GB http://slave4:18080/history/application_1679295911706_0010/jobs/job/?id=4

            # web_sales
            13,  # Q2 100GB tpcds stage3 1093MB http://slave4:18080/history/application_1679304940725_0015/jobs/job/?id=3
            # /?id=3
            70,  # Q2 500GB tpcds stage3 # http://slave4:18080/history/application_1679295911706_0004/jobs/job/?id=3
            2,  # Q10 100GB tpcds 60.3MB http://slave4:18080/history/application_1679304940725_0023/jobs/job/?id=3
            4,
            # Q10 100GB tpcds stage 5 316.4MB http://slave4:18080/history/application_1679304940725_0023/jobs/job/?id=3

            # store_returns
            5.5,  # q1 100GB tpcds http://slave4:18080/history/application_1679304940725_0014/jobs/job/?id=2
            9,  # q1 500GB tpcds http://slave4:18080/history/application_1679295911706_0003/jobs/job/?id=2
            27,  # Q5 100GB tpcds stage 8 3.2G http://slave4:18080/history/application_1679304940725_0018/jobs/job/?id=4
            47,
            # Q5 500GB tpcds stage 8 16GB http://slave4:18080/history/application_1679304940725_0005/jobs/job/?id=4

            # 2,8,5,5
            # catalog_sales
            11,  # Q5 100GB tpcds stage 6 1770.2MB
            1.7 * 60,
            # Q5 500GB tpcds stage 6 8.6GB http://slave4:18080/history/application_1679304940725_0005/jobs/job/?id=4
            30,
            # Q2 100GB tpcds stage 3  1093MB http://slave4:18080/history/application_1682218828348_0037/jobs/job/?id=3
            60,  # # Q2 500GB 5.3G tpcds http://slave4:18080/history/application_1682218828348_0007/jobs/job/?id=3
            4,  # Q10 100GB tpcds stage 6 193.3 MB http://slave4:18080/history/application_1682218828348_0045/jobs/job/?id=3
            10,
            # Q10 500GB tpcds stage 1017MB http://slave4:18080/history/application_1682218828348_0015/jobs/job/?id=3

            # store_sales
            25,
            # Q3 100GB tpcds stage 2 2.5GiB http://slave4:18080/history/application_1682218828348_0038/jobs/job/?id=2
            70,  # Q3 500GB tpcds stage 2 http://slave4:18080/history/application_1682218828348_0008/jobs/job/?id=2
            #                    13,  # Q6 100GB tpcds stage 16
            #                    29,  # Q6 500GB tpcds stage 16
            26,  # Q7 100GB tpcds 3.4GB http://slave4:18080/history/application_1682218828348_0042/jobs/job/?id=4
            1.1*60,
            # Q7 500GB tpcds 17.3 GB stage 4 http://slave4:18080/history/application_1679304940725_0007/jobs/job/?id=4
            12,  # Q8 100GB tpcds stage 8 1553MB http://slave4:18080/history/application_1682218828348_0043/jobs/job/?id=4
            25,
            # Q8 500GB tpcds stage 8 7.7GB http://slave4:18080/history/application_1682218828348_0013/jobs/job/?id=4

            # web_sales
            30,
            # Q2 100GB tpcds stage3 1093MB http://slave4:18080/history/application_1682218828348_0037/jobs/job/?id=3
            # /?id=3
            70,  # Q2 500GB tpcds stage3 # http://slave4:18080/history/application_1679295911706_0004/jobs/job/?id=3
            3,  # Q10 100GB tpcds 60.3MB http://slave4:18080/history/application_1682218828348_0045/jobs/job/?id=3
            8,
            # Q10 100GB tpcds stage 5 316.4MB http://slave4:18080/history/application_1682218828348_0045/jobs/job/?id=3

            # store_returns
            5.5,  # q1 100GB tpcds http://slave4:18080/history/application_1679304940725_0014/jobs/job/?id=2
            10,  # q1 500GB tpcds 1551.5MiB http://slave4:18080/history/application_1682218828348_0006/jobs/job/?id=2
            28,  # Q5 100GB tpcds stage 8 3.2G http://slave4:18080/history/application_1682218828348_0040/jobs/job/?id=4
            1.5*60,
            # Q5 500GB tpcds stage 8 16GB http://slave4:18080/history/application_1679304940725_0005/jobs/job/?id=4

            # 2,8,6,6
            # catalog_sales
            25,  # Q5 100GB tpcds stage 6 1770.2MB http://slave4:18080/history/application_1679366052438_0019/jobs/job/?id=4
            24,
            # Q5 500GB tpcds stage 6 8.6GB http://slave4:18080/history/application_1679366052438_0006/jobs/job/?id=4
            27,
            # Q2 100GB tpcds stage 3  1093MB http://slave4:18080/history/application_1679366052438_0016/jobs/job/?id=3
            # /?id=3
            1.2*60, # Q2 500GB 5.3GB # http://slave4:18080/history/application_1679304940725_0002/jobs/job/?id=3
            2,  # Q10 100GB 193.3 MiB http://slave4:18080/history/application_1679366052438_0024/jobs/job/?id=3
            12,
            # Q10 500GB tpcds stage 1017MB http://slave4:18080/history/application_1682413175115_0011/jobs/job/?id=3

            # store_sales
            26,
            # Q3 100GB tpcds stage 2 2.5GiB http://slave4:18080/history/application_1682413175115_0016/jobs/job/?id=2
            55,  # Q3 500GB tpcds 12.6GB # http://slave4:18080/history/application_1682413175115_0004/jobs/job/?id=2
            #                    13,  # Q6 100GB tpcds stage 16
            #                    29,  # Q6 500GB tpcds stage 16
            25,  # Q7 100GB tpcds 3.4GB http://slave4:18080/history/application_1682413175115_0020/jobs/job/?id=4
            1.4*60,
            # Q7 500GB tpcds 17.3 GB stage 4 http://slave4:18080/history/application_1682413175115_0008/jobs/job/?id=4
            12,  # Q8 100GB tpcds 1553.9 MiB http://slave4:18080/history/application_1682413175115_0021/jobs/job/?id=4
            44,
            # Q8 500GB tpcds stage 8 7.7GB http://slave4:18080/history/application_1682413175115_0009/jobs/job/?id=4

            # web_sales
            27,
            # Q2 100GB tpcds stage3 1093MB http://slave4:18080/history/application_1682413175115_0015/jobs/job/?id=3
            1.2*60,  # Q2 500GB tpcds 5.3GB http://slave4:18080/history/application_1679366052438_0003/jobs/job/?id=3
            2,  # Q10 100GB tpcds 60.3MB http://slave4:18080/history/application_1682413175115_0023/jobs/job/?id=3
            8,
            # Q10 100GB 316.4MB http://slave4:18080/history/application_1682413175115_0023/jobs/job/?id=3

            # store_returns
            9,  # q1 100GB 308.3 MB http://slave4:18080/history/application_1682413175115_0014/jobs/job/?id=2
            11,  # q1 500GB 1551.6 MB http://slave4:18080/history/application_1682413175115_0002/jobs/job/?id=2
            11,  # Q5 100GB tpcds stage 8 3.2G http://slave4:18080/history/application_1679366052438_0019/jobs/job/?id=4
            1.6*60
            # Q5 500GB tpcds stage 8 16GB http://slave4:18080/history/application_1679366052438_0006/jobs/job/?id=4
        ])

        # FirstStagePrediction.svm_reg = LinearSVR(epsilon=0.5)
        # FirstStagePrediction.svm_reg.fit(input_x_first_stage, output_y_sizes)
        #
        # FirstStagePrediction.tree_reg_datasets = DecisionTreeRegressor(max_depth=3)
        # FirstStagePrediction.tree_reg_datasets.fit(input_x_first_stage, output_y_sizes)
        #
        # FirstStagePrediction.tree_reg_runtimes = DecisionTreeRegressor(max_depth=3)
        # FirstStagePrediction.tree_reg_runtimes.fit(input_x_first_stage_runtime, output_y_runtimes)
        #
        # FirstStagePrediction.svm_reg_runtimes = LinearSVR(epsilon=0.5)
        # FirstStagePrediction.svm_reg_runtimes.fit(input_x_first_stage_runtime, output_y_runtimes)

        ## # load the model from disk
        # svm_reg_outputsize = pickle.load(open(filename, 'rb'))
        prefix = "../jupyter-notebook/"
        filename=prefix + "model-foresight_first_stage_prediction_TPC-DS-500GB-svm_reg_outputsize.model"
        FirstStagePrediction.svm_reg_outputsize = pickle.load(open(filename, 'rb'))
        filename = prefix+"model-foresight_first_stage_prediction_TPC-DS-500GB-tree_reg_runtimes.model"
        FirstStagePrediction.tree_reg_runtimes = pickle.load(open(filename, 'rb'))
        filename = prefix + "model-foresight_first_stage_prediction_TPC-DS-500GB-svm_reg_runtimes.model"
        FirstStagePrediction.svm_reg_runtimes = pickle.load(open(filename, 'rb'))

    def __init__(self):
        return

    @staticmethod
    def predict_stage( test_x_datasize,test_x_runtime):
        # print("in first stage performance model.py")

        # predict_output_size = FirstStagePrediction.svm_reg.predict(test_x_datasize)[0]
        # predict_output_size = FirstStagePrediction.tree_reg_datasets.predict(test_x_datasize)[0]
        # # print("svm_reg： predict_output_size is {} MB".format(predict_output_size))
        # tree_predict_output_runtime = FirstStagePrediction.tree_reg_runtimes.predict(test_x_runtime)[0]
        # # print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(tree_predict_output_runtime))
        #
        # # svm_predict_output_runtime = FirstStagePrediction.svm_reg_runtimes.predict(test_x_runtime)[0]
        # # print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(svm_predict_output_runtime))
        # print("test_X_datasize is {}, predict_output_size is {}".format(test_x_datasize[0],predict_output_size))
        # print("test_X_runtime is {} tree_predict_output_runtime is {}".format(test_x_runtime[0],tree_predict_output_runtime))
        # return predict_output_size, tree_predict_output_runtime

        predict_output_size = FirstStagePrediction.svm_reg_outputsize.predict(test_x_datasize)[0]
        tree_predict_output_runtime = FirstStagePrediction.tree_reg_runtimes.predict(test_x_runtime)[0]

        # print("test_X_datasize is {}, predict_output_size is {}".format(test_x_datasize[0], predict_output_size))
        # print("test_X_runtime is {} tree_predict_output_runtime is {}".format(test_x_runtime[0],
        #                                                                       tree_predict_output_runtime))

        if predict_output_size <=1:
            predict_output_size = 1
        return predict_output_size, tree_predict_output_runtime

    @staticmethod
    def test_prediction():

        # input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#,
        # project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#

        # test_x = np.array([
        #     [465.8, 3, 2, 0, 2, 3, 1, 1, 3, 3],  # catalog_sales, Q15 100GB tpcds stage 3
        #
        #     #     [2.3 *1000,   3, 2, 0, 2,  3,  1,1, 3, 3]# Q15 500GB tpcds stage 3
        #
        #     [9.9 * 1000, 7, 3, 0, 0, 0, 0, 1, 0, 0],
        #
        #     # tpch-q1
        #     [26 * 1000, 7, 1, 0, 0, 6, 12, 1, 0, 1],
        #     # tpch q3 500G
        #     [34 * 1000, 4, 2, 0, 0, 3, 0, 1, 0, 1]
        #
        # ])
        #
        # test_x_runtime = np.array([
        #     [2, 8, 2, 2,465.8, 3, 2, 0, 2, 3, 1, 1, 3, 3],  # catalog_sales, Q15 100GB tpcds stage 3
        #
        #     #     [2.3 *1000,   3, 2, 0, 2,  3,  1,1, 3, 3]# Q15 500GB tpcds stage 3
        #
        #     [2, 8, 2, 2,9.9 * 1000, 7, 3, 0, 0, 0, 0, 1, 0, 0],
        #
        #     # tpch-q1
        #     [2, 8, 2, 2,26 * 1000, 7, 1, 0, 0, 6, 12, 1, 0, 1],
        #     # tpch q3 500G
        #     [2, 8, 2, 2,34 * 1000, 4, 2, 0, 0, 3, 0, 1, 0, 1]
        #
        # ])

        test_x = np.array([
            [2, 960, 2, 3, 1551.6, 4, 3, 0, 2, 3, 3, 1, 1, 1, 0, 120],  # Q1,500,4,store_returns,421.9
            [2, 2011, 10, 3, 5427.2, 2, 2, 0, 2, 3, 1, 1, 1, 2, 0, 200]  # Q2,500,3,catalog_sales,5.5

        ])

        # actual [0.1 ] MB
        # predict_output_size = svm_reg.predict(test_x)
        # print("predict_output_size is {} MB".format(predict_output_size) )

        predict_output_size = FirstStagePrediction.svm_reg_outputsize.predict(test_x)
        # print("svm_reg： predict_output_size is {} MB".format(predict_output_size))

        actual_oput_size = np.array([
            421.9,  # Q1,500,4,store_returns,
            5.5  # Q2,500,3,catalog_sales
        ])
        # actual runtime [13] seconds

        actual_oputput_runtime = np.array([
            22,  # Q11 100GB tpcds stage 4
            24,  # Q11 100GB tpcds stage 5
        ])

        # print("actual_oputput_runtime is {} seconds".format(actual_oputput_runtime))
        predict_output_runtime = FirstStagePrediction.tree_reg_runtimes.predict(test_x)
        print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

        predict_output_runtime = FirstStagePrediction.svm_reg_runtimes.predict(test_x)
        # print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))


if __name__ == '__main__':
    FirstStagePrediction.model_init()
    FirstStagePrediction.test_prediction()
