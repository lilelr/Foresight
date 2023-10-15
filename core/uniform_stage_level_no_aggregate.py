import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor


from sklearn.linear_model import LinearRegression


# using linear regression to estimate output datasize since the training dataset is small-scale
## focus on the table called catalog_sales with two types: 1: filescan->exchange 2: filescan-project-aggregate-join
# input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#, project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#

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
    [80.7, 3, 2, 0, 0, 0, 0, 1, 0, 0]  # Q10 500GB tpcds stage 4

    #     [308.3,    4, 4, 0, 0,  0,  0,3  ] # Q50 100GB tpcds stage 3
])

# for i, elements in enumerate(input_x_no_join_project):
#     if i % 2 == 1:  # odd
#         print("500GB input data size is {}".format(input_x_no_join_project[i][0]))
#         relation = input_x_no_join_project[i][0] / input_x_no_join_project[i - 1][0]
#         print("relation is {}".format(relation))

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
    104.1  # Q10 500GB tpcds stage 4
])

output_y_runtimes = np.array([
    # catalog_sales
    23,  # Q4 100GB tpcds stage 6
    42,  # Q4 500GB tpcds stage 6

    # store_sales
    13,  # Q6 100GB tpcds stage 16
    29,  # Q6 500GB tpcds stage 16
    # web_sales
    17,  # Q4 100GB tpcds stage 9
    42,  # Q4 500GB tpcds stage 2
    # customer
    2,  # Q1 100GB tpcds stage 2
    4,  # Q1 500GB tpcds stage 2
    13,  # Q6 100GB tpcds stage 16
    29,  # Q6 500GB tpcds stage 16
    1,  # Q10 100GB tpcds stage 4
    3  # Q10 500GB tpcds stage 4
])

from sklearn.svm import LinearSVR
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_classification

reg_nnls = LinearRegression(positive=True)
reg_nnls.fit(input_x_no_join_project, output_y_sizes)

svm_reg = LinearSVR(epsilon=4)
svm_reg.fit(input_x_no_join_project, output_y_sizes)

tree_reg_runtimes = DecisionTreeRegressor(max_depth=5)
tree_reg_runtimes.fit(input_x_no_join_project, output_y_runtimes)

svm_reg_runtimes = LinearSVR(epsilon=0.5)
svm_reg_runtimes.fit(input_x_no_join_project, output_y_runtimes)

# input data sizes (MB)1, filescan 2#, filter isnotnull 3 #,  filter other  4#,  BroadcastHashJoin  5#, project 6#, hashAggregate 7 #, exchange hashpartitioning 8 #, broadcasthashJoin depth 9#, project depth 10#

test_x = np.array([

    [2.3 * 1000, 3, 2, 0, 0, 0, 0, 1, 0, 0],  # Q15 500GB tpcds stage 1
    [1.3 * 1000, 2, 0, 0, 0, 0, 0, 1, 0, 0],
    [9.9 * 1000, 7, 3, 0, 0, 0, 0, 1, 0, 0],
    # customer
    [75.6, 8, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 100GB tpcds stage 2
    [252.5, 8, 2, 0, 0, 0, 0, 1, 0, 0],  # Q11 500GB tpcds stage 3

    # tpch q3 500G
    [34 * 1000, 4, 2, 0, 0, 3, 0, 1, 0, 1]

])

# actual [0.1 ] MB
predict_output_size = reg_nnls.predict(test_x)
print("reg_nnls： predict_output_size is {} MB".format(predict_output_size))

predict_output_size = svm_reg.predict(test_x)
print("svm_reg： predict_output_size is {} MB".format(predict_output_size))

actual_oput_size = np.array([
    7.3 * 1000,  # Q15 500GB tpcds stage 1
    0,
    0,
    145,  # Q11 100GB tpcds stage 2
    505.4  # Q11 500GB tpcds stage 3
])

# actual runtime [13] seconds

predict_output_runtime = tree_reg_runtimes.predict(test_x)
print("tree_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

predict_output_runtime = svm_reg_runtimes.predict(test_x)
print("svm_reg_runtimes: predict_output_runtime is {} seconds".format(predict_output_runtime))

actual_oputput_runtime = np.array([
    52,  # Q15 500GB tpcds stage 1
    0,
    0,
    7,  # Q11 100GB tpcds stage 2
    12  # Q11 500GB tpcds stage 3

])

print(reg_nnls.intercept_, reg_nnls.coef_)

# In[ ]:
