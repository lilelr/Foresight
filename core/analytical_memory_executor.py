
# !/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.svm import LinearSVR

def compute_memory(input_size ,p):
    cores_n = 2
    data_skewness = 0.5
    execution_memory = 0.3
    management_overhead = 0.1
    executor_memory = 0
    data_transformation =0.2
    input_executor = input_size / p * cores_n
    print("the input size for an executor is {} MB".format(round(input_executor)))
    part = input_executor / (data_transformation * data_skewness * execution_memory)
    print(part)
    if part > 384:
        executor_memory = part / (1 - management_overhead)
    else:
        executor_memory = 384 + part
    executor_memory = round(executor_memory + 1)
    return executor_memory


if __name__ == '__main__':
    # input_size = 12.6 * 1024  # in MB Q3
    # # p = 200
    # p = 600

    # input_size = 1017 * 1024  # in MB
    input_size = 11.4*1024   # in MB
    # p = 200
    p = 200
    m = compute_memory(input_size, p)
    print("the estimated memory is {} MB per executor".format(m))
