# Foresight
#  Introduction

Foresight is a deadline-oriented resource optimizer for SparkSQL applications.  It leverages the stage-level information from the DAGs of a small number of SparkSQL queries to build a performance model to accurately predict the execution time of unseen queries. As such, a provider can correctly answer whether the deadline of an unseen SparkSQL application can be met.

<!-- (see the script below **core/foresight_stage_level_prediction_varying_resources.py** ) -->

Moreover, Foresight employs a critical-path based approach to optimize the CPU resources and a stage input/output data size based method to optimize the memory resources of a SparkSQL application.

<!-- (see the scripts below **core/foresight_stage_level_prediction_varying_resources.py** and **core/analytical_memory_executor.py**) -->

In general, Foresight consists of five components: data size predictor (DSP), stage performance predictor
(SPP), stage memory predictor (SMP), CPU resource opti-
mizer (CRO), and memory resource optimizer (MRO). 

# Installation

We implement Foresight with python 3.9.

Since Foresight focuses on predicting deadline violation of Spark SQL in a cluster. Please install the Spark 3.4.5 and hadoop 3.3.2 in the cluster following the two links below. 
- Spark 3.4.5. https://spark.apache.org/docs/latest/
- Hadoop 3.3.2 http://apache.github.io/hadoop/hadoop-project-dist/hadoop-common/ClusterSetup.html

## python dependencies
To run the python scripts of Foresight, please install the third-party libraries.
```python
 pip install -r requirements.txt 
 ```

 # Benchmark

We use the version of TPC-DS v3 of SparkSQL queries to evaluate Foresight.  
The following links shows the instruction to generate 100 GB and 500 GB datasets for TPC-DS.
 https://github.com/xinjinhan/BaBench

 # Launching Foresight


The entry of Foresight the script "foresight\_main.py". There are three parameters for this scritpt. The first is the deadline, $deadline$, in minutes. The second is the index, $query$, of TPC-DS query for which you want to predict the performance ($84 <= i<= 103$). The thrid is the data size, $datasize$, specified by the 100 or 500 where 100 or 500 represents the 100 GB and 500 GB input data, respectively.  Currently, we only suport this two data sizes. For exmaple, if you decide to judge whether Q91 can complete in 3 minutes (deadline) in the purchased Spark cluster, you can lauch this script as follows. 


```python
  python3 foresight_main.py --deadline 3 --query 91 --datasize 500
```

 Foresight will iterate all possible CPU cores in the cluster to explore whether the deadline of the specified query can be met.  
If there is no deadline violation, Foresight will output the following message sigifying a success.

```python
"The deadline can be met.
The predicted runtime by Foresight is 112 seconds, which is less than the deadline 180 seconds."
```

Otherwise, Foresight will report a deadline violation as follows. 

```python
"A deadline violation"
```



# data size predictor (DSP) and stage performance predictor (SPP)
Both DSP and SSP leverage the stage-level information and machine learning algorithms to build their respective models. We provide a number of training samples to show how to train them. The details can be found in  first_stage_performance_model.py and normal_stage_performance_model.py.

# stage memory predictor (SMP)

This script (analytical_memory_executor.py) introduces the analytical model of memory optimizer.

# CPU resource optimizer (CRO) and memory resource optimizer (MRO)

The details of CRO and MRO are implemented in the file called **foresight_main.py**.

 This script is to perform the stage-level performance prediction for a given Query with varying executor counts (i.e., CPU cores) to see if the deadline of it can be satisfied. Plus, if the **deadline** can be satisfied, Foresight ouputs the minimum exeuctor count (i.e., minimum CPU cores) accordingly.  

To be specific, the function **predict_runtime (Line 173)** is the online usage of Foresight. If the predicted runtime is below the user specified deadline, Foresight reports the optimized amount of CPU and resource resources as well.  Lines 173-256 corresponds to extract the DAG comprising a number of stages from the physical plan of a query.

In practice, the Algorithm 1 (Predicting Output Datasize of Each Stage) and Algorithm 2 ( Optimizing CPU resources of a SparkSQL Query) can be coalesced into a function from Lines 451 - 484. 


# other utility
### analytical_memory_executor.py
#### a simple example of memory_optimizer

This script introduces the analytical model of memory optimizer for a SparkSQL application, which is used in the "foresight_stage_level_prediction_varying_resources.py" above.

### first_stage_performance_model.py
the performance prediction for the first stage which includes the aggregate operators. 


### first_stage_no_aggregate_performance_model.py
the performance prediction for the first stage which excludes the aggregate operators.
the first stage also maps to the **initial stages** called in our paper.  

### normal_stage_performance_model.py
the performance prediction for the normal stages. 



 
### parsePlan_lele_cp-tpcds-operators.py
generating the corresponding DAG comprising a number of stages from the physical plan of a query. 
getting the depth, dag_width, stage_count, predicted_runtime, operators dictionary of each query. 
Foresight: Runtime prediction for a query.

### lele_dag_sql_generation_new.py
generating one sql according to the physical plan.



