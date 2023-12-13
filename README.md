##  Foresight: A Deadline Oriented Resource Optimizer for SparkSQL Applications

Foresight is a deadline-oriented resource optimizer for SparkSQL applications.  It leverages the stage-level information from the DAGs of a small number of SparkSQL queries to build a performance model (see the script below **foresight_stage_level_prediction_varying_resources.py** ) to accurately predict the execution time of unseen queries. As such, a provider can correctly answer whether the deadline of an unseen SparkSQL application can be met.

Moreover, Foresight employs a critical-path based approach to optimize the CPU resources and a stage input/output data size based method to optimize the memory resources of a SparkSQL application (see the script below **foresight_stage_level_prediction_varying_resources.py** and **analytical_memory_executor.py**). 

##  stage-level performance model for a query
### foresight_stage_level_prediction_varying_resources.py

This script is to perform the stage-level performance prediction for a given Query with varying executor counts (i.e., CPU cores) to see if the deadline of it can be satisfied. Plus, if the **deadline** can be satisfied, Foresight ouputs the minimum exeuctor count (i.e., minimum CPU cores) accordingly.  

To be specific, the function **predict_runtime (Line 173)** is the online usage of Foresight. If the predicted runtime is below the user specified deadline, Foresight reports the optimized amount of CPU and resource resources as well.  Lines 173-256 corresponds to extract the DAG comprising a number of stages from the physical plan of a query.

In practice, the Algorithm 1 (Predicting Output Datasize of Each Stage) and Algorithm 2 ( Optimizing CPU resources of a SparkSQL Query) can be coalesced into a function from Lines 451 - 484. 

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



