### fill_physical_plan 
Select two tables from TPC-DS to generate one meaningful physical plan. 

### notes
1. Can we generate the final SQL using the currently generated physical plan?
2. For the handling of different operations, we can separate them with different python files instead of one.
3. How to deal with random sequences of LSTM branches?
4. How to maintain the relationships of tables in TPC-DS upfront, especially for the join keys?
5. How to determine the data objects and the specific computation operations such as max, average, or minimum, with regard to a physical operator?  project,filter, sort

 ### lele_dag_sql_output_to_file.py
 generate the branchs of dag rather than the sql
 
 ### sythesiz_sql....
 1. choose the fixed set of tables
    single branch 
    two tables TPC-DS: 
    store_returns date_dim
    catl         date_dim
    three tables TPC-DS:
    date_dim store_returns 
 
 2. single branch
   three tables
   
 3. sql variability
    - length
    - random project/hash aggreate fields
    - tables
    
### synthesize_sql_from_generated_plan_new.py
generate the sql with varied number of tables in a single branch
- please notice the semantics of names
- selected table might can be the same
- PEB

  connected_tables_2
    connected_tables_3 = [store_returns, date_dim, date_dim_k] # A 表为核心
    connected_tables_4 = [store_returns, date_dim, date_dim_e, data]
    connected_tables_5,6,7,8