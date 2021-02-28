# CS3223_proj

## What we have implemented

### [Sort](COMPONENT/src/qp/operators/Sort.java)
The sort operator is used by Sort Merge Join, Order By and Group By operators. There are two phases in the sort algorithm: generate sorted runs and merge sorted runs. In the function open(), we first generate the sorted runs using the B buffer pages. We read in B pages each time, sort the records in memory using a priority queue, and produce a B page sorted run. The records can be sorted in both ascending and descending order depending on the arguments. To merge the sorted runs, we use (B-1) buffer pages for input and one buffer page for output. We perform a (B-1)-way merge, merging (B-1) sorted runs each time, until one sorted run is produced. The function next() returns the sorted file batch-by-batch. 

### [Block Nested Loop Join](COMPONENT/src/qp/operators/BlockNestedJoin.java)
The block nested loop join implementation is similar to that of the page nested loop join implementation. Instead of having 1 buffer page for the leftbatch, we have a leftblock, which can hold (B-2) batches, where B is the number of buffers available. We also have an extra cursor for the leftblock (lblockcurs).

The function next() is mainly where the algorithm lies. We first read (B-2) batches from the left table into the buffers (leftblock). Then, we will scan the entire right table, reading in the right table into the rightbatch batch-by-batch. We then compare each tuple in the rightbatch with each tuple in the leftblock, and if they satisfy the predicate, we add them to the outbatch. When the outbatch is full, we will keep track of where we stop by updating the cursors, and then return the outbatch.

### [Hash Join](COMPONENT/src/qp/operators/HashJoin.java)
The hash join algorithm operates in two phases: partition phase and join phase. In the function open(), we partition both the left table and right table into (B-1) buckets each, using the same hash function, where B is the number of buffers available. When the bucket is full, we write out the partition into a file with the partition number and intialise a new bucket to continue with the partitioning of the tables. 

In the function next(), we carry out the join phase. We read in a partition of the left table and build a hash table with (B-2) buckets for that partition using a different hash function from the partition phase. If the partition does not fit into memory, we apply the block nested loop join algorithm on that partition and its corresponding partition in the right table. Otherwise, we scan the matching partition in the right table and search for matches, which will be added to outbatch, until outbatch is full or all partitions have been processed. 

### [Sort-Merge Join](COMPONENT/src/qp/operators/SortMergeJoin.java)
The sort-merge join algorithm uses the Sort operator in the function open() to sort the left table and right table on the join column. In the function next(), we read in the sorted left and right table batch-by-batch. We advance the cursor of the left table until the left tuple sort key is greater than or equal to the right tuple sort key. We then advance the cursor of the right table until the right tuple sort key is greater than or equal to the left tuple sort key. This is repeated until the left and right tuple sort keys are equal. We then read in all tuples in the left and right table with this sort key and perform a cross product, adding the joined tuples to the outbatch. Subsequently, the algorithm will resume scanning of the left and right tables to continue merging tuples in both tables on the join columns. 

### [Distinct](COMPONENT/src/qp/operators/Distinct.java)
We used the sort-based optimised approach to implement the distinct operator. The implementation is similar to how Sort is implemented. However, when generating the sorted runs, we remove duplicates as well. Furthermore, we also remove the duplicates while merging the sorted runs.

### [Order By](COMPONENT/src/qp/operators/OrderBy.java)
Since the grammar of the SQL query does not support ASC and DESC, we modified the parser to support these so that the parser can support the order by operator. The order by operator uses the Sort operator in the function open() to sort the table on the order by attributes. The sorted table is then read in batch-by-batch in the function next(). The tuples in each batch are then added to the outbatch, until the outbatch is full or the entire sorted table has been processed. 

### [Group By](COMPONENT/src/qp/operators/GroupBy.java)
The group by operator uses the Sort operator in the function open() to sort the table on the group by attributes. In the function next(), we read in the sorted table batch-by-batch. For each tuple, we compare it with the previous tuple to check if they belong to the same group. If they belong to the same group, we check that the attributes to project are the same as that of the previous tuple, and raise an error if they are not. Otherwise, we project the results and add the resultant tuples to the outbatch. 

### [Bug Identified / Fixed](COMPONENT/src/qp/operators/CrossProduct.java)
We find that when we select from multiple tables without the WHERE clause, the output is unexpected. It will just select from the last table given. For example: 
SELECT *
FROM SCHEDULE, CERTIFIED

The output we get from running the above query is: 
```
CERTIFIED.eid  CERTIFIED.aid 
4408   10361 
47     4980  
7229   3101  
...
```

Since this query is essentially computation of the cross product, we have implemented the CrossProduct operator. Its implementation is similar to that of Block Nested Loop Join, except that it does not have any condition, and does not do any tuple comparison. We will createCrossProductOp whenever the number of joins is 0, but the size of fromList is greater than 1. With that, the output that we got from running the same query is: 
```
SCHEDULE.flno  SCHEDULE.aid  CERTIFIED.eid  CERTIFIED.aid 
10527  8772   4408   10361 
10527  8772   47     4980  
10527  8772   7229   3101  
...
```

## Our [testcases](COMPONENT/testcases) Folder
### Running Experiments
For the purpose of replicating test results, we have generated AIRCRAFTS, CERTIFIED, EMPLOYEES, FLIGHTS, and SCHEDULE table. 
We have included the queries that we used, [experiment1_1.sql](COMPONENT/testcases/experiment1_1.sql), [experiment1_2.sql](COMPONENT/testcases/experiment1_2.sql), [experiment1_3.sql](COMPONENT/testcases/experiment1_3.sql), [experiment2.sql](COMPONENT/testcases/experiment2.sql).
Results of our queries after running each experiments for Page Nested Loop Join, Block Nested Loop Join, Hash Join are named 'nested<expt#>.out', 'block<expt#>.out' and 'hash<expt#>.out' respectively.

### Testing Operators other than Join (distinct, group by, order by, cross product)
We created 3 test tables (TEST1, TEST2, TEST3) to test the other operators that we have implemented.
The query that we ran and the corresponding outputs are in '<operator>.sql' and '<operator>.out'