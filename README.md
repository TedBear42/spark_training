# spark.trainer
A copy of code used at Spark Summit for training

#Code Examples

This project is pretty much a bunch of examples for training.

## Nesting Examples 

Here are a number of examples of using nesting with RDD, DataFrames, and DataSets.

- src/main/scala/com/malaska/spark/training/nested/

## Customer Partitioning

Here is an example of building a custom partition

- /src/main/scala/com/malaska/spark/training/partitioning/

## Windowing 

Windowing is both a complex topic and one that show cases the idea of the Big and the Small.  

- /src/main/scala/com/malaska/spark/training/windowing/

## Spark Streaming Demo Notes

### Single batch counting

Run program
```
CountingInAStreamExpBatchCounting localhost 9999 ./checkpoint
```
Send it messages
```
nc -lk 9999
hi there
hi bob
```

### Live time counting
Run program
```
CountingInAStreamExpUpdateStateByKey localhost 9999 ./checkpoint
```
Send it messages
```
nc -lk 9999
hi there
hi bob
```

### Structured Streaming session counting
Run program
```
CountingInAStreamExpGroupBy localhost 9999
```
Send it messages
```
nc -lk 9999
hi there
hi bob
```

### Structured Streaming session counting Dataset
Run program
```
CountingInAStreamDatasetExpGroupBy localhost 9999
```
Send it messages
```
nc -lk 9999
bob,tim,FB,10,123
bob,tim,FB,11,124
bob,tim,FB,10,125
tim,cat,G,20,124
tim,cat,G,20,125
tim,cat,G,20,126
```

### Structured Streaming session counting mapWithState
Run program
```
CountingInAStreamMapWithState localhost 9999
```
Send it messages
```
nc -lk 9999
hi there
hi bob
```


