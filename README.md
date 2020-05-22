## Distributed fault-tolerant Map-reduce for counting words

This will take input files from ```files/input-*``` and output the combined word count from input files.

Summary
1. Master starts and creates tasks that will be performed by multiple workers.
2. Multiple Worker starts and asks master for work.
3. When a worker is done with assigned task it contacts master asking for another task, if all tasks are finished master asks worker to shutdown itself.
4. If a worker dies/become slow due to some reason, master reassigns that task to some other worker.
5. If a dead worker returns again, a new task is assigned to it and its old work is discarded as the worker was considered dead.
6. Workers talks to Master via go RPCs and assumes they are running on different machines.

Usage:
1. ```go run master.go rpc.go files/input-* ``` for running Master.
2. Run ```go run worker.go rpc.go``` in one or more tabs for parallelism
3. After master is finished run ```cat files/output-* > output.txt'```to get the final word count in ```output.txt```

This relies on the master and workers sharing a common file system.

References:
1. MIT 6.824
2. Implementation is based on this paper [MapReduce: Simplified Data Processing on Large Clusters](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/16cb30b4b92fd4989b8619a61752a2387c6dd474.pdf)