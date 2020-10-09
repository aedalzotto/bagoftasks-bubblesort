# bagoftasks-bubblesort
Distributed bubble sort instances using MPI

## Building

Compile with standard array count (1.000) and array size (100.000)
```
$ mpicc bubblesort.c -o bubblesort -std=c99
```

To choose different array count (N) and array size (M), define in compiler's command line
```
$ mpicc bubblesort.c -o bubblesort -std=c99 -DN=100 -DM=10000
```

To compile in DEBUG mode, also define in compiler's command line. This will always
run with NP*2 arrays with size of 40.
```
$ mpicc bubblesort.c -o bubblesort_debug -std=c99 -DDEBUG=1
```

## Running

If you run with 1 processor, the sequential version will be executed. Otherwise,
the parallel version will be executed with 1 master and NP-1 slaves.
```
$ mpirun -np NP bubblesort
```
