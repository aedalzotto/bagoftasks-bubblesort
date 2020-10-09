#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>

/* Define DEBUG by compiler's command line or use default PRODUCTION */
#ifndef DEBUG
	#define DEBUG 0
#endif

/* Communication tags. SVC for service and ARRAY for 'payload' */
#define TAG_SVC		0
#define TAG_ARRAY	1

/* List of Slave -> Master and Master -> Slave services */
#define SVC_REQUEST -1		/* Slave -> Master. Ask for array */
#define SVC_TERMINATE -2	/* Master -> Slave. Send suicide message */

int sequential(const int ARRAY_CNT, const int ARRAY_SZ);
int parallel(const int SIZE, const int RANK, const int ARRAY_CNT, const int ARRAY_SZ);
int parallel_master(const int SLAVE_CNT, const int ARRAY_CNT, const int ARRAY_SZ);
void master_send_array(int *array, const int SIZE, const int INDEX, const int DST);
void master_send_terminate(const int DST);
void master_receive_array(int *array, const int ARRAY_SZ, const int INDEX, const int SRC);
int parallel_slave(const int RANK, const int ARRAY_SZ, const int MASTER);
void slave_work(int *array, const int ARRAY_SZ, const int IDX, const int RANK, const int MASTER);
void populate_matrix(int *matrix, const int SIZE);
void bubblesort(int *array, const int SIZE);
#if DEBUG == 1
void print_arrays(int *matrix, const int ARRAY_CNT, const int ARRAY_SZ);
#endif

int main(int argc, char *argv[])
{
	/* Initialize MPI */
	MPI_Init(&argc, &argv);

	/* Get MPI processor identification */
	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	/* Get MPI processor number */
	int size;
	MPI_Comm_size(MPI_COMM_WORLD, &size);

#if DEBUG == 1
	/* DEBUG Matrix with size*2 arrays of 40 entries each */
	const int ARRAY_CNT = size*2;
	const int ARRAY_SZ = 40;
#else
	/* PRODUCTION Matrix default with 1.000 arrays of 100.000 entries each */
	/* To change, configure by compiler's command line N and M defines */
	#ifndef N
		const int ARRAY_CNT = 1000;
	#else
		const int ARRAY_CNT = N;
	#endif
	#ifndef M
		const int ARRAY_SZ = 100000;
	#else
		const int ARRAY_SZ = M;
	#endif
#endif

	int ret;
	if(size == 1){
		/* If running a single process, run in sequential mode */
		ret = sequential(ARRAY_CNT, ARRAY_SZ);
	} else {
		/* Otherwise, run in parallel mode (master + slave(s)) */
		ret = parallel(size, rank, ARRAY_CNT, ARRAY_SZ);
	}

	MPI_Finalize();
	return ret;
}

int sequential(const int ARRAY_CNT, const int ARRAY_SZ)
{
	printf("Running in sequential mode\n\n");

#if DEBUG == 1
	printf("Allocating matrix\n");
#endif

	/* Try to allocate memory for all values */
	int *values = (int*)malloc(ARRAY_CNT*ARRAY_SZ*sizeof(int));
	if(values == NULL){
		printf("Not enough memory to allocate matrix\n");
		return 1;
	}

#if DEBUG == 1
	printf("Matrix allocated\n");
	printf("Populating matrix\n");
#endif

	/* Populate the allocated matrix */
	populate_matrix(values, ARRAY_CNT*ARRAY_SZ);

#if DEBUG == 1
	print_arrays(values, ARRAY_CNT, ARRAY_SZ);
#else
	/* Preparations finished. Start counting time here */
	double then = MPI_Wtime();
#endif

	/* Sort all arrays */
	for(int i = 0; i < ARRAY_CNT; i++)
		bubblesort(&values[i*ARRAY_SZ], ARRAY_SZ);

#if DEBUG == 0
	double now = MPI_Wtime();
	printf("All arrays sorted in %f\n", now - then);
#else
	printf("All arrays sorted!\n");
	print_arrays(values, ARRAY_CNT, ARRAY_SZ);
#endif

	/* Safely free the memory */
	free(values);
	values = NULL;

	return 0;
}

int parallel(const int SIZE, const int RANK, const int ARRAY_CNT, const int ARRAY_SZ)
{
	int ret;
	if(RANK == SIZE - 1){
		/* Last processor is MASTER */
		ret = parallel_master(SIZE - 1, ARRAY_CNT, ARRAY_SZ);
	} else {
		/* All others are SLAVES */
		ret = parallel_slave(RANK, ARRAY_SZ, SIZE - 1);
	}
	return ret;
}

int parallel_master(const int SLAVE_CNT, const int ARRAY_CNT, const int ARRAY_SZ)
{
	printf("Running in parallel mode with %d slaves\n\n", SLAVE_CNT);

	#if DEBUG == 1
	printf("M: Allocating global matrix\n");
	#endif

	/* Try to allocate memory for all values */
	int *values = (int*)malloc(ARRAY_CNT*ARRAY_SZ*sizeof(int));
	if(values == NULL){
		printf("M: Not enough memory to allocate global matrix\n");
		return 1;
	}

#if DEBUG == 1
	printf("M: Global vector allocated\n");
	printf("M: Populating global vector\n");
#endif

	/* Populate the allocated matrix */
	populate_matrix(values, ARRAY_CNT*ARRAY_SZ);

#if DEBUG == 1
	print_arrays(values, ARRAY_CNT, ARRAY_SZ);
#else
	/* Preparations finished. Start counting time here */
	double then = MPI_Wtime();
#endif

	int terminated_cnt = 0;	/* Number of terminated slaves */
	int sorted_cnt = 0;		/* Number of sorted arrays */

	/* Master must command until all slaves are terminated */
	while(terminated_cnt < SLAVE_CNT){
		int service;
		MPI_Status mpi_status;

		/* Wait for a message from any slave */ 
		MPI_Recv(&service, 1, MPI_INT, MPI_ANY_SOURCE, TAG_SVC, MPI_COMM_WORLD, &mpi_status);

		switch(service){
			case SVC_REQUEST:
				/* Slave requested for an array */
				if(sorted_cnt < ARRAY_CNT){
					/* There are arrays to be sorted */
					master_send_array(&values[sorted_cnt*ARRAY_SZ], ARRAY_SZ, sorted_cnt, mpi_status.MPI_SOURCE);
					sorted_cnt++;
				} else {
					/* All arrays sorted. Send message to terminate */
					master_send_terminate(mpi_status.MPI_SOURCE);
					terminated_cnt++;
				}
				break;
			default:
				/* Slave responded with the index of a sorted array. Receive it back */
				master_receive_array(&values[service*ARRAY_SZ], ARRAY_SZ, service, mpi_status.MPI_SOURCE);
				break;
		}
	}

#if DEBUG == 0
	/* Print time information */
	double now = MPI_Wtime();
	printf("M: All arrays sorted in %f\n", now - then);
#else
	printf("M: All arrays sorted!\n");
	print_arrays(values, ARRAY_CNT, ARRAY_SZ);
#endif

	/* Safely free memory */
	free(values);
	values = NULL;

	return 0;
}

void master_send_array(int *array, const int SIZE, const int INDEX, const int DST)
{
	/* Send message indicating that the slave must receive the 'INDEX' array */
	MPI_Send(&INDEX, 1, MPI_INT, DST, TAG_SVC, MPI_COMM_WORLD);

	/* Send the array to sort */
	MPI_Send(array, SIZE, MPI_INT, DST, TAG_ARRAY, MPI_COMM_WORLD);
	
#if DEBUG == 1
	printf("M: Sent array %d to S%d\n", INDEX, DST);
#endif
}

void master_send_terminate(const int DST)
{
	const int SVC = SVC_TERMINATE;
	MPI_Send(&SVC, 1, MPI_INT, DST, TAG_SVC, MPI_COMM_WORLD);

#if DEBUG == 1
	printf("M: Sent suicide message to S%d\n", DST);
#endif
}

void master_receive_array(int *array, const int ARRAY_SZ, const int INDEX, const int SRC)
{
#if DEBUG == 1
	printf("M: Receiving array %d from S%d\n", INDEX, SRC);
#endif
	MPI_Status mpi_status;
	MPI_Recv(array, ARRAY_SZ, MPI_INT, SRC, TAG_ARRAY, MPI_COMM_WORLD, &mpi_status);
}

int parallel_slave(const int RANK, const int ARRAY_SZ, const int MASTER)
{
#if DEBUG == 1
	printf("S%d: Allocating local vector\n", RANK);
#endif

	/* Try to allocate memory for one array */
	int *values = (int*)malloc(ARRAY_SZ*sizeof(int));
	if(values == NULL){
		printf("S%d: Not enough memory to allocate local vector\n", RANK);
		return 1;
	}

	bool terminate = false;
	while(!terminate){
		/* Ask for an array */
		int service = SVC_REQUEST;
		MPI_Send(&service, 1, MPI_INT, MASTER, TAG_SVC, MPI_COMM_WORLD);

		/* Wait for response */
		MPI_Status mpi_status;
		MPI_Recv(&service, 1, MPI_INT, MASTER, TAG_SVC, MPI_COMM_WORLD, &mpi_status);

		switch(service){
			case SVC_TERMINATE:
				/* Terminate this task */
				terminate = true;
			#if DEBUG == 1
				printf("S%d: Terminating process\n", RANK);
			#endif
				break;
			default:
				/* Received an array index */
				/* Receive, sort, and send back the array */
				slave_work(values, ARRAY_SZ, service, RANK, MASTER);
				break;
		}
	}

	free(values);
	values = NULL;

	return 0;
}

void slave_work(int *array, const int ARRAY_SZ, const int IDX, const int RANK, const int MASTER)
{
	/* Receive the array */
	MPI_Status mpi_status;
	MPI_Recv(array, ARRAY_SZ, MPI_INT, MASTER, TAG_ARRAY, MPI_COMM_WORLD, &mpi_status);

#if DEBUG == 1
	printf("S%d: Sorting array %d\n", RANK, IDX);
#endif

	/* Sort the array: bubblesort */
	bubblesort(array, ARRAY_SZ);

#if DEBUG == 1
	printf("S%d: Array %d sorted. Sending it back.\n", RANK, IDX);
#endif

	/* Inform the master that the array is sorted */
	MPI_Send(&IDX, 1, MPI_INT, MASTER, TAG_SVC, MPI_COMM_WORLD);

	/* Send the array back to the master */
	MPI_Send(array, ARRAY_SZ, MPI_INT, MASTER, TAG_ARRAY, MPI_COMM_WORLD);
}

void populate_matrix(int *matrix, const int SIZE)
{
	/* Populate matrix in decreasing order: worst case for bubblesort */
	for(int i = 0; i < SIZE; i++)
		matrix[i] = SIZE - i;
}

#if DEBUG == 1
void print_arrays(int *matrix, const int ARRAY_CNT, const int ARRAY_SZ)
{
	printf("\n");
	for(int i = 0; i < ARRAY_CNT; i++){
		printf("A%d: ", i);
		for(int j = 0; j < ARRAY_SZ; j++){
			printf("%d ", matrix[i*ARRAY_SZ + j]);
		}
		printf("\n");
	}
	printf("\n");
}
#endif

void bubblesort(int *array, const int SIZE)
{
	/* Sort the array: bubblesort */
	bool swapped = true;
	for(int i = 0; swapped && i < SIZE; i++){
		swapped = false;
		for(int j = i + 1; j < SIZE; j++){
			if(array[j] < array[i]){
				int swap = array[i];
				array[i] = array[j];
				array[j] = swap;
				swapped = true;
			}
		}
	}
}
