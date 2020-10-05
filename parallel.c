#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>

/* Define DEBUG by compiler's command line */
#ifndef DEBUG
	#define DEBUG 0
#endif

#if DEBUG == 1
	/* DEBUG matrix with 8 arrays of 40 entries */
	#define N 8
	#define M 40
#else
	/* Matrix with 1.000 arrays of 100.000 entries */
	#define N 8		// Change to 1000
	#define M 10000	// Change to 100000
#endif

/* Communication tags. SVC for service and ARRAY for 'payload' */
#define TAG_SVC		0
#define TAG_ARRAY	1

/* List of Slave -> Master and Master -> Slave services */
#define SVC_REQUEST -1	/* Slave -> Master. Ask for array */
#define SVC_TERMINATE -2	/* Master -> Slave. Send suicide message */

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

	if(rank == size - 1){
		/* Last processor is MASTER */
		#if DEBUG == 1
			printf("M: Allocating global vector\n");
		#endif

		/* Try to allocate memory for all our values */
		int *values = (int*)malloc(M*N*sizeof(int));
		if(values == NULL){
			#if DEBUG == 1
				printf("M: Not enough memory to allocate global vector\n");
			#endif
			MPI_Finalize();
			exit(1);
		}

		#if DEBUG == 1
			printf("M: Global vector allocated\n");
			printf("M: Populating global vector\n");
		#endif

		/* Populate matrix in decreasing order: worst case for bubblesort */
		for(int i = 0; i < M*N; i++)
			values[i] = M*N - i;

		#if DEBUG == 1
			printf("\n");
			for(int i = 0; i < N; i++){
				printf("A%d: ", i);
				for(int j = 0; j < M; j++){
					printf("%d ", values[i*M + j]);
				}
				printf("\n");
			}
			printf("\n");
		#endif

		#if DEBUG == 0
			/* Preparations finished. Start counting time here */
			double then = MPI_Wtime();
		#endif

		int terminated_cnt = 0;	/* Number of terminated slaves */
		int sorted_cnt = 0;		/* Number of sorted arrays */

		/* Master must command until all slaves are terminated */
		while(terminated_cnt < size - 1){
			int service;
			MPI_Status mpi_status;

			/* Wait for a message from any slave */ 
			MPI_Recv(&service, 1, MPI_INT, MPI_ANY_SOURCE, TAG_SVC, MPI_COMM_WORLD, &mpi_status);

			switch(service){
				case SVC_REQUEST:
					/* Slave requested for an array */
					if(sorted_cnt < N){
						/* There are arrays to be sorted */
						/* Send message indicating that the slave must receive the 'sorted_cnt' array */
						MPI_Send(&sorted_cnt, 1, MPI_INT, mpi_status.MPI_SOURCE, TAG_SVC, MPI_COMM_WORLD);

						/* Send the array to sort */
						MPI_Send(&values[sorted_cnt*M], M, MPI_INT, mpi_status.MPI_SOURCE, TAG_ARRAY, MPI_COMM_WORLD);
						
						#if DEBUG == 1
							printf("M: Sent array %d to S%d\n", sorted_cnt, mpi_status.MPI_SOURCE);
						#endif

						sorted_cnt++;
					} else {
						/* All arrays sorted. Send message to terminate */
						service = SVC_TERMINATE;
						MPI_Send(&service, 1, MPI_INT, mpi_status.MPI_SOURCE, TAG_SVC, MPI_COMM_WORLD);
						terminated_cnt++;

						#if DEBUG == 1
							printf("M: Sent suicide message to S%d\n", mpi_status.MPI_SOURCE);
						#endif
					}
					break;
				default:
					/* Slave responded with the index of a sorted vector. Receive it back */
					#if DEBUG == 1
						printf("M: Receiving array %d from S%d\n", service, mpi_status.MPI_SOURCE);
					#endif

					MPI_Recv(&values[service*M], M, MPI_INT, mpi_status.MPI_SOURCE, TAG_ARRAY, MPI_COMM_WORLD, &mpi_status);
					break;
			}
		}

		#if DEBUG == 0
			/* Print time information */
			double now = MPI_Wtime();
			printf("M: All arrays sorted in %f\n", now - then);
		#else
			printf("M: All arrays sorted!\n");
			printf("\n");
			for(int i = 0; i < N; i++){
				printf("A%d: ", i);
				for(int j = 0; j < M; j++){
					printf("%d ", values[i*M + j]);
				}
				printf("\n");
			}
			printf("\n");
		#endif

	} else {
		/* All others are SLAVES */
		#if DEBUG == 1
			printf("S%d: Allocating local vector\n", rank);
		#endif

		/* Try to allocate memory for one array */
		int *values = (int*)malloc(M*sizeof(int));
		if(values == NULL){
			#if DEBUG == 1
				printf("S%d: Not enough memory to allocate local vector\n", rank);
			#endif
			MPI_Finalize();
			exit(1);
		}

		bool terminate = false;
		while(!terminate){
			/* Ask for an array */
			int service = SVC_REQUEST;
			MPI_Send(&service, 1, MPI_INT, size - 1, TAG_SVC, MPI_COMM_WORLD);

			/* Wait for response */
			MPI_Status mpi_status;
			MPI_Recv(&service, 1, MPI_INT, size - 1, TAG_SVC, MPI_COMM_WORLD, &mpi_status);

			switch(service){
				case SVC_TERMINATE:
					/* Terminate this task */
					terminate = true;

					#if DEBUG == 1
						printf("S%d: Terminating process\n", rank);
					#endif
					break;
				default:
					/* Received an array index */
					/* Receive the array */
					MPI_Recv(values, M, MPI_INT, size - 1, TAG_ARRAY, MPI_COMM_WORLD, &mpi_status);

					#if DEBUG == 1
						printf("S%d: Sorting array %d\n", rank, service);
					#endif

					/* Sort the array: bubblesort */
					bool swapped = true;
					for(int i = 0; swapped && i < M; i++){
						swapped = false;
						for(int j = i + 1; j < M; j++){
							if(values[j] < values[i]){
								int swap = values[i];
								values[i] = values[j];
								values[j] = swap;
								swapped = true;
							}
						}
					}

					#if DEBUG == 1
						printf("S%d: Array %d sorted. Sending it back.\n", rank, service);
					#endif

					/* Inform the master that the array is sorted */
					MPI_Send(&service, 1, MPI_INT, size - 1, TAG_SVC, MPI_COMM_WORLD);

					/* Send the array back to the master */
					MPI_Send(values, M, MPI_INT, size - 1, TAG_ARRAY, MPI_COMM_WORLD);
					break;
			}
		}

	}

	MPI_Finalize();
}
