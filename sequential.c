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

int main(int argc, char *argv[])
{
	MPI_Init(&argc, &argv);

	int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	if(rank == 0){
		#if DEBUG == 1
			printf("Allocating global vector\n");
		#endif

		/* Try to allocate memory for all our values */
		int *values = (int*)malloc(M*N*sizeof(int));
		if(values == NULL){
			#if DEBUG == 1
				printf("Not enough memory to allocate global vector\n");
			#endif
			exit(1);
		}

		#if DEBUG == 1
			printf("Global vector allocated\n");
			printf("Populating global vector\n");
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

		/* Sort all arrays */
		for(int a = 0; a < N; a++){
			/* Sort the array: bubblesort */
			bool swapped = true;
			for(int i = 0; swapped && i < M; i++){
				swapped = false;
				for(int j = i + 1; j < M; j++){
					if(values[a*M + j] < values[a*M + i]){
						int swap = values[a*M + i];
						values[a*M + i] = values[a*M + j];
						values[a*M + j] = swap;
						swapped = true;
					}
				}
			}
		}

		#if DEBUG == 0
			/* Print time information */
			double now = MPI_Wtime();
			printf("All arrays sorted in %f\n", now - then);
		#else
			printf("All arrays sorted!\n");
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
	}
	
	MPI_Finalize();
}
