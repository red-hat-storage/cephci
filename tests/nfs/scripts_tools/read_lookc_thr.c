#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>

#define NUM_THREADS 5
#define MAX_PATH_LEN 1024

char filepath[MAX_PATH_LEN];

void *thread_function(void *arg) {
   int rfd;
   struct flock rfl;
   long thread_id = (long)arg;
   int toggle = 0;

   while (1) {
       // Open the file in read-only mode
       rfd = open(filepath, O_RDONLY);
       if (rfd == -1) {
           printf("Thread %ld: Failed to open file %s\n", thread_id, filepath);
           continue;
       }

       printf("Thread %ld: Open success: %s\n", thread_id, filepath);

       // Alternate lock region between 0 and 5
       rfl.l_type = F_RDLCK;
       rfl.l_whence = SEEK_SET;
       rfl.l_start = toggle ? 5 : 0;
       rfl.l_len = 0;

       printf("Thread %ld: Trying to acquire read lock at offset %lld...\n",
              thread_id, (long long)rfl.l_start);

       if (fcntl(rfd, F_SETLKW, &rfl) == -1) {
           printf("Thread %ld: Failed to set F_RDLCK\n", thread_id);
           close(rfd);
           continue;
       } else {
           printf("Thread %ld: F_RDLCK granted at offset %lld\n",
                  thread_id, (long long)rfl.l_start);
       }

       // Unlock the file
       rfl.l_type = F_UNLCK;
       if (fcntl(rfd, F_SETLKW, &rfl) == -1) {
           printf("Thread %ld: Failed to unlock the file\n", thread_id);
       } else {
           printf("Thread %ld: File unlocked at offset %lld\n",
                  thread_id, (long long)rfl.l_start);
       }

       close(rfd);
       toggle = !toggle; // Alternate the region
   }

   pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
   if (argc != 2) {
       fprintf(stderr, "Usage: %s <directory>\n", argv[0]);
       exit(EXIT_FAILURE);
   }

   snprintf(filepath, MAX_PATH_LEN, "%s/testfile.txt", argv[1]);

   pthread_t threads[NUM_THREADS];
   int rc;
   long t;

   for (t = 0; t < NUM_THREADS; t++) {
       printf("Creating thread %ld\n", t);
       rc = pthread_create(&threads[t], NULL, thread_function, (void *)t);
       if (rc) {
           printf("ERROR: return code from pthread_create() is %d\n", rc);
           exit(EXIT_FAILURE);
       }
   }

   // Wait for all threads to complete
   for (t = 0; t < NUM_THREADS; t++) {
       pthread_join(threads[t], NULL);
   }

   pthread_exit(NULL);
}
