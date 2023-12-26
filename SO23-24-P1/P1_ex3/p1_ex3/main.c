#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <pthread.h>


#include "constants.h"
#include "eventlist.h"
#include "operations.h"
#include "parser.h"

#define MAX_COMMANDS 150000
#define MAX_BUF_SIZE 10000
#define MAX_INPUT_SIZE 1000

typedef struct {
  pthread_mutex_t mutex_ref;   /* Can be either a mutex lock (...) */
  pthread_rwlock_t rwlock_ref; /* (...) or a r/w lock. */
} pthread_lock_ref;

struct input_thread {
  unsigned int in_fd;
  int out_fd;
  int id;
  pthread_mutex_t *mutex; 
};

struct Barrier {
  pthread_mutex_t *barrier_mutex;
  int count;
  int n_threads;
  int barrier_reached;
};

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Locks: for accessing the commands vector and for applying them. */
pthread_lock_ref lock_ref1, lock_ref2;

char inputCommands[MAX_COMMANDS][MAX_INPUT_SIZE];

int numberCommands = 0;

int headQueue = 0;

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

void errorParse();
int readCommands(int in_fd, int out_fd, pthread_mutex_t *mutex);

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

void *wait_thread(void *arg) {
  struct input_thread *input_thread = (struct input_thread *)arg;
  int delay = input_thread->in_fd;

  ems_wait(delay);

  pthread_exit(NULL);
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

void barrier_init(struct Barrier *barrier, int n_threads) {
  pthread_mutex_init(barrier->barrier_mutex, NULL);
  barrier->count = 0;
  barrier->n_threads = n_threads;
  barrier->barrier_reached = 0;
}

void barrier_wait(struct Barrier *barrier) {
  pthread_mutex_lock(barrier->barrier_mutex);
  barrier->count++;

  if (barrier->count == barrier->n_threads) {
    barrier->barrier_reached = 1;
    pthread_mutex_unlock(barrier->barrier_mutex);
  }

  barrier->count--;
  if (barrier->count == 0) {
    barrier->barrier_reached = 0;
    pthread_mutex_unlock(barrier->barrier_mutex);
  }

  pthread_mutex_unlock(barrier->barrier_mutex);
}

void barrier_destroy(struct Barrier *barrier) {
  pthread_mutex_destroy(barrier->barrier_mutex);
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Error function for initializing locks.
 */
void error_init_locks()
{
  perror("Error: Unable to initialize locks.\n");
  exit(EXIT_FAILURE);
}

/*
 * Error function for destroying locks.
 */
void error_destroy_locks()
{
  perror("Error: Unable to destroy locks.\n");
  exit(EXIT_FAILURE);
}

/*
 * Error function for locking.
 */
void error_lock()
{
  perror("Error: Unable to lock.\n");
  exit(EXIT_FAILURE);
}

/*
 * Error function for unlocking.
 */
void error_unlock()
{
  perror("Error: Unable to unlock.\n");
  exit(EXIT_FAILURE);
}

/*
 * Error function for invalid command.
 */
void errorParse()
{
  perror("Invalid command. See HELP for usage\n");
  exit(EXIT_FAILURE);
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Inicializes mutex locks.
 */
void init_mutex_locks() {
  if (pthread_mutex_init(&lock_ref1.mutex_ref, NULL) || pthread_mutex_init(&lock_ref2.mutex_ref, NULL)) {
    error_init_locks();
  }
}

/*
 * Inicializes rw locks.
 */
void init_rw_locks() {
  if (pthread_rwlock_init(&lock_ref1.rwlock_ref, NULL) || pthread_mutex_init(&lock_ref2.mutex_ref, NULL)) {
    error_init_locks();
  }
}

/*
 * Destroys all mutex locks.
 */
void destroy_mutex_locks() {
  if (pthread_mutex_destroy(&lock_ref1.mutex_ref) || pthread_mutex_destroy(&lock_ref2.mutex_ref)) {
    error_destroy_locks(); 
  }
}

/*
 * Destroys all rw locks.
 */
void destroy_rw_locks() {
  if (pthread_rwlock_destroy(&lock_ref1.rwlock_ref) || pthread_rwlock_destroy(&lock_ref2.rwlock_ref)) {
    error_destroy_locks(); 
  }
}

/*
 * Locks a critical section.
 */
void mutex_lock() {
  if (pthread_mutex_lock(&lock_ref1.mutex_ref)) {
    error_lock();
  }
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

void parentWait() {
  int status;
  pid_t child_pid = wait(&status);

  if (WIFEXITED(status)){
    printf("Child PID: %d exited with status %d\n", child_pid, WEXITSTATUS(status));
  }
  else {
    printf("Child PID: %d terminated abnormally\n", child_pid);
  }
}

int readCommands(int in_fd, int out_fd, pthread_mutex_t *mutex) {
  while (1) {
    unsigned int event_id, delay;
    size_t num_rows, num_columns, num_coords;
    size_t xs[MAX_RESERVATION_SIZE], ys[MAX_RESERVATION_SIZE];

    pthread_mutex_lock(mutex);

    switch (get_next(in_fd)) {
    case CMD_CREATE:
      if (parse_create(in_fd, &event_id, &num_rows, &num_columns) != 0) {
        errorParse();
        pthread_mutex_unlock(mutex);
        continue;
      }

      pthread_mutex_unlock(mutex);

      if (ems_create(event_id, num_rows, num_columns)) {
        exit(EXIT_FAILURE);
      }

      break;

    case CMD_RESERVE:
      num_coords = parse_reserve(in_fd, MAX_RESERVATION_SIZE, &event_id, xs, ys);

      if (num_coords == 0) {
        errorParse();
        pthread_mutex_unlock(mutex);
        continue;
      }

      pthread_mutex_unlock(mutex);

      if (ems_reserve(event_id, num_coords, xs, ys)) {
        perror("Failed to reserve seats\n");
        pthread_mutex_unlock(mutex);
        exit(EXIT_FAILURE);
      }

      break;

    case CMD_SHOW:
      if (parse_show(in_fd, &event_id) != 0) {
        errorParse();
        pthread_mutex_unlock(mutex);
        continue;
      }

      pthread_mutex_unlock(mutex);

      if (ems_show(event_id, out_fd)) {
        perror("Failed to show event\n");
        pthread_mutex_unlock(mutex);
        exit(EXIT_FAILURE);
      }

      break;

    case CMD_LIST_EVENTS:
      pthread_mutex_unlock(mutex);

      if (ems_list_events(out_fd)) {
        perror("Failed to list events\n");
        pthread_mutex_unlock(mutex);
        exit(EXIT_FAILURE);
      }

      break;

    case CMD_WAIT:
      pthread_mutex_unlock(mutex);

      if (parse_wait(in_fd, &delay, NULL) == -1) {
        errorParse();
        pthread_mutex_unlock(mutex);
        continue;
      }

      if (delay > 0) {
        perror("Waiting...\n");
        ems_wait(delay);
      }

      break;

    case CMD_INVALID:
      pthread_mutex_unlock(mutex);
      errorParse();
      break;

    case CMD_HELP:
      pthread_mutex_unlock(mutex);
      perror(
          "Available commands:\n"
          "  CREATE <event_id> <num_rows> <num_columns>\n"
          "  RESERVE <event_id> [(<x1>,<y1>) (<x2>,<y2>) ...]\n"
          "  SHOW <event_id>\n"
          "  LIST\n"
          "  WAIT <delay_ms> [thread_id]\n"
          "  BARRIER\n"
          "  HELP\n");

      break;

    case CMD_BARRIER:
      pthread_mutex_unlock(mutex);
      break;
    case CMD_EMPTY:
      pthread_mutex_unlock(mutex);
      break;

    case EOC:
      pthread_mutex_unlock(mutex);
      return 0;
    }
  }
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

void *readCommands_thread(void *arg) {
  struct input_thread *input_thread = (struct input_thread *)arg;
  int in_fd = input_thread->in_fd;
  int out_fd = input_thread->out_fd;

  pthread_mutex_t *mutex = input_thread->mutex;
  readCommands(in_fd, out_fd, mutex);

  pthread_exit(NULL);
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

void processFile(char *dir_name, char *file_name, int NUM_THREADS) { 
  char buf_jobs[MAX_BUF_SIZE];
  buf_jobs[0] = '\0';
  strcpy(buf_jobs, dir_name);
  strcat(buf_jobs, "/");
  strcat(buf_jobs, file_name);

  /* open the input file (...) */
  int jobs_fd = open(buf_jobs, O_RDONLY);
  if (jobs_fd < 0) {
    perror("Could not open the input file\n");
    exit(EXIT_FAILURE);
  }

  char buf_out[MAX_BUF_SIZE];
  buf_out[0] = '\0';
  char *dot = strrchr(file_name, '.');

  if (dot != NULL) {
    strncat(buf_out, file_name, strlen(file_name) - strlen(dot));
    strcat(buf_out, ".out");
  } else {
    perror("File name has no extension\n");
  }

  int open_flags = O_CREAT | O_WRONLY | O_TRUNC;
  mode_t file_perms = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH; /* rw-rw-rw- */

  /* open the output file (...) */
  int output_fd = open(buf_out, open_flags, file_perms);
  if (output_fd < 0) {
    perror("Could not open the output file\n");
    exit(EXIT_FAILURE);
  }

  pthread_t id[NUM_THREADS];
  struct input_thread input_thread[NUM_THREADS];
  pthread_mutex_t mutex;

  pthread_mutex_init(&mutex, NULL);

  for (int i = 0; i < NUM_THREADS; i++) {
    input_thread[i].in_fd = jobs_fd;
    input_thread[i].out_fd = output_fd;
    input_thread[i].id = i;
    input_thread[i].mutex = &mutex;

    pthread_create(&id[i], NULL, &readCommands_thread, &input_thread[i]);
  }

  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(id[i], NULL);
  }

  /* (...) close the input file */
  if (close(jobs_fd) < 0) {
    perror("Could not close the input file\n");
    exit(EXIT_FAILURE);
  }

  /* (...) close the output file */
  if (close(output_fd) < 0) {
    perror("Could not close the input file\n");
    exit(EXIT_FAILURE);
  }
}

void processDirectory(char *dir_name, int MAX_PROC, int MAX_THREADS, unsigned int state_access_delay_ms) {
  if (dir_name == NULL) {
    perror("Wrong directory name\n");
    exit(EXIT_FAILURE);
  }

  /* open the directory jobs (...) */
  DIR *dir = opendir(dir_name);
  if (dir == NULL) {
    perror("Error opening directory\n");
    exit(EXIT_FAILURE);
  }

  int number_proc = 0;
  for (;;) {
    struct dirent *read_dir = readdir(dir);
    if (read_dir == NULL) {
      break;
    }

    if (strcmp(read_dir->d_name, ".") == 0 || strcmp(read_dir->d_name, "..") == 0 || strstr(read_dir->d_name, ".jobs") == NULL) {
      continue;
    }

    if (number_proc >= MAX_PROC) {
      parentWait();
      number_proc--;
    }

    pid_t pid = fork();
    if (pid == -1) {
      perror("Fork error\n");
      exit(EXIT_FAILURE);
    }

    if (pid == 0) {
      // Child process
      if (ems_init(state_access_delay_ms)) {
        perror("Failed to initialize EMS\n");
        exit(EXIT_FAILURE);
      }

      processFile(dir_name, read_dir->d_name, MAX_THREADS); 

      printf("Child PID: %d\n", getpid());
      ems_terminate();
      exit(EXIT_SUCCESS);
    }

    number_proc++;
  }

  for (; number_proc > 0; number_proc--) {
    parentWait();
  }

  /* (...) close the directory jobs */
  if (closedir(dir) < 0) {
    perror("Could not close the jobs directory\n");
    exit(EXIT_FAILURE);
  }
}

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

int main(int argc, char *argv[]) {
  unsigned int state_access_delay_ms = STATE_ACCESS_DELAY_MS;

  if (argc < 2) {
    perror("Please specify the pathname to the jobs file\n");
    exit(EXIT_FAILURE);
  }

  if (argc != 4) {
    perror("Expected Format: ./ems <inputfile> <MAX_THREADS> <delay>\n");
    exit(EXIT_FAILURE);
  }

  if (argc > 1) {
    char *endptr;
    unsigned long int delay = strtoul(argv[3], &endptr, 10);

    if (*endptr != '\0' || delay > UINT_MAX) {
      perror("Invalid delay value or value too large\n");
      return 1;
    }

    state_access_delay_ms = (unsigned int)delay;
  }

  int MAX_THREADS = atoi(argv[2]);
  if (MAX_THREADS < 0) {
    perror("Invalid maximum thread number\n");
    exit(EXIT_FAILURE);
  }

  char *dir_name = argv[1];
  int MAX_PROC = 1; /* default value */
  processDirectory(dir_name, MAX_PROC, MAX_THREADS, state_access_delay_ms);

  return 0;
}

