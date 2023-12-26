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
#include <pthread.h>

#include "constants.h"
#include "operations.h"
#include "parser.h"

#define MAX_BUF_SIZE 10000

/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

void errorParse() {
  perror("Invalid command. See HELP for usage\n");
  exit(EXIT_FAILURE);
}

void parentWait() {
  int status;
  pid_t child_pid = wait(&status);

  if (WIFEXITED(status)) {
    printf("Child PID: %d exited with status %d\n", child_pid, WEXITSTATUS(status));
  } else {
    printf("Child PID: %d terminated abnormally\n", child_pid);
  }
}

int readCommands(int in_fd, int out_fd) {
  while(1) {
    unsigned int event_id, delay;
    size_t num_rows, num_columns, num_coords;
    size_t xs[MAX_RESERVATION_SIZE], ys[MAX_RESERVATION_SIZE];

    switch (get_next(in_fd)) {
      case CMD_CREATE:
        if (parse_create(in_fd, &event_id, &num_rows, &num_columns) != 0) {
          errorParse();
          continue;
        }

        if (ems_create(event_id, num_rows, num_columns)) {
          perror("Failed to create event\n");
          exit(EXIT_FAILURE);
        }

        break;

      case CMD_RESERVE:
        num_coords = parse_reserve(in_fd, MAX_RESERVATION_SIZE, &event_id, xs, ys);

        if (num_coords == 0) {
          errorParse();
          continue;
        }

        if (ems_reserve(event_id, num_coords, xs, ys)) {
          perror("Failed to reserve seats\n");
          exit(EXIT_FAILURE);
        }

        break;

      case CMD_SHOW:
        if (parse_show(in_fd, &event_id) != 0) {
          errorParse();
          continue;
        }

        if (ems_show(event_id, out_fd)) {
          perror("Failed to show event\n");
          exit(EXIT_FAILURE);
        }

        break;

      case CMD_LIST_EVENTS:
        if (ems_list_events(out_fd)) {
          perror("Failed to list events\n");
          exit(EXIT_FAILURE);
        }

        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {  
          errorParse();
          continue;
        }

        if (delay > 0) {
          perror("Waiting...\n");
          ems_wait(delay);
        }

        break;

      case CMD_INVALID:
        errorParse();
        break;

      case CMD_HELP:
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
      case CMD_EMPTY:
        break;

      case EOC:
        return 0;
    }
  }
}

void processFile(char* dir_name, char* file_name) { 
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
  
  /* change file extension to .out */
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
  if(output_fd < 0) {
    perror("Could not open the output file\n");
    exit(EXIT_FAILURE);
  }

  readCommands(jobs_fd, output_fd); 
  
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

void processDirectory(char* dir_name, int MAX_PROC, unsigned int state_access_delay_ms) {
  if(dir_name == NULL) {
    perror("Wrong directory name\n");
    exit(EXIT_FAILURE);
  }

  /* open the directory jobs (...) */
  DIR* dir = opendir(dir_name);
  if (dir == NULL) {
    perror("Error opening directory\n");
    exit(EXIT_FAILURE);
  }

  int number_proc = 0;
  for(;;) {
    struct dirent* read_dir = readdir(dir);
    if(read_dir == NULL) {
      break;
    } 

    if(strcmp(read_dir->d_name, ".") == 0 || strcmp(read_dir->d_name, "..") == 0 || strstr(read_dir->d_name, ".jobs") == NULL) { 
      continue;
    } 
    
    if(number_proc >= MAX_PROC) {
      parentWait();
      number_proc--;
    }

    pid_t pid = fork();
    if (pid == -1) {
      perror("Fork error\n");
      exit(EXIT_FAILURE);
    }

    number_proc++;

    if (pid == 0) {
      // Child process
      if (ems_init(state_access_delay_ms)) {
        perror("Failed to initialize EMS\n");
        exit(EXIT_FAILURE);
      }
      
      processFile(dir_name, read_dir->d_name); 

      printf("Child PID: %d\n", getpid());
      ems_terminate();
      exit(EXIT_SUCCESS);       
    } 

    number_proc++;
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

  if(argc != 4) {
    perror("Expected Format: ./ems <inputfile> <MAX_PROC> <delay>\n");
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
  
  int MAX_PROC = atoi(argv[2]);
  if (MAX_PROC < 0) {
    perror("Invalid maximum processes number\n");
    exit(EXIT_FAILURE);
  }

  char* dir_name = argv[1];
  processDirectory(dir_name, MAX_PROC, state_access_delay_ms );
}
