#include <string.h>
#include "buffers.h"
#include "../properties.h"

// File system calls
#include <fcntl.h>
#include <unistd.h>

char WRITE_BUFFER[PAGE_SIZE+1];
char READ_BUFFER[PAGE_SIZE+1];

char TABLE_METADATA_PAGE_BUFFER_A[PAGE_SIZE+1];
char TABLE_METADATA_PAGE_BUFFER_B[PAGE_SIZE+1];

char CURRENT_TABLE_PAGE_BUFFER_A[PAGE_SIZE+1];
char CURRENT_TABLE_PAGE_BUFFER_B[PAGE_SIZE+1];

char WORKBUFFER_A[PAGE_SIZE+1];
char WORKBUFFER_B[PAGE_SIZE+1];
char WORKBUFFER_C[PAGE_SIZE+1];
char WORKBUFFER_D[PAGE_SIZE+1];

void writeToFile(int fd, char BUFFER[], int totWrite){
    memcpy(WRITE_BUFFER, BUFFER, totWrite);
    write(fd, WRITE_BUFFER, totWrite);
}

void readFromFile(int fd, char BUFFER[], int totRead){
    read(fd, READ_BUFFER, totRead);
    memcpy(BUFFER, READ_BUFFER, totRead);
}