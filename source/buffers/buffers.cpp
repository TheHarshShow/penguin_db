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

int32_t writeToFile(int fd, char BUFFER[], int totWrite){
    memcpy(WRITE_BUFFER, BUFFER, totWrite);
    return write(fd, WRITE_BUFFER, totWrite);
}

int32_t readFromFile(int fd, char BUFFER[], int totRead){
    int32_t bytesRead = read(fd, READ_BUFFER, totRead);
    memcpy(BUFFER, READ_BUFFER, totRead);
    return bytesRead;
}