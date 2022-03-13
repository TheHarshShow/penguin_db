#include <string.h>
#include "../database/database.h"
#include "buffers.h"
#include "../properties.h"
#include "../logger/logger.h"

// File system calls
#include <fcntl.h>
#include <unistd.h>

char WRITE_BUFFER[PAGE_SIZE+1];
char READ_BUFFER[PAGE_SIZE+1];

char TABLE_METADATA_PAGE_BUFFER_A[PAGE_SIZE+1];
char TABLE_METADATA_PAGE_BUFFER_B[PAGE_SIZE+1];

char CURRENT_TABLE_PAGE_BUFFER_A[PAGE_SIZE+1];
char CURRENT_TABLE_PAGE_BUFFER_B[PAGE_SIZE+1];

uint64_t QUERY_TIMESTAMPS[PAGE_SIZE / sizeof(uint64_t)];
uint32_t QUERY_TIMESTAMP_PTR = 0;

char WORKBUFFER_A[PAGE_SIZE+1];
char WORKBUFFER_B[PAGE_SIZE+1];
char WORKBUFFER_C[PAGE_SIZE+1];
char WORKBUFFER_D[PAGE_SIZE+1];

int getFileDesriptor(uint64_t fileId, uint64_t pageNumber, int flags, mode_t mode){
    if(pageNumber >= ((uint64_t)1 << LOG_MAX_PAGES)){
        Logger::logError("Page number "+std::to_string(pageNumber)+" too large");
        return -1;
    }
    if(!Database::isDatabaseChosen()){
        Logger::logError("Database not chosen");
        return -1;
    }
    std::string dbName = Database::getCurrentDatabase();

    if(fileId == 0){
        // Table metadata file
        return open((DATABASE_DIRECTORY + dbName + "/tables").c_str(), flags, mode);
    } else if(fileId < ((uint64_t)1 << LOG_MAX_TABLES)) {
        // Table data file
        return open((DATABASE_DIRECTORY + dbName + "/data/table__"+std::to_string(fileId)).c_str(), flags, mode);
    } else {
        //Query data file
        return open((DATABASE_DIRECTORY + dbName + "/data/query__"+std::to_string(fileId)).c_str(), flags, mode);
    }
}

uint32_t readPage(char BUFFER[], uint64_t fileId, uint64_t pageNumber){

    int fd = getFileDesriptor(fileId, pageNumber, O_RDONLY, 0);

    if(fd < 0){
        Logger::logError("Error in loading tables metadata file");
        close(fd);
        return 0;
    }

    lseek(fd, pageNumber*PAGE_SIZE, SEEK_SET);
    uint32_t totRead = readFromFile(fd,BUFFER);
    close(fd);

    if(totRead == -1){
        totRead = 0;
    }

    return totRead;
}

bool writeToPage(char BUFFER[], uint64_t fileId, uint64_t pageNumber, int additionalFlags, mode_t mode){

    int fd = getFileDesriptor(fileId, pageNumber, O_WRONLY | additionalFlags, mode);

    if(fd < 0){
        Logger::logError("Error in loading tables metadata file");
        close(fd);
        return false;
    }

    lseek(fd, pageNumber*PAGE_SIZE, SEEK_SET);
    uint32_t totRead = writeToFile(fd,BUFFER);
    close(fd);

    if(totRead == -1){
        return false;
    }

    return true;
}

int32_t writeToFile(int fd, char BUFFER[], int totWrite){
    memcpy(WRITE_BUFFER, BUFFER, totWrite);
    return write(fd, WRITE_BUFFER, totWrite);
}

int32_t readFromFile(int fd, char BUFFER[], int totRead){
    int32_t bytesRead = read(fd, READ_BUFFER, totRead);
    memcpy(BUFFER, READ_BUFFER, totRead);
    return bytesRead;
}