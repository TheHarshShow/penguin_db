#include "../properties.h"

extern char WRITE_BUFFER[];
extern char READ_BUFFER[];

extern char TABLE_METADATA_PAGE_BUFFER_A[];
extern char TABLE_METADATA_PAGE_BUFFER_B[];

extern char CURRENT_TABLE_PAGE_BUFFER_A[];
extern char CURRENT_TABLE_PAGE_BUFFER_B[];

extern char WORKBUFFER_A[];
extern char WORKBUFFER_B[];
extern char WORKBUFFER_C[];
extern char WORKBUFFER_D[];

/**
 * @note For now, metadata files don't use these functions. metadata files are and read directly because of smaller expected size.
 */

uint32_t readPage(char BUFFER[], uint64_t fileId, uint64_t pageNumber);
bool writeToPage(char BUFFER[], uint64_t fileId, uint64_t pageNumber, int additionalFlags = 0, mode_t mode = 0);
int32_t writeToFile(int fd, char BUFFER[], int totWrite = PAGE_SIZE);
int32_t readFromFile(int fd, char BUFFER[], int totRead = PAGE_SIZE);