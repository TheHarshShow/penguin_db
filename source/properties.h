#ifndef PROPERTIES_H
#define PROPERTIES_H

#include <string>
#include "version/version.h"

extern bool PROG_RUNNING;

const Version VERSION(1,0,0,Version::VersionType::RELEASE);
const bool DEBUG = false;
const std::string DATABASE_DIRECTORY = "/Users/harshmotwani/RDBMS/penguin_db/databases/";
const std::string QUERY_DIRECTORY = "/Users/harshmotwani/RDBMS/penguin_db/queries/";

const uint32_t PAGE_SIZE = 4096; // 4096 Bytes
const uint32_t LOG_MAX_PAGES = 32;
/**
 * @brief Tables start at ID 1 and go until ID (1<<LOG_MAX_TABLES)-1.
 * Queries start at ID (1<<LOG_MAX_TABLES) and go until (1<<(LOG_MAX_TABLES+1)) - 1
 */
const uint32_t LOG_MAX_TABLES = 31; // Maximum number of tables or query pages

#endif // PROPERTIES_H