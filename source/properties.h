#ifndef PROPERTIES_H
#define PROPERTIES_H

#include <string>
#include "version/version.h"

extern bool PROG_RUNNING;

const Version VERSION(1,0,0,Version::VersionType::DEV);
const bool DEBUG = true;
const std::string DATABASE_DIRECTORY = "/Users/harshmotwani/RDBMS/penguin_db/databases/";
const std::string QUERY_DIRECTORY = "/Users/harshmotwani/RDBMS/penguin_db/queries/";

const uint32_t PAGE_SIZE = 4096; // 4096 Bytes
const uint32_t LOG_MAX_PAGES = 32;

#endif // PROPERTIES_H