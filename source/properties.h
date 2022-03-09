#ifndef PROPERTIES_H
#define PROPERTIES_H

#include <string>
#include "version/version.h"

const Version VERSION(1,0,0,Version::VersionType::DEV);
const bool DEBUG = false;
const std::string DATABASE_DIRECTORY = "/Users/harshmotwani/RDBMS/penguin_db/databases/";
const uint32_t PAGE_SIZE = 4096; // 4096 Bytes

#endif // PROPERTIES_H