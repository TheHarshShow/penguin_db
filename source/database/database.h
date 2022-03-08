#ifndef DATABASE_H
#define DATABASE_H

#include <vector>

class Database {
public:
    /**
     * @brief Create database with the provided name
     * 
     * @param name name of the database
     */
    static void createDatabase(const std::vector<std::string>& tokens);

    /**
     * @brief Change current database to requested database
     * 
     * @param name name of the database
     */
    static void useDatabase(const std::vector<std::string>& tokens);
};

#endif // DATABASE_H