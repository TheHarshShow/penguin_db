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
    
    /**
     * @brief Get the Current Database string
     * 
     * @return std::string the current database name 
     */
    static std::string getCurrentDatabase();

    /**
     * @brief Check if a database is currently chosen (not nul)
     * 
     * @return true if database is not nul
     * @return false otherwise
     */
    static bool isDatabaseChosen();

    static bool checkIfTableExists(const std::string& tableName);

    static const std::vector< std::vector< std::string > > getColumnsOfTable(const std::string& tableName);

    // /**
    //  * @brief Saves table info (name to column mapping) to currentTables map
    //  * 
    //  * @param tableName name of the table for which mapping is to be saved
    //  * @param columns the info of the columns of this table
    //  */
    // static void cacheTableInfo(const std::string& tableName, const std::vector< std::vector< std::string >>& columns, const std::string& id);

};

#endif // DATABASE_H