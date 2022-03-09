#ifndef TABLE_H
#define TABLE_H

#include <vector>
#include <string>

class Table {
public:
    static void createTable(const std::vector<std::string>& tokens);
    static void insertIntoTable(const std::vector<std::string>& tokens);
};

#endif // TABLE_H