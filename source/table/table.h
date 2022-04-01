#ifndef TABLE_H
#define TABLE_H

#include <vector>
#include <string>
#include <memory>
#include <iomanip>
#include "../condition/condition.h"
#include "../database/database.h"
#include "../properties.h"

// enum class COMPARISON {
//     EQUAL,
//     GREATER,
//     LESS,
//     G_EQUAL,
//     L_EQUAL,
//     ASSIGNMENT,
//     INVALID
// };

// struct condition {
//     std::string columnName;
//     COMPARISON operation;
//     std::string value;
//     condition(std::string cName, COMPARISON op, std::string val): columnName(cName), operation(op), value(val) {}
//     void invert();
//     std::string toString();
// };

class Table {
public:
    static void createTable(const std::vector<std::string>& tokens);
    static void insertIntoTable(const std::vector<std::string>& tokens);

    static void handleSearchQuery(const std::vector<std::string>& tokens);
    
    static void handleUpdateTable(const std::vector<std::string>& tokens);
    static void handleDeleteRow(const std::vector<std::string>& tokens);
};

#endif // TABLE_H