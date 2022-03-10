#ifndef TABLE_H
#define TABLE_H

#include <vector>
#include <string>

enum class COMPARISON {
    EQUAL,
    GREATER,
    LESS,
    G_EQUAL,
    L_EQUAL,
    INVALID
};

COMPARISON stringToComparison(const std::string& comp);
COMPARISON getCompResult(std::string lVal, std::string rVal, std::string type);
bool isComparisonValid(COMPARISON expected, COMPARISON obtained);

struct condition {
    std::string columnName;
    COMPARISON operation;
    std::string value;
    condition(std::string cName, COMPARISON op, std::string val): columnName(cName), operation(op), value(val) {}
};

class Table {
public:
    static void createTable(const std::vector<std::string>& tokens);
    static void insertIntoTable(const std::vector<std::string>& tokens);
    static void handleSelect(const std::vector<std::string>& tokens);
};

#endif // TABLE_H