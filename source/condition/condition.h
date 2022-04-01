#ifndef CONDITION_H
#define CONDITION_H

#include <string>

enum class COMPARISON {
    EQUAL,
    GREATER,
    LESS,
    G_EQUAL,
    L_EQUAL,
    ASSIGNMENT,
    INVALID
};

COMPARISON stringToComparison(const std::string& comp);
COMPARISON getCompResult(std::string lVal, std::string rVal, std::string type);
COMPARISON getInverseComparison(COMPARISON comp);

bool isComparisonValid(COMPARISON expected, COMPARISON obtained);

struct condition {
    std::string columnName;
    COMPARISON operation;
    std::string value;
    condition(std::string cName, COMPARISON op, std::string val): columnName(cName), operation(op), value(val) {}
    void invert();
    std::string toString();
};

condition getCondition(const std::vector< std::string >& currentComparison);

#endif // CONDITION_H