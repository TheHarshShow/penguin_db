#ifndef TYPE_H
#define TYPE_H
#include <string>

enum class TYPE {
    INT,
    FLOAT,
    CHAR,
    STRING,
    UNSUPPORTED
};

TYPE getTypeFromString(const std::string type);
uint32_t getStringLength(const std::string& type);

#endif // TYPE_H