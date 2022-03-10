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
uint32_t getTypeSize(const std::string& type);
bool matchType(const std::string& value, const std::string& type);
std::string getBytesFromValue(const std::string& value, const std::string& type);
std::string getValueFromBytes(char buffer[], const std::string& type, int start, int end);

#endif // TYPE_H