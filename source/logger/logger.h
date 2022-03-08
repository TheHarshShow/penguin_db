#ifndef LOGGER_H
#define LOGGER_H

#include <string>

class Logger {
public:
    static void logSuccess(std::string successString);
    static void logError(std::string errorString);
    static void logDebug(std::string debugString);
};

#endif // LOGGER_H