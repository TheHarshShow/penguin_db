#include <iostream>
#include "../formatter/formatter.h"
#include "logger.h"

void Logger::logSuccess(std::string successString){
    std::cout << Formatter::bold_green_on << "Success: " << Formatter::off << successString << std::endl;
}

void Logger::logError(std::string errorString){
    std::cout << Formatter::bold_red_on << "ERROR: " << Formatter::off << errorString << std::endl;
}

void Logger::logDebug(std::string debugString){
    std::cout << "DEBUG: " << debugString << std::endl;
}