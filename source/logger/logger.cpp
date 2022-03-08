#include "logger.h"
#include <iostream>

void Logger::logSuccess(std::string successString){
    std::cout << "Success: " << successString << std::endl;
}

void Logger::logError(std::string errorString){
    std::cout << "ERROR: " << errorString << std::endl;
}

void Logger::logDebug(std::string debugString){
    std::cout << "DEBUG: " << debugString << std::endl;
}