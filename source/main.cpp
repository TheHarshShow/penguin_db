#include <iostream>
#include <filesystem>
#include "version/version.h"
#include "properties.h"
#include "parse/parse.h"
#include "formatter/formatter.h"

bool PROG_RUNNING = true;

int main(){

    // Speed up cin/cout
    std::ios_base::sync_with_stdio(false);

    std::cout << Formatter::bold_on << "Penguin DB " << Formatter::off << "Version " << VERSION << "\n" << std::endl;

    if(!std::filesystem::exists(DATABASE_DIRECTORY)){
        std::filesystem::create_directories(DATABASE_DIRECTORY);
    }

    if(!std::filesystem::exists(QUERY_DIRECTORY)){
        std::filesystem::create_directories(QUERY_DIRECTORY);
    }

    while(PROG_RUNNING){
        std::cout << Formatter::bold_on << "penguin_db > " << Formatter::off;
        std::string command;
        getline(std::cin, command, ';');
        
        processCommand(command);
    }
}