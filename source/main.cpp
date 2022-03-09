#include <iostream>
#include "version/version.h"
#include "properties.h"
#include "parse/parse.h"
#include "formatter/formatter.h"

int main(){
    std::cout << Formatter::bold_on << "Penguin DB " << Formatter::off << "Version " << VERSION << "\n" << std::endl;

    while(true){
        std::cout << "penguin_db > ";
        std::string command;
        getline(std::cin, command, ';');
        
        processCommand(command);
    }
    
}