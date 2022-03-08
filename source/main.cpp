#include <iostream>
#include "version/version.h"
#include "properties.h"
#include "parse/parse.h"

int main(){
    std::cout << "Penguin DB Version " << VERSION << "\n" << std::endl;

    while(true){
        std::cout << "penguin_db > ";
        std::string command;
        getline(std::cin, command, ';');
        
        processCommand(command);
    }
    
}