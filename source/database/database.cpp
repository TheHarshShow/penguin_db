#include <string>
#include <filesystem>
#include "database.h"
#include "../properties.h"
#include "../logger/logger.h"

// File system calls
#include <fcntl.h>
#include <unistd.h>

bool validateDatabaseName(const std::string& name);
void saveDatabase(const std::string &dbName);

void Database::createDatabase(const std::vector<std::string>& tokens){
    if(tokens.size()!=3){
        Logger::logError("Instruction has incorrect number of arguments");
        return;
    }

    std::string dbName = tokens[2];

    if(!validateDatabaseName(dbName)){
        Logger::logError("Database name is not valid. Name should be an alphanumeric string between 4 and 16 characters in length (inclusive). Also, the first character needs to be an alphabet");
        return;
    }

    if(std::filesystem::exists(DATABASE_DIRECTORY+dbName)){
        Logger::logError("Database with provided name already exists");
        return;
    }

    saveDatabase(dbName);

}

/**
 * @todo Create the function
 */
void Database::useDatabase(const std::vector<std::string>& tokens){

}

bool validateDatabaseName(const std::string& name){
    if(name.length()<4 || name.length()>16)
        return false;
    
    if(name[0]>='0' && name[0]<='9'){
        return false;
    }

    for(int i=0; i<name.length(); i++){
        if((name[i]<'0' || name[i]>'9') && (name[i]<'a' || name[i]>'z') && (name[i]<'A' || name[i]>'Z')){
            return false;
        }
    }

    return true;
}

void saveDatabase(const std::string &dbName){
	try {
		std::filesystem::create_directories(DATABASE_DIRECTORY+dbName);
		std::filesystem::create_directories(DATABASE_DIRECTORY+dbName+"/data"); // To store database data

		// Create tables file
		int fd = creat((DATABASE_DIRECTORY+dbName+"/tables").c_str(),S_IRUSR|S_IWUSR);
		if(DEBUG == true){
			std::cout<<"File Descriptor: "<<fd<<std::endl;
		}
		if(fd<0){
			Logger::logError("Unable to create tables file");
			close(fd);
			// Delete database
			std::filesystem::remove_all(DATABASE_DIRECTORY+dbName);
			return;
		}
		close(fd);

        Logger::logSuccess("Database with name "+dbName+" created");

	} catch(const std::exception& e){
		std::string errorMessage = "unknown error in creating database ";
		Logger::logError(errorMessage);
		if(DEBUG == true){
			std::cout<<e.what();
		}
		std::filesystem::remove_all(DATABASE_DIRECTORY+dbName);
		return;
	}
}