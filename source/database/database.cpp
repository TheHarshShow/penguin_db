#include <string>
#include <filesystem>
#include <map>
#include <algorithm>
#include "database.h"
#include "../properties.h"
#include "../logger/logger.h"
#include "../buffers/buffers.h"

// File system calls
#include <fcntl.h>
#include <unistd.h>

bool validateDatabaseName(const std::string& name);
void saveDatabase(const std::string &dbName);

static std::string CURRENT_DATABASE = "NUL";
static std::map< std::string, std::vector< std::vector < std::string > > > currentTables;

/**
 * @brief Caches a single table line read from the tables file
 * 
 * @param currentLine Line giving table description read from the tables file
 */
void processTableLine(const std::string& currentLine){
    
}

bool loadTables(const std::string& dbName){
    int fd = open((DATABASE_DIRECTORY + dbName + "/tables").c_str(), O_RDONLY);
    if(fd<0){
        close(fd);
        Logger::logError("Unable to load table info");
        return false;
    }
    
    uint32_t prevSeek = 0;
    uint32_t totRead = 1; // Arbitrary definition

    while(totRead){

        uint32_t additionalSeek = 0;

        lseek(fd, prevSeek, SEEK_SET);
        totRead = read(fd,READ_BUFFER,PAGE_SIZE);

        std::string currentLine;
        for(int i=0; i < std::min(totRead,PAGE_SIZE); i++){
            if(READ_BUFFER[i] != '\n'){
                currentLine+=READ_BUFFER[i];
            } else {

                processTableLine(currentLine);

                additionalSeek = i+1;
                currentLine = "";
            }
        }

        prevSeek += additionalSeek;

    }

    close(fd);
    return true;
}

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

void Database::useDatabase(const std::vector<std::string>& tokens){
    if(tokens.size()!=3){
        Logger::logError("Instruction has incorrect number of arguments");
        return;
    }

    std::string dbName = tokens[2];

    if(!(std::filesystem::exists(DATABASE_DIRECTORY+dbName))){
        Logger::logError("Database with provided name does not exist");
        return;
    }

    CURRENT_DATABASE = dbName;
    currentTables.clear();

    /**
     * @todo LOAD TABLES
     * 
     */
    loadTables(dbName);

    Logger::logSuccess("current database: "+dbName);

}

std::string Database::getCurrentDatabase(){
    return CURRENT_DATABASE;
}

void Database::cacheTableInfo(const std::string& tableName, const std::vector< std::vector< std::string >>& columns){
    currentTables[tableName] = columns;
}

bool Database::isDatabaseChosen(){
    return (CURRENT_DATABASE!="NUL");
}

bool Database::checkIfTableExists(const std::string& tableName){
    return (currentTables.find(tableName) != currentTables.end());
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