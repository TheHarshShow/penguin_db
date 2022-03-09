#include <string>
#include <filesystem>
#include <map>
#include <string.h>
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
bool loadTables(const std::string& dbName);

static std::string CURRENT_DATABASE = "NUL";
static std::map< std::string, std::vector< std::vector < std::string > > > currentTables;
static std::map< std::string, std::string > tableIds;

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
    tableIds.clear();

    /**
     * @todo LOAD TABLES
     * 
     */
    if(!loadTables(dbName)){
        Logger::logError("Unable to load table metadata");
        return;
    }

    if(DEBUG == true){
        for(auto u:currentTables){
            std::cout << tableIds[u.first] << " ";
            std::cout << u.first << ": ";
            for(int i=0;i<u.second.size();i++){
                for(int j=0;j<u.second[i].size();j++){
                    std::cout << u.second[i][j] << " ";
                }
                std::cout << "| ";
            }
            std::cout << std::endl;
        }
    }

    Logger::logSuccess("current database: "+dbName);

}

std::string Database::getCurrentDatabase(){
    return CURRENT_DATABASE;
}

void Database::cacheTableInfo(const std::string& tableName, const std::vector< std::vector< std::string >>& columns, const std::string& id){
    currentTables[tableName] = columns;
    tableIds[tableName] = id;
}

bool Database::isDatabaseChosen(){
    return (CURRENT_DATABASE!="NUL");
}

bool Database::checkIfTableExists(const std::string& tableName){
    return (currentTables.find(tableName) != currentTables.end());
}

const std::vector< std::vector< std::string > >& Database::getColumnsOfTable(const std::string& tableName){
    return currentTables[tableName];
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

		// Create tables file. Initially it only stores next table id.
		int fd = open((DATABASE_DIRECTORY+dbName+"/tables").c_str(),O_RDWR | O_CREAT,S_IRUSR|S_IWUSR);
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
        uint64_t nextTableId = 1;
        memcpy(WRITE_BUFFER,&nextTableId,8);
        write(fd,WRITE_BUFFER,8);

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

/**
 * @brief Caches a single table line read from the tables file
 * 
 * @param currentLine Line giving table description read from the tables file
 */
void processTableLine(const std::string& currentLine){
    std::string id;
    std::string tableName;

    int i;
    for(i=0; i<currentLine.size(); i++){
        if(currentLine[i]==' '){
            i++;
            break;
        }
        id+=currentLine[i];
    }
    for(; i<currentLine.size(); i++){
        if(currentLine[i]==' '){
            break;
        }
        tableName+=currentLine[i];
    }

    std::vector< std::string > currentColumn;
    std::vector< std::vector< std::string > > columns;
    std::string word;

    for(;i<currentLine.size();i++){
        if(currentLine[i] == '$'){
            // Column end
            currentColumn.push_back(word);
            columns.push_back(currentColumn);
            currentColumn.clear();
            word = "";
        } else if(currentLine[i] == ' '){
            if(word.size()){ 
                currentColumn.push_back(word);
                word = "";
            }
        } else {
            word+=currentLine[i];
        }
    }

    Database::cacheTableInfo(tableName,columns,id);

}

/**
 * @brief Load table metadata of a database into the in memory cache
 * 
 * @param dbName name of a database
 * @return true if it worked
 * @return false if it didn't work
 */
bool loadTables(const std::string& dbName){
    int fd = open((DATABASE_DIRECTORY + dbName + "/tables").c_str(), O_RDONLY);
    if(fd<0){
        close(fd);
        Logger::logError("Unable to load table info");
        return false;
    }
    
    uint32_t prevSeek = 8; // nextId takes 8 bytes.
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
