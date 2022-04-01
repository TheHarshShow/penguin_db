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
// bool loadTables(const std::string& dbName);

static std::string CURRENT_DATABASE = "NUL";

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
    if(tokens.size()!=2){
        Logger::logError("Instruction has incorrect number of arguments");
        return;
    }

    std::string dbName = tokens[1];

    if(!(std::filesystem::exists(DATABASE_DIRECTORY+dbName))){
        Logger::logError("Database with provided name does not exist");
        return;
    }

    CURRENT_DATABASE = dbName;

    Logger::logSuccess("current database: "+dbName);

}

std::string Database::getCurrentDatabase(){
    return CURRENT_DATABASE;
}

bool Database::isDatabaseChosen(){
    return (CURRENT_DATABASE!="NUL");
}

uint64_t Database::getTableId(const std::string& tableName){

    if(!Database::isDatabaseChosen()){
        return 0;
    }

    uint64_t currentPage = 1;

    while(readPage(WORKBUFFER_A, 0, currentPage)){

        uint32_t latestLineStart = 0;

        for(int i=0;i<PAGE_SIZE && WORKBUFFER_A[i] != (char)0;i++){
            if(WORKBUFFER_A[i] == '<') {
                std::string idString;

                int32_t lastSpace = latestLineStart;
                bool matched = true;
                for(int j=latestLineStart; j<i; j++){
                    if(WORKBUFFER_A[j]==' '){
                        lastSpace = j + 1;
                        break;
                    } else {
                        idString += WORKBUFFER_A[j];
                    }
                }

                if(tableName.length() != i-lastSpace){
                    matched = false;
                }

                for(int j=lastSpace; j<i; j++){
                    if(tableName[j - lastSpace] != WORKBUFFER_A[j]){
                        matched = false;
                        break;
                    }
                }
                
                if(matched){
                    if(DEBUG == true){
                        std::cout << "Match found: " << idString << " " << idString.length() << " " << (int)idString[0] << " " << (int)idString[1] << std::endl;
                    }
                    return std::stoll(idString);
                }

                latestLineStart = i+1;
            }
        }
        currentPage++;
    }

    return 0;
}

std::string Database::getTableName(uint64_t tableId){
    
    std::string tableName;

    if(!Database::isDatabaseChosen()){
        return tableName;
    }

    uint64_t currentPage = 1;

    while(readPage(WORKBUFFER_A, 0, currentPage)){

        uint32_t latestLineStart = 0;

        for(int i=0;i<PAGE_SIZE && WORKBUFFER_A[i] != (char)0;i++){
            if(WORKBUFFER_A[i] == '<') {
                std::string idString;

                int32_t lastSpace = latestLineStart;
                bool matched = true;
                for(int j=latestLineStart; j<i; j++){
                    if(WORKBUFFER_A[j]==' '){
                        lastSpace = j + 1;
                        break;
                    } else {
                        idString += WORKBUFFER_A[j];
                    }
                }

                for(int j=lastSpace; j<i; j++){
                    tableName += WORKBUFFER_A[j];
                }

                if(std::stoll(idString) != tableId){
                    tableName.clear();
                    matched = false;
                }
                
                if(matched){
                    if(DEBUG == true){
                        std::cout << "Match found: " << idString << " " << idString.length() << " " << (int)idString[0] << " " << (int)idString[1] << std::endl;
                    }
                    return tableName;
                }

                latestLineStart = i+1;
            }
        }
        currentPage++;
    }

    return tableName;
}

const std::vector< std::vector< std::string > > Database::getColumnsOfTable(uint64_t tableId){
    
    std::vector< std::vector <std::string > > columns;

    if(!Database::isDatabaseChosen()){
        return columns;
    }

    if(!tableId){
        return columns;
    }
    memset(WORKBUFFER_A, 0, PAGE_SIZE);
    if(!readPage(WORKBUFFER_A, tableId, 0)){
        return columns;
    }

    uint32_t dataStart = 3*sizeof(uint64_t);
    uint8_t numSpaces = 0;
    uint32_t lastSpace = dataStart;

    for(int i=dataStart; i < PAGE_SIZE; i++){
        if(WORKBUFFER_A[i] == '<'){
            // process columns
            std::vector< std::string > currentColumn;
            std::string word;
            for(int j=lastSpace; j<i; j++){
                if(WORKBUFFER_A[j] == '$'){
                    //Column end
                    currentColumn.push_back(word);
                    columns.push_back(currentColumn);
                    currentColumn.clear();
                    word = "";
                } else if(WORKBUFFER_A[j] == ' '){
                    if(word.size()){
                        currentColumn.push_back(word);
                        word = "";
                    }
                } else {
                    word+=WORKBUFFER_A[j];
                }
            }
            return columns;
        } else if(WORKBUFFER_A[i] == ' '){
            if(numSpaces == 0){
                lastSpace = i+1;
                numSpaces++;
            } else if(numSpaces == 1){
                lastSpace = i+1;
                numSpaces++;
            }
        }
    }

    return columns;

}

const std::vector< std::vector< std::string > > Database::getColumnsOfTable(const std::string& tableName){
    uint64_t tableId = getTableId(tableName);
    return getColumnsOfTable(tableId);
}

bool validateDatabaseName(const std::string& name){
    if(name.length()<4 || name.length()>16){
        return false;
    }
    
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
		int fd = open((DATABASE_DIRECTORY + dbName + "/tables").c_str(),O_RDWR | O_CREAT,S_IRUSR|S_IWUSR);
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
        uint64_t totPages = 1;

        memset(WORKBUFFER_A, 0, PAGE_SIZE);
        memcpy(WORKBUFFER_A ,&nextTableId, sizeof(nextTableId));
        memcpy(WORKBUFFER_A + sizeof(nextTableId),&totPages, sizeof(totPages));
        writeToFile(fd, WORKBUFFER_A);

        lseek(fd,PAGE_SIZE,SEEK_SET);
        memset(WORKBUFFER_A, 0, PAGE_SIZE);
        writeToFile(fd, WORKBUFFER_A);

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
