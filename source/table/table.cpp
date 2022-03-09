#include <utility>
#include <filesystem>
#include <string.h>
#include "table.h"
#include "../database/database.h"
#include "../properties.h"
#include "../logger/logger.h"
#include "../type/type.h"
#include "../buffers/buffers.h"

// File system calls
#include <fcntl.h>
#include <unistd.h>

bool validateTableName(const std::string& name);
std::pair<bool, std::string> validateAndProcessColumns(std::vector< std::vector< std::string > >& columns);
bool validateColumnName(const std::string& name);
bool saveTable(const std::string &tableString, const std::string& tableName);
uint32_t getRowSize(const std::vector< std::vector< std::string > > &columns);

void Table::createTable(const std::vector<std::string>& tokens){
    std::string tableName = tokens[2];

    if(!Database::isDatabaseChosen()){
        Logger::logError("No database chosen.");
        return;
    }

    if(!validateTableName(tableName)){
        Logger::logError("Table name is not valid. Name must be an alphanumeric string between 4 and 16 characters long (inclusive). Also, the first character cannot be a number.");
        return;
    }

    if(Database::checkIfTableExists(tableName)){
        Logger::logError("Table with given name already exists");
        return;
    }

    if(tokens.size() < 5){
        Logger::logError("Too few tokens in create table instruction");
        return;
    }
    
    if(tokens[3]!="(" && tokens[4]!=")"){
        Logger::logError("Parentheses not found at the correct positions");
        return;
    }

    std::vector< std::vector< std::string > > columns;

    std::vector< std::string > currentColumn;
    for(int i=4;i<tokens.size()-1;i++){
        if(tokens[i] != ","){
            currentColumn.push_back(tokens[i]);
        } else {
            if(currentColumn.size() > 0){
                columns.push_back(currentColumn);
                currentColumn.clear();
            } else {
                Logger::logError("Empty columns are not allowed");
                return;
            }
        }
    }
    
    if(currentColumn.size()){
        columns.push_back(currentColumn);
    } else {
        Logger::logError("Empty columns are not allowed");
        return;
    }

    std::pair< bool, std::string > validation = validateAndProcessColumns(columns);

    if(!validation.first){
        Logger::logError(validation.second);
        return;
    }

    uint32_t rowSize = getRowSize(columns);
    if(DEBUG == true){
        std::cout << "Row size: " << rowSize << std::endl;
    }

    if(rowSize > PAGE_SIZE){
        Logger::logError("Every row must fit inside a single page. Change the page size if you want to process larger rows.");
        return;
    }

    std::string tableString = validation.second;
    /**
     * @brief Table name followed by all columns
     * 
     */
    tableString = tableName + tableString;
    if(!saveTable(tableString, tableName)){
        Logger::logError("Unable to write table info");
        return;
    }
    Database::cacheTableInfo(tableName, columns);


    return;
}

bool validateTableName(const std::string& name){
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

std::pair<bool, std::string> validateAndProcessColumns(std::vector< std::vector< std::string > >& columns){
    for(int i=0;i<columns.size();i++){
        // Remove later when adding UNIQUE DEFAULT etc.
        if(columns[i].size()>2){
            return std::make_pair(false, "Column has too many arguments");
        }
        if(!validateColumnName(columns[i][0])){
            return std::make_pair(false, "Column name invalid. Name must an alphanumeric string be between 2 and 16 characters (inclusive). Also, the first character cannot be a number.");
        }
        if(getTypeFromString(columns[i][1]) == TYPE::UNSUPPORTED){
            return std::make_pair(false, "Column type not supported.");
        }
    }
    std::string tableString;
    for(int i=0;i<columns.size();i++){
        for(int j=0;j<columns[i].size();j++){
            tableString+=" "+columns[i][j];
        }
        tableString+="$";
    }

    return std::make_pair(true, tableString+'\n');
}

uint32_t getRowSize(const std::vector< std::vector< std::string > > &columns){
    uint32_t rowSize = 8; // ID field has 8 bytes
    for(int i=0;i<columns.size();i++){
        rowSize += getTypeSize(columns[i][1]);
    }
    return rowSize;
}

bool validateColumnName(const std::string& name){
    if(name.length()<2 || name.length()>16)
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

bool saveTable(const std::string &tableString, const std::string& tableName){
    
    /**
     * @brief If the line is too long, it won't fit into the read buffer
     */
    if(tableString.length() > PAGE_SIZE){
        return false;
    }

    std::string currentDatabase = Database::getCurrentDatabase();

    // Create tables file
    int fd = open((DATABASE_DIRECTORY+currentDatabase+"/tables").c_str(), O_WRONLY | O_APPEND);
    if(DEBUG == true){
        std::cout<<"File Descriptor: "<<fd<<std::endl;
    }
    if(fd<0){
        close(fd);
        return false;
    }

    strcpy(WRITE_BUFFER, tableString.c_str());

    write(fd,WRITE_BUFFER,tableString.length());

    close(fd);

    /**
     * @brief Write initial data to table file
     * Table structure:
     * 1) One page completely reserved for metadata!
     * 2) First 8 bytes stores total number of bytes occupied. Second 8 bytes stores total number of pages allocated so far. Third 8 bytes store next unique ID
     * 2) The rest of the pages store data
     */
    std::string dbName = Database::getCurrentDatabase();
    fd = open((DATABASE_DIRECTORY + dbName + "/data/table__"+tableName).c_str(), O_CREAT | O_WRONLY, S_IRUSR|S_IWUSR);
    if(fd < 0){
        Logger::logError("Unable to create table properly");
        close(fd);
        return false;
    }

    memset(WRITE_BUFFER,0,PAGE_SIZE+1);
    uint64_t totBytes = 0;
    uint64_t totPages = 1;
    uint64_t nextId = 1;
    memcpy(WRITE_BUFFER, &totBytes, sizeof(totBytes));
    memcpy(WRITE_BUFFER+sizeof(totBytes), &totPages, sizeof(totBytes));
    memcpy(WRITE_BUFFER + sizeof(totBytes) + sizeof(totPages), &nextId, sizeof(totBytes));

    write(fd,WRITE_BUFFER,PAGE_SIZE);
    memset(WRITE_BUFFER,0,PAGE_SIZE);

    lseek(fd,PAGE_SIZE,SEEK_SET);
    write(fd,WRITE_BUFFER,PAGE_SIZE);

    close(fd);
    Logger::logSuccess("Successfully created table with name: "+ tableName);

    return true;
}