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
bool saveTable(const std::string &tableString);

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

    std::string tableString = validation.second;
    /**
     * @brief Table name followed by all columns
     * 
     */
    tableString = tableName + tableString;
    if(!saveTable(tableString)){
        Logger::logError("Unable to write table info");
        return;
    } else {
        Database::cacheTableInfo(tableName, columns);

        Logger::logSuccess("Successfully created table with name: "+ tableName);
        return;
    }
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
        if(getTypeFromString(columns[i][1]) == TYPE::STRING){
            /**
             * @brief If string, push length of string to position 2 of vector
             * @todo Make better string parser
             */
            uint32_t stringLength = getStringLength(columns[i][1]);
            columns[i][1] = "string";
            columns[i].insert(columns[i].begin()+2, std::to_string(stringLength));
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

bool saveTable(const std::string &tableString){
    
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

    return true;
}