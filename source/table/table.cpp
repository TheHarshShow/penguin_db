#include <utility>
#include <filesystem>
#include <string.h>
#include <set>
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
bool saveTable(const std::string &tableString, const std::string& tableName, const std::vector< std::vector< std::string > >& columns);
uint32_t getRowSize(const std::vector< std::vector< std::string > > &columns);
std::vector< std::string > getColumnValues(const std::vector<std::string>& tokens, int startIndex, int endIndex);
bool verifyInsertedColumns(const std::vector< std::string >& values, const std::vector< std::vector< std::string > >& columns);

void Table::createTable(const std::vector<std::string>& tokens){
    std::string tableName = tokens[2];

    if(!Database::isDatabaseChosen()){
        Logger::logError("No database chosen.");
        return;
    }

    if(!validateTableName(tableName)){
        Logger::logError("Table name is not valid. Name must be an alphanumeric string between 4 and \
16 characters long (inclusive). Also, the first character cannot be a number.");
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
    
    // First 4 bytes of page represent total used bytes of page
    if(rowSize + 4 > PAGE_SIZE){
        Logger::logError("Every row must fit inside a single page. Change the page size if you want to process larger rows.");
        return;
    }

    std::string tableString = validation.second;
    /**
     * @brief Table name followed by all columns
     * 
     */
    tableString = tableName + tableString;
    if(!saveTable(tableString, tableName, columns)){
        Logger::logError("Unable to write table info");
        return;
    }

    return;
}

void Table::insertIntoTable(const std::vector<std::string>& tokens){
    std::string tableName = tokens[2];
    
    if(!Database::isDatabaseChosen()){
        Logger::logError("No database chosen");
        return;
    }

    if(!Database::checkIfTableExists(tableName)){
        Logger::logError("The specified table doesn't exist");
        return;
    }

    if(tokens[3] != "values"){
        Logger::logError("Currently only ordered exhaustive insert supported");
        return;
    }

    if(tokens[4] !="(" || tokens[tokens.size()-1] != ")"){
        Logger::logError("Parenthesis not found at the correct positions");
        return;
    }

    std::string dbName = Database::getCurrentDatabase();

    const std::vector< std::vector< std::string > >& columns = Database::getColumnsOfTable(tableName);
    std::vector< std::string > columnValues = getColumnValues(tokens, 4, tokens.size()-1);

    if(columns.size() != columnValues.size()){
        Logger::logError("Incorrect number of arguments. Required columns: "+std::to_string(columns.size()));
        return;
    }

    if(!verifyInsertedColumns(columnValues, columns)){
        Logger::logError("Column data type mismatch");
        return;
    }

    int fd = open((DATABASE_DIRECTORY + dbName + "/data/table__" + tableName).c_str(), O_RDWR);
    if(fd < 0){
        Logger::logError("Can't open table data file");
        close(fd);
        return;
    }
    
    readFromFile(fd,TABLE_METADATA_PAGE_BUFFER_A);

    uint64_t totBytes, totPages, nextId;
    memcpy(&totBytes, TABLE_METADATA_PAGE_BUFFER_A, sizeof(totBytes));
    memcpy(&totPages, TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes), sizeof(totPages));
    memcpy(&nextId, TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes) + sizeof(totPages), sizeof(nextId));

    memset(WORKBUFFER_A, 0, PAGE_SIZE);
    memcpy(WORKBUFFER_A, &nextId, sizeof(nextId));

    uint32_t ptr = sizeof(nextId);

    for(int i=0; i<columnValues.size(); i++){
        std::string bytes = getBytesFromValue(columnValues[i], columns[i][1]);
        memcpy(WORKBUFFER_A + ptr, bytes.c_str(), bytes.length());
        ptr+=bytes.length();

        if(DEBUG == true){
            std::cout << "BYTES: " << bytes.length() << std::endl;
        }

    }

    uint32_t rowSize = getRowSize(columns);
    if(ptr != rowSize){
        if(DEBUG == true){
            std::cout << ptr << " " << rowSize << std::endl;
        }
        Logger::logError("Row size doesn't match");
        return;
    }

    // Read last page
    lseek(fd, totPages*PAGE_SIZE, SEEK_SET);
    readFromFile(fd, CURRENT_TABLE_PAGE_BUFFER_A);

    uint32_t totPageBytes;
    memcpy(&totPageBytes, CURRENT_TABLE_PAGE_BUFFER_A, sizeof(totPageBytes));

    if(DEBUG == true){
        std::cout << "Tot Bytes: " << totBytes << " Tot Pages: " << totPages << " Next ID: " << nextId << std::endl;
    }

    if(totPageBytes + rowSize > PAGE_SIZE){
        // We need a new page
        totPageBytes = rowSize;
        memset(CURRENT_TABLE_PAGE_BUFFER_A, 0, PAGE_SIZE);
        memcpy(CURRENT_TABLE_PAGE_BUFFER_A, &totPageBytes, sizeof(totPageBytes));
        memcpy(CURRENT_TABLE_PAGE_BUFFER_A + sizeof(totPageBytes), WORKBUFFER_A, rowSize);
        totPages++;
        nextId++;
        totBytes += rowSize;
    } else {
        for(int i=sizeof(totPageBytes); i + rowSize - 1<PAGE_SIZE; i+=rowSize){
            uint64_t currentId;
            memcpy(&currentId, CURRENT_TABLE_PAGE_BUFFER_A+i, sizeof(currentId));
            if(currentId == 0){
                //Empty position
                memcpy(CURRENT_TABLE_PAGE_BUFFER_A + i, WORKBUFFER_A, rowSize);
                totPageBytes += rowSize;
                nextId++;
                totBytes += rowSize;
                memcpy(CURRENT_TABLE_PAGE_BUFFER_A, &totPageBytes, sizeof(totPageBytes));
                break;
            }
        }
    }

    lseek(fd, totPages*PAGE_SIZE, SEEK_SET);
    writeToFile(fd, CURRENT_TABLE_PAGE_BUFFER_A);

    memcpy(TABLE_METADATA_PAGE_BUFFER_A, &totBytes, sizeof(totBytes));
    memcpy(TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes), &totPages, sizeof(totPages));
    memcpy(TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes) + sizeof(totPages), &nextId, sizeof(nextId));

    lseek(fd, 0, SEEK_SET);
    writeToFile(fd, TABLE_METADATA_PAGE_BUFFER_A);

    Logger::logSuccess("Successfully inserted row");

    std::cout << "Tot Bytes in table: " << totBytes << std::endl;
    std::cout << "Tot Pages in Table: " << totPages << std::endl;
    std::cout << "Next ID: " << nextId << std::endl;

    close(fd);

}

/**
 * @brief Get the column values from an insert command
 * 
 * @param tokens insert command tokens
 * @param startIndex index of first parentheses
 * @param endIndex index of final parenthesis
 * @return std::vector<std::string> list of column values
 */
std::vector< std::string > getColumnValues(const std::vector<std::string>& tokens, int startIndex, int endIndex){
    std::vector< std::string > values;
    for(int i=startIndex+1; i<endIndex; i++){
        if(tokens[i] != ","){
            values.push_back(tokens[i]);
        }
    }
    return values;
}

/**
 * @brief Check if inserted values match column types
 * 
 * @param values insert values
 * @param columns Info on the columns
 * @return true if they match
 * @return false otherwise
 */
bool verifyInsertedColumns(const std::vector< std::string >& values, const std::vector< std::vector< std::string > >& columns){
    if(values.size() != columns.size()){
        return false;
    }
    for(int i=0; i<values.size(); i++){
        if(!matchType(values[i], columns[i][1])){
            return false;
        }
    }
    return true;
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
    std::set< std::string > columnNames;
    for(int i=0;i<columns.size();i++){
        if(columnNames.find(columns[i][0]) != columnNames.end()){
            return std::make_pair(false, "Duplicate column names found");
        }
        columnNames.insert(columns[i][0]);

        // Remove later when adding UNIQUE DEFAULT etc.
        if(columns[i].size()>2){
            return std::make_pair(false, "Column has too many arguments");
        }
        if(!validateColumnName(columns[i][0])){
            return std::make_pair(false, "Column name invalid. Name must an alphanumeric string be \
between 2 and 16 characters (inclusive). Also, the first character cannot be a number.");
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

bool saveTable(const std::string &tableString, const std::string& tableName, const std::vector< std::vector< std::string > >& columns){

    std::string currentDatabase = Database::getCurrentDatabase();

    // Write to table metadata file
    /**
     * @brief Table metadata file format:
     * 1) first 8 bytes is the next table id
     * 2) subsequent lines store table metadata
     */
    int fd = open((DATABASE_DIRECTORY+currentDatabase+"/tables").c_str(), O_RDWR);
    if(DEBUG == true){
        std::cout<<"File Descriptor: "<<fd<<std::endl;
    }
    if(fd<0){
        close(fd);
        return false;
    }

    //Update next id
    read(fd,READ_BUFFER,8);
    uint64_t nextTableId;
    memcpy(&nextTableId, READ_BUFFER, 8);
    
    if(DEBUG == true){
        std::cout << "Next Table ID: " << nextTableId << std::endl;
    }

    std::string _tableString = std::to_string(nextTableId) + " " + tableString;
    /**
     * @brief If the line is too long, it won't fit into the read buffer
     */
    if(tableString.length() > PAGE_SIZE){
        close(fd);
        return false;
    }

    Database::cacheTableInfo(tableName, columns, std::to_string(nextTableId));

    lseek(fd,0,SEEK_END);
    strcpy(WRITE_BUFFER, _tableString.c_str());
    write(fd,WRITE_BUFFER, _tableString.length());

    nextTableId++;
    memcpy(WRITE_BUFFER, &nextTableId, 8);
    lseek(fd,0,SEEK_SET);

    write(fd,WRITE_BUFFER,8);

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

    memcpy(WORKBUFFER_A, &totBytes, sizeof(totBytes));
    memcpy(WORKBUFFER_A + sizeof(totBytes), &totPages, sizeof(totBytes));
    memcpy(WORKBUFFER_A + sizeof(totBytes) + sizeof(totPages), &nextId, sizeof(totBytes));
    writeToFile(fd,WORKBUFFER_A);

    memset(WORKBUFFER_A,0,PAGE_SIZE);
    lseek(fd,PAGE_SIZE,SEEK_SET);
    writeToFile(fd,WORKBUFFER_A);

    close(fd);

    Logger::logSuccess("Successfully created table with name: "+ tableName);

    return true;
}