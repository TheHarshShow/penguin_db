#include <utility>
#include <filesystem>
#include <string.h>
#include <set>
#include <map>
#include "table.h"
#include "../database/database.h"
#include "../properties.h"
#include "../logger/logger.h"
#include "../type/type.h"
#include "../buffers/buffers.h"
#include "../formatter/formatter.h"

// File system calls
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

inline bool validateTableName(const std::string& name);
std::pair<bool, std::string> validateAndProcessColumns(std::vector< std::vector< std::string > >& columns);
bool validateColumnName(const std::string& name);
bool saveTableWithId(uint64_t tableId, const std::string& tableString);
bool saveTableWithName(const std::string& tableName, const std::string &tableString);
uint32_t getRowSize(const std::vector< std::vector< std::string > > &columns);
std::vector< std::string > getColumnValues(const std::vector<std::string>& tokens, int startIndex, int endIndex);
bool verifyInsertedColumns(const std::vector< std::string >& values, const std::vector< std::vector< std::string > >& columns);
uint32_t loadRowBytes(const std::vector< std::vector< std::string > >& columns, const std::vector< std::string >& columnValues);
bool saveRow(uint64_t tableId, uint32_t rowSize, char* BUFFER = WORKBUFFER_A);
int verifyConditions(char rowBuffer[], const std::vector< std::vector< std::string > >& columns,const std::vector<condition>& conditions, uint32_t rowSize);
void printQuery(uint64_t fileId, bool atLeastOneMatch);
condition getCondition(const std::vector< std::string >& currentComparison);
bool updateRow(char BUFFER[], const std::vector< condition >& assignments, const std::vector< std::vector< std::string > >& columns);
void consolidate(uint64_t fileId, uint32_t rowSize);

COMPARISON stringToComparison(const std::string& comp){
    if(comp == "=="){
        return COMPARISON::EQUAL;
    } else if(comp == ">"){
        return COMPARISON::GREATER;    
    } else if(comp == "<"){
        return COMPARISON::LESS;
    } else if(comp == "<="){
        return COMPARISON::L_EQUAL;
    } else if(comp == ">="){
        return COMPARISON::G_EQUAL;
    }
    return COMPARISON::INVALID;
}

bool isComparisonValid(COMPARISON expected, COMPARISON obtained){
    if(expected == COMPARISON::G_EQUAL){
        if(obtained == COMPARISON::GREATER || obtained == COMPARISON::EQUAL){
            return true;
        }
        return false;
    }
    if(expected == COMPARISON::L_EQUAL){
        if(obtained == COMPARISON::LESS || obtained == COMPARISON::EQUAL){
            return true;
        }
        return false;
    }
    return (expected == obtained);
}

uint64_t timeSinceEpochMillisec() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

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

    if(Database::getTableId(tableName)){
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
        currentColumn.clear();
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
    if(!saveTableWithName(tableName, tableString)){
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

    if(!Database::getTableId(tableName)){
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

    uint64_t tableId = Database::getTableId(tableName);
    // Load into WORKBUFFER_A
    uint32_t rowSize = loadRowBytes(columns, columnValues);
    if(rowSize == 0 || tableId == 0){
        Logger::logError("Error in loading row info into buffer");
        return;
    }

    bool saveInfo = saveRow(tableId, rowSize);

    if(saveInfo){
        Logger::logError("Error in saving row info to table");
        return;
    }

    Logger::logSuccess("Successfully inserted row");

}

void Table::handleSelect(const std::vector<std::string>& tokens){
    /**
     * @todo Add support for join etc.
     */
    std::string tableName = tokens[3];

    if(!Database::isDatabaseChosen()){
        Logger::logError("Databse not selected");
        return;
    }

    if(!Database::getTableId(tableName)){
        Logger::logError("Table does not exist");
        return;
    }

    std::vector< std::vector< std::string > > columns = Database::getColumnsOfTable(tableName);
    uint32_t rowSize = getRowSize(columns);

    std::string dbName = Database::getCurrentDatabase();

    std::vector<condition> conditions;

    if(tokens.size()>4){
        if(tokens[4] == "where"){
            std::vector< std::string > currentCondition;
            for(int i=5; i<tokens.size(); i++){
                if(tokens[i] == "and" || tokens[i] == "&&"){
                    if(currentCondition.size() == 0){
                        Logger::logError("empty condition found");
                        return;
                    }
                    condition cd = getCondition(currentCondition);
                    currentCondition.clear();
                    if(cd.operation == COMPARISON::INVALID){
                        Logger::logError("Invalid condition provided");
                        return;
                    }
                    conditions.push_back(cd);
                } else {
                    currentCondition.push_back(tokens[i]);
                }
            }
            if(!currentCondition.size()){
                Logger::logError("Empty condition provided");
                return;
            }
            condition cd = getCondition(currentCondition);
            if(cd.operation == COMPARISON::INVALID){
                Logger::logError("Invalid condition provided");
                return;
            }
            conditions.push_back(cd);
        } else {
            Logger::logError("Only where clause supported now");
            return;
        }
    }

    uint64_t timestamp = timeSinceEpochMillisec(); // Query timestamp
    uint64_t queryFileId = ( ( timestamp % ((uint64_t)1 << LOG_MAX_TABLES) ) + ( (uint64_t)1 << LOG_MAX_TABLES) );

    // Maximum name length is 16
    std::string queryTableName = "query"+std::to_string(queryFileId%1000000000);

    // Table string for query table
    std::string queryTableString = std::to_string(queryFileId) + " " + queryTableName + validateAndProcessColumns(columns).second;
    saveTableWithId(queryFileId, queryTableString);

    memset(WORKBUFFER_B, 0, PAGE_SIZE);

    uint64_t tableId = Database::getTableId(tableName);

    readPage(TABLE_METADATA_PAGE_BUFFER_A, tableId, 0);
    uint64_t totPages;
    memcpy(&totPages, TABLE_METADATA_PAGE_BUFFER_A + sizeof(uint64_t), sizeof(totPages));

    bool atLeastOneMatch = false;

    for(uint64_t i=1; i<=totPages; i++){
        readPage(CURRENT_TABLE_PAGE_BUFFER_A, tableId, i);

        for(uint32_t j=4; j+rowSize-1<PAGE_SIZE; j+=rowSize){
            uint64_t currentId;
            memcpy(&currentId, CURRENT_TABLE_PAGE_BUFFER_A+j, sizeof(currentId));

            if(currentId != 0){
                // Non empty row
                memcpy(WORKBUFFER_A, CURRENT_TABLE_PAGE_BUFFER_A+j, rowSize);
                int check = verifyConditions(WORKBUFFER_A, columns, conditions, rowSize);

                if(check == 1){
                    
                    atLeastOneMatch = true;
                    memcpy(WORKBUFFER_C, WORKBUFFER_A, rowSize);
                    
                    // Id will be set by saveRow
                    memset(WORKBUFFER_C, 0, sizeof(currentId));
                    saveRow(queryFileId, rowSize, WORKBUFFER_C);

                    // if(ptr + queryRowSize - 1 >= PAGE_SIZE){
                        
                    //     if(!writeToPage(WORKBUFFER_B, queryFileId, queryCurrentPage,  O_CREAT, S_IRUSR|S_IWUSR)){
                    //         Logger::logError("Error in writing to query file");
                    //         return;
                    //     }
                    //     ptr=0;
                    //     memset(WORKBUFFER_B, 0, PAGE_SIZE);
                    //     queryCurrentPage++;
                    // }
                    // memcpy(WORKBUFFER_B + ptr, WORKBUFFER_A, queryRowSize);
                    // ptr += queryRowSize;
                } else if(check == -1){
                    Logger::logError("Comparisons not in correct format");
                    return;
                }
            }
        }
    }

    if(DEBUG == true){
        std::cout << "At least one match: " << atLeastOneMatch << std::endl;
    }

    // if(ptr){
    //     writeToPage(WORKBUFFER_B, queryFileId, queryCurrentPage, O_CREAT, S_IRUSR|S_IWUSR);
    //     ptr=0;
    //     memset(WORKBUFFER_B, 0, PAGE_SIZE);
    //     queryCurrentPage++;
    // }

    printQuery(queryFileId, atLeastOneMatch);

}

void Table::handleUpdateTable(const std::vector<std::string>& tokens){
    if(!Database::isDatabaseChosen()){
        Logger::logError("Database is not selected");
        return;
    }

    std::string tableName = tokens[1];
    uint64_t tableId;

    if(!(tableId = Database::getTableId(tableName))){
        Logger::logError("Table with given name does not exist");
        return;
    }

    std::vector< std::string > currentAssignment;
    std::vector< condition > assignments;
    std::vector< condition > conditions;
    std::string word;

    int i;
    for(i=3; i<tokens.size(); i++){
        if(tokens[i] == "where"){
            break;
        }
        if(tokens[i] == "and"  || tokens[i] == "&&"){
            if(currentAssignment.size() != 3 || currentAssignment[1] != "="){
                Logger::logError("Error in one of the assignment syntaxes");
                return;
            }
            condition assignment(currentAssignment[0], COMPARISON::ASSIGNMENT, currentAssignment[2]);
            assignments.push_back(assignment);
            currentAssignment.clear();
        } else {
            currentAssignment.push_back(tokens[i]);
        }
    }

    if(currentAssignment.size() == 0){
        Logger::logError("An assignment is missing");
        return;
    }

    if(currentAssignment.size() != 3 || currentAssignment[1] != "="){
        Logger::logError("Error in one of the assignment syntaxes");
        return;
    }
    condition assignment(currentAssignment[0], COMPARISON::ASSIGNMENT, currentAssignment[2]);
    assignments.push_back(assignment);
    currentAssignment.clear();


    std::vector< std::string > currentCondition;
    if(i++ != tokens.size()){
        //where clause; process conditions;
        for(;i<tokens.size();i++){
            if(tokens[i] == "and" || tokens[i] == "&&"){
                condition cd = getCondition(currentCondition);
                if(cd.operation == COMPARISON::INVALID){
                    Logger::logError("Invalid condition");
                    return;
                }
                conditions.push_back(cd);
                currentCondition.clear();
            } else {
                currentCondition.push_back(tokens[i]);
            }
        }
        if(currentCondition.size() == 0){
            Logger::logError("condition missing");
            return;
        }
        condition cd = getCondition(currentCondition);
        if(cd.operation == COMPARISON::INVALID){
            Logger::logError("Invalid condition");
            return;
        }
        conditions.push_back(cd);
        currentCondition.clear();
    }

    std::vector< std::vector< std::string > > columns = Database::getColumnsOfTable(tableName);
    uint32_t rowSize = getRowSize(columns);
    
    readPage(TABLE_METADATA_PAGE_BUFFER_A, tableId, 0);
    uint64_t totalPages;
    memcpy(&totalPages, TABLE_METADATA_PAGE_BUFFER_A + sizeof(totalPages), sizeof(totalPages));

    for(uint64_t i=1; i<=totalPages; i++){
        readPage(CURRENT_TABLE_PAGE_BUFFER_A, tableId, i);
        bool pageChanged = false;

        for(uint32_t j = 4; j + rowSize - 1 < PAGE_SIZE; j+=rowSize){
            uint64_t currentId;
            memcpy(&currentId, CURRENT_TABLE_PAGE_BUFFER_A+j, sizeof(currentId));
            if(currentId != 0){
                memset(WORKBUFFER_A, 0 , PAGE_SIZE);
                memcpy(WORKBUFFER_A, CURRENT_TABLE_PAGE_BUFFER_A + j, rowSize);
                int check = verifyConditions(WORKBUFFER_A, columns, conditions, rowSize);
                if(check == 1){
                    //Update row
                    if(!updateRow(WORKBUFFER_A, assignments, columns)){
                        Logger::logError("Column name not found or value type mismatch");
                        return;
                    }
                    memcpy(CURRENT_TABLE_PAGE_BUFFER_A + j, WORKBUFFER_A, rowSize);
                    pageChanged = true;
                } else if(check == -1) {
                    Logger::logError("Comparisons not in correct format");
                    return;
                }
            }
        }

        if(pageChanged){
            if(!writeToPage(CURRENT_TABLE_PAGE_BUFFER_A, tableId, i)){
                Logger::logError("Error in writing changes to disk");
                return;
            }
        }
    }
    Logger::logSuccess("Successfully updated table");
}

void Table::handleDeleteRow(const std::vector<std::string>& tokens){
    if(!Database::isDatabaseChosen()){
        Logger::logError("No database chosen");
        return;
    }
    std::string tableName = tokens[2];
    uint64_t tableId;
    if((tableId = Database::getTableId(tableName)) == 0){
        Logger::logError("Table with name "+tableName +" does not exist");
        return;
    }
    if(tokens[3] != "where"){
        Logger::logError("conditions missing");
        return;
    }

    std::vector< condition > conditions;
    std::vector< std::string > currentCondition;
    for(int i=4; i<tokens.size();i++){
        if(tokens[i] == "and" || tokens[i] == "&&"){
            if(currentCondition.size()==0){
                Logger::logError("empty condition provided");
                return;
            }
            condition cd = getCondition(currentCondition);
            currentCondition.clear();
            if(cd.operation == COMPARISON::INVALID){
                Logger::logDebug("Invalid condition provided");
                return;
            }
            conditions.push_back(cd);
        } else {
            currentCondition.push_back(tokens[i]);
        }
    }
    if(!currentCondition.size()){
        Logger::logError("Empty condition provided");
        return;
    }
    condition cd = getCondition(currentCondition);
    conditions.push_back(cd);
    currentCondition.clear();

    std::vector< std::vector< std::string > > columns = Database::getColumnsOfTable(tableName);
    uint32_t rowSize = getRowSize(columns);

    readPage(TABLE_METADATA_PAGE_BUFFER_A, tableId, 0);
    uint64_t totBytes, totalPages;
    memcpy(&totBytes, TABLE_METADATA_PAGE_BUFFER_A, sizeof(totBytes));
    memcpy(&totalPages, TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes), sizeof(totalPages));

    for(uint64_t i=1; i<=totalPages; i++){
        readPage(CURRENT_TABLE_PAGE_BUFFER_A, tableId, i);
        bool pageChanged = false;

        for(uint32_t j=4; j+rowSize-1<PAGE_SIZE; j+=rowSize){
            uint64_t currentId;
            memcpy(&currentId, CURRENT_TABLE_PAGE_BUFFER_A+j, sizeof(currentId));
            if(currentId != 0){
                // Row not empty
                memset(WORKBUFFER_A, 0, PAGE_SIZE);
                memcpy(WORKBUFFER_A, CURRENT_TABLE_PAGE_BUFFER_A+j, rowSize);
                int check = verifyConditions(WORKBUFFER_A, columns, conditions, rowSize);
                if(check == 1){
                    //Delete row
                    memset(WORKBUFFER_A,0,rowSize);
                    memcpy(CURRENT_TABLE_PAGE_BUFFER_A+j,WORKBUFFER_A,rowSize);
                    totBytes -= rowSize;
                    pageChanged = true;
                } else if(check == -1) {
                    Logger::logError("Comparisons not in correct format");
                    return;
                }
            }
        }

        if(pageChanged){
            if(!writeToPage(CURRENT_TABLE_PAGE_BUFFER_A, tableId, i)){
                Logger::logError("Error in writing changes to disk");
                return;
            }
        }
    }
    
    memcpy(TABLE_METADATA_PAGE_BUFFER_A, &totBytes, sizeof(totBytes));

    if(!writeToPage(TABLE_METADATA_PAGE_BUFFER_A, tableId, 0)){
        Logger::logError("Error in writing back updated metadata");
        return;
    }

    consolidate(tableId, rowSize);

    if(DEBUG == true){
        readPage(TABLE_METADATA_PAGE_BUFFER_A, tableId, 0);
        uint64_t totBytesEnd;
        memcpy(&totBytesEnd, TABLE_METADATA_PAGE_BUFFER_A, sizeof(totBytesEnd));
        std::cout << "Tot bytes after delete: " << totBytesEnd << std::endl;
    }

    Logger::logSuccess("Successfully updated table");

}

condition getCondition(const std::vector< std::string >& currentComparison){
    if(currentComparison.size()==3 && currentComparison[1] == ">"){
        return condition(currentComparison[0], COMPARISON::GREATER, currentComparison[2]);
    } else if(currentComparison.size()==3 && currentComparison[1] == "<"){
        return condition(currentComparison[0], COMPARISON::LESS, currentComparison[2]);
    } else if(currentComparison.size()==4 && currentComparison[1] == "=" && currentComparison[2] == "="){
        return condition(currentComparison[0], COMPARISON::EQUAL, currentComparison[3]);
    } else if(currentComparison.size()==4 && currentComparison[1] == ">" && currentComparison[2] == "="){
        return condition(currentComparison[0], COMPARISON::G_EQUAL, currentComparison[3]);
    } else if(currentComparison.size()==4 && currentComparison[1] == "<" && currentComparison[2] == "="){
        return condition(currentComparison[0], COMPARISON::L_EQUAL, currentComparison[3]);
    } else {
        return condition("",COMPARISON::INVALID,"");
    }
}

bool updateRow(char BUFFER[], const std::vector< condition >& assignments, const std::vector< std::vector< std::string > >& columns){
    std::map<std::string, uint32_t> offsets;
    std::map<std::string, uint32_t> sizes;
    std::map<std::string, std::string> types;

    uint32_t offset = 8;
    for(int i=0;i<columns.size();i++){
        types[columns[i][0]] = columns[i][1];
        offsets[columns[i][0]] = offset;
        uint32_t sz = getTypeSize(columns[i][1]);
        sizes[columns[i][0]] = sz;

        offset += sz;
    }

    for(int i=0;i<assignments.size();i++){
        if(offsets.find(assignments[i].columnName) == offsets.end()){
            return false;
        }
        std::string type = types[assignments[i].columnName];
        if(!matchType(assignments[i].value, type)){
            return false;
        }
        uint32_t sz = sizes[assignments[i].columnName];
        uint32_t offset = offsets[assignments[i].columnName];
        std::string bytes = getBytesFromValue(assignments[i].value, type);
        memcpy(BUFFER + offset, bytes.c_str(), sz);
    }
    return true;
}

int verifyConditions(char rowBuffer[], const std::vector< std::vector< std::string > >& columns,const std::vector<condition>& conditions, uint32_t rowSize){
    std::map<std::string, uint32_t> offsets;
    std::map<std::string, std::string> types;
    std::map<std::string, uint32_t> sizes;

    uint32_t offset = 8;
    for(int i=0; i<columns.size(); i++){
        types[columns[i][0]] = columns[i][1];
        offsets[columns[i][0]] = offset;
        uint32_t sz = getTypeSize(columns[i][1]);
        sizes[columns[i][0]] = sz;

        offset += sz;
    }

    for(int i=0; i<conditions.size(); i++){
        if(offsets.find(conditions[i].columnName) == offsets.end()){
            return -1;
        }
        std::string lVal;
        uint32_t offset = offsets[conditions[i].columnName];
        std::string type = types[conditions[i].columnName];
        uint32_t sz = sizes[conditions[i].columnName];

        lVal = getValueFromBytes(rowBuffer, type, offset, offset + sz);

        COMPARISON compResult = getCompResult(lVal ,conditions[i].value, type);
        if(compResult == COMPARISON::INVALID){
            return -1;
        }

        if(!isComparisonValid(conditions[i].operation, compResult)){
            return 0;
        }
    }
    return 1;
}


COMPARISON getCompResult(std::string lVal, std::string rVal, std::string type){
    if(!matchType(lVal, type) || !matchType(rVal, type)){
        return COMPARISON::INVALID;
    }
    TYPE _type = getTypeFromString(type);

    switch(_type){
        case TYPE::INT:
            if(stoll(lVal) > stoll(rVal)){
                return COMPARISON::GREATER;
            } else if(stoll(lVal) < stoll(rVal)){
                return COMPARISON::LESS;
            } else if(stoll(lVal) == stoll(rVal)){
                return COMPARISON::EQUAL;
            }
        case TYPE::FLOAT:
            if(stod(lVal) > stod(rVal)){
                return COMPARISON::GREATER;
            } else if(stod(lVal) < stod(rVal)){
                return COMPARISON::LESS;
            } else if(stod(lVal) == stod(rVal)){
                return COMPARISON::EQUAL;
            }
        case TYPE::CHAR:
            if(lVal[1] > rVal[1]){
                return COMPARISON::GREATER;
            } else if(lVal[1] < rVal[0]){
                return COMPARISON::LESS;
            } else if(lVal[1] == rVal[0]){
                return COMPARISON::EQUAL;
            }
        case TYPE::STRING:
            for(int i=0;i<std::min(lVal.size(), rVal.size()); i++){
                if(lVal[i] > rVal[i]){
                    return COMPARISON::GREATER;
                } else if(lVal[i] < rVal[i]){
                    return COMPARISON::LESS;
                }
            }
            return COMPARISON::EQUAL;
        default:
            return COMPARISON::INVALID;
    }

}

void printQuery(uint64_t fileId, bool atLeastOneMatch){

    std::vector< std::vector< std::string > > columns = Database::getColumnsOfTable(fileId);
    uint32_t rowSize = getRowSize(columns);

    std::cout << Formatter::bold_on;
    for(int i=0; i < columns.size(); i++){
        std::cout << std::setw(20) << columns[i][0];
    }
    std::cout << Formatter::off << '\n';

    if(!atLeastOneMatch){
        return;
    }

    memset(WORKBUFFER_C, 0, PAGE_SIZE);
    readPage(TABLE_METADATA_PAGE_BUFFER_A, fileId, 0);
    uint64_t totPages;
    memcpy(&totPages, TABLE_METADATA_PAGE_BUFFER_A + sizeof(uint64_t), sizeof(totPages));

    uint32_t bytesRead;

    for(uint64_t currentPage = 1; currentPage <= totPages; currentPage++){
        if((bytesRead = readPage(WORKBUFFER_C, fileId, currentPage)) == 0){
            Logger::logError("Error in reading from query page");
            return;
        }

        for(int i=sizeof(uint32_t); i<bytesRead; i+=rowSize){
            uint64_t currentId;
            memcpy(&currentId, WORKBUFFER_C+i, 8);
            if(currentId){
                // Row not empty. Process row
                uint32_t offset = 8;
                for(int j=0; j<columns.size();j++){
                    uint32_t sz = getTypeSize(columns[j][1]);
                    std::string printVal = getValueFromBytes(WORKBUFFER_C, columns[j][1], i + offset, i + offset + sz);
                    offset += sz;
                    std::cout << std::setw(20) << printVal ;
                }
                std::cout << '\n';
            }
        }
    }
}

uint32_t loadRowBytes(const std::vector< std::vector< std::string > >& columns, const std::vector< std::string >& columnValues){
    memset(WORKBUFFER_A, 0, PAGE_SIZE);

    // First 8 bytes are reserved for nextId
    uint32_t ptr = sizeof(uint64_t);

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
        return 0;
    }

    return rowSize;
}

bool saveRow(uint64_t tableId, uint32_t rowSize, char* BUFFER){
    std::string dbName = Database::getCurrentDatabase();
    
    readPage(TABLE_METADATA_PAGE_BUFFER_A, tableId, 0);

    uint64_t totBytes, totPages, nextId;
    memcpy(&totBytes, TABLE_METADATA_PAGE_BUFFER_A, sizeof(totBytes));
    memcpy(&totPages, TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes), sizeof(totPages));
    memcpy(&nextId, TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes) + sizeof(totPages), sizeof(nextId));

    // Add ID to loaded row
    memcpy(BUFFER, &nextId, sizeof(nextId));

    // Read last page
    readPage(CURRENT_TABLE_PAGE_BUFFER_A, tableId, totPages);

    uint32_t totPageBytes;
    memcpy(&totPageBytes, CURRENT_TABLE_PAGE_BUFFER_A, sizeof(totPageBytes));

    if(DEBUG == true){
        std::cout << "Tot Bytes: " << totBytes << " Tot Pages: " << totPages << " Next ID: " << nextId << std::endl;
    }

    if(totPageBytes + rowSize + sizeof(totPageBytes) > PAGE_SIZE){
        // We need a new page
        totPageBytes = rowSize;
        memset(CURRENT_TABLE_PAGE_BUFFER_A, 0, PAGE_SIZE);
        memcpy(CURRENT_TABLE_PAGE_BUFFER_A, &totPageBytes, sizeof(totPageBytes));
        memcpy(CURRENT_TABLE_PAGE_BUFFER_A + sizeof(totPageBytes), BUFFER, rowSize);
        totPages++;
        nextId++;
        totBytes += rowSize;
    } else {
        for(int i=sizeof(totPageBytes); i + rowSize - 1<PAGE_SIZE; i+=rowSize){
            uint64_t currentId;
            memcpy(&currentId, CURRENT_TABLE_PAGE_BUFFER_A+i, sizeof(currentId));
            if(currentId == 0){
                //Empty position
                memcpy(CURRENT_TABLE_PAGE_BUFFER_A + i, BUFFER, rowSize);
                totPageBytes += rowSize;
                nextId++;
                totBytes += rowSize;
                memcpy(CURRENT_TABLE_PAGE_BUFFER_A, &totPageBytes, sizeof(totPageBytes));
                break;
            }
        }
    }

    writeToPage(CURRENT_TABLE_PAGE_BUFFER_A, tableId, totPages);

    memcpy(TABLE_METADATA_PAGE_BUFFER_A, &totBytes, sizeof(totBytes));
    memcpy(TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes), &totPages, sizeof(totPages));
    memcpy(TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes) + sizeof(totPages), &nextId, sizeof(nextId));

    writeToPage(TABLE_METADATA_PAGE_BUFFER_A, tableId, 0);

    std::cout << "Tot Bytes in table: " << totBytes << std::endl;
    std::cout << "Tot Pages in Table: " << totPages << std::endl;
    std::cout << "Next ID: " << nextId << std::endl;

    return false;
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

inline bool validateTableName(const std::string& name){
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

    return std::make_pair(true, tableString+'<');
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

/**
 * @brief Write initial data to table file
 * Table structure:
 * 1) One page completely reserved for metadata!
 * 2) First 8 bytes stores total number of bytes occupied. Second 8 bytes stores total number of pages allocated so far. Third 8 bytes store next unique ID
 * 2) The rest of the pages store data
 */
bool saveTableWithId(uint64_t tableId, const std::string& tableString){
    
    memset(WORKBUFFER_A,0,PAGE_SIZE+1);
    uint64_t totBytes = 0;
    uint64_t totPages = 1;
    uint64_t nextId = 1;

    memcpy(WORKBUFFER_A, &totBytes, sizeof(totBytes));
    memcpy(WORKBUFFER_A + sizeof(totBytes), &totPages, sizeof(totPages));
    memcpy(WORKBUFFER_A + sizeof(totBytes) + sizeof(totPages), &nextId, sizeof(nextId));
    strcpy(WORKBUFFER_A + sizeof(totBytes) + sizeof(totPages) + sizeof(nextId), tableString.c_str());

    if(!writeToPage(WORKBUFFER_A, tableId, 0, O_CREAT, S_IRUSR|S_IWUSR)){
        return false;
    }

    memset(WORKBUFFER_A,0,PAGE_SIZE);
    if(!writeToPage(WORKBUFFER_A, tableId, 1)){
        return false;
    }

    Logger::logSuccess("Successfully created table with ID: "+ std::to_string(tableId));

    return true;
}

bool saveTableWithName(const std::string& tableName, const std::string &tableString){

    std::string currentDatabase = Database::getCurrentDatabase();

    memset(WORKBUFFER_A, 0, PAGE_SIZE);
    //Read table metadata file of database
    if(!readPage(WORKBUFFER_A, 0, 0)){
        return false;
    }

    // Write to table metadata file
    /**
     * @brief Table metadata file format:
     * 1) first 8 bytes of first page is the next table id. Next 8 bytes is total pages
     * 2) subsequent pages store table metadata
     */

    uint64_t currentTableId, totMetadataPages;
    memcpy(&currentTableId, WORKBUFFER_A, sizeof(currentTableId));
    memcpy(&totMetadataPages, WORKBUFFER_A + sizeof(currentTableId), sizeof(totMetadataPages));

    std::string _tableString = std::to_string(currentTableId) + " " + tableString;
    std::string _tableNameIdString = std::to_string(currentTableId) + " " + tableName + "<";

    if(DEBUG == true){
        std::cout << "Current Table ID: " << currentTableId << std::endl;
        std::cout << "Tot Metadata Pages: " << totMetadataPages << std::endl;
    }

    /**
     * @brief If the line is too long, it won't fit into the read buffer
     */
    if(_tableString.length() + 3*sizeof(uint64_t) > PAGE_SIZE){
        return false;
    }

    memset(WORKBUFFER_B, 0, PAGE_SIZE);

    bool written = false;
    while(true){
        if(!readPage(WORKBUFFER_B, 0, totMetadataPages)){
            return false;
        }

        for(int i=0; i<PAGE_SIZE - ((int)_tableNameIdString.length() - 1); i++){
            if(WORKBUFFER_B[i] == (char)0){
                strcpy(WORKBUFFER_B + i, _tableNameIdString.c_str());
                if(DEBUG == true){
                    std::cout << "copied at index: " << i << " " << WORKBUFFER_B[i] << std::endl;
                }
                written = true;
                break;
            }
        }
        if(written){
            break;
        }
        totMetadataPages++;
        memset(WORKBUFFER_B, 0, PAGE_SIZE);
    }

    if(DEBUG == true){
        std::cout << "Page being written to " << totMetadataPages << std::endl;
    }

    if(!writeToPage(WORKBUFFER_B, 0, totMetadataPages)){
        return false;
    }

    uint64_t nextTableId = currentTableId+1;
    memcpy(WORKBUFFER_A, &nextTableId, sizeof(nextTableId));
    memcpy(WORKBUFFER_A + sizeof(nextTableId), &totMetadataPages, sizeof(totMetadataPages));

    if(!writeToPage(WORKBUFFER_A, 0, 0)){
        return false;
    }

    return saveTableWithId(currentTableId, _tableString);

}

void consolidate(uint64_t fileId, uint32_t rowSize){
    if(fileId == 0 || fileId >= ((uint64_t)1<<LOG_MAX_TABLES)){
        return;
    }

    if(!readPage(TABLE_METADATA_PAGE_BUFFER_A, fileId, 0)){
        return;
    }
    uint64_t totBytes, totPages;
    memcpy(&totBytes, TABLE_METADATA_PAGE_BUFFER_A, sizeof(totBytes));
    memcpy(&totPages, TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes), sizeof(totPages));
    if(totPages <= 2 || totPages*PAGE_SIZE <= 2*totBytes){
        //Consolidate only if relatively large number of pages
        return;
    }

    memset(WORKBUFFER_A, 0, PAGE_SIZE);
    uint64_t p1 = 1;
    uint32_t ptr = sizeof(uint32_t);
    for(int i=1;i<=totPages;i++){
        readPage(CURRENT_TABLE_PAGE_BUFFER_A, fileId, i);
        for(uint32_t j=4; j+rowSize-1 < PAGE_SIZE; j+=rowSize){
            uint64_t currentId;
            memcpy(&currentId, CURRENT_TABLE_PAGE_BUFFER_A+j, sizeof(currentId));
            if(currentId){
                //non empty
                memcpy(WORKBUFFER_A+ptr, CURRENT_TABLE_PAGE_BUFFER_A+j, rowSize);
                ptr+=rowSize;
                if(ptr + rowSize - 1 >=PAGE_SIZE){
                    uint32_t totBytesOccupied = (ptr-sizeof(uint32_t));
                    memcpy(WORKBUFFER_A, &totBytesOccupied, sizeof(totBytesOccupied));

                    writeToPage(WORKBUFFER_A, fileId, p1);
                    p1++;
                    ptr = sizeof(uint32_t);
                    memset(WORKBUFFER_A, 0, PAGE_SIZE);
                }
            }
        }
    }
    if(ptr > sizeof(uint32_t) || p1==1){
        uint32_t totBytesOccupied = (ptr-sizeof(totBytesOccupied));
        memcpy(WORKBUFFER_A, &totBytesOccupied, sizeof(totBytesOccupied));
        writeToPage(WORKBUFFER_A, fileId, p1);
        p1++;
        memset(WORKBUFFER_A, 0, PAGE_SIZE);
    }
    // p1 is the first unused page after consolidation
    if(p1>1){
        p1--;
    }
    memcpy(TABLE_METADATA_PAGE_BUFFER_A + sizeof(totBytes), &p1, sizeof(p1));
    writeToPage(TABLE_METADATA_PAGE_BUFFER_A, fileId, 0);
    // One page for metadata
    truncateFile(fileId, p1+1);
}

/**
 * 
 * @deprecated Replaced by other save row function
 * @brief Save row to the database
 * 
 * @param tableName Name of the table into which the row is inserted
 * @param columns column metadata of the table
 * @param columnValues values of the row
 * @return bool for whether it worked or not
 */
bool saveRow(const std::string& tableName, const std::vector< std::vector< std::string > >& columns, const std::vector< std::string >& columnValues){
    uint64_t tableId = Database::getTableId(tableName);
    uint32_t rowSize = getRowSize(columns);
    return saveRow(tableId, rowSize);
}

/**
 * @deprecated Replaced by the other printQuery function
 */
void printQuery(uint64_t fileId, uint32_t rowSize, const std::vector< std::vector< std::string > >& columns, bool atLeastOneMatch){
    
    std::cout << Formatter::bold_on;
    for(int i=0; i < columns.size(); i++){
        std::cout << std::setw(20) << columns[i][0];
    }
    std::cout << Formatter::off << '\n';

    if(!atLeastOneMatch){
        return;
    }

    memset(WORKBUFFER_C, 0, PAGE_SIZE);

    int bytesRead;
    uint32_t currentPage = 0;

    while((bytesRead = readPage(WORKBUFFER_C, fileId, currentPage))){
        for(int i=0; i<bytesRead; i+=rowSize){
            uint64_t currentId;
            memcpy(&currentId, WORKBUFFER_C+i, 8);
            if(currentId){
                // Row not empty. Process row
                uint32_t offset = 8;
                for(int j=0; j<columns.size();j++){
                    uint32_t sz = getTypeSize(columns[j][1]);
                    std::string printVal = getValueFromBytes(WORKBUFFER_C, columns[j][1], i + offset, i + offset + sz);
                    offset += sz;
                    std::cout << std::setw(20) << printVal ;
                }
                std::cout << '\n';
            }
        }

        currentPage++;
    }
}