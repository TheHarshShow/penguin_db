#include "tableV2.h"
#include "../buffers/buffers.h"
#include "../type/type.h"
#include <stdlib.h>
#include <utility>
#include <string>
#include <string.h>
#include <map>
#include <algorithm>

bool TableV2::operator==(int x){
    return (mId == x);
}

TableV2::TableV2(std::string tableName){
    mName = tableName;
    mId = Database::getTableId(tableName);

    if(mId){
        metadataBuffer = (char *)calloc(PAGE_SIZE+1, sizeof(char));
        currentPageBuffer = (char *)calloc(PAGE_SIZE+1, sizeof(char));

        readPage(metadataBuffer, mId, 0);
        memcpy(&mTotBytes, metadataBuffer, sizeof(mTotBytes));
        memcpy(&mTotPages, metadataBuffer + sizeof(mTotBytes), sizeof(mTotPages));
        memcpy(&mNextId, metadataBuffer + sizeof(mTotBytes) + sizeof(mTotPages), sizeof(mNextId));

        // Get columns
        std::vector< std::vector <std::string > > columns;

        uint32_t dataStart = 3*sizeof(uint64_t);
        uint8_t numSpaces = 0;
        uint32_t lastSpace = dataStart;

        for(int i=dataStart; i<PAGE_SIZE; i++){
            if(metadataBuffer[i] == '<'){
                // process columns
                std::vector< std::string > currentColumn;
                std::string word;
                for(int j=lastSpace; j<i; j++){
                    if(metadataBuffer[j] == '$'){
                        //Column end
                        currentColumn.push_back(word);
                        columns.push_back(currentColumn);
                        currentColumn.clear();
                        word = "";
                    } else if(metadataBuffer[j] == ' '){
                        if(word.size()){
                            currentColumn.push_back(word);
                            word = "";
                        }
                    } else {
                        word+=metadataBuffer[j];
                    }
                }
                mColumns = columns;

                if(DEBUG == true){
                    std::cout << "Columns: " << std::endl;
                    for(int i=0;i<columns.size();i++){
                        std::cout << columns[i][0] << " " << columns[i][1] << std::endl;
                    }    
                }

                // calculate row size
                mRowSize = 8;
                for(int i=0; i<mColumns.size();i++){
                    mRowSize += getTypeSize(columns[i][1]);
                }

                break;
            } else if(metadataBuffer[i] == ' '){
                if(numSpaces == 0){
                    lastSpace = i+1;
                    numSpaces++;
                } else if(numSpaces == 1){
                    lastSpace = i+1;
                    numSpaces++;
                }
            }
        }
    } else {
        mName = "";
    }
}

TableV2::~TableV2(){
    if(mId != 0){
        free(metadataBuffer);
        free(currentPageBuffer);
    }
}

bool TableV2::loadPage(uint64_t pageIndex){
    mCurrentPage = pageIndex;
    return readPage(currentPageBuffer, mId, pageIndex);
}

bool TableV2::loadLastPage(){
    if(mId == 0){
        return false;
    }
    
    mCurrentPage = mTotPages;
    return readPage(currentPageBuffer, mId, mTotPages);
}

bool TableV2::loadFirstPage(){
    if(mId == 0){
        return false;
    }

    mCurrentPage = 1;
    return readPage(currentPageBuffer, mId, 1);
}

bool TableV2::loadNextPage(){
    if(mId == 0){
        return false;
    }
    mCurrentPage++;
    if(mCurrentPage > mTotPages){
        memset(currentPageBuffer, 0, PAGE_SIZE);
        return true;
    }
    if(readPage(currentPageBuffer, mId, mTotPages)){
        return true;
    }
    return false;
}

bool TableV2::insert(const std::vector< std::string >& tokens){
    if(!loadLastPage()){
        return false;
    }
    uint32_t totBytesInPage;
    memcpy(&totBytesInPage, currentPageBuffer, sizeof(totBytesInPage));

    // validate insert info
    if(tokens.size() != mColumns.size()){
        if(DEBUG == true){
            std::cout << "number of columns doesn't match" << std::endl;
        }
        return false;
    }

    std::string rowBytes;

    char idBytes[sizeof(uint64_t)];
    memcpy(idBytes, &mNextId, sizeof(uint64_t));
    for(int i=0; i < sizeof(uint64_t); i++){
        rowBytes += idBytes[i];
    }

    for(int i=0; i<tokens.size(); i++){
        if(!matchType(tokens[i], mColumns[i][1])){
            if(DEBUG == true){
                std::cout << "Column types don't match. Value: <" << tokens[i] << "> Type: " << mColumns[i][1] << std::endl;
            }
            return false;
        }
        rowBytes += getBytesFromValue(tokens[i], mColumns[i][1]);
    }

    if(rowBytes.length() != mRowSize){
        if(DEBUG == true){
            std::cout << "row size doesn't match" << std::endl;
        }
        return false;
    }

    if(sizeof(uint32_t) + totBytesInPage + mRowSize > PAGE_SIZE){
        // Last page is full. We need a new page.
        loadNextPage();
        totBytesInPage = 0;
        mTotPages++;
    }

    for(int i=sizeof(totBytesInPage); i<PAGE_SIZE; i+=mRowSize){
        uint64_t currentRowId;
        memcpy(&currentRowId, currentPageBuffer + i, sizeof(currentRowId));
        if(!currentRowId){
            // empty row found

            totBytesInPage += mRowSize;
            memcpy(currentPageBuffer + i, rowBytes.c_str(), rowBytes.length());
            memcpy(currentPageBuffer, &totBytesInPage, sizeof(totBytesInPage));
            mNextId++;
            mTotBytes += mRowSize;
            break;
        }
    }

    // Update Metadata
    memcpy(metadataBuffer, &mTotBytes, sizeof(mTotBytes));
    memcpy(metadataBuffer + sizeof(mTotBytes), &mTotPages, sizeof(mTotPages));
    memcpy(metadataBuffer + sizeof(mTotBytes) + sizeof(mTotPages), &mNextId, sizeof(mNextId));

    if(!writeToPage(currentPageBuffer, mId, mCurrentPage) || !writeToPage(metadataBuffer, mId, 0)){
        if(DEBUG == true){
            std::cout << "Unable to write to table" << std::endl;    
        }
        return false;
    }

    return true;
}

bool TableV2::update(std::vector< std::pair< std::string, std::string > >& assignments, std::vector< condition >& conditions){
    std::map< std::string, uint32_t > offsets;
    std::map< std::string, uint32_t > sizes;
    std::map< std::string, std::string > types;
    
    // Offset of ID
    uint32_t currentOffset = sizeof(uint64_t);

    for(int i=0;i<mColumns.size();i++){
        uint32_t sz = getTypeSize(mColumns[i][1]);
        if(sz == 0){
            if(DEBUG == true){
                std::cout << "Size is zero" << std::endl;
            }
            return false;
        }
        offsets[mColumns[i][0]] = currentOffset;
        sizes[mColumns[i][0]] = sz;
        types[mColumns[i][0]] = mColumns[i][1];
        currentOffset += sz;
    }

    std::sort(assignments.begin(), assignments.end());

    // validating assignments
    for(int i=0; i<assignments.size(); i++){
        // If column does not exist or type is incorrect or multiple values are assigned
        if(offsets.find(assignments[i].first) == offsets.end()
            || !matchType(assignments[i].second, types[assignments[i].first])
            || (i>0 && assignments[i].first == assignments[i-1].first)
        ){
            if(DEBUG == true){
                std::cout << "Assignment validation error: " << assignments[i].first << " " << assignments[i].second << std::endl;
            }
            return false;
        }
    }

    // validating conditions
    for(int i=0; i<conditions.size(); i++){
        if(offsets.find(conditions[i].columnName) == offsets.end()
             || !matchType(conditions[i].value, types[conditions[i].columnName])
             || conditions[i].operation == COMPARISON::INVALID
        ){
            if(DEBUG == true){
                std::cout << "Condition validation error" << std::endl;
            }
            return false;
        }
    }

    for(int i=1; i <= mTotPages; i++){
        loadPage(i);

        bool atLeastOneMatched = false;

        for(int j=sizeof(uint32_t); j+mRowSize-1<PAGE_SIZE; j+=mRowSize){
            uint64_t currentRowId;
            memcpy(&currentRowId, currentPageBuffer + j, sizeof(currentRowId));

            if(currentRowId){
                // non empty row
                std::unique_ptr<char[]> rowBuffer = std::make_unique<char[]>(mRowSize + 1);
                memcpy(rowBuffer.get(), currentPageBuffer + j, mRowSize);

                bool matched = true;

                // verify conditions
                for(int k=0;k<conditions.size();k++){
                    std::string cName = conditions[k].columnName;
                    std::string val = conditions[k].value;
                    COMPARISON comp = conditions[k].operation;
                    
                    uint32_t offset = offsets[cName];
                    uint32_t sz = sizes[cName];
                    std::string lVal = getValueFromBytes(rowBuffer.get(), types[cName], offset, offset+sz);

                    COMPARISON compResult = getCompResult(lVal, val, types[cName]);
                    if(!isComparisonValid(comp, compResult)){
                        matched = false;
                        break;
                    }
                }

                if(matched){
                    atLeastOneMatched = true;

                    // If the row satisfies conditions
                    for(int k=0; k<assignments.size(); k++){
                        std::string cName = assignments[k].first;
                        std::string bytes = getBytesFromValue(assignments[k].second, types[cName]);
                        uint32_t offset = offsets[cName];
                        uint32_t sz = sizes[cName];
                        
                        memcpy(rowBuffer.get() + offset, bytes.c_str(), sz);
                    }
                    memcpy(currentPageBuffer + j, rowBuffer.get(), mRowSize);
                }

            }
        }
        if(atLeastOneMatched){
            if(!writeToPage(currentPageBuffer, mId, mCurrentPage)){
                if(DEBUG == true){
                    std::cout << "Error in writing to page" << std::endl;
                }
                return false;
            }
        }
    }

    return true;

}

bool TableV2::deleteRow(const std::vector< condition >& conditions){
    std::map< std::string, uint32_t > offsets;
    std::map< std::string, uint32_t > sizes;
    std::map< std::string, std::string > types;
    
    // Offset of ID
    uint32_t currentOffset = sizeof(uint64_t);

    for(int i=0;i<mColumns.size();i++){
        uint32_t sz = getTypeSize(mColumns[i][1]);
        if(sz == 0){
            if(DEBUG == true){
                std::cout << "Size is zero" << std::endl;
            }
            return false;
        }
        offsets[mColumns[i][0]] = currentOffset;
        sizes[mColumns[i][0]] = sz;
        types[mColumns[i][0]] = mColumns[i][1];
        currentOffset += sz;
    }

    // validating conditions
    for(int i=0; i<conditions.size(); i++){
        if(offsets.find(conditions[i].columnName) == offsets.end()
             || !matchType(conditions[i].value, types[conditions[i].columnName])
             || conditions[i].operation == COMPARISON::INVALID
        ){
            if(DEBUG == true){
                std::cout << "Condition validation error" << std::endl;
            }
            return false;
        }
    }

    for(int i=1; i <= mTotPages; i++){
        loadPage(i);

        bool atLeastOneMatched = false;

        for(int j=sizeof(uint32_t); j+mRowSize-1<PAGE_SIZE; j+=mRowSize){
            uint64_t currentRowId;
            memcpy(&currentRowId, currentPageBuffer + j, sizeof(currentRowId));

            if(currentRowId){

                // non empty row
                std::unique_ptr<char[]> rowBuffer = std::make_unique<char[]>(mRowSize + 1);
                memcpy(rowBuffer.get(), currentPageBuffer + j, mRowSize);

                bool matched = true;

                // verify conditions
                for(int k=0;k<conditions.size();k++){
                    std::string cName = conditions[k].columnName;
                    std::string val = conditions[k].value;
                    COMPARISON comp = conditions[k].operation;
                    
                    uint32_t offset = offsets[cName];
                    uint32_t sz = sizes[cName];
                    std::string lVal = getValueFromBytes(rowBuffer.get(), types[cName], offset, offset+sz);

                    COMPARISON compResult = getCompResult(lVal, val, types[cName]);
                    if(!isComparisonValid(comp, compResult)){
                        matched = false;
                        break;
                    }
                }

                // If matched clear row
                if(matched){
                    atLeastOneMatched = true;
                    memset(currentPageBuffer + j, 0, mRowSize);
                    mTotBytes -= mRowSize;
                }

            }
        }
        if(atLeastOneMatched){
            if(!writeToPage(currentPageBuffer, mId, mCurrentPage)){
                if(DEBUG == true){
                    std::cout << "Error in writing to page" << std::endl;
                }
                return false;
            }
        }
    }

    memcpy(metadataBuffer, &mTotBytes, sizeof(mTotBytes));
    writeToPage(metadataBuffer, mId, 0);

    return true;
}