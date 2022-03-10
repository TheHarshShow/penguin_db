#include <string.h>
#include <iostream>
#include "type.h"

bool verifyStringType(const std::string& type);

TYPE getTypeFromString(const std::string type){
    if(type == "int"){
        return TYPE::INT;
    } else if(type == "float"){
        return TYPE::FLOAT;
    } else if(type == "char"){
        return TYPE::CHAR;
    } else if(type.size()>6 && type.substr(0,6)=="string"){
        if(!verifyStringType(type)){
            return TYPE::UNSUPPORTED;
        }
        return TYPE::STRING;
    } else {
        return TYPE::UNSUPPORTED;
    }
}

uint32_t getStringLength(const std::string& type){
    
    std::string lengthString = "";

    for(int i=7;i<type.size()-1;i++){
        if(type[i]<'0' || type[i]>'9'){
            return 0;
        }
        lengthString+=type[i];
    }

    /**
     * @brief Currently only supporting strings with length < 1e9
     * 
     */
    if(lengthString.length()>9) {
        return 0;
    }

    if(stoi(lengthString) == 0){
        return 0;
    }

    return stoi(lengthString);
}

uint32_t getTypeSize(const std::string& type){
    TYPE _type = getTypeFromString(type);
    switch(_type){
        case TYPE::INT:
            return 8;
        case TYPE::FLOAT:
            return 8;
        case TYPE::CHAR:
            return 1;
        case TYPE::STRING:
            return getStringLength(type);
        case TYPE::UNSUPPORTED:
            return 0;
        default:
            return 0;
    }
}

bool verifyStringType(const std::string& type){
    if(type[6]!='[' || type[type.size()-1]!=']'){
        return false;
    }
    
    if(getStringLength(type) == 0){
        return false;
    }

    return true;
}

bool matchType(const std::string& value, const std::string& type){
    TYPE _type = getTypeFromString(type);
    uint32_t maxLen;
    switch(_type){
        case TYPE::INT:
            if(value[0] != '-' && (value[0] < '0' || value[0] > '9')){
                return false;
            }
            for(int i=1; i < value.size(); i++){
                if(value[i] < '0' || value[i] > '9'){
                    return false;
                }
            }
            return true;
        case TYPE::CHAR:
            if(value[0] != '\'' || value[value.size()-1] != '\''){
                return false;
            }
            if(value.size()>4 || value.size()<3){
                return false;
            }
            /**
             * @brief Process chars like \n, \r etc later
             */
            if(value.size() == 4){
                return false;
            }
            if(value.size()==3){
                return true;
            }
        case TYPE::FLOAT:
            // Unsophisticated solution. Probably will be replaced later
            try {
                std::stod(value);
                return true;
            } catch(...) {
                return false;
            }
        case TYPE::STRING:
            if(value[0] != '\'' || value[value.size()-1] != '\''){
                return false;
            }
            maxLen = getStringLength(type);
            if(value.length() > maxLen + 2){
                return false;
            }
            return true;
        default:
            return false;
    }
}

std::string getBytesFromValue(const std::string& value, const std::string& type){
    TYPE _type = getTypeFromString(type);
    std::string finalBytes, padding;
    uint32_t maxLen, reqPadding;
    int64_t intVal;
    double db;
    switch(_type){
        case TYPE::INT:
            intVal = std::stoll(value);
            char intBytes[9];
            memcpy(intBytes, &intVal, 8);
            for(int i=0;i<8;i++){
                finalBytes += intBytes[i];
            }
            return finalBytes;
        case TYPE::FLOAT:
            db = std::stod(value);
            char floatBytes[9];
            memcpy(floatBytes, &db, 8);
            for(int i=0;i<8;i++){
                finalBytes += floatBytes[i];
            }
            return finalBytes;
        case TYPE::CHAR:
            /**
             * @todo replace when adding support for \n etc.
             */
            finalBytes += value[1];
            return finalBytes;
        case TYPE::STRING:
            maxLen = getStringLength(type);
            finalBytes = value.substr(1,value.size()-2);
            reqPadding = maxLen - finalBytes.length();
            for(int i=0; i < reqPadding; i++){
                padding+=(char)0;
            }
            finalBytes = padding + finalBytes;
            return finalBytes;
        default:
            return "";
    }
}

std::string getValueFromBytes(char buffer[], const std::string& type, int start, int end){
    TYPE _type = getTypeFromString(type);

    int64_t intValue;
    std::string returnValue;
    double db;
    switch(_type){
        case TYPE::INT:
            memcpy(&intValue, buffer + start, 8);
            return std::to_string(intValue);
        case TYPE::CHAR:
            returnValue += '\'';
            returnValue += buffer[start];
            returnValue += '\'';
            return returnValue;
        case TYPE::FLOAT:
            memcpy(&db, buffer + start, 8);
            return std::to_string(db);
        case TYPE::STRING:
            returnValue += '\'';
            for(int i=start; i<end; i++){
                // Add only if not padding byte
                if(buffer[i] != (char)0)
                    returnValue+=buffer[i];
            }
            returnValue += '\'';
            return returnValue;
        default:
            return "";
    }
}