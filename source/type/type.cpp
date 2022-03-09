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

bool verifyStringType(const std::string& type){
    if(type[6]!='[' || type[type.size()-1]!=']'){
        return false;
    }
    
    if(getStringLength(type) == 0){
        return false;
    }

    return true;
}