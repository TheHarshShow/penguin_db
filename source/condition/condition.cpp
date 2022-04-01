#include "condition.h"
#include "../type/type.h"
#include <vector>

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

COMPARISON getInverseComparison(COMPARISON comp){
    switch(comp){
        case COMPARISON::EQUAL:
            return COMPARISON::EQUAL;
        case COMPARISON::GREATER:
            return COMPARISON::LESS;
        case COMPARISON::LESS:
            return COMPARISON::GREATER;
        case COMPARISON::G_EQUAL:
            return COMPARISON::L_EQUAL;
        case COMPARISON::L_EQUAL:
            return COMPARISON::G_EQUAL;
        case COMPARISON::ASSIGNMENT:
            return COMPARISON::INVALID;
        default:
            return COMPARISON::INVALID;
    }
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