#include <vector>
#include "parse.h"
#include "../condition/condition.h"
#include "../logger/logger.h"
#include "../database/database.h"
#include "../table/table.h"
#include "../table/tableV2.h"

void stripString(std::string &s);
std::vector<std::string> generateTokens(const std::string& command);

void handleInsertIntoTable(const std::vector< std::string >& tokens){

	if(!Database::isDatabaseChosen()){
		Logger::logError("No database chosen");
		return;
	}

	if(tokens[3] != "values" || tokens[4] != "(" || tokens[tokens.size()-1] != ")"){
		Logger::logError("Syntax error in insert statement.");
		return;
	}

	std::string tableName = tokens[2];

	std::vector< std::string > columnValues;
	for(int i=5; i<tokens.size()-1; i++){
		if(tokens[i]!=","){
			columnValues.push_back(tokens[i]);
		}
	}

	TableV2 tab(tableName);
	if(tab == 0){
		Logger::logError("Table with given name doesn't exist");
		return;
	}

	if(!tab.insert(columnValues)){
		Logger::logError("Error in inserting into table");
		return;
	}

	Logger::logSuccess("Successfully inserted row");
}

void handleUpdateTable(const std::vector< std::string >& tokens){
	TableV2 tab(tokens[1]);
	if(tab == 0){
		Logger::logError("Table " + tokens[1] +"doesn't exist");
		return;
	}

	std::vector< std::pair< std::string, std::string > > assignments;
	std::vector< condition > conditions;

	std::vector< std::string > currentTokens;

	int i=3;

	for(; i < tokens.size(); i++){
		if(tokens[i] == "where"){
			i++;
			break;
		}
		if(tokens[i] == ","){
			if(currentTokens.size() != 3){
				Logger::logError("Syntax error in update statement");
				return;
			}
			assignments.push_back(make_pair(currentTokens[0], currentTokens[2]));
			currentTokens.clear();
		} else {
			currentTokens.push_back(tokens[i]);
		}
	}

	if(currentTokens.size() != 3){
		Logger::logError("Syntax error in update statement");
		return;
	}

	assignments.push_back(make_pair(currentTokens[0], currentTokens[2]));
	currentTokens.clear();

	for(;i<tokens.size();i++){
		if(tokens[i] == "and" || tokens[i] == "&&"){
			condition cd = getCondition(currentTokens);
			currentTokens.clear();
			if(cd.operation == COMPARISON::INVALID){
				Logger::logError("condition invalid");
				return;
			}
			conditions.push_back(cd);
		} else {
			currentTokens.push_back(tokens[i]);
		}
	}

	if(currentTokens.size()==0){
		Logger::logError("Syntax error");
		return;
	}
	condition cd = getCondition(currentTokens);
	if(cd.operation == COMPARISON::INVALID){
		Logger::logError("Invalid operation");
		return;
	}
	conditions.push_back(cd);

	if(!tab.update(assignments, conditions)){
		Logger::logError("Error in updating table");
		return;
	}
	Logger::logSuccess("Successfully updated table");
	return;

}

void handleDeleteRow(const std::vector< std::string >& tokens){
	TableV2 tab(tokens[2]);
	if(tab == 0){
		Logger::logError("Table does not exist");
		return;
	}
	if(tokens[3] != "where"){
		Logger::logError("Syntax error: where clause expected");
		return;
	}
	std::vector< condition > conditions;
	std::vector< std::string > currentTokens;
	for(int i=4; i < tokens.size(); i++){
		if(tokens[i] == "and" || tokens[i] == "&&"){
			condition cd = getCondition(currentTokens);
			if(cd.operation == COMPARISON::INVALID){
				Logger::logError("Syntax error: invalid condition");
				return;
			}
			conditions.push_back(cd);
			currentTokens.clear();
		} else {
			currentTokens.push_back(tokens[i]);
		}
	}
	if(currentTokens.size()==0){
		Logger::logError("Syntax error: no condition provided");
		return;
	}
	condition cd = getCondition(currentTokens);
	if(cd.operation == COMPARISON::INVALID){
		Logger::logError("Invalid condition provided");
		return;
	}
	conditions.push_back(cd);
	if(!tab.deleteRow(conditions)){
		Logger::logError("Fatal: Error in deleting rows");
		return;
	}
	Logger::logSuccess("Successfully deleted rows");
	return;
}

void processCommand(const std::string& command){

	if(command.size() > 2*PAGE_SIZE){
		Logger::logError("Query must fit into two pages.");
		return;
	}

    std::string _command = command;
    stripString(_command);
    std::vector<std::string> tokens = generateTokens(_command);

    if(DEBUG == true){
        std::cout << "\nGenerated tokens: " << std::endl;
        for(int i=0; i<tokens.size(); i++){
            std::cout << tokens[i] << std::endl;
        }
        std::cout << std::endl;
    }

    if(tokens.size()>2 && tokens[0] == "create" && tokens[1] == "database"){
        if(DEBUG == true){
            std::cout << "create database query observed" << std::endl;
        }
        Database::createDatabase(tokens);
    } else if(tokens.size()>1 && tokens[0] == "use"){
		if(DEBUG == true){
			std::cout << "use database query observed" << std::endl;
		}
		Database::useDatabase(tokens);
	} else if(tokens.size()>2 && tokens[0] == "create" && tokens[1] == "table"){
		if(DEBUG == true){
			std::cout << "create table query observed" << std::endl;
		}
		Table::createTable(tokens);
	} else if(tokens.size()>2 && tokens[0] == "insert" && tokens[1] == "into"){
		if(DEBUG == true){
			std::cout << "insert into query observed" << std::endl;
		}
		handleInsertIntoTable(tokens);
		// Table::insertIntoTable(tokens);
	} else if(tokens.size() > 3 && tokens[0] == "select" && tokens[1] == "*" && tokens[2] == "from"){
		if(DEBUG == true){
			std::cout << "select from query observed" << std::endl;
		}
		Table::handleSearchQuery(tokens);
	} else if(tokens.size()>3 && tokens[0] == "update" && tokens[2] == "set"){
		if(DEBUG == true){
			std::cout << "update query observed" << std::endl;
		}
		handleUpdateTable(tokens);
		// Table::handleUpdateTable(tokens);
	} else if(tokens.size()>4 && tokens[0] == "delete" && tokens[1] == "from"){
		if(DEBUG == true){
			std::cout << "delete from query observed" << std::endl;
		}
		handleDeleteRow(tokens);
		// Table::handleDeleteRow(tokens);

	} else if(tokens.size() == 1 && tokens[0] == "exit"){
		std::cout << "Bye!" << std::endl;
		PROG_RUNNING = false;
	}
	
	std::cout << std::endl;

}

void stripString(std::string &s){
	for(int i=0;i<s.length();i++){
		if(s[i]!=' '){
			s=s.substr(i);
			break;
		}
	}
	while(s.length()>0 && s[s.length()-1]==' ')s.pop_back();
}

std::vector<std::string> generateTokens(const std::string& command){
    std::vector<std::string> tokens;
	std::string word="";

	bool inChar=false;

	for(int i=0;i<(int)command.length();i++){
		if(command[i]==' '){
			if(inChar){
				word+=' ';
			} else {
				if(word.length()){
					tokens.push_back(word);
					word="";
				}
			}
		} else if(command[i]=='\n'){
			if(inChar){
				word+='\n';
			} else {
				if(word.length()){
					tokens.push_back(word);
					word="";
				}
			}
		} else if(command[i]=='\'') {
			if(inChar){
				word+='\'';
				tokens.push_back(word);
				word="";
			} else {
				if(word.length()){
					tokens.push_back(word);
					word="";
				}
				word+='\'';
			}
			inChar = !inChar;
		} else if(command[i]=='=') {
			if(inChar){
				word+='=';
			} else {
				if(word.length()){
					tokens.push_back(word);
				}
				tokens.push_back("=");
				word="";
			}
		} else if(command[i] =='>'){
			if(inChar){
				word+='>';
			} else {
				if(word.length()){
					tokens.push_back(word);
				}
				tokens.push_back(">");
				word="";
			}
		} else if(command[i] =='<'){
			if(inChar){
				word+='<';
			} else {
				if(word.length()){
					tokens.push_back(word);
				}
				tokens.push_back("<");
				word="";
			}
		} else if(command[i]=='('){
			if(inChar){
				word+='(';
			} else {
				if(word.length()){
					tokens.push_back(word);
				}
				tokens.push_back("(");
				word="";
			}
		} else if(command[i]==')'){
			if(inChar){
				word+=')';
			} else {
				if(word.length()){
					tokens.push_back(word);
				}
				tokens.push_back(")");
				word="";
			}
		} else if(command[i]==','){
			if(inChar){
				word+=',';
			} else {
				if(word.length()){
					tokens.push_back(word);
				}
				tokens.push_back(",");
				word="";
			}
		} else if(command[i]==';') {
			if(inChar){
				word+=';';
			} else {
				if(word.length()){
					tokens.push_back(word);
					word="";
				}
				return tokens;
			}
		} else {
			word+=command[i];
		}
	}

	if(word.length()){
		tokens.push_back(word);
		word="";
	}

	return tokens;
}