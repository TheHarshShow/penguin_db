#include <vector>
#include "parse.h"
#include "../database/database.h"

void stripString(std::string &s);
std::vector<std::string> generateTokens(const std::string& command);

void processCommand(const std::string& command){
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
            std::cout << "\ncreate database instruction observed" << std::endl;
        }
        Database::createDatabase(tokens);
    }

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