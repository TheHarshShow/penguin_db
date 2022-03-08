#include <iostream>
#include "version.h"

Version::Version(uint32_t first, uint32_t second, uint32_t third, VersionType vt){
	this->first = first;
    this->second = second;
    this->third = third;
    this->versionType = vt;
}

bool Version::operator<(Version const& rhs) const {
	return ((this->first)<rhs.first || ((this->first)==rhs.first && (this->second)<rhs.second) || ((this->first)==rhs.first && (this->second)==rhs.second && (this->third)<rhs.third));
}

bool Version::operator<=(Version const& rhs) const {
	return ((this->first)<=rhs.first || ((this->first)==rhs.first && (this->second)<=rhs.second) || ((this->first)==rhs.first && (this->second)==rhs.second && (this->third)<=rhs.third));
}

std::ostream& operator<<(std::ostream& os, const Version &v){
	os<<(std::to_string(v.first)+"."+std::to_string(v.second)+"."+std::to_string(v.third)+" "+Version::getVersionTypeString(v.versionType));
	return os;
}

bool Version::operator>(Version const& rhs) const {
    return ((this->first)>rhs.first || ((this->first)==rhs.first && (this->second)>rhs.second) || ((this->first)==rhs.first && (this->second)==rhs.second && (this->third)>rhs.third));
}

bool Version::operator>=(Version const& rhs) const {
    return ((this->first)>=rhs.first || ((this->first)==rhs.first && (this->second)>=rhs.second) || ((this->first)==rhs.first && (this->second)==rhs.second && (this->third)>=rhs.third));
}

bool Version::operator==(Version const& rhs) const {
    return ((this->first)==rhs.first && (this->second)==rhs.second && (this->third)==rhs.third);
}
