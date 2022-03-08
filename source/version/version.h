#ifndef VERSION_H
#define VERSION_H

#include <iostream>

class Version {
public:

    enum class VersionType {
        DEV,
        RELEASE
    };

    static std::string getVersionTypeString(VersionType vt){
        if(vt == VersionType::DEV){
            return "dev";
        } else if(vt == VersionType::RELEASE){
            return "r";
        } else {
            return "null";
        }
    }

    Version(uint32_t first=0, uint32_t second=0, uint32_t third=0, VersionType vt = VersionType::DEV);

	friend std::ostream& operator<<(std::ostream& os, const Version &v);

	bool operator<(Version const& rhs) const;

	bool operator<=(Version const& rhs) const;

	bool operator>(Version const& rhs) const;

	bool operator>=(Version const& rhs) const;

	bool operator==(Version const& rhs) const;

private:
	uint32_t first;
	uint32_t second;
	uint32_t third;
    VersionType versionType;
};

#endif // VERSION_H