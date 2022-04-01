#include <string>
#include <memory>
#include "../database/database.h"
#include "../properties.h"
#include "../condition/condition.h"


class TableV2 {
    std::string mName = "";
    uint64_t mId = 0;
    uint64_t mTotBytes = 0;
    uint64_t mTotPages = 1;
    uint64_t mNextId = 1;
    uint64_t mCurrentPage = 0;
    std::vector< std::vector< std::string > > mColumns;
    uint32_t mRowSize = 0;
public:
    char* metadataBuffer;
    char* currentPageBuffer;

    bool operator==(int x);

    TableV2(std::string tableName);

    bool loadPage(uint64_t pageIndex);
    bool loadFirstPage();
    bool loadLastPage();
    bool loadNextPage();

    /**
     * @brief Inserts tokens into table
     * 
     * @param tokens List of tokens corresponding to columns
     * @return true If insertion worked
     * @return false If it didn't work
     */
    bool insert(const std::vector< std::string >& tokens);

    /**
     * @brief updates rows with given assignments
     * 
     * @param assignments list of columnName-value pairs
     * @param condition list of conditions
     * @return true if it worked
     * @return false if it didn't
     */
    bool update(std::vector< std::pair< std::string, std::string > >& assignments, std::vector< condition >& conditions);

    /**
     * @brief Deletes rows with given conditions
     * 
     * @param conditions list of conditions
     * @return true if it worked
     * @return false if it didn't
     */
    bool deleteRow(const std::vector< condition >& conditions);

    inline uint64_t getId(){ return mId; };

    ~TableV2();
};
