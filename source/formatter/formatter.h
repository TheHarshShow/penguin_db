#ifndef FORMATTER_H
#define FORMATTER_H

#include <iostream>

class Formatter {
public:
    static std::ostream& bold_on(std::ostream& os);

    static std::ostream& bold_red_on(std::ostream& os);

    static std::ostream& bold_green_on(std::ostream& os);

    static std::ostream& off(std::ostream& os);
};

#endif // FORMATTER_H