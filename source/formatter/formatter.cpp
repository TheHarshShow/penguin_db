#include "formatter.h"

std::ostream& Formatter::bold_on(std::ostream& os)
{
    return os << "\e[1m";
}

std::ostream& Formatter::bold_red_on(std::ostream& os)
{
    return os << "\e[1;31m";
}

std::ostream& Formatter::bold_green_on(std::ostream& os)
{
    return os << "\e[1;32m";
}

std::ostream& Formatter::off(std::ostream& os)
{
    return os << "\e[0m";
}