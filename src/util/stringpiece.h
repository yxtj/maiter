#ifndef UTIL_STRING_H_
#define UTIL_STRING_H_

#include <string>
#include <vector>
#include <cstdint>
#include <cstdarg>

namespace dsm {

using std::string;

class StringPiece {
public:
    StringPiece() = default;
    StringPiece(const StringPiece& s) = default;
    StringPiece(const string& s);
    StringPiece(const string& s, int len);
    StringPiece(const char* c);
    StringPiece(const char* c, int len);

    // Remove whitespace from either side
    void strip();

    uint32_t hash() const;
    string AsString() const { return data; }

    int size() const { return len; }

    string data;
    int len;

    static std::vector<StringPiece> split(StringPiece sp, StringPiece delim);
};

static bool operator==(const StringPiece& a, const StringPiece& b) {
    return a.data == b.data;
}

template <class Iterator>
string JoinString(Iterator start, Iterator end, string delim=" ") {
    string out;
    while (start != end) {
        out += *start;
        ++start;
        if (start != end) { out += delim; }
    }
    return out;
}

string StringPrintf(string fmt...);
string VStringPrintf(const string& fmt, va_list args);

string ToString(int32_t);
string ToString(int64_t);
string ToString(string);
string ToString(StringPiece);

}

//
namespace std {
template <>
struct hash<dsm::StringPiece> {//: public unary_function<dsm::StringPiece, size_t> {
    size_t operator()(const dsm::StringPiece& k) const {
        return k.hash();
    }
};
}


#endif /* STRING_H_ */
