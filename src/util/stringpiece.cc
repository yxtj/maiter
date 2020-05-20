#include "util/stringpiece.h"
#include "util/hash.h"
#include "util/static-initializers.h"

#include "glog/logging.h"

#include <stdarg.h>
#include <stdio.h>

using std::vector;

namespace dsm {
StringPiece::StringPiece(const string& s) : data(s) {
	len = static_cast<int>(data.size());
}
StringPiece::StringPiece(const string& s, int len) : data(s.data(), len) {
	this->len = static_cast<int>(data.size());
}
StringPiece::StringPiece(const char* c) : data(c) {
	len = static_cast<int>(data.size());
}
StringPiece::StringPiece(const char* c, int len) : data(c, len) {
	this->len = static_cast<int>(data.size());
}

uint32_t StringPiece::hash() const {
	return SuperFastHash(data.data(), size());
}

void StringPiece::strip() {
	size_t p = 0;
	size_t l = 0;
	while(p < l && isspace(data[0]))
		++p;
	while(l > 0 && isspace(data[l - 1]))
		--l;
	data = data.substr(p, l - p);
	len = static_cast<int>(data.size());
}

static void StringPieceTestStrip() {
	StringPiece p = "abc def;";
	p.strip();
	CHECK_EQ(p.AsString(), "abc def;");

	StringPiece q = "   abc def;   ";
	q.strip();
	CHECK_EQ(q.AsString(), "abc def;");
}
REGISTER_TEST(StringPieceStrip, StringPieceTestStrip());

vector<StringPiece> StringPiece::split(StringPiece sp, StringPiece delim) {
	vector<StringPiece> out;
	size_t p = 0, plast = 0;
	while(plast < sp.data.size()){
		p = sp.data.find(delim.data, plast);
		out.emplace_back(sp.data.substr(plast, p - plast));
		plast = p + delim.data.size();
	}
	return out;
}

static void StringPieceTestSplit() {
	vector<StringPiece> sp = StringPiece::split("a,b,c,d", ",");
	CHECK_EQ(sp[0].AsString(), "a");
	CHECK_EQ(sp[1].AsString(), "b");
	CHECK_EQ(sp[2].AsString(), "c");
	CHECK_EQ(sp[3].AsString(), "d");
}
REGISTER_TEST(StringPieceSplit, StringPieceTestSplit());

string StringPrintf(string fmt...) {
	va_list l;
	va_start(l, fmt);
	string result = VStringPrintf(fmt, l);
	va_end(l);

	return result;
}

string VStringPrintf(const string& fmt, va_list l) {
	const string& str = fmt;
	int len = 512 >= str.size() * 2 ? 512 : str.size() * 2;
	char* buffer = new char[len];
	vsnprintf(buffer, len, str.c_str(), l);
	string res(buffer);
	delete[] buffer;
	return res;
}

}
