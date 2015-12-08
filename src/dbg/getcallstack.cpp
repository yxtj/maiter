/*
 * getcallstack.cpp
 *
 *  Created on: Dec 2, 2015
 *      Author: tzhou
 */

#include "getcallstack.h"

#include <execinfo.h>
#include <cxxabi.h>
#include <string>
#include <sstream>

using namespace std;

std::string getcallstack(){
	ostringstream out;
	out<<"stack trace:";

	static constexpr int max_frames=63;
	// storage array for stack trace address data
	void* addrlist[max_frames + 1];

	// retrieve current stack addresses
	int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

	if(addrlen == 0){
		out<<"\n  <empty, possibly corrupt>";
		return out.str();
	}
	// resolve addresses into strings containing "filename(function+address)",
	// this array must be free()-ed
	char** symbollist = backtrace_symbols(addrlist, addrlen);

	// allocate string which will be filled with the demangled function name
	size_t funcnamesize = 256;
//	char* funcname = (char*)malloc(funcnamesize);
	char funcname[256];

//	for(int i=1;i<addrlen;++i){
//		out<<symbollist[i]<<"\n";
//	}
//	return out.str();

	// iterate over the returned symbol lines. skip the first, it is the
	// address of this function.
	for(int i = 1; i < addrlen; i++){
//		out<<"\n"<<symbollist[i];
		char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

		// find parentheses and +address offset surrounding the mangled name:
		// ./module(function+0x15c) [0x8048a6d]
		for(char *p = symbollist[i]; *p; ++p){
			if(*p == '(')
				begin_name = p;
			else if(*p == '+')
				begin_offset = p;
			else if(*p == ')' && begin_offset){
				end_offset = p;
				break;
			}
		}

		if(begin_name && begin_offset && end_offset
				&& begin_name < begin_offset){
			*begin_name++ = '\0';
			*begin_offset++ = '\0';
			*end_offset = '\0';

			// mangled name is now in [begin_name, begin_offset) and caller
			// offset in [begin_offset, end_offset). now apply
			// __cxa_demangle():

			int status;
			char* ret = abi::__cxa_demangle(begin_name,funcname, &funcnamesize, &status);
			if(status == 0){
//				funcname = ret; // use possibly realloc()-ed string
//				fprintf(out, "  %s : %s+%s\n",symbollist[i], funcname, begin_offset);
//				out<<"  "<<symbollist[i]<<" : "<<funcname<<"+"<<begin_offset<<"\n";
				out<<"\n  "<<symbollist[i]<<" : "<<ret<<"+"<<begin_offset;
			}else{
				// demangling failed. Output function name as a C function with
				// no arguments.
//				fprintf(out, "  %s : %s()+%s\n",symbollist[i], begin_name, begin_offset);
				out<<"\n  "<<symbollist[i]<<" : "<<begin_name<<"()+"<<begin_offset;
			}
		}else{
			// couldn't parse the line? print the whole line.
//			fprintf(out, "  %s\n", symbollist[i]);
			out<<"\n  "<<symbollist[i];
		}
	}

//	free(funcname);
//	free(symbollist);
	return out.str();
}
