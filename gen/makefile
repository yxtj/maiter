CXX ?= g++
CC ?= gcc
CPPFLAGS = -std=c++11

DBGFLAGS = $(CPPFLAGS) -g
RELFLAGS = $(CPPFLAGS) -O2

export CXX CC CFLAGS CPPFLAGS OPROFILE

all: release 

release: 
	$(CXX) $(RELFLAGS) gen.cpp -o gen
	$(CXX) $(RELFLAGS) deltaGen.cpp -o deltaGen

debug: 
	$(CXX) $(DBGFLAGS) gen.cpp -o gen
	$(CXX) $(DBGFLAGS) deltaGen.cpp -o deltaGen

clean:
	rm -rf bin/*



