CXX ?= g++
CC ?= gcc
CPPFLAGS = -std=c++11

DBGFLAGS = $(CPPFLAGS) -g
RELFLAGS = $(CPPFLAGS) -O2

export CXX CC CFLAGS CPPFLAGS OPROFILE

all: release 

release: 
	$(CXX) $(RELFLAGS) gen.cpp -o gen.exe
	$(CXX) $(RELFLAGS) deltaGen.cpp -o deltaGen.exe

debug: 
	$(CXX) $(DBGFLAGS) gen.cpp -o gen.exe
	$(CXX) $(DBGFLAGS) deltaGen.cpp -o deltaGen.exe

clean:
	rm -rf bin/*



