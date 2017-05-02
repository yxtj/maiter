CXX ?= g++
CC ?= gcc
CPPFLAGS = -std=c++11

DBGFLAGS = $(CPPFLAGS) -g
RELFLAGS = $(CPPFLAGS) -O2

export CXX CC CFLAGS CPPFLAGS OPROFILE

all: release 

gen.exe: gen.cpp
	$(CXX) $(RELFLAGS) $^ -o $@

deltaGen.exe: deltaGen.cpp
	$(CXX) $(RELFLAGS) $^ -o $@

release: gen.exe deltaGen.exe

debug: 
	$(CXX) $(DBGFLAGS) gen.cpp -o gen.exe
	$(CXX) $(DBGFLAGS) deltaGen.cpp -o deltaGen.exe

clean:
	rm -rf *.exe


