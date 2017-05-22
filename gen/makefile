CXX ?= g++
CC ?= gcc
MAKE ?= make
CPPFLAGS += -std=c++11
EXE = gen.exe deltaGen.exe delta-gen2.exe

DBGFLAGS = $(CPPFLAGS) -g
RELFLAGS = $(CPPFLAGS) -O2

export CXX CC CFLAGS CPPFLAGS OPROFILE

all: release 

gen.exe: gen.cpp
	$(CXX) $(CPPFLAGS) $^ -o $@

deltaGen.exe: deltaGen.cpp
	$(CXX) $(CPPFLAGS) $^ -o $@

common.o: common.cpp
	$(CXX) $(CPPFLAGS) -c $^ -o $@
	
delta-gen2.exe: delta-gen2.cpp common.h common.o
	$(CXX) $(CPPFLAGS) ${^:%.h=} -o $@

release: 
	$(MAKE) $(EXE) CPPFLAGS="$(RELFLAGS)"

debug: 
	$(MAKE) $(EXE) CPPFLAGS="$(DBGFLAGS)"

clean:
	rm -rf *.o
	rm -rf *.exe


