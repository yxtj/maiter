CXX ?= g++
CC ?= gcc
MAKE ?= make
CPPFLAGS += -std=c++11
EXE = gen.exe deltaGen.exe delta-gen2.exe

DBGFLAGS = $(CPPFLAGS) -g
RELFLAGS = $(CPPFLAGS) -O2

export CXX CC CFLAGS CPPFLAGS OPROFILE

all: release 

%.o: %.cpp
	$(CXX) $(CPPFLAGS) $^ -c -o $@
	
gen.exe: gen.o
	$(CXX) $(CPPFLAGS) $^ -o $@

deltaGen.exe: deltaGen.o
	$(CXX) $(CPPFLAGS) $^ -o $@

delta-gen2.exe: delta-gen2.o common.h common.o
	$(CXX) $(CPPFLAGS) ${^:%.h=} -o $@

release: 
	$(MAKE) $(EXE) CPPFLAGS="$(RELFLAGS)"

debug: 
	$(MAKE) $(EXE) CPPFLAGS="$(DBGFLAGS)"

clean:
	rm -rf *.o
	rm -rf *.exe


