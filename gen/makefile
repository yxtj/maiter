CXX ?= g++
CC ?= gcc
MAKE ?= make
CPPFLAGS += -std=c++11
EXE = gen deltaGen delta-gen2

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
	$(MAKE) $(EXE:=.exe) CPPFLAGS="$(RELFLAGS)"

debug: 
	$(MAKE) $(EXE:=.exe) CPPFLAGS="$(DBGFLAGS)"

clean:
	rm -rf *.o
	rm -rf *.exe


