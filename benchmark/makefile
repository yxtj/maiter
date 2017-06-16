CXX ?= g++
CC ?= gcc
CPPFLAGS += -std=c++11

DBGFLAGS = $(CPPFLAGS) -g
RELFLAGS = $(CPPFLAGS) -O2

EXE = sssp sswp #pagerank 

#export CXX CC CFLAGS CPPFLAGS

all: release

%.o: %.cpp
	$(CXX) $(CPPFLAGS) $^ -c -o $@

sssp.exe: sssp.o common.o
	$(CXX) $(CPPFLAGS) $^ -o $@

sswp.exe: sswp.o common.o
	$(CXX) $(CPPFLAGS) $^ -o $@

release: 
	$(MAKE) $(EXE:=.exe) CPPFLAGS="$(RELFLAGS)"

debug: 
	$(MAKE) $(EXE:=.exe) CPPFLAGS="$(DBGFLAGS)"
	
clean:
	rm -rf *.o
	rm -rf *.exe

