CXX ?= g++
CC ?= gcc
CPPFLAGS += -std=c++11

DBGFLAGS = $(CPPFLAGS) -g
RELFLAGS = $(CPPFLAGS) -O2

SRC = sssp #pagerank 

#export CXX CC CFLAGS CPPFLAGS

all: release

%.o: %.cpp
	$(CXX) $(RELFLAGS) $^ -c -o $@

sssp.exe: sssp.o common.o
	$(CXX) $(RELFLAGS) $^ -o $@

release: $(SRC:=.exe)

debug: 
	$(CXX) $(DBGFLAGS) sssp.cpp -o sssp.exe
	$(CXX) $(DBGFLAGS) pagerank.cpp -o pagerank.exe

clean:
	rm -rf *.o
	rm -rf *.exe

