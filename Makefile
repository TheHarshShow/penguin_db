ODIR = obj
CC = g++
CXXFLAGS = -std=c++17 -g -Wall

_OBJS = main.o version.o parse.o logger.o database.o
OBJS = $(patsubst %,$(ODIR)/%,$(_OBJS))

penguin: $(OBJS)
	$(CC) -std=c++17 -g -Wall $^ -o penguin

$(ODIR)/main.o: source/main.cpp
	mkdir -p $(ODIR)
	$(CC) $(CXXFLAGS) -c source/main.cpp -o $@

$(ODIR)/version.o: source/version/version.cpp source/version/version.h
	mkdir -p $(ODIR)
	$(CC) $(CXXFLAGS) -c source/version/version.cpp -o $@

$(ODIR)/parse.o: source/parse/parse.cpp source/parse/parse.h
	mkdir -p $(ODIR)
	$(CC) $(CXXFLAGS) -c source/parse/parse.cpp -o $@

$(ODIR)/logger.o: source/logger/logger.cpp source/logger/logger.h
	mkdir -p $(ODIR)
	$(CC) $(CXXFLAGS) -c source/logger/logger.cpp -o $@

$(ODIR)/database.o: source/database/database.cpp source/database/database.h
	mkdir -p $(ODIR)
	$(CC) $(CXXFLAGS) -c source/database/database.cpp -o $@

clean:
	rm -rf penguin $(ODIR)