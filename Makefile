ODIR := obj
SDIR := source
CC := g++
CXXFLAGS := -std=c++17 -g -Wall

_OBJS = main.o version.o parse.o logger.o database.o formatter.o table.o type.o buffers.o tableV2.o condition.o
OBJS = $(patsubst %,$(ODIR)/%,$(_OBJS))

penguin: $(OBJS)
	$(CC) -std=c++17 -g -Wall $^ -o penguin

$(ODIR)/%.o: $(SDIR)/%.cpp
	mkdir -p $(ODIR)
	$(CC) $(CXXFLAGS) -c $< -o $@

$(ODIR)/%.o: $(SDIR)/*/%.cpp $(SDIR)/*/%.h
	mkdir -p $(ODIR)
	$(CC) $(CXXFLAGS) -c $< -o $@

.PHONY: clean

clean:
	rm -rf penguin $(ODIR)