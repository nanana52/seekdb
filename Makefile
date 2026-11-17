CXX ?= g++
CXXFLAGS ?= -std=c++17 -O3 -DNDEBUG -Wall -Wextra -Wno-misleading-indentation
build/minivectordb: src/main.cpp vendor/nlohmann/json.hpp
	mkdir -p build
	$(CXX) $(CXXFLAGS) -Ivendor src/main.cpp -o $@
.PHONY: clean
clean:
	rm -f build/minivectordb
