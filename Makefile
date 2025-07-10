# Makefile para compilar el indexador ScyllaDB

# Configuración del compilador
CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O3 -g
LDFLAGS = -lcassandra -lpthread

# Archivos fuente
BASIC_SRC = indexcylla-cpp/src/indexcylla_example.cpp
ADVANCED_SRC = indexcylla-cpp/src/indexcylla.cpp

# Targets
all: indexcylla_example indexcylla




indexcylla_example: $(BASIC_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

indexcylla: $(ADVANCED_SRC)
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDFLAGS)

# Instalar dependencias (Ubuntu/Debian)
install-deps:
	sudo apt-get update
	sudo apt-get install -y build-essential cmake
	sudo apt-get install -y libssl-dev libuv1-dev
	# Para el driver de ScyllaDB, compilar desde fuente:
	# git clone https://github.com/scylladb/cpp-driver.git
	# cd cpp-driver && mkdir build && cd build
	# cmake .. && make && sudo make install

	sudo wget https://github.com/scylladb/cpp-driver/releases/download/2.15.2-1/scylla-cpp-driver_2.15.2-1_amd64.deb https://github.com/scylladb/cpp-driver/releases/download/2.15.2-1/scylla-cpp-driver-dev_2.15.2-1_amd64.deb
	sudo apt-get update
	sudo apt-get install -y ./scylla-cpp-driver_2.15.2-1_amd64.deb ./scylla-cpp-driver-dev_2.15.2-1_amd64.deb



# Instalar dependencias (CentOS/RHEL)
install-deps-centos:
	sudo yum update
	sudo yum groupinstall -y "Development Tools"
	sudo yum install -y cmake openssl-devel libuv-devel

clean:
	rm -f indexcylla indexcylla_example

.PHONY: all clean install-deps install-deps-centos
