all: hello_world.cc
	g++ -I../../src/include -g -c hello_world.cc -o hello_world.o
	libtool --mode=link g++ -L../../src -g hello_world.o -lrados -o librados_hello_world
clean:
	rm hello_world.o librados_hello_world
