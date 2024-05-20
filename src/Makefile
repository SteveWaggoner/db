default: db 

db: db.C sqlite3.o db_py.o
	g++ -O3 -DNDEBUG -I/usr/include/python3.8/ -o $@ -Wall $^ -lpython3.8 -pthread  -ldl

db_py.o: db_py.C
	g++ -O3 -DNDEBUG -I/usr/include/python3.8/ -c db_py.C

sqlite3.o: sqlite/sqlite3.c
	gcc -O3 -DSQLITE_ENABLE_MATH_FUNCTIONS -c sqlite/sqlite3.c

