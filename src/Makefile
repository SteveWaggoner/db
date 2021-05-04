default: db

db: db.C sqlite3.o
	g++ -O3 -DNDEBUG -o $@ -Wall $^
	
sqlite3.o: sqlite/sqlite3.c
	gcc -O3 -DSQLITE_ENABLE_MATH_FUNCTIONS -c sqlite/sqlite3.c

