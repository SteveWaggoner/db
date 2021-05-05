#ifndef __DB_PY_H
#define __DB_PY_H

#include "db.h"

#include "sqlite/sqlite3.h"

int runPython(sqlite3 *db, const char* script, bool verbose);

int runPythonEx(sqlite3 *db, const char* script, const char* moduleName);

class Statement;
void loadTableIntoPythonDict(sqlite3 *db, Statement *stmt, const char* varName, const char* pkName);
void loadTableIntoPythonList(sqlite3 *db, Statement *stmt, const char* varName);

#endif

