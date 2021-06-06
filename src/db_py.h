#ifndef __DB_PY_H
#define __DB_PY_H

#include "db.h"

#include "sqlite/sqlite3.h"

void terminatePython(bool verbose);

int runPython(sqlite3 *db, const char* script, bool verbose);

int runPythonEx(sqlite3 *db, const char* script, const char* moduleName);

int loadWorkbook(sqlite3 *db, const char* markdownFile, bool verbose);
int runWorkbooks(sqlite3 *db, bool verbose);

class Statement;
void loadTableIntoPythonDict(sqlite3 *db, Statement *stmt, const char* varName, const char* pkName, bool verbose);
void loadTableIntoPythonList(sqlite3 *db, Statement *stmt, const char* varName, bool verbose);

#define PY_SSIZE_T_CLEAN
#include <Python.h>

PyObject* loadTableIntoPythonDictObject(sqlite3 *db, Statement *pStmt, const char* pkName);
PyObject* loadTableIntoPythonListObject(sqlite3 *db, Statement *pStmt);

#endif

