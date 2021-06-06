#ifndef __DB_H
#define __DB_H

#include <vector>
#include <string>
#include <map>

#include "sqlite/sqlite3.h"

#include "db_py.h"

extern sqlite3 *dbHandleG;

using namespace std;

class Statement;

struct Variant {
    Variant() { type = 'U'; str[0]=0; num=0; real=0; }
    char   type;
    char   str[256];  // 's'
    int    num;  // 'i'
    double real; // 'd'
};


const char* api_lastError();

void api_runCommand(const char* zSQL);

Statement* api_newStatement(const char* zSQL);
void api_deleteStatement(Statement* pStmt);
void api_getColumnNames(Statement *pStmt, vector<string> *pColNames);
bool api_nextRow(Statement *pStmt, vector<Variant> *pRowValues);

#endif

