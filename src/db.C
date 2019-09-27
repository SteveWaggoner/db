#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <assert.h>

#include <vector>
#include <string>
#include <map>

#define NENTRIES(array) ((int) (sizeof(array) / sizeof(array[0])))

inline bool streq(const char *s1, const char *s2) {
    return strcmp(s1, s2) == 0;
}

char *trimWhiteSpace(char *str)
{
    while (*str == ' ' || *str == '\t') {
        str++;
    }
    char *zAnswer = str;
    while (*str != 0) {
        str++;
    }
    while (str > zAnswer && (str[-1] == ' ' || str[-1] == '\t')) {
        str--;
        *str = 0;
    }
    return zAnswer;
}

class TextFileReader {

    FILE *ptr_file;
    char buf[8000];
    int line_number;

    // trim line ending off
    void trimBuf() {
        int n=strlen(buf);
        while(n>0) {
            n--;
            if(buf[n]=='\n' || buf[n]=='\r') {
                buf[n]=0;
                continue;
            } else {
                break;
            }
        }
    }

public:
    TextFileReader(const char* zFile) {
    	ptr_file =fopen(zFile,"r");
        line_number = 0;
        buf[0] = 0;
        if (! ptr_file) {
            fprintf(stderr,"File not found: %s\n", zFile);
            exit(1);
        }
    }

    ~TextFileReader() {
        if (ptr_file) {
		    fclose(ptr_file);
        }
    }
   
    bool AtEof() {
        if (fgets(buf,8000, ptr_file) == NULL) {
            trimBuf();
            return true;
        } else {
            trimBuf();
            line_number++;
            return false;
        }
    }

    char* NextLine() {
        return buf;
    }

    int LineNumber() {
        return line_number;
    }
};

using namespace std;

#include "sqlite/sqlite3.h"

//forward declarations
void closeDatabase();

void usage(void)
{
    fprintf(stderr,
"Usage: db [[OPTION] [FILE]...]  [[OPTION] [QUERY] ... ]\n"
"\n"
"Run SQLite queries on FILE(s) and print result to standard output. If FILE is - then it will be\n"
"read from standard input. Various OPTIONS can be specified before FILE is loaded and queries are\n"
"run to customize. OPTIONS will be persisted to succeeding FILES and QUERIES. Defaults to store in\n"
"memory, auto-detect field titles, auto-detect data types and auto-detect delimiter.\n"
"\n"
"  -d CHAR     use CHAR for field delimiter\n"
"  -e          turn on escaping values inside double quotes so can read TNS data files\n"
"  -ee         same as -e but also removed surrounding quotes and trim whitespace\n"
"  -l          turn on escaping curlybrackets so can read WL events from logs\n"
"  -b          turn on escaping backslashes so can read Ad pipeline files (-wl-csv)\n"
"  -t          turn on field titles (FILES to have first line as titles and print with field names)\n"
"  -n          turn off field titles\n"
"  -v          set verbose mode\n"
"  -vv         set debug mode. Turn on verbose and print query plans and execution times\n"
"  -D          store everything on disk instead of in memory (re-initializes db)\n"
"  -F DBPATH   use a specific database file (creates it if doesn't exist)\n"
"  -A DBPATH   attach an existing database file\n"
"  -T          turn off auto-detect data types and use TEXT\n"
"  -M FIELD DT use a specific data type for a fieldname (ex. -M prefix TEXT)\n"
"  -C          chop stdin to common length. if line longer that first ignore extra fields (no error)\n"
"  -R COUNT    force tables to have at least this many fields (useful when armFind finds no events)\n"
"  -N INDEX    starting number for first unnamed field (defaults to using ordinal number)\n"
"  -U          combine imported tables in to single table (requires all tables have same column names)\n"
"\n"
"  -u FIELDS   get unique field count\n"
"  -s FIELDS   sort table\n"
"  -i FIELDS   index fields\n"
"  -w FILTER   filter records\n"
"\n"
"  -q QUERY    run QUERY statement delimited by semicolons\n"
"  -Q QUERY    run QUERY statement delimited by semicolons (pretty format)\n"
"  -r CMD      run CMD   statement delimited by semicolons (without printing results)\n"
"  -O <NAME>   write last table to stdout (prepend with @NAME)\n"
"  -o[FIELDS]  write last table to stdout\n"
"  -p[FIELDS]  write last table to stdout (pretty format with max 30 records)\n"
"  -a          sample 6 records from all tables\n"
"  -m SIZE     truncate field values to size for use with '-p' and '-a'\n"
"  -S[FIELDS]  sum fields\n"
"\n"
"When FILE is -, read standard input.\n"
"\n"
"If running out of diskspace, customize the temp directory:"
"\n"
"  export DB_TEMP_STORE_DIRECTORY=/tmp/lots_of_space_dir"
"\n"
"See also:\n"
"  http://www.sqlite.org/lang.html\n"
"\n"
"  SQLITE_VERSION   = " SQLITE_VERSION "\n"
"  SQLITE_SOURCE_ID = " SQLITE_SOURCE_ID  "\n"
"\n"
);
    closeDatabase();
    exit(1);
}


sqlite3 *dbHandleG = NULL;

bool fAutoDetectSepG = true;
char fieldSeperatorG = '|';

int fEscapeQuotedValuesG=0;  //0=no escape, 1=escape but keep quotes, 2=escape and remove quotes
bool fEscapeCurlyBracketsG=false;
bool fEscapeBackslashesG=false;

bool fAutoDetectTitlesG=true;
bool fAutoDetectDataTypesG=true;
bool fHasColumnNamesG=false;
int  nTableCountG = 0;
int  nDbCountG = 0;
bool fVerboseG = false;
bool fDebugModeG = false;
bool fChopStdinFieldsG=false;
map<string,string> sourceMapG;
int  nMaxColumnWidthG = 0;
map<string,string> fieldDataTypeMapG;
int nRequiredFieldCountG = 0;
bool fCombineImportedTablesG = false;
int nNumberOfFirstUnnamedFieldG = -1;

void printError(const char *zSql=NULL)
{
    if(zSql)
        fprintf(stderr, "%s - \"%s\"\n", sqlite3_errmsg(dbHandleG), zSql);
    else
        fprintf(stderr, "%s\n", sqlite3_errmsg(dbHandleG));
}

void checkReturn(int rc, const char *zSql=NULL)
{
    if ( rc != SQLITE_OK) {
        printError(zSql);
        closeDatabase();
        exit(1);
    }
}

bool isPipe(const char *zFilename)
{
    struct stat sb;
    if ( stat(zFilename, &sb) == -1 ) {
        //file not found
        return false;             
    }
    return S_ISFIFO(sb.st_mode); 
}

class Statement
{
    sqlite3_stmt *stmtHandleM;
    const char *zTailM;

    vector<int> vColWidthsM;

    bool isFieldNumeric(int i)
    {
        int type = sqlite3_column_type(stmtHandleM, i);
        return type == SQLITE_FLOAT || type == SQLITE_INTEGER;
    }

    //returns actual characters instead of bytes
    int strlen_utf8(const char *s)
    {
        int i=0,j=0;
        while (s[i]) {
            if ((s[i] & 0xC0) != 0x80)
                j++;
            i++;
        }
        return j;
    }

    char *getFieldValue(int i)
    {
        char* zValue = (char*) sqlite3_column_text(stmtHandleM, i);
        if(zValue!=NULL) {
            zValue = trimWhiteSpace(zValue);
            if(nMaxColumnWidthG > 0) {
                int len     = (int)strlen_utf8(zValue);
                int nameLen = strlen_utf8(sqlite3_column_name(stmtHandleM, i));
                int width   = nMaxColumnWidthG>nameLen?nMaxColumnWidthG:nameLen;

                if(len>width) {
                    zValue[width-1] = '>';
                    zValue[width-0] = 0;
                }
            }
        }
        return zValue;
    }

    void updateColWidths()
    {
        vector<int> colWidths;
        for(int i=0; i<sqlite3_column_count(stmtHandleM);i++) {
            char* zValue = getFieldValue(i);

            int nWidth = 0;
            if(zValue!=NULL) {
                nWidth = strlen_utf8(zValue);
                if (nWidth > nMaxColumnWidthG && nMaxColumnWidthG > 0) {
                    nWidth = nMaxColumnWidthG;
                }

            } else {
                nWidth = 6; //"(null)" = 6 chars.
            }
            if((int)vColWidthsM.size() > i) {
                if(vColWidthsM[i] < nWidth) {
                   vColWidthsM[i] = nWidth;
                }
            } else {
                vColWidthsM.push_back(nWidth);
            }
        }
    }


    void printDelimitedLine(bool fPad, int nMaxWidth)
    {
        int nTotalWidth = 0;
        for(int i=0; i < sqlite3_column_count(stmtHandleM) &&
                nTotalWidth + GetColumnWidth(i) < nMaxWidth; i++) {
            if(i>0) {
                if(fPad)
                    fprintf(stderr,"%c", '|');
                else
                    printf("%c", fieldSeperatorG);
            }
            char *zValue = getFieldValue(i);
            bool  fNumeric = isFieldNumeric(i);
            nTotalWidth += PrintField(i, zValue, fPad, fNumeric);
        }
        if(fPad) 
            fprintf(stderr,"\n");
        else
            printf("\n");
    }

    int GetColumnWidth(int i)
    {
        if(i>=0 && i<(int)vColWidthsM.size()) {
            return vColWidthsM[i];
        }
        return 0;
    }

    bool DefaultColumnName(const char* cname) {
        if (cname) {
            if (*cname++ == 'c' ) {
                if (isdigit(*cname++)) {
                    while (*cname) {

                        if (! isdigit(*cname++) ) {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    bool IsPrintableColumnNames(vector<string>& columnNames)
    {
        if(columnNames.size()==0) return false;
    
        bool fCustomNames=false;
        for(unsigned i=0; i<columnNames.size(); i++) {
            if(!DefaultColumnName(columnNames[i].c_str())) {
                fCustomNames=true;
                break;
            }
        }
        if(fAutoDetectTitlesG)
            return fCustomNames;
        else
            return fHasColumnNamesG;
    }

    int PrintField(int i, const char *zValue, bool fPad, bool fNumeric) 
    {
        if(zValue==NULL) {
            zValue = "(null)";
        }
        if (fPad) {
            int len = strlen_utf8(zValue);
            int width = GetColumnWidth(i);
            if ( ! fNumeric ) {
                fprintf(stderr,"%s", zValue);
            }
            fprintf(stderr,"%*s", width - len, "");
            if ( fNumeric ) {
                fprintf(stderr,"%s", zValue);
            }
            return width+1;
        } else {
            printf("%s", zValue);
        }
        return 0;
    }

    void FirstPassThru()
    {
        vector<string> columnNames;
        GetColumnNames(columnNames);
        for(unsigned i=0; i<columnNames.size(); i++) {
            vColWidthsM.push_back(strlen_utf8(columnNames[i].c_str()));
        }

        int status = sqlite3_step(stmtHandleM);
        while(status == SQLITE_ROW)
        {
            updateColWidths();
            status = sqlite3_step(stmtHandleM);
        }

        if(status != SQLITE_DONE)
        {
            printError(sqlite3_sql(stmtHandleM));
            closeDatabase();
            exit(1);
        }
        sqlite3_reset(stmtHandleM);
    }

public:

    Statement(const char *zSql) {
        checkReturn ( sqlite3_prepare_v2(dbHandleG, zSql, strlen(zSql), &stmtHandleM, &zTailM), zSql );
    }
    ~Statement() {
        checkReturn ( sqlite3_finalize(stmtHandleM) );
    }

    void BindParameter(int nColumn, const char *zText) {
        if (strcmp(zText,"(null)")==0) {
            checkReturn ( sqlite3_bind_null(stmtHandleM, nColumn) );
        } else {
            checkReturn ( sqlite3_bind_text(stmtHandleM, nColumn, zText, -1, SQLITE_STATIC) );
        }
    }

    void GetColumnNames(vector<string>& columns) {

        for(int i=0; i< sqlite3_column_count(stmtHandleM);i++) {
            const char *zValue = sqlite3_column_name(stmtHandleM, i);
            if(strchr(zValue,' ')!=NULL||strchr(zValue,':')!=NULL) {
                columns.push_back( string("\"")+ zValue + "\"" );
            } else {
                columns.push_back(zValue);
            }
        }

    }

    void ExecuteQuery(bool fSample)
    {
        //get width of console
        struct winsize w;
        ioctl(STDERR_FILENO, TIOCGWINSZ, &w);

        bool fPad     = fSample;
        int  maxWidth = fSample?w.ws_col:30000;

        if(fPad) {
            FirstPassThru();
        }

        vector<string> columnNames;
        GetColumnNames(columnNames);
        if(IsPrintableColumnNames(columnNames) || fPad)  {
            int nTotalWidth = 0;
            int nCnt = 0;
            for(unsigned i=0; i < columnNames.size() && nTotalWidth + GetColumnWidth(i) < maxWidth; i++) {
                if(i>0) {
                    if(fPad)
                        fprintf(stderr,"%c",'|');
                    else
                        printf("%c",fieldSeperatorG);
                }
                nTotalWidth += PrintField(i, (char*)columnNames[i].c_str(), fPad, 0);
                nCnt++;
            }
            if(fPad) {
                fprintf(stderr,"\n");
                for(int i=0; i < nCnt; i++) {
                    if(i>0) fprintf(stderr,"%c", '|');
                    int width = GetColumnWidth(i);
                    string hyphens = string(width, '-');
                    fprintf(stderr,"%s", hyphens.c_str());
                }
                fprintf(stderr,"\n");
            } else {
                printf("\n");
            }
        }

        int status = sqlite3_step(stmtHandleM);
        while(status == SQLITE_ROW)
        {
            printDelimitedLine(fPad, maxWidth);
            status = sqlite3_step(stmtHandleM);
        }

        if(status != SQLITE_DONE)
        {
            printError(sqlite3_sql(stmtHandleM));
            closeDatabase();
            exit(1);
        }
        sqlite3_reset(stmtHandleM);
    }

    void Execute()
    {
        int status = sqlite3_step(stmtHandleM);
        while(status == SQLITE_ROW)
        {
            status = sqlite3_step(stmtHandleM);
        }
        if(status != SQLITE_DONE)
        {
            printError(sqlite3_sql(stmtHandleM));
            closeDatabase();
            exit(1);
        }
        sqlite3_reset(stmtHandleM);
    }

    bool FetchString(string* pString)
    {
        int status = sqlite3_step(stmtHandleM);
        if(status == SQLITE_ROW)
        {
            const char* zVal = (const char*) sqlite3_column_text(stmtHandleM, 0);
            if ( zVal == NULL )
            {
                zVal = "(null)";
            }
            *pString = zVal;
            return true;
        }
        return false;
    }

    bool FetchNumbers(int nValueArray[], int nCount) {
        int status = sqlite3_step(stmtHandleM);
        if(status == SQLITE_ROW)
        {
            for(int f=0;f<nCount;f++) {
                nValueArray[f] = sqlite3_column_int(stmtHandleM, f);
            }
            return true;
        }
        return false;
    }

    const char *NextSql()
    {
        return zTailM;
    }
};


void closeDatabase() 
{
    if (dbHandleG) {
        int rc = sqlite3_close(dbHandleG);
        if (rc!=SQLITE_OK)
            printError();
        dbHandleG=NULL;
    }
}


void initializeDatabase(const char *zDbPath=":memory:") 
{
    closeDatabase();
    if (fVerboseG) {
        fprintf(stderr,"initializing database: %s\n", zDbPath);
    }
    checkReturn( sqlite3_open(zDbPath, &dbHandleG) );
    checkReturn( sqlite3_enable_load_extension(dbHandleG, 1) );
}


void getColumnNames (const char *zTable, vector<string>& columns)
{
    char zBuffer[1024];
    snprintf(zBuffer,1024, "SELECT * FROM \"%s\"", zTable);
    Statement stmt (zBuffer);
    stmt.GetColumnNames(columns);
}

int getRowCount (const char *zTable)
{
    char zBuffer[1024];
    snprintf(zBuffer,1024,"SELECT COUNT(*) FROM \"%s\"", zTable);
    Statement stmt (zBuffer);
    int nVal[1] = {0};
    stmt.FetchNumbers(nVal,1);
    return nVal[0];
}

void getTableNames (vector<string>& tables)
{
    for(int i = 1; i <=  nDbCountG; i++) {
        char zCmd[256];
        sprintf(zCmd,"SELECT name FROM DATA%d.sqlite_master "
                     "WHERE type='table' order by rootpage", i);
        Statement stmt(zCmd);
        string name;
        while(stmt.FetchString(&name)) {
            tables.push_back(name);
        }
    }

    Statement stmt("SELECT name, 0 as sortorder, rootpage FROM sqlite_master "
                   "WHERE type='table' "
                   " UNION ALL "
                   "SELECT name, 1 as sortorder, rootpage FROM sqlite_temp_master "
                   "WHERE type='table' "
                   "ORDER BY sortorder, rootpage");
    string name;
    while(stmt.FetchString(&name)) {
        tables.push_back(name);
    }
}

string getLastTable()
{
    vector<string> tables;
    getTableNames(tables);
    if(tables.size()>0) return tables.back();
    else                return "";
}

void runCommand(string sql, bool printCmd=true)
{
    const char *zHead = sql.c_str();

    //don't print debug info for these commands
    bool isTrans = streq(zHead,"BEGIN TRANSACTION") || streq(zHead,"END TRANSACTION");
    if(fVerboseG && printCmd && !isTrans) {
        fprintf(stderr, "runCommand: %s\n", zHead);
    }

    struct timeval  tv1, tv2;
    if ( fDebugModeG ) {

        if ( strlen(zHead)<1000 && !isTrans) {
            char zPlan[1024];
            sprintf(zPlan,"EXPLAIN QUERY PLAN %s", zHead);
            fprintf(stderr,"-------[Query Plan]-------\n");
            Statement stmt(zPlan);
            stmt.ExecuteQuery(true);
            fprintf(stderr,"--------------------------\n");
        }

        gettimeofday(&tv1, NULL);
    }

    while(strlen(zHead) > 0) {

        Statement stmt(zHead);
        stmt.Execute();
        zHead = stmt.NextSql();
    }

    if(fVerboseG) {
        char s[16] = {};
        strncpy(s, sql.c_str(), 10);
        s[6] = 0;
        strupr(s);
        if (strncmp(s,"UPDATE",6)==0||
                strncmp(s,"DELETE",6)==0||
                strncmp(s,"INSERT",6)==0) {
            int affectedRows = sqlite3_changes(dbHandleG);
            fprintf(stderr, " (affected rows: %d)\n", affectedRows);
        }
    }

    if ( fDebugModeG && !isTrans ) {
        gettimeofday(&tv2, NULL);
        fprintf(stderr," (took %f seconds)\n",
                    (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
                    (double) (tv2.tv_sec - tv1.tv_sec));
    }
}

void runQuery(string sql, bool fSample=false)
{
    const char *zHead = sql.c_str();

    if(fVerboseG) {
        fprintf(stderr, "runQuery: %s\n", zHead);
    }

    struct timeval  tv1, tv2;
    if ( fDebugModeG ) {

        if ( strlen(zHead)<1000 ) {
            char zPlan[1024];
            sprintf(zPlan,"EXPLAIN QUERY PLAN %s", zHead);
            fprintf(stderr,"-------[Query Plan]-------\n");
            Statement stmt(zPlan);
            stmt.ExecuteQuery(true);
            fprintf(stderr,"--------------------------\n");
        }

        gettimeofday(&tv1, NULL);
    }

    while(strlen(zHead) > 0) {

        Statement stmt(zHead);
        stmt.ExecuteQuery(fSample);
        zHead = stmt.NextSql();
    }

    if ( fDebugModeG ) {
        gettimeofday(&tv2, NULL);
        fprintf(stderr," (took %f seconds)\n",
                    (double) (tv2.tv_usec - tv1.tv_usec) / 1000000 +
                    (double) (tv2.tv_sec - tv1.tv_sec));
    }
}

string intToText(int num)
{
    char buf[128];
    sprintf(buf,"%d",num);
    return buf;
}

string int64ToText(long long num)
{
    char buf[128];
    sprintf(buf,"%lld",num);
    return buf;
}

//simply check if obvious number for now
bool isNumeric(const char *s)
{
    if(s && *s)
    {
        bool fFoundDecimal=false;
        int  nLen=0;
        if (*s == '-') {
            s++;
        }
        while(*s)
        {
            char ch = *s++;
            if(ch == '.' && !fFoundDecimal) {
                fFoundDecimal = true;
                continue;
            }
            if(!isdigit(ch)) 
                return false;
            //assume integer not greater than billion
            //so TSNs don't get cast to strange numbers
            if (++nLen>9 && !fFoundDecimal) {
                return false;
            }
        }
        return true;
    }
    return false;
}

bool needQuotes(string& colName)
{
    //already quoted..dont need quotes
    if (colName.size()>0 && 
            colName[0]=='\"' &&
            colName[colName.size()-1]=='\"') {
        return false;
    }

    //ignore leading spaces
    unsigned i=0;
    while(i < colName.size() - 1) {
        if ( colName[i] != ' ' ) break;
        i++;
    }

    //if invalid chars..need quotes
    for(; i<colName.size();i++) {
        char ch = colName[i];
        bool isValid = ch=='_' || isalpha(ch) || (i>0&&isalnum(ch));
        if(!isValid) return true;
    }

    //otherwise no quotes needed
    return false;
}

void createTable(int columns, vector<string>& columnNames, 
                 vector<const char*>& sampleValues, bool recreate, 
                 string description="")
{
    if ( recreate )
        runCommand("DROP TABLE t"+intToText(nTableCountG));
    else
        nTableCountG++;

    string newTable = "t"+intToText(nTableCountG);
    if (description.size()>0) {
        sourceMapG[newTable] = description;
    }

    if (columns < nRequiredFieldCountG) {
        columns = nRequiredFieldCountG;
    }
    
    string ddl = "CREATE TABLE t"+intToText(nTableCountG)+" (";
    for(int i=0; i<columns; i++)
    {
        if (i>0) ddl+=",";
        string dataType = " TEXT";
        if(fAutoDetectDataTypesG && sampleValues.size() > (unsigned) i &&
            isNumeric(sampleValues[i]))
        {
            dataType = " NUMERIC";
        }
        
        if(columnNames.size() > (unsigned) i) {
            //if we explicit specified this datatype then just use it
            if (fieldDataTypeMapG.count(columnNames[i])) {
                dataType = " " + fieldDataTypeMapG[columnNames[i]];
            }
            if (needQuotes(columnNames[i])) {
                ddl += "\""+columnNames[i]+"\""+dataType;
            } else {
                ddl += columnNames[i] + dataType;
            }
        } else {
            if (fieldDataTypeMapG.count(intToText(i+1))) {
                dataType = " " + fieldDataTypeMapG[intToText(i+1)];
            }
            if ( nNumberOfFirstUnnamedFieldG == -1 ) {
                ddl += string("c")+intToText(i+1)+dataType;
            } else {
                ddl += string("c")+intToText(i-columnNames.size()+nNumberOfFirstUnnamedFieldG)+dataType;
            }
        }
    }
    ddl+=")";

    runCommand(ddl.c_str());
}

class Insertor
{
    int nColumnsM;
    Statement *pInsertStmtM;
public:
    Insertor() : pInsertStmtM(NULL)
    {
    }
    Insertor(int tableNumber) : pInsertStmtM(NULL)
    {
        ConnectTo(tableNumber);
    }
    ~Insertor() {
        Disconnect();
    }

    void Disconnect()
    {
        if (pInsertStmtM) {
            delete pInsertStmtM;
            pInsertStmtM = NULL;
        }
    }

    void ConnectTo(int tableNumber)
    {
        Disconnect();

        string tableName = "t"+intToText(tableNumber);

        vector<string> columnNames;
        getColumnNames(tableName.c_str(),columnNames);
        nColumnsM = columnNames.size();

        string ddl1 = "INSERT INTO "+tableName+" (";
        string ddl2 = " VALUES (";

        for(unsigned i=0;i<columnNames.size();i++) {
            if(i>0) {
                ddl1 += ",";
                ddl2 += ",";
            }
            if(needQuotes(columnNames[i])) {
                ddl1+="\""+columnNames[i]+"\"";
            } else {
                ddl1+=columnNames[i];
            }
            ddl2+="?";
        }
        ddl1+=")";
        ddl2+=")";

        pInsertStmtM = new Statement((ddl1+ddl2).c_str());
    }
    
    void InsertRow(vector<const char *>& values)
    {
        for(int i=0; i<nColumnsM && i<(int)values.size(); i++) {
            pInsertStmtM->BindParameter(i+1, values[i]);
        }
        for(int i=(int)values.size();i<nColumnsM;i++) {
            pInsertStmtM->BindParameter(i+1, "");
        }

        pInsertStmtM->Execute();
    }
};


// A simple tokenizer.  It expects to take apart a line that consists of a
// bunch of token separated fields.
class EscapingTokenizer {
  public:
    // Create a tokenizer that will take apart zBuffer.  zBuffer will be
    // destroyed during tokenization.
    EscapingTokenizer(char *zBuffer,int escapeQuotedValues,bool escapeCurlyBrackets,bool escapeBackslashes) : 
            zLineM(zBuffer), fEscapeQuotedValuesM(escapeQuotedValues), fEscapeCurlyBracketsM(escapeCurlyBrackets), 
            fEscapeBackslashesM(escapeBackslashes) {
    }

    char *zLineM;
    int  fEscapeQuotedValuesM;
    bool fEscapeCurlyBracketsM;
    bool fEscapeBackslashesM;

    // Parse out a string.  fieldSep is the next field separator.
    // Returns NULL if no data is left.
    char *StringToken(char fieldSep = 0) {
        if (*zLineM == 0) {
            return NULL;
        }
        char *s = zLineM;
        char *zAnswer = zLineM;
        while (*s != fieldSep && *s != 0) {

            //escape double quotes.. if not doing the other style '\' escaping
            if ( s[0] == '\\' && s[1] == '"' && !fEscapeBackslashesM) {
                memmove(s, s+1, strlen(s));
            }
            //if '-l' then avoid tokenizing values within curly brackets
            if(fEscapeCurlyBracketsM && *s == '{' )
            {
                while(*s != '}' && *s != 0)
                {
                    s++;
                }
            }
            //if '-e' then avoid tokenizing values within quotes
            else if(fEscapeQuotedValuesM && *s == '"')
            {
                s++;

                if (fEscapeQuotedValuesM==2) {
                    zAnswer = s;
                }

                while(*s != '"' && *s != 0)
                {
                    s++;
                    //possible to have quotes inside quotes if escaped
                    if ( s[0] == '\\' && s[1] == '"' ) {
                        s++;
                        s++;
                    }
                }
                if(*s=='"') 
                {
                    if (fEscapeQuotedValuesM==2) {
                        *s=0;
                        zAnswer = trimWhiteSpace(zAnswer);
                    }

                    s++;
                }
            }
            //if '-b' then avoid tokenizing values within backslashes
            else if(fEscapeBackslashesM && *s == '\\')
            {
                s++;
                while(*s != '\\' && *s != 0)
                {
                    s++;
                }
                if(*s=='\\') 
                {
                    s++;
                }
            }
            else
            {
                s++;
            }
        }
        if (*s != 0) {
            *s = 0;
            s++;
        }
        zLineM = s;

        return zAnswer;
    }
};


void tokenizeBuffer(char *zText, int nTextLen, char fieldSeperator, vector<const char *>& tokenPtrs)
{
    tokenPtrs.clear();

    //tokenizer alters the text so do this beforehand (handle "a,b,c,," case)
    bool fAddTailNull = false;
    if( nTextLen > 0 ) {
        if(zText[nTextLen-1] == fieldSeperator) {
            fAddTailNull = true;
        }
    }

    EscapingTokenizer tokenizer(zText, fEscapeQuotedValuesG, fEscapeCurlyBracketsG, fEscapeBackslashesG);
    char *zToken = tokenizer.StringToken(fieldSeperator);
    while(zToken != NULL) {
        tokenPtrs.push_back(zToken);
        zToken = tokenizer.StringToken(fieldSeperator);
    }
   
    if( fAddTailNull ) {
        tokenPtrs.push_back("");
    }
}

void tokenizeBuffer(char *zText, int nTextLen, vector<const char *>& tokenPtrs)
{
    if(fAutoDetectSepG) {

        nTextLen = 0;
        //auto-detect delimiter
        fieldSeperatorG = '|';

        int nPipes=0;
        int nCommas=0;
        int nSpaces=0;
        int nTabs=0;
        char *zPtr = zText;
        while(*zPtr) {
            switch(*zPtr)
            {
              case '|': nPipes++; break;
              case ',': nCommas++; break;
              case ' ': nSpaces++; break;
              case '\t': nTabs++; break;
            }
            zPtr++;
            nTextLen++;
        }

        if(nPipes == 0) {

            if(nSpaces > 0) fieldSeperatorG = ' ';
            if(nCommas > 0) fieldSeperatorG = ',';
            if(nTabs   > 0) fieldSeperatorG = '\t';
        }
        else
        {
            if(nCommas > nPipes) {
                fieldSeperatorG = ',';
            }
        }

    } else {
        if(nTextLen<0) {
            nTextLen = strlen(zText);
        }
    }
    tokenizeBuffer(zText, nTextLen, fieldSeperatorG, tokenPtrs);
}

void copyTokens(vector<const char *>& tokenPtrs, vector<string>& tokens)
{
    tokens.clear();
    for(unsigned i=0; i<tokenPtrs.size(); i++) tokens.push_back(tokenPtrs[i]);
}

void tokenizeString(const string& text, char fieldSeperator, vector<string>& tokens)
{
    char zBuffer[1024];
    snprintf(zBuffer, 1024, "%s", text.c_str());
    vector<const char *> tokenPtrs;
    tokenizeBuffer(zBuffer, text.size(), fieldSeperator, tokenPtrs);
    copyTokens(tokenPtrs, tokens);
}

void tokenizeString(const string& text, vector<string>& tokens)
{
    char zBuffer[1024];
    snprintf(zBuffer, 1024, "%s", text.c_str());
    vector<const char *> tokenPtrs;
    tokenizeBuffer(zBuffer, text.size(), tokenPtrs);
    copyTokens(tokenPtrs, tokens);
}

bool isTitles(vector<const char *>& tokenPtrs)
{
    if (fAutoDetectTitlesG) {
        
        for(unsigned i=0; i<tokenPtrs.size(); i++) {
            char ch = tokenPtrs[i][0];
            //ignore leading spaces
            int p=0;
            while ( ch == ' ' && tokenPtrs[i][p]) {
                p++;
                ch = tokenPtrs[i][p];
            }
            if (! (isalpha ( ch ) || ch=='"' ) )
                return false;
        }
        return true;

    } else {
        return fHasColumnNamesG;
    }
}

void importStream(FILE* pIn, const char* zName)
{
    if(fVerboseG) {
        fprintf(stderr, "importTable: (%s)\n", zName);
    }

    runCommand("BEGIN TRANSACTION");

    Insertor insertor;
    
    int i=0,maxColumns=1;
    vector<string> columnNames;
    vector<const char *> tokenPtrs;

    string tableDesc = zName;
    
    char zBuffer[8192]={0};
    while (fgets(zBuffer, sizeof(zBuffer), pIn) != NULL) {
       
        int nLen = strlen(zBuffer);

        //trim cr/nl
        char *zTail = zBuffer + nLen;

        while (zTail > zBuffer && (zTail[-1] == '\n' || zTail[-1] == '\r')) {
            zTail--;
            *zTail = 0;
            nLen--;
        }
        
        //if blank skip line
        if (zBuffer[0]==0) continue;

        //if line begins with '@' then is start of another table
        if (zBuffer[0]=='@') {
            if (zBuffer[1]) {
                tableDesc = zBuffer+1;
            }
            i=0;
            continue;
        }

        //save original line for error messages
        char zLine[8192];
        memcpy(zLine, zBuffer, nLen+1);

        tokenizeBuffer(zBuffer, nLen, tokenPtrs);

        if (i==0) {
            if(isTitles(tokenPtrs)) {
                copyTokens(tokenPtrs, columnNames);
            }
            
            maxColumns = (int)tokenPtrs.size();

            if(fCombineImportedTablesG && nTableCountG>0) {
                
                //check if union is valid.. tables must have same structure
                string lastTable = "t"+intToText(nTableCountG); 
                if ( fVerboseG ) {
                    fprintf(stderr, "Union '%s' with '%s' (table: %s)\n", zName,
                            sourceMapG[lastTable].c_str(),
                            lastTable.c_str());
                }
                vector<string> lastColNames;
                getColumnNames(lastTable.c_str(), lastColNames);
                if ( lastColNames.size() < columnNames.size()) {
                    fprintf(stderr, "Table has more fields than first: %s\n", zName);
                    exit(1);
                } else {
                    for(int i=0; i< (int) columnNames.size(); i++)
                    {
                        if (lastColNames[i] != columnNames[i]) {
                            fprintf(stderr, "Table has different fields than first: %s (%s != %s)\n", 
                                    zName,
                                    lastColNames[i].c_str(),
                                    columnNames[i].c_str());
                            exit(1);
                        }
                    }
                }

            } else {
                createTable(maxColumns, columnNames, tokenPtrs, false, tableDesc);
            }
            insertor.ConnectTo(nTableCountG);
            if(columnNames.size()==0) {    
                insertor.InsertRow(tokenPtrs);
            }
        } else {
            int tokenCnt = (int)tokenPtrs.size();
            if (tokenCnt > maxColumns && tokenCnt > nRequiredFieldCountG) {

                if (fChopStdinFieldsG) {
                    tokenPtrs.erase (tokenPtrs.begin()+maxColumns, tokenPtrs.end());
                } else {
                    fprintf(stderr,"Too many fields in stdin line %d. First line had %d "
                           "but this line has %d.\nLine: %s\n",
                            i+1, maxColumns, (int) tokenPtrs.size(), zLine);
                    exit(1);
                }
            }
            //correct datatypes now that we have an actual data row
            if(i==1 && columnNames.size() > 0) {
                if (fCombineImportedTablesG && nTableCountG>0) {
                    //don't recreate!
                } else {
                    createTable(maxColumns, columnNames, tokenPtrs, true, tableDesc);
                }
            }
            insertor.InsertRow(tokenPtrs);
        }
        i++;

        if ( i % 100000 == 0 ) {
            if ( fVerboseG ) {
                fprintf(stderr, " (reading %s: %d records)\r", zName, i);
            }
            runCommand("END TRANSACTION", false);
            runCommand("BEGIN TRANSACTION", false);
        }
    }
    if ( fVerboseG ) {
        fprintf(stderr, " (reading %s: %d records)\n", zName, i);
    }

    runCommand("END TRANSACTION");

    //if no input rows.. create a dummy table with no rows
    if (i==0) {
        vector<string> notUsed;
        vector<const char*>  notUsed2;
        if (fCombineImportedTablesG && nTableCountG>0) {
            //don't create new dummy
        } else {
            createTable(1, notUsed, notUsed2, false, "empty"); //dummy table with 1 column
        }
    }
}

void importStdin()
{
    importStream(stdin,"stdin");
}

void importPipe(const char *zFilename)
{
    FILE *fp = fopen(zFilename, "r");
    if ( fp != NULL ) {
        importStream(fp, zFilename);
        fclose(fp);
    }
}


void importFile(const char *zFilename)
{
    if(fVerboseG) {
        fprintf(stderr,"importTable: %s\n", zFilename);
    }

    runCommand("BEGIN TRANSACTION");
    
    int maxColumnCount=1;
    vector<string> columnNames;
    vector<const char*>  tokenPtrs;
    
    {
        TextFileReader reader(zFilename);

        while (!reader.AtEof()) {
            char *zLine = reader.NextLine();

            tokenizeBuffer(zLine, -1, tokenPtrs);

            if(reader.LineNumber() == 1 && isTitles(tokenPtrs)) {
                copyTokens(tokenPtrs, columnNames);
            }

            if (maxColumnCount < (int)tokenPtrs.size())
                maxColumnCount = (int)tokenPtrs.size();
        }

        if ( fCombineImportedTablesG && nTableCountG > 0 ) {

            //check if union is valid.. tables must have same structure
            string lastTable = "t"+intToText(nTableCountG); 
            if ( fVerboseG ) {
                fprintf(stderr, "Union '%s' with '%s' (table: %s)\n", zFilename,
                        sourceMapG[lastTable].c_str(),
                        lastTable.c_str());
            }
            vector<string> lastColNames;
            getColumnNames(lastTable.c_str(), lastColNames);
            if ( lastColNames.size() < columnNames.size()) {
                fprintf(stderr, "Table has more fields than first: %s\n", zFilename);
                exit(1);
            } else {
                for(int i=0; i< (int) columnNames.size(); i++)
                {
                    if (lastColNames[i] != columnNames[i]) {
                        fprintf(stderr, "Table has different fields than first: %s\n", zFilename);
                        exit(1);
                    }
                }
            }

        } else {
            createTable(maxColumnCount, columnNames, tokenPtrs, false, zFilename);
        }
    }

    TextFileReader reader(zFilename);

    Insertor insertor(nTableCountG);
    int i=0;
    while (!reader.AtEof()) {
        char *zLine = reader.NextLine();

        if(reader.LineNumber() == 1 && columnNames.size() > 0) {
            continue;
        }

        //if blank skip
        if(zLine[0]==0) {
            continue;
        }

        int nLen = strlen(zLine);
        
        //trim cr/nl
        char *zTail = zLine + nLen;
        while (zTail > zLine && (zTail[-1] == '\n' || zTail[-1] == '\r')) {
            zTail--;
            *zTail = 0;
        }

        vector<const char*> values;
        tokenizeBuffer(zLine, nLen, values);

        insertor.InsertRow(values);

        i++;
        if ( i % 100000 == 0 ) {
            if ( fVerboseG ) {
                fprintf(stderr, "(reading %s: %d records)\r", zFilename, i);
            }
            runCommand("END TRANSACTION", false);
            runCommand("BEGIN TRANSACTION", false);
        }
    }
    if ( fVerboseG ) {
        fprintf(stderr, " (reading %s: %d records)\n", zFilename, i);
    }
    
    runCommand("END TRANSACTION");
}

//Five field formats:
// -x Name,Address,City
// -x 1,2,3
// -x 1-3
// -x 2-
// -x -2
void getArgumentFields(char *zArgument, vector<string>& vFields)
{
    vFields.clear();

    vector<string> vNames;
    getColumnNames (getLastTable().c_str(), vNames);

    vector<string> vSpecs;
    tokenizeString(zArgument, ',', vSpecs);

    for(unsigned i=0; i<vSpecs.size(); i++) {
        int nColNo = atoi(vSpecs[i].c_str());
        if(nColNo!=0) {

            vector<string> vRange;
            tokenizeString(vSpecs[i], '-', vRange);
            if(vRange.size()==2) {
               int nStart = atoi(vRange[0].c_str());
               int nEnd   = atoi(vRange[1].c_str());

               if (nStart == 0) {
                   nStart = 1;
               }
               if (nEnd == 0) {
                   nEnd =(int)vNames.size(); 
               }

               for(int n = nStart-1; n <= nEnd-1; n++) {
                   if (n < 0 || n >= (int)vNames.size()) {
                       usage(); 
                   }
                   vFields.push_back(vNames[n]);
               }
            } else {
                if (nColNo < 0 || nColNo > (int)vNames.size()) {
                    usage();
                }
                vFields.push_back(vNames[nColNo-1]);
            }
        } else {
            vFields.push_back(vSpecs[i]);
        }
    }
}
////////////////////////////////////////////////////////////////////////////

void checkArg(char *zArg)
{
    if (zArg == NULL || zArg[0] == '-') {
        usage();
    }
}

void onControlC(int s)
{
    fprintf(stderr,"\nterminated (signal %d).\n", s);
    exit(1);
}

int main(int argc, char *argv[])
{
    signal(SIGINT,  onControlC);
    signal(SIGQUIT, onControlC);

    if(argc==1) {
        usage();
    }

    initializeDatabase();
    bool fOutputData = false;
    for (int argindex = 1; argindex < argc; argindex++) {
        if (argv[argindex][0] == '-' && !streq(argv[argindex],"-")) {
	    char *argstr = argv[argindex] + 1;
            do {
                switch (argstr[0]) {
                  case 'F':
                    {
                    argindex++;
                    checkArg(argv[argindex]);
                    char *zDbPath = argv[argindex];
                    initializeDatabase(zDbPath);
                    //make it go as fast as it can go
                    runCommand("PRAGMA journal_mode=off;PRAGMA locking_mode=exclusive;PRAGMA synchronous=off");
                    runCommand("PRAGMA cache_size=100000");
                    break;
                    }

                  case 'A':
                    {
                    argindex++;
                    checkArg(argv[argindex]);
                    char *zDbPath = argv[argindex];
                    if ( access( zDbPath, R_OK ) != -1 ) {
                        char zBuf[2048];
                        sprintf(zBuf,"ATTACH DATABASE '%s' AS DATA%d", zDbPath, ++nDbCountG);
                        runCommand(zBuf);
                    } else {
                        printf("File not found: %s\n", zDbPath);
                    }
                    }
                    break;

                  case 'D':
                    {
                    initializeDatabase("");
                    //make it go as fast as it can go
                    runCommand("PRAGMA journal_mode=off;PRAGMA locking_mode=exclusive;PRAGMA synchronous=off");
                    runCommand("PRAGMA cache_size=100000");

                    //
                    // temporary data is stored in /tmp which usually is the
                    // best place. You want this to be a local disk but if
                    // short of space you can use the environment var to change
                    // this to a place with more space. Use this with caution.
                    //
                    char *zTempStoreDirectory = getenv("DB_TEMP_STORE_DIRECTORY");
                    if (zTempStoreDirectory != NULL &&
                            zTempStoreDirectory[0] != 0) {
                        if ( access( zTempStoreDirectory, W_OK ) != -1 ) {
                            char zCmd[1024];
                            sprintf(zCmd, "PRAGMA temp_store_directory = '%s'",
                                    zTempStoreDirectory);
                            runCommand(zCmd);
                        }
                    }
                    break;
                    }

                  case 'q':
                    {
                    argindex++;
                    checkArg(argv[argindex]);
                    char *zSql = argv[argindex];
                    //run query
                    runQuery(zSql);
                    fOutputData = false;
                    break;
                    }

                  case 'Q':
                    {
                    argindex++;
                    checkArg(argv[argindex]);
                    char *zSql = argv[argindex];
                    //run query
                    fprintf(stderr,"Query: %s\n", zSql);
                    runQuery(zSql,true);
                    fOutputData = false;
                    break;
                    }

                  case 'r':
                    {
                    argindex++;
                    checkArg(argv[argindex]);
                    char *zSql = argv[argindex];
                    //run non-query 
                    runCommand(zSql);
                    break;
                    }

                  case 'O':
                    {
                        argindex++;
                        checkArg(argv[argindex]);
                        char* zName = argv[argindex];
                        printf("@%s\n", zName);

                        //fall-thru to output
                    }

                  case 'o':
                    {
                    vector<string> vFields;
                    getArgumentFields(argstr+1, vFields);
                    argstr += strlen(argstr) - 1;

                    string sqlFields;
                    if (vFields.size() > 0) {
                        for(unsigned i=0; i<vFields.size(); i++) {
                            if(i>0) {
                                sqlFields += ",";
                            }
                            sqlFields += vFields[i];
                        }
                    } else {
                        sqlFields = "*";
                    }

                    //run query
                    runQuery("select "+sqlFields+" from "+getLastTable());
                    fOutputData = false;
                    break;
                    }

                  case 'S':
                    {
                    vector<string> vFields;
                    getArgumentFields(argstr+1, vFields);
                    argstr += strlen(argstr) - 1;
                   
                    if(vFields.size() == 0) {
                        getColumnNames (getLastTable().c_str(), vFields);
                    }

                    string sqlFields;
                    for(unsigned i=0; i<vFields.size(); i++) {
                        sqlFields += ",SUM("+vFields[i]+")";
                    }

                    //run query
                    runQuery("select COUNT(*)"+sqlFields+" " 
                             "from "+getLastTable(), true);

                    fOutputData = false;
                    break;
                    }

                  case 'u':
                    {
                    vector<string> vFields;
                    getArgumentFields(argstr+1, vFields);
                    argstr += strlen(argstr) - 1;

                    string sqlFields;
                    if (vFields.size() > 0) {
                        for(unsigned i=0; i<vFields.size(); i++) {
                            if(i>0) {
                                sqlFields += ",";
                            }
                            sqlFields += vFields[i];
                        }
                    } else {
                        usage();
                    }

                    //run query
                    nTableCountG++;
                    runCommand("create table t"+intToText(nTableCountG)+" as "
                             "select "+sqlFields+", count(*) as cnt " 
                             "from "+getLastTable()+" group by "+sqlFields);

                    fOutputData = true;
                    break;
                    }

                  case 's':
                    {
                    vector<string> vFields;
                    getArgumentFields(argstr+1, vFields);
                    argstr += strlen(argstr) - 1;

                    string sqlFields;
                    if (vFields.size() > 0) {
                        for(unsigned i=0; i<vFields.size(); i++) {
                            if(i>0) {
                                sqlFields += ",";
                            }
                            sqlFields += vFields[i];
                        }
                    } else {
                        usage();
                    }

                    //run query
                    nTableCountG++;
                    runCommand("create table t"+intToText(nTableCountG)+" as "
                             "select * from "+getLastTable()+" order by "+sqlFields);

                    fOutputData = true;
                    break;
                    }

                  case 'i':
                    {
                    vector<string> vFields;
                    int len = strlen(argstr+1);
                    if (len>0) {
                        tokenizeString(argstr+1, ',', vFields);
                        argstr += strlen(argstr+1);
                    } else {
                        argindex++;
                        checkArg(argv[argindex]);
                        tokenizeString(argv[argindex], ',', vFields);
                    }
                                            
                    for(unsigned f=0;f<vFields.size();f++) {

                        vector<string> fieldSpec;
                        tokenizeString(vFields[f], '.', fieldSpec);

                        string table,field;
                        if (fieldSpec.size()==2) {
                            table = fieldSpec[0];
                            field = fieldSpec[1];
                        } else {
                            table = getLastTable();
                            field = fieldSpec[0];
                        }
                       
                        char zBuffer[1024];
                        snprintf(zBuffer,1024, "CREATE INDEX IF NOT EXISTS ix_%s_%s ON %s (%s)", 
                                table.c_str(), field.c_str(), table.c_str(), field.c_str());
                        runCommand(zBuffer);
                    }
                                                              
                    break;
                    }

                  case 'w':
                    {
                    int len = strlen(argstr+1);

                    string whereClause;
                    if(len>0) {
                        usage();
                    } else {
                        argindex++;
                        checkArg(argv[argindex]);
                        whereClause = argv[argindex];
                    }

                    //run query
                    nTableCountG++;
                    runCommand("create table t"+intToText(nTableCountG)+" as select * from "+getLastTable()+
                             " where "+whereClause);

                    fOutputData = true;
                    break;
                    }

                  case 'a':
                    {
                    vector<string> vTableNames;
                    getTableNames(vTableNames);

                    for(unsigned i=0; i<vTableNames.size(); i++) {
                        const char *zTable = vTableNames[i].c_str();
                        vector<string> colNames;
                        getColumnNames(zTable, colNames);
                        int rowCnt = getRowCount(zTable);
                        string source = sourceMapG[zTable];
                        fprintf(stderr,"TableName:%-8s Rows:%-4d Fields:%-2d   %s%s\n", 
                                zTable, rowCnt, (int)colNames.size(),
                                source.size()?"Source:":"", source.c_str());
                        runQuery("select * from "+vTableNames[i]+" limit 6", true);
                    }
                    fOutputData = false;
                    break;
                    }

                  case 'p':
                    {
                    vector<string> vFields;
                    getArgumentFields(argstr+1, vFields);
                    argstr += strlen(argstr) - 1;

                    string sqlFields;
                    if (vFields.size() > 0) {
                        for(unsigned i=0; i<vFields.size(); i++) {
                            if(i>0) {
                                sqlFields += ",";
                            }
                            sqlFields += vFields[i];
                        }
                    } else {
                        sqlFields = "*";
                    }

                    //run query
                    string table = getLastTable();
                    vector<string> colNames;
                    getColumnNames(table.c_str(), colNames);
                    int rowCnt = getRowCount(table.c_str());
                    string source = sourceMapG[table];
                    fprintf(stderr,"TableName:%-8s Rows:%-4d Fields:%-2d   %s%s\n", 
                            table.c_str(), rowCnt, (int)colNames.size(),
                            source.size()?"Source:":"", source.c_str());
                    runQuery("select "+sqlFields+" from "+table+" limit 30", true);
                    fOutputData = false;
                    break;
                    }

                  case 'm':
                    {            
                    int len = strlen(argstr+1);
                    if(len) {
                        nMaxColumnWidthG = atoi(argstr+1);
                        argstr+=len;
                    } else {
                        argindex++;
                        checkArg(argv[argindex]);
                        nMaxColumnWidthG = atoi(argv[argindex]);
                    }
                    break;
                    }

                  case 'd':

                    fAutoDetectSepG = false;
                    if(argstr[1] == '\"') {
                        argstr++;
                        argstr++;
                        fieldSeperatorG = argstr[0];
                        argstr++;
                    } else {
                        argstr++;
                        fieldSeperatorG = argstr[0];
                    }
                    if(fieldSeperatorG=='t') {
                        fieldSeperatorG = '\t';
                    }
                    break;

                  case 't':

                    fAutoDetectTitlesG = false;
                    fHasColumnNamesG = true;
                    break;

                  case 'n':

                    fAutoDetectTitlesG = false;
                    fHasColumnNamesG = false;
                    break;

                  case 'e':
                    fEscapeQuotedValuesG++;
                    break;

                  case 'l':
                    fEscapeCurlyBracketsG = true;
                    break;

                  case 'b':
                    fEscapeBackslashesG = true;
                    break;

                  case 'v':

                    if ( fVerboseG ) {
                        fDebugModeG = true;
                    } else {
                        fVerboseG = true;
                    }
                    break;

                  case 'T':

                    fAutoDetectDataTypesG = false;
                    break;

                  case 'M':
                    {
                    argindex++;
                    checkArg(argv[argindex]);
                    char *zFieldname = argv[argindex];
                    argindex++;
                    checkArg(argv[argindex]);
                    char *zDatatype = argv[argindex];
                    fieldDataTypeMapG[zFieldname] = zDatatype;
                    break;
                    }

                  case 'C':

                    fChopStdinFieldsG = true;
                    break;

                  case 'R':
                    {
                    argindex++;
                    checkArg(argv[argindex]);
                    nRequiredFieldCountG = atoi(argv[argindex]);
                    break;
                    }

                  case 'U':

                    fCombineImportedTablesG = true;
                    break;

                  case 'N':
                    {
                    argindex++;
                    checkArg(argv[argindex]);
                    nNumberOfFirstUnnamedFieldG = atoi(argv[argindex]);
                    break;
                    }

                  default:

                    fprintf(stderr,"Argument not recognized: %c\n", argstr[0]);
                    usage();
                }
                argstr++;
            } while (argstr[0] != 0);
        } else {
            //import table
            char *zFilename = argv[argindex];
            if (streq(zFilename,"-")) {
                importStdin();
            } else if ( isPipe( zFilename) ) {
                //read in single pass
                importPipe(zFilename);
            } else {
                importFile(zFilename);
            }
            fOutputData = true;
        }
    }

    if(fOutputData) {
        runQuery("select * from "+getLastTable());
    }

    closeDatabase();
    return 0;
}

