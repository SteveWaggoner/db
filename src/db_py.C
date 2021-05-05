#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include "db_py.h"

struct PythonFunction {

    PythonFunction(const char* name, PyObject* pyFuncObj, PythonFunction* next) {
        strcpy(this->name, name);
        this->pyFuncObj = pyFuncObj;
        this->next = next;
    }

    char name[64];
    PyObject* pyFuncObj;
    PythonFunction* next;
};

PythonFunction* pythonFunctionG = NULL;

PythonFunction* addPythonFunction(const char* name, PyObject* pyFunc) {
    pythonFunctionG = new PythonFunction(name, pyFunc, pythonFunctionG);
    return pythonFunctionG;
}

PythonFunction* findPythonFunction(const char* name) {
    PythonFunction* p = pythonFunctionG;
    while(p) {
        if (strcmp(p->name, name)==0) {
            return p;
        }
        p = p->next;
    }
    return NULL;
}


int isPyInitialized=0;
int pyModuleCount=0;

void ensurePythonInitialized(bool verbose) {

    if ( isPyInitialized == 0 ) {
        if (verbose) {
            fprintf(stderr,"starting python...\n");
        }
        Py_UnbufferedStdioFlag = 1;
        Py_Initialize();
        isPyInitialized = 1;
    }
}

void terminatePython(bool verbose) {

    if ( isPyInitialized == 1 ) {
        if ( verbose ) {
            fprintf(stderr,"stopping python...\n");
        }
        runPython(NULL, "import sys\nsys.stdout.flush()", false);
        Py_Finalize();
    }
}

int runPython(sqlite3 *db, const char* script, bool verbose) 
{
    ensurePythonInitialized(verbose);

    if ( verbose ) {
        fprintf(stderr, "Python:\n%s\n", script);
    }

    assert(script!=NULL);
    assert(strlen(script)<64000-2000);

    char zScriptBuffer[64000] = {0};
    char zModuleName[64] = {0};

    for(int i=0; i<pyModuleCount; i++) {
        sprintf(zModuleName,"from pymod%d import *\n", i+1);
        strcat(zScriptBuffer, zModuleName); 
    }
    strcat(zScriptBuffer, script);

    pyModuleCount++;
    sprintf(zModuleName,"pymod%d", pyModuleCount);

    return runPythonEx(db, zScriptBuffer, zModuleName);
}


PyObject* sqliteArgsToPythonTuple(int argc, sqlite3_value **argv) {

    PyObject* tuple = PyTuple_New(argc);

    for (int i=0; i<argc; i++) {
        switch(sqlite3_value_type(argv[i])) {

            case SQLITE_INTEGER:
                {
                int num = sqlite3_value_int(argv[i]);
                PyTuple_SetItem(tuple,i,Py_BuildValue("i", num));
                break; 
                }
  
            case SQLITE_FLOAT:
                {
                double num = sqlite3_value_double(argv[i]);
                PyTuple_SetItem(tuple,i,Py_BuildValue("d", num));
                break;
                }

            default: 
                {
                const unsigned char *text = sqlite3_value_text(argv[i]);
                PyTuple_SetItem(tuple,i,Py_BuildValue("s", text));
                break;
                }
        }
    }
  
    return tuple;
}

void pyObjectToSqliteResult(sqlite3_context *context, PyObject* pyVal) {
  
    if (PyUnicode_Check(pyVal)) {
        PyObject* pyStr = PyUnicode_AsEncodedString(pyVal, "utf-8", "Error ~");
        const char *pyCStr = PyBytes_AS_STRING(pyStr);
        sqlite3_result_text(context, pyCStr, -1, SQLITE_TRANSIENT);
 	    Py_XDECREF(pyStr);	
    }
    else if (PyLong_Check(pyVal)) {
        int num = PyLong_AsLong(pyVal);
        sqlite3_result_int(context, num);
    }
    else if (PyFloat_Check(pyVal)) {
        double num = PyFloat_AsDouble(pyVal);
        sqlite3_result_double(context, num);
    } else {
        sqlite3_result_null(context);
    }

}


static void callPythonFunction(sqlite3_context *context, int argc, sqlite3_value **argv)
{
 	PyObject *pReturn = NULL, *pArgs = NULL;
    PythonFunction* pyFunc = (PythonFunction*) sqlite3_user_data(context);

    // Call the function
    pArgs = sqliteArgsToPythonTuple(argc, argv);
    pReturn = PyObject_CallObject(pyFunc->pyFuncObj, pArgs);
 
    if (!pReturn || PyErr_Occurred())
    {
          PyErr_Print();
          return;
    }

    pyObjectToSqliteResult(context, pReturn);
 	
    Py_XDECREF(pArgs);	
 	Py_XDECREF(pReturn);	
}


PyObject *pLastModuleG = NULL;

int runPythonEx(sqlite3 *db, const char *script, const char* moduleName) 
{
 	PyObject *pModule = NULL, *pDict = NULL, *pFunc = NULL, *pCode = NULL, *pReturn = NULL;

 	/* Compile the script defined in the array script */
 	pCode = Py_CompileString(script, moduleName, Py_file_input);
 	if (!pCode || PyErr_Occurred())
 	{		
 		PyErr_Print(); 
 		return 0;
 	}

 	/* Import the code compiled */
 	pModule = PyImport_ExecCodeModule(moduleName, pCode);
 	if (!pModule || PyErr_Occurred())
 	{
 		PyErr_Print(); 
 		return 0;
 	}

    //remember for "-L ..."
    pLastModuleG = pModule;

 	/* Get the Dictionary of the module */
 	pDict = PyModule_GetDict(pModule);
 	if (!pDict || PyErr_Occurred())
 	{
 		PyErr_Print();
 		return 0;
 	}

    PyObject *key, *value;
    Py_ssize_t pos = 0;
    while(PyDict_Next(pDict, &pos, &key, &value)) {
      if(PyCallable_Check(value)) {
         PyObject* fc = PyObject_GetAttrString(value, "__code__");
         if(fc) {
            PyObject* fn = PyObject_GetAttrString(fc, "co_name");
            PyObject* ac = PyObject_GetAttrString(fc, "co_argcount");
            if(ac) {

               const int nArgs = PyLong_AsLong(ac);
               // we now have the argument count, do something with this function

               PyObject* fnStr = PyUnicode_AsEncodedString(fn, "utf-8", "Error ~");
               const char *fnCStr = PyBytes_AS_STRING(fnStr);

               PythonFunction *pPythonFunction = addPythonFunction(fnCStr, value);
               if ( db ) {
                sqlite3_create_function(db, fnCStr, nArgs, SQLITE_UTF8, pPythonFunction, &callPythonFunction, NULL, NULL);
               }

               Py_DECREF(fnStr);
            }
            Py_DECREF(ac);
            Py_DECREF(fn);
            Py_DECREF(fc);
         }
      }
    }

 	// Cleanup 
    /*
 	Py_XDECREF(pReturn);
 	Py_XDECREF(pCode);
 	Py_XDECREF(pFunc);
 	Py_XDECREF(pDict);
 	Py_XDECREF(pModule);	
 	Py_Finalize();
    */

    return 0;
}

#include "db.h"
void loadTableIntoPythonDict(sqlite3 *db, Statement *pStmt, const char* varName, const char* pkName, bool verbose) {

    ensurePythonInitialized(verbose);

    vector<string> colNames;
    vector<Variant> rowValues;

    PyObject* pDict = PyDict_New();

    api_getColumnNames(pStmt, &colNames);

    int pkOrd = -1;
    for(int i=0; i<colNames.size();i++) {
        if ( colNames[i] == pkName ) {
            pkOrd = i;
            break;
        }
    }

    int n=0;
    while(api_nextRow(pStmt, &rowValues)) {
        n++;

        int nCol = rowValues.size();
        PyObject* pRowDict = PyDict_New();

        for (int i=0; i<nCol; i++) {

            PyObject* pColName = Py_BuildValue("s", colNames[i].c_str());
            switch(rowValues[i].type) {

                case 'i':
                {
                PyDict_SetItem(pRowDict, pColName, Py_BuildValue("i", rowValues[i].num));
                break; 
                }
  
                case 'd':
                {
                PyDict_SetItem(pRowDict, pColName, Py_BuildValue("d", rowValues[i].real));
                break;
                }

                case 's': 
                {
                const char *text = rowValues[i].str;
                PyDict_SetItem(pRowDict, pColName, Py_BuildValue("s", text));
                break;
                }
            }
        }

        if ( pkOrd >= 0 ) {
            PyDict_SetItem(pDict, PyDict_GetItemString(pRowDict, pkName), pRowDict);
        } else {
            PyDict_SetItem(pDict, Py_BuildValue("i", n), pRowDict);
        }

    }

    if ( pLastModuleG == NULL ) {
        runPython(NULL,"",false);
    }
    PyObject_SetAttrString(pLastModuleG, varName, pDict);
}

void loadTableIntoPythonList(sqlite3 *db, Statement *pStmt, const char* varName, bool verbose) {

    ensurePythonInitialized(verbose);

    vector<Variant> rowValues;

    PyObject* pList = PyList_New(0);

    int n=0;
    while(api_nextRow(pStmt, &rowValues)) {
        n++;

        int nCol = rowValues.size();
        PyObject* tuple = PyTuple_New(nCol);

        for (int i=0; i<nCol; i++) {
            switch(rowValues[i].type) {

                case 'i':
                {
                PyTuple_SetItem(tuple,i,Py_BuildValue("i", rowValues[i].num));
                break; 
                }
  
                case 'd':
                {
                PyTuple_SetItem(tuple,i,Py_BuildValue("d", rowValues[i].real));
                break;
                }

                case 's': 
                {
                const char *text = rowValues[i].str;
                PyTuple_SetItem(tuple,i,Py_BuildValue("s", text));
                break;
                }
            }
        }

        PyList_Append(pList, tuple);
    }

    if ( pLastModuleG == NULL ) {
        runPython(NULL,"",false);
    }
    PyObject_SetAttrString(pLastModuleG, varName, pList);
}

