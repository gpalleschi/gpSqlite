#ifndef SQLITE3_FILE_IO_IMPORT_H
#define SQLITE3_FILE_IO_IMPORT_H

#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

typedef int typeFile;
typedef int fHeader;

/*
 * TYPES FILE MANAGED
 */

#define CSV_FILE 0
#define ASCII_FILE 1

#define FLAG_NO  0
#define FLAG_YES 1

/*
 * MESSAGE CODE
 */ 
 
#define E_OK   0
#define E_KO   1


/*
 * For Limits see http://www.sqlite.org/limits.html 
 */
 
 
 int sqlite3_import_file(sqlite3 *db, char *fileName, char *tableName, typeFile tF, fHeader fH, char *cpMsg  ) {
 
 


 
   return E_Ok; 
 } 