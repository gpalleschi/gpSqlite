/*************************************************************************
* C/C++ API interface call to import a file into an SQLite database
*
* Fehmi Noyan ISI - fnoyanisi@yahoo.com
*
* Copyright (c) 2012-2016, Fehmi Noyan ISI fnoyanisi@yahoo.com
* All rights reserved.
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
* 1. Redistributions of source code must retain the above copyright
*   notice, this list of conditions and the following disclaimer.
* 2. Redistributions in binary form must reproduce the above copyright
*   notice, this list of conditions and the following disclaimer in the
*   documentation and/or other materials provided with the distribution.
* THIS SOFTWARE IS PROVIDED BY Fehmi Noyan ISI ''AS IS'' AND ANY
* EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
* WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL Fehmi Noyan ISI BE LIABLE FOR ANY
* DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
* (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
* ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*************************************************************************/
#ifndef SQLITE3_FILE_IO_IMPORT_H
#define SQLITE3_FILE_IO_IMPORT_H

/* zstring.h and sqlite3.h should be in the INCLUDE PATH , -I for GCC and clang */
#include <zstring.h>
#include <sqlite3.h>

#include <stdio.h>
#include <string.h>

/*
* The limits here should comply with the sqlite3 defaults
* http://www.sqlite.org/limits.html
*/

/*************************************************************************
* Constants
*************************************************************************/
#define MAX_COLUMN_COUNT	2000		/* sqlite3 default is SQLITE_MAX_COLUMN = 2000
										 * set this constant according to your db design
										 */
#define ELEMENT_LENGTH		256			/* Max allowed char size, see notes above */
#define INSERT_PHRASE_LENGTH	ELEMENT_LENGTH+32	/* String size for "COLNAME TEXT(XXX)"
													 * statement
													 */
#define SQL_CMD_LENGTH		1000000	/* See SQLite3 limits SQLITE_MAX_SQL_LENGTH */
#define MAX_LINE_LENGTH		MAX_COLUMN_COUNT * (ELEMENT_LENGTH + 2)
#define DQUOTES  '\"'

int sqlite3_file_io_import_verbose (sqlite3 *db, char *table, char *fp, const char *sep, sqlite3_file_io_verbose *p) {
	char *element;						/* Pointer for a single element */
	char *bad_chars=".`~*\\?=)(/&%+^'!|}][{$#@\t\n"; /* list of characters to remove */
	char line[MAX_LINE_LENGTH];			/* Array to store each line read from the import file */
	char sql_cmd[SQL_CMD_LENGTH];		/* SQL Command string */
	char buf[INSERT_PHRASE_LENGTH];	    /* Phrase for SQL INSERT's TEXT(XXX) statement */
	char dquotes[2];                    /* Character string for DQUOTES - to be passed zstring_remove_chr() */
	int cnt, j, i;						/* Loop counters */
	int in_field=0;                     /* Logical test */
	FILE *fd;							/* File descriptor for the import file */
	sqlite3_stmt *stmt;					/* sqlite3 statement */

    /* Reset the line number for each transaction */
	if(p)
        p->line_no=1;

    element = (char *)malloc(ELEMENT_LENGTH);
    dquotes[0] = DQUOTES;
    dquotes[1] = '\0';

	/* Do all these stuff within a transaction. This makes it much more faster */
	if(sqlite3_exec(db,"BEGIN TRANSACTION", NULL, NULL, NULL) != SQLITE_OK){
        if (p)
            sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR   >\t\t%s\n",sqlite3_errmsg(db));
        return sqlite3_errcode(db);
    }
    if(p)
        sqlite3_file_io_verbose_print(p, WO_LINE,"\n\nSQL CMD >\t\tBEGIN TRANSACTION\n");

	if ((fd=fopen(fp,"r"))==NULL){
        if(p)
            sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR fopen() >\t\tcannot open the import file\n");
		return -1;
	}

	if(p)
        sqlite3_file_io_verbose_print(p, WO_LINE,"MESSAGE >\t\tImport file opened.\n");

	if (fgets(line,sizeof(line),fd)==NULL){
        if(p)
            sqlite3_file_io_verbose_print(p, WO_LINE, "ERROR fgets() >\t\tcannot read the line from the import file\n");
        fclose(fd);
		return -2;
	}

	if(p)
        sqlite3_file_io_verbose_print(p, W_LINE, "%s", line);

	/*
	* Count number of separator characters within the first line
	* Ignore any separators inside double-qoutes
	*/
	cnt = 0;
	for (j=0 ;line[j]; j++)
        if (line[j] == DQUOTES && in_field == 0)
            in_field = 1;
        else if (line[j] == DQUOTES && in_field == 1)
            in_field = 0;
		else if (line[j] == sep[0] && in_field == 0)
			cnt++;

	if ((cnt+1) > MAX_COLUMN_COUNT)
		cnt = MAX_COLUMN_COUNT;

	/* Create SQL table */
	if (!zstring_remove_chr(line,bad_chars)){
        if (p)
            sqlite3_file_io_verbose_print(p, WO_LINE, "ERROR zstring_remove_chr() >\t\t%s\n",line);
        fclose(fd);
		return -3;
	}

	/* make sure memory is clear */
	memset(sql_cmd,'\0',MAX_LINE_LENGTH);
	sprintf(sql_cmd,"CREATE TABLE %s (\"",table);
	sprintf(buf,"\" TEXT(%d),",ELEMENT_LENGTH);

	/* Populate table column names */
	element = zstring_strtok_dquotes(line,sep,DQUOTES);
	strncat(sql_cmd,zstring_remove_chr(element,dquotes),ELEMENT_LENGTH);
	strcat(sql_cmd,buf);

	for(j=1; j<cnt; j++){
		strcat(sql_cmd,"\"");
		element = zstring_strtok_dquotes(NULL,sep,DQUOTES);
		strncat(sql_cmd,zstring_remove_chr(element,dquotes),ELEMENT_LENGTH);
		strcat(sql_cmd,buf);
	}

	/* For the last column name */
	strcat(sql_cmd,"\"");
	element = zstring_strtok_dquotes(NULL,sep,DQUOTES);
	strncat(sql_cmd,zstring_remove_chr(element,dquotes),ELEMENT_LENGTH);

	/* reset buf for the last column */
	memset(buf,'\0',INSERT_PHRASE_LENGTH);
	sprintf(buf,"\" TEXT(%d));",ELEMENT_LENGTH);
	strcat(sql_cmd,buf);

	if (sqlite3_prepare_v2(db,sql_cmd,strlen(sql_cmd)+1,&stmt,NULL)!=SQLITE_OK){
        if (p) {
            sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR sqlite3_prepare_v2() >\t\tCREATE TABLE\n");
            sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR   >\t\t%s\nSQL CMD >\t\t%s\n",sqlite3_errmsg(db),sql_cmd);
        }
		fclose(fd);
		return sqlite3_errcode(db);
	}

	if(p)
        sqlite3_file_io_verbose_print(p, WO_LINE,"SQL CMD >\t\t%s\n",sql_cmd);

	if (sqlite3_step(stmt) != SQLITE_DONE){
        if (p)
            sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR sqlite3_step() >\t\tCREATE TABLE\n%s\n",sqlite3_errmsg(db));
		fclose(fd);
		return sqlite3_errcode(db);
	}

	if (sqlite3_finalize(stmt) != SQLITE_OK){
        if(p)
            sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR sqlite3_finalize() >\t\tCREATE TABLE\n%s\n",sqlite3_errmsg(db));
		fclose(fd);
		return sqlite3_errcode(db);
	}

	/* Prepared statement for INSERT */
	memset(sql_cmd,'\0',MAX_LINE_LENGTH);
	sprintf(sql_cmd,"INSERT INTO %s VALUES(?",table);
	i=strlen(sql_cmd);
	for(j=0; j<cnt; j++){
		sql_cmd[i++] = ',';
		sql_cmd[i++] = '?';
	}
	sql_cmd[i++] = ')';
	sql_cmd[i++] = ';';
	sql_cmd[i] = 0;

	if (sqlite3_prepare_v2(db,sql_cmd,strlen(sql_cmd)+1,&stmt,NULL)!=SQLITE_OK){
        if(p)
            sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR sqlite3_prepare_v2() >\t\tINSERT INTO\n%s\n",sqlite3_errmsg(db));
		fclose(fd);
		return sqlite3_errcode(db);
	}

	while(fgets(line,sizeof(line),fd)){

        if (p)
            sqlite3_file_io_verbose_print(p, W_LINE,"%s",line);

		if (!zstring_remove_chr(line,bad_chars)){
            if(p)
                sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR zstring_remove_chr() >\t\tINSERT INTO\n%s\n",sqlite3_errmsg(db));
			fclose(fd);
			return -3;
		}

		/* Modify previously created statement */
		element = zstring_strtok_dquotes(line,sep,DQUOTES);
		if (sqlite3_bind_text(stmt, 1,(strcmp(element,sep)==0)?"NULL":element , -1, SQLITE_STATIC)!=SQLITE_OK){
            if(p)
                sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR sqlite3_bind_text() >\t\tINSERT INTO\n%s\n",sqlite3_errmsg(db));
			fclose(fd);
			return -3;
		}
		for (j=2;j<cnt+1;j++) {
			element = zstring_strtok_dquotes(NULL,sep,DQUOTES);
			if (sqlite3_bind_text(stmt, j, (strcmp(element,sep)==0)?"NULL":element, -1, SQLITE_STATIC)!=SQLITE_OK){
                if(p)
                    sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR sqlite3_bind_text() >\t\tINSERT INTO\n%s\n",sqlite3_errmsg(db));
                fclose(fd);
                return -3;
			}
		}
		element = zstring_strtok_dquotes(NULL,sep,DQUOTES);
		if (element[strlen(element)-1] == '\n')
			element[strlen(element)-1] = 0;

		if(sqlite3_bind_text(stmt, j, element, -1, NULL)!=SQLITE_OK) {
            if(p)
                sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR sqlite3_bind_text() >\t\tINSERT INTO\n%s\n",sqlite3_errmsg(db));
			fclose(fd);
			return -3;
		}

		/* Evaluate the SQL statement  */
		if (sqlite3_step(stmt)!=SQLITE_DONE) {
            if (p) {
                sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR sqlite3_step() >\t\tINSERT INTO\n%s\n",sqlite3_errmsg(db));
                sqlite3_file_io_verbose_print(p, WO_LINE,"SQL CMD >\t\t%s\n", sqlite3_sql(stmt));
            }
			fclose(fd);
			return sqlite3_errcode(db);
		}

		sqlite3_reset(stmt);
	}

	sqlite3_finalize(stmt);
	fclose(fd);

	if (sqlite3_exec(db, "END TRANSACTION", NULL, NULL, NULL) != SQLITE_OK) {
        if (p)
            sqlite3_file_io_verbose_print(p, WO_LINE,"ERROR   >\t\t%s\n",sqlite3_errmsg(db));
        return sqlite3_errcode(db);
    }
	if(p)
        sqlite3_file_io_verbose_print(p, WO_LINE,"SQL CMD >\t\tEND TRANSACTION\n");

//    free(element);
	return 0;
}

int sqlite3_file_io_import (sqlite3 *db, char *table, char *fp, const char *sep) {
    return sqlite3_file_io_import_verbose (db, table, fp, sep, NULL);
}

#endif
