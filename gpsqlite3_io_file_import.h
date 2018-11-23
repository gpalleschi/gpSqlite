/******************************************************************************
* gpsqlite3_io_file_import.h
* Copyright (c) 2018, GPSoft - SkyBalls gpsoftskyballs@gmail.com
* All rights reserved.
*
* This file is part of gpSqlite3 C Utilities Library .
*
*    gpSqlite3 is free software: you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
*
*    Nome-Programma is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with Nome-Programma.  If not, see <http://www.gnu.org/licenses/>.
*
*  Function arguments:
*
*       int gpsqlite3_io_csv_import (sqlite3 *db, char *table, char *fd, char *sep, char *fs, int header, char *retmsg) 
*
*      - db            : db sqllite3
*      - tableName     : table name
*      - fd            : data file (with absolute path)
*      - sep           : separator character (if null will be specified structure file)
*      - fs            : structure file (whit absolute path) (if null no structure data will be applied and a file csv will be )
*      - header        : flag to indicate if is present header (1) or not (0)
*      - retmsg        : msg code return
*
*  Return values
*      - 0 Ok
*      - 1 Error
*
*      - In field "retmsg" will be setted error message
*
*  File Structure has these fields :
*
*  
*  1) Name Column
*  2) Byte from
*  3) Byte to
*  4) Type field
*  5) Expression
*
******************************************************************************/

#ifndef GPSQLITE3_IO_FILE_IMPORT_H
#define GPSQLITE3_IO_FILE_IMPORT_H

#define MAX_LEN_NAME 128
#define MAX_LEN_EXPR 1024 
#define MAX_LEN_MSG  2048 
#define MAX_LEN_LINE 1024
#define SQL_CMD_LENGTH 4096
#define SQL_APP_LENGTH 4096
#define MAX_FIELD_FS 5

typedef struct {
  char   name[MAX_LEN_NAME]; 
  int    byte_from; 
  int    byte_to; 
  char   type;
  char   expr[MAX_LEN_EXPR];
} T_STRUCT;


int gpsqlite3_io_file_import (sqlite3 *db, char *table, char *fd, char *sep, char *fs, int header, char *retmsg) 
{
   bool bErr = true;
   char msgErr[MAX_LEN_MSG];
   char *vSep = NULL;
   FILE *pfd;
   FILE *pfs;
   char line[MAX_LEN_LINE];
   char sql_cmd[SQL_CMD_LENGTH];
   char sql_app[SQL_APP_LENGTH];
   sqlite3_stmt *stmt;

   int  ret = 0;
   int  ind;
   int  totFields;
   char **fields;

   int totFieldsS = 0;
   T_STRUCT *ptStruct = NULL;

   if ( retmsg == NULL ) bErr = false;

   /* if file structure is null, csv format is readed
    * *********************************************** */

   if ( fs == NULL ) {
      if ( sep != NULL ) {
         vSep = sep; 
      } else {
        if ( bErr == true ) sprintf(retmsg,ERRMSG0001);
        return 1;
      } 

   } else {

   /* reading  file structure
    * *********************** */
     if ( (pfs=fopen(fs,"r")) == NULL ) {
       if ( bErr == true ) sprintf(retmsg,ERRMSG0002,"structure",fs,strerror(errno));
       return 1;
     }

     while(fgets(line, sizeof(line), pfs) != NULL)
     {
       totFields = 0;
       fields = NULL;
       if ( ( ret = gpString_split(line,";",&totFields,&fields)) != 0 ) {
          if ( bErr == true ) sprintf(retmsg,ERRMSG0003,line, ret);
          return 1;
       }

       if ( totFields == 0 ) {
          if ( bErr == true ) sprintf(retmsg,ERRMSG0004,line);
          return 1;
       }

       if ( totFields != MAX_FIELD_FS ) {
          if ( bErr == true ) sprintf(retmsg,ERRMSG0005,line, MAX_FIELD_FS);
          return 1;
       }

       ptStruct = (T_STRUCT *)realloc(ptStruct,sizeof(T_STRUCT)*(totFieldsS+1));

       if ( ptStruct == NULL ) {
          if ( bErr == true ) sprintf(retmsg,ERRMSG0006,strerror(errno));
          return 1;
       }

       if ( fields[0] == NULL ) {
          if ( bErr == true ) sprintf(retmsg,ERRMSG0007,line);
          return 1;
       }

       strcpy(ptStruct[totFieldsS].name,fields[0]);

       if ( fields[1] != NULL ) {

          if ( gpString_isnum(fields[1]) ) {
             if ( bErr == true ) sprintf(retmsg,ERRMSG0008,fields[1],line);
             return 1;
          }

          if ( vSep != NULL ) {
             if ( bErr == true ) sprintf(retmsg,ERRMSG0009,fields[1],vSep);
             return 1;
          }

          ptStruct[totFieldsS].byte_from = atoi(fields[1]);

       } else {
           
          if ( vSep == NULL ) {
             if ( bErr == true ) sprintf(retmsg,ERRMSG0010,fields[1]);
             return 1;
          }
          
          ptStruct[totFieldsS].byte_from = -1;
       }

       if ( fields[2] != NULL ) {

          if ( gpString_isnum(fields[2]) ) {
             if ( bErr == true ) sprintf(retmsg,ERRMSG0011,fields[2],line);
             return 1;
          }

          if ( vSep != NULL ) {
             if ( bErr == true ) sprintf(retmsg,ERRMSG0012,fields[2],vSep);
             return 1;
          }

          ptStruct[totFieldsS].byte_to = atoi(fields[2]);

       } else {
          if ( vSep == NULL ) {
             if ( bErr == true ) sprintf(retmsg,ERRMSG0013,fields[2]);
             return 1;
          }
          ptStruct[totFieldsS].byte_to = -1;
       }

       if ( fields[3] == NULL )  {
          if ( bErr == true ) sprintf(retmsg,ERRMSG0014,line);
          return 1;
       }
       ptStruct[totFieldsS].type = fields[3][0];

       if ( fields[4][strlen(fields[4])==0?0:(strlen(fields[4])-1)] == '\n' ) {
         ptStruct[totFieldsS].expr[0] = 0;
       } else {
         strcpy(ptStruct[totFieldsS].expr,fields[4]);
       }

       /* Free fields readed from structure file
        * ************************************** */

       for(ind=0;ind<totFields;ind++) free(fields[ind]);
       free(fields);
       fields = NULL;
       totFields = 0;

       totFieldsS++;
     }

     if ( fclose(pfs) != 0 ) {
       if ( bErr == true ) sprintf(retmsg,ERRMSG0015,"structure",fs,strerror(errno));
       return 1;
     }

     // DEBUG 
     //
     /*
      * for(ind=0;ind<totFieldsS;ind++) {
        printf("\n Rec %d\n\n",ind);

    printf("name      : <%s>\n",ptStruct[ind].name);
    printf("byte_from : %d\n",ptStruct[ind].byte_from);
    printf("byte_to   : %d\n",ptStruct[ind].byte_to);
    printf("type      : %c\n",ptStruct[ind].type);
    printf("expr      : <%s>\n",ptStruct[ind].expr);
     }
     */
     //
   }

   /* Read Data File
    * ************** */
   if ( (pfd=fopen(fd,"r")) == NULL ) {
   
      if ( bErr == true ) sprintf(retmsg,ERRMSG0002,"data",fd,strerror(errno));
      return 1;
   
   }

   /* Do all these stuff within a transaction. This makes it much more faster */
   if(sqlite3_exec(db,"BEGIN TRANSACTION", NULL, NULL, NULL) != SQLITE_OK){
     
      if ( bErr == true ) sprintf(retmsg,ERRMSG0025,"BEGIN TRANSACTION",sqlite3_errmsg(db),sqlite3_errcode(db));
      return 1;

   }
   
   /* Create Table 
    * ************ */
   sprintf(sql_cmd,"CREATE TABLE %s (",table);

   if ( fgets(line, sizeof(line), pfd) == NULL ) {

     if ( bErr == true ) sprintf(retmsg,ERRMSG0016,fd,strerror(errno));

   } else {

        if ( vSep != NULL ) {

          if ( ( ret = gpString_split(line,vSep,&totFields,&fields)) != 0 ) {
             if ( bErr == true ) sprintf(retmsg,ERRMSG0003,line, ret);
             return 1;
          } 

          if ( totFields == 0 ) {
             if ( bErr == true ) sprintf(retmsg,ERRMSG0017,line);
          } 


          for(ind=0;ind<totFields;ind++) {
              
             if ( ind > 0 && ind <= totFields-1 ) strcat(sql_cmd,", ");

             /* To modify to manage another types from structure file
              * ***************************************************** */

             /* If header is present
              * ******************** */

             if ( header ) {

                sprintf(sql_app,"%s text",fields[ind]);

             } else {

             /* If header is not present
              * ************************ */

                sprintf(sql_app,"FIELD_%04d text",ind+1);
             } 
             strcat(sql_cmd,sql_app);
          } 
          strcat(sql_cmd,");");

        } else {
            
          /* NO SEPARATOR *
           ****************/          
        }    
        
        if ( sqlite3_prepare_v2(db,sql_cmd,strlen(sql_cmd)+1,&stmt,NULL) != SQLITE_OK ) {
            
            if ( bErr == true ) sprintf(retmsg,ERRMSG0018,sql_cmd,sqlite3_errmsg(db));
            return 1;
              
        } 

        if ( sqlite3_step(stmt) != SQLITE_DONE ) {
            
            if ( bErr == true ) sprintf(retmsg,ERRMSG0019,sql_cmd,sqlite3_errmsg(db));
            return 1;
        
        } 

        if ( sqlite3_finalize(stmt) != SQLITE_OK ) {
            
            if ( bErr == true ) sprintf(retmsg,ERRMSG0020,sql_cmd,sqlite3_errmsg(db));
            return 1;
        } 

      // DEBUG 
      //
        printf("\n sql_cmd >%s<\n",sql_cmd);
      //

        if ( header ) {
            if ( fgets(line, sizeof(line), pfd) == NULL ) {
                if ( bErr == true ) sprintf(retmsg,ERRMSG0021,"data",fd,strerror(errno));
                return 1;
            }
        } 

     /* 
      * Prepared statement for INSERT 
      * ***************************** */

        sprintf(sql_cmd,"INSERT INTO %s VALUES(?",table);
        for(ind=1; ind<totFields; ind++){
            strcat(sql_cmd,",?");
        }
        strcat(sql_cmd,");");

        
        if ( sqlite3_prepare_v2(db,sql_cmd,strlen(sql_cmd)+1,&stmt,NULL) != SQLITE_OK ) {
            
           if ( bErr == true ) sprintf(retmsg,ERRMSG0022,sql_cmd,sqlite3_errmsg(db));
           return 1;

        }
        
        /* Loop read data file
        * **************************** */

        do {
            if ( vSep != NULL ) {  
                
                if ( ( ret = gpString_split(line,vSep,&totFields,&fields)) != 0 ) {
                
                     if ( bErr == true ) sprintf(retmsg,ERRMSG0003,line, ret);
                    return 1;
                } 
                
            } else { 

                /* NO SEPARATOR *
                 ****************/
                 
            }
            
            if ( totFields == 0 ) {
                if ( bErr == true ) sprintf(retmsg,ERRMSG0004,line);
                return 1;
            }

            if ( fields[totFields-1][strlen(fields[totFields-1])==0?0:(strlen(fields[totFields-1])-1)] == '\n' ) {
               fields[totFields-1][strlen(fields[totFields-1])==0?0:(strlen(fields[totFields-1])-1)] = 0;
            }
            
            if (sqlite3_bind_text(stmt, 1,(strlen(fields[0])==0)?"NULL":fields[0] , -1, SQLITE_STATIC)!=SQLITE_OK){
               
               if ( bErr == true ) sprintf(retmsg,ERRMSG0023,0,fields[0],sqlite3_errmsg(db));
               return 1;
            
            }
            
            for (ind=1;ind<totFields;ind++) {

                printf ("\n Bind Campo %d <%s>\n",ind, fields[ind]);

                if (sqlite3_bind_text(stmt, ind+1,(strlen(fields[ind])==0)?"NULL":fields[ind] , -1, SQLITE_STATIC)!=SQLITE_OK){
               
                    if ( bErr == true ) sprintf(retmsg,ERRMSG0023,ind,fields[ind],sqlite3_errmsg(db));
                    return 1;
            
                }
            
            }

            /* Evaluate the SQL statement  
             * ************************** */
            
            if (sqlite3_step(stmt)!=SQLITE_DONE) {
                   if ( bErr == true ) sprintf(retmsg,ERRMSG0024,sqlite3_sql(stmt),sqlite3_errmsg(db),sqlite3_errcode(db));
                return 1;
            }
            
            sqlite3_reset(stmt);
            
            /* Free fields readed from structure file
             * ************************************** */
            for(ind=0;ind<totFields;ind++) free(fields[ind]);
            free(fields);
            fields = NULL;
            totFields = 0;

        } while ( fgets(line, sizeof(line), pfd) != NULL );
        
        sqlite3_finalize(stmt);
        
        if (sqlite3_exec(db, "END TRANSACTION", NULL, NULL, NULL) != SQLITE_OK) {
               if ( bErr == true ) sprintf(retmsg,ERRMSG0025,"END TRANSACTION",sqlite3_errmsg(db),sqlite3_errcode(db));
            return 1;        
        }
    }  

   if ( fclose(pfd) != 0 ) {
     if ( bErr == true ) sprintf(retmsg,ERRMSG0015,"data",fd,strerror(errno));
     return 1;
   }
   
   return 0;
}

#endif

