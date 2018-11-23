/******************************************************************************
* gpSqlite3_errno.h
* Copyright (c) 2018, GPSoft - SkyBalls gpsoftskyballs@gmail.com
* All rights reserved.
*
* This file is part of gpSqlite3 C Utilities Library .
*
*    gpString is free software: you can redistribute it and/or modify
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
******************************************************************************/
 
#define ERRMSG0001 "Error sctructure file not present and not setted separator."
#define ERRMSG0002 "Error open %s file <%s> errno <%s>."
#define ERRMSG0003 "Error splitting record <%s> ret code %d."
#define ERRMSG0004 "Error splitting record <%s> no fields splitted."
#define ERRMSG0005 "Error splitting record <%s> fields number different from %d."
#define ERRMSG0006 "Error realloc <%s>."
#define ERRMSG0007 "Error name null in line <%s>."
#define ERRMSG0008 "Error byte from <%s> in line <%s>."
#define ERRMSG0009 "Error specified byte from <%s> with separator caracter <%s>."
#define ERRMSG0010 "Error not specified byte from <%s> with separator caracter null."
#define ERRMSG0011 "Error byte to <%s> null in line <%s>."
#define ERRMSG0012 "Error specified byte to <%s> with separator caracter <%s>."
#define ERRMSG0013 "Error not specified byte to <%s> with separator caracter null."
#define ERRMSG0014 "Error type null in line <%s>."
#define ERRMSG0015 "Error close %s file <%s> errno <%s>."
#define ERRMSG0016 "Error data file <%s> is empty or errno <%s>."
#define ERRMSG0017 "Error no fields found in first line of data file <%s>."
#define ERRMSG0018 "Error in sqlite3_prepare_v2 for statement <%s> errmsg <%s>."
#define ERRMSG0019 "Error in sqlite3_step for statement <%s> errmsg <%s>."
#define ERRMSG0020 "Error in sqlite3_finalize for statement <%s> errmsg <%s>."
#define ERRMSG0021 "Error %s file <%s> is empty (only header record present) or errno <%s>."
#define ERRMSG0022 "ERROR sqlite3_prepare_v2() >%s< - >%s<."
#define ERRMSG0023 "ERROR sqlite3_bind_text() Field %d >%s< - >%s<."
#define ERRMSG0024 "ERROR sqlite3_step Statement >%s< Errorcode >%s< %d."
#define ERRMSG0025 "ERROR sqlite3_exec %s Errorcode >%s< %d."

