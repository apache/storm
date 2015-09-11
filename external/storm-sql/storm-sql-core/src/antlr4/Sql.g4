/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The SQL parser is loosely based on
 * https://github.com/antlr/grammars-v4/blob/master/sqlite/SQLite.g4
 **/

grammar Sql;

parse
 : ( sql_stmt_list | error )* EOF
 ;

error
 : UNEXPECTED_CHAR 
   { 
     throw new RuntimeException("UNEXPECTED_CHAR=" + $UNEXPECTED_CHAR.text); 
   }
 ;

sql_stmt_list
 : ';'* sql_stmt ( ';'+ sql_stmt )* ';'*
 ;


sql_stmt
 : ( EXPLAIN ( QUERY PLAN )? )? ( compound_select_stmt
                                 | create_table_stmt
                                 | create_view_stmt
                                 | factored_select_stmt
                                 | insert_stmt
                                 | pragma_stmt
                                 | simple_select_stmt
                                 | select_stmt )
 ;


compound_select_stmt
 : ( WITH RECURSIVE? common_table_expression ( ',' common_table_expression )* )?
   select_core ( ( UNION ALL? | INTERSECT | EXCEPT ) select_core )+
   ( ORDER BY ordering_term ( ',' ordering_term )* )?
   ( LIMIT expr ( ( OFFSET | ',' ) expr )? )?
 ;

create_table_stmt
 : CREATE EXTERNAL TABLE ( IF NOT EXISTS )?
   ( database_name '.' )? table_name
   ( '(' column_def ( ',' column_def )* ')'
   | AS select_stmt
   ) ( LOCATION location_uri )?
 ;

location_uri
 : STRING_LITERAL
 ;

create_view_stmt
 : CREATE ( TEMP | TEMPORARY )? VIEW ( IF NOT EXISTS )?
   ( database_name '.' )? view_name AS select_stmt
 ;

factored_select_stmt
 : ( WITH RECURSIVE? common_table_expression ( ',' common_table_expression )* )?
   select_core ( compound_operator select_core )*
   ( ORDER BY ordering_term ( ',' ordering_term )* )?
   ( LIMIT expr ( ( OFFSET | ',' ) expr )? )?
 ;

insert_stmt
 : with_clause? ( INSERT
                | INSERT OR FAIL
                | INSERT OR IGNORE ) INTO
   ( database_name '.' )? table_name ( '(' column_name ( ',' column_name )* ')' )?
   ( VALUES '(' expr ( ',' expr )* ')' ( ',' '(' expr ( ',' expr )* ')' )*
   | select_stmt
   | DEFAULT VALUES
   )
 ;

pragma_stmt
 : PRAGMA ( database_name '.' )? pragma_name ( '=' pragma_value
                                               | '(' pragma_value ')' )?
 ;

simple_select_stmt
 : ( WITH RECURSIVE? common_table_expression ( ',' common_table_expression )* )?
   select_core ( ORDER BY ordering_term ( ',' ordering_term )* )?
   ( LIMIT expr ( ( OFFSET | ',' ) expr )? )?
 ;

select_stmt
 : ( WITH RECURSIVE? common_table_expression ( ',' common_table_expression )* )?
   select_or_values ( compound_operator select_or_values )*
   ( ORDER BY ordering_term ( ',' ordering_term )* )?
   ( LIMIT expr ( ( OFFSET | ',' ) expr )? )?
 ;

select_or_values
 : SELECT ( DISTINCT | ALL )? result_column ( ',' result_column )*
   ( FROM ( table_or_subquery ( ',' table_or_subquery )* | join_clause ) )?
   ( WHERE expr )?
   ( GROUP BY expr ( ',' expr )* ( HAVING expr )? )?
 | VALUES '(' expr ( ',' expr )* ')' ( ',' '(' expr ( ',' expr )* ')' )*
 ;

column_def
 : column_name data_type column_constraint*
 ;

data_type
 : BIGINT
 | BINARY
 | BOOLEAN
 | DATE
 | decimal_type
 | DOUBLE
 | FLOAT
 | INT
 | INTEGER
 | SMALLINT
 | TIMESTAMP
 | TINYINT
 | varchar_type
 ;

decimal_type
 : DECIMAL ( '(' signed_number ',' signed_number ')' )?
 ;

varchar_type
 : VARCHAR ( '(' signed_number ')' )?
 ;

column_constraint
 : ( CONSTRAINT name )?
   ( PRIMARY KEY )
 ;


/*
    SQLite understands the following binary operators, in order from highest to
    lowest precedence:

    ||
    *    /    %
    +    -
    <<   >>   &    |
    <    <=   >    >=
    =    ==   !=   <>   IS   IS NOT   IN   LIKE   GLOB   MATCH   REGEXP
    AND
    OR
*/
expr
 : literal_value
 | BIND_PARAMETER
 | ( ( database_name '.' )? table_name '.' )? column_name
 | unary_operator expr
 | expr '||' expr
 | expr ( '*' | '/' | '%' ) expr
 | expr ( '+' | '-' ) expr
 | expr ( '<<' | '>>' | '&' | '|' ) expr
 | expr ( '<' | '<=' | '>' | '>=' ) expr
 | expr ( '=' | '==' | '!=' | '<>' | IS | IS NOT | IN | LIKE | GLOB | MATCH | REGEXP ) expr
 | expr AND expr
 | expr OR expr
 | function_name '(' ( DISTINCT? expr ( ',' expr )* | '*' )? ')'
 | '(' expr ')'
 | CAST '(' expr AS data_type ')'
 | expr COLLATE collation_name
 | expr NOT? ( LIKE | GLOB | REGEXP | MATCH ) expr ( ESCAPE expr )?
 | expr ( ISNULL | NOTNULL | NOT NULL )
 | expr IS NOT? expr
 | expr NOT? BETWEEN expr AND expr
 | expr NOT? IN ( '(' ( select_stmt
                          | expr ( ',' expr )*
                          )?
                      ')'
                    | ( database_name '.' )? table_name )
 | ( ( NOT )? EXISTS )? '(' select_stmt ')'
 | CASE expr? ( WHEN expr THEN expr )+ ( ELSE expr )? END
 | raise_function
 ;

raise_function
 : RAISE '(' ( IGNORE | FAIL ',' error_message ) ')'
 ;

with_clause
 : WITH RECURSIVE? cte_table_name AS '(' select_stmt ')' ( ',' cte_table_name AS '(' select_stmt ')' )*
 ;

qualified_table_name
 : ( database_name '.' )? table_name
 ;

ordering_term
 : expr ( COLLATE collation_name )? ( ASC | DESC )?
 ;

pragma_value
 : signed_number
 | name
 | STRING_LITERAL
 ;

common_table_expression
 : table_name ( '(' column_name ( ',' column_name )* ')' )? AS '(' select_stmt ')'
 ;

result_column
 : '*'
 | table_name '.' '*'
 | expr ( AS? column_alias )?
 ;

table_or_subquery
 : ( database_name '.' )? table_name ( AS? table_alias )?
 | '(' ( table_or_subquery ( ',' table_or_subquery )*
       | join_clause )
   ')' ( AS? table_alias )?
 | '(' select_stmt ')' ( AS? table_alias )?
 ;

join_clause
 : table_or_subquery ( join_operator table_or_subquery join_constraint )*
 ;

join_operator
 : ','
 | NATURAL? ( LEFT OUTER? | INNER | CROSS )? JOIN
 ;

join_constraint
 : ( ON expr
   | USING '(' column_name ( ',' column_name )* ')' )?
 ;

select_core
 : SELECT ( DISTINCT | ALL )? result_column ( ',' result_column )*
   ( FROM ( table_or_subquery ( ',' table_or_subquery )* | join_clause ) )?
   ( WHERE expr )?
   ( GROUP BY expr ( ',' expr )* ( HAVING expr )? )?
 | VALUES '(' expr ( ',' expr )* ')' ( ',' '(' expr ( ',' expr )* ')' )*
 ;

compound_operator
 : UNION
 | UNION ALL
 | INTERSECT
 | EXCEPT
 ;

cte_table_name
 : table_name ( '(' column_name ( ',' column_name )* ')' )?
 ;

signed_number
 : ( '+' | '-' )? NUMERIC_LITERAL
 ;

literal_value
 : NUMERIC_LITERAL
 | STRING_LITERAL
 | BLOB_LITERAL
 | NULL
 | CURRENT_TIME
 | CURRENT_DATE
 | CURRENT_TIMESTAMP
 ;

unary_operator
 : '-'
 | '+'
 | '~'
 | NOT
 ;

error_message
 : STRING_LITERAL
 ;

module_argument // TODO check what exactly is permitted here
 : expr
 | column_def
 ;

column_alias
 : IDENTIFIER
 | STRING_LITERAL
 ;


keyword
 : ABS
 | ALL
 | ALLOCATE
 | ALTER
 | AND
 | ANY
 | ARE
 | ARRAY
 | ARRAY_AGG
 | ARRAY_MAX_CARDINALITY
 | AS
 | ASC
 | ASENSITIVE
 | ASYMMETRIC
 | AT
 | ATOMIC
 | AUTHORIZATION
 | AVG
 | BEGIN
 | BEGIN_FRAME
 | BEGIN_PARTITION
 | BETWEEN
 | BIGINT
 | BINARY
 | BLOB
 | BOOLEAN
 | BOTH
 | BY
 | CALL
 | CALLED
 | CARDINALITY
 | CASCADED
 | CASE
 | CAST
 | CEIL
 | CEILING
 | CHAR
 | CHARACTER
 | CHARACTER_LENGTH
 | CHAR_LENGTH
 | CHECK
 | CLOB
 | CLOSE
 | COALESCE
 | COLLATE
 | COLLECT
 | COLUMN
 | COMMIT
 | CONDITION
 | CONNECT
 | CONSTRAINT
 | CONTAINS
 | CONVERT
 | CORR
 | CORRESPONDING
 | COUNT
 | COVAR_POP
 | COVAR_SAMP
 | CREATE
 | CROSS
 | CUBE
 | CUME_DIST
 | CURRENT
 | CURRENT_CATALOG
 | CURRENT_DATE
 | CURRENT_DEFAULT_TRANSFORM_GROUP
 | CURRENT_PATH
 | CURRENT_ROLE
 | CURRENT_ROW
 | CURRENT_SCHEMA
 | CURRENT_TIME
 | CURRENT_TIMESTAMP
 | CURRENT_TRANSFORM_GROUP_FOR_TYPE
 | CURRENT_USER
 | CURSOR
 | CYCLE
 | DATALINK
 | DATE
 | DAY
 | DEALLOCATE
 | DEC
 | DECIMAL
 | DECLARE
 | DEFAULT
 | DELETE
 | DENSE_RANK
 | DEREF
 | DESC
 | DESCRIBE
 | DETERMINISTIC
 | DISCONNECT
 | DISTINCT
 | DLNEWCOPY
 | DLPREVIOUSCOPY
 | DLURLCOMPLETE
 | DLURLCOMPLETEONLY
 | DLURLCOMPLETEWRITE
 | DLURLPATH
 | DLURLPATHONLY
 | DLURLPATHWRITE
 | DLURLSCHEME
 | DLURLSERVER
 | DLVALUE
 | DOUBLE
 | DROP
 | DYNAMIC
 | EACH
 | ELEMENT
 | ELSE
 | END
 | END_EXEC
 | END_FRAME
 | END_PARTITION
 | EQUALS
 | ESCAPE
 | EVERY
 | EXCEPT
 | EXEC
 | EXECUTE
 | EXISTS
 | EXP
 | EXPLAIN
 | EXTERNAL
 | EXTRACT
 | FAIL
 | FALSE
 | FETCH
 | FILTER
 | FIRST_VALUE
 | FLOAT
 | FLOOR
 | FOR
 | FOREIGN
 | FRAME_ROW
 | FREE
 | FROM
 | FULL
 | FUNCTION
 | FUSION
 | GET
 | GLOB
 | GLOBAL
 | GRANT
 | GROUP
 | GROUPING
 | GROUPS
 | HAVING
 | HOLD
 | HOUR
 | IDENTITY
 | IF
 | IGNORE
 | IMPORT
 | IN
 | INDICATOR
 | INNER
 | INOUT
 | INSENSITIVE
 | INSERT
 | INT
 | INTEGER
 | INTERSECT
 | INTERSECTION
 | INTERVAL
 | INTO
 | IS
 | ISNULL
 | JOIN
 | KEY
 | LAG
 | LANGUAGE
 | LARGE
 | LAST_VALUE
 | LATERAL
 | LEAD
 | LEADING
 | LEFT
 | LIKE
 | LIKE_REGEX
 | LIMIT
 | LN
 | LOCAL
 | LOCALTIME
 | LOCALTIMESTAMP
 | LOCATION
 | LOWER
 | MATCH
 | MAX
 | MEMBER
 | MERGE
 | METHOD
 | MIN
 | MINUTE
 | K_MOD
 | MODIFIES
 | MODULE
 | MONTH
 | MULTISET
 | NATIONAL
 | NATURAL
 | NCHAR
 | NCLOB
 | NEW
 | NO
 | NONE
 | NORMALIZE
 | NOT
 | NOTNULL
 | NTH_VALUE
 | NTILE
 | NULL
 | NULLIF
 | NUMERIC
 | OCCURRENCES_REGEX
 | OCTET_LENGTH
 | OF
 | OFFSET
 | OLD
 | ON
 | ONLY
 | OPEN
 | OR
 | ORDER
 | OUT
 | OUTER
 | OVER
 | OVERLAPS
 | OVERLAY
 | PARAMETER
 | PARTITION
 | PERCENT
 | PERCENTILE_CONT
 | PERCENTILE_DISC
 | PERCENT_RANK
 | PERIOD
 | PLAN
 | PORTION
 | POSITION
 | POSITION_REGEX
 | POWER
 | PRAGMA
 | PRECEDES
 | PRECISION
 | PREPARE
 | PRIMARY
 | PROCEDURE
 | QUERY
 | RAISE
 | RANGE
 | RANK
 | READS
 | REAL
 | RECURSIVE
 | REF
 | REFERENCES
 | REFERENCING
 | REGEXP
 | REGR_AVGX
 | REGR_AVGY
 | REGR_COUNT
 | REGR_INTERCEPT
 | REGR_R2
 | REGR_SLOPE
 | REGR_SXX
 | REGR_SXY
 | REGR_SYY
 | RELEASE
 | RESULT
 | RETURN
 | RETURNS
 | REVOKE
 | RIGHT
 | ROLLBACK
 | ROLLUP
 | ROW
 | ROWS
 | ROW_NUMBER
 | SAVEPOINT
 | SCOPE
 | SCROLL
 | SEARCH
 | SECOND
 | SELECT
 | SENSITIVE
 | SESSION_USER
 | SET
 | SIMILAR
 | SMALLINT
 | SOME
 | SPECIFIC
 | SPECIFICTYPE
 | SQL
 | SQLEXCEPTION
 | SQLSTATE
 | SQLWARNING
 | SQRT
 | START
 | STATIC
 | STDDEV_POP
 | STDDEV_SAMP
 | SUBMULTISET
 | SUBSTRING
 | SUBSTRING_REGEX
 | SUCCEEDS
 | SUM
 | SYMMETRIC
 | SYSTEM
 | SYSTEM_TIME
 | SYSTEM_USER
 | TABLE
 | TABLESAMPLE
 | TEMP
 | TEMPORARY
 | THEN
 | TIME
 | TIMESTAMP
 | TIMEZONE_HOUR
 | TIMEZONE_MINUTE
 | TINYINT
 | TO
 | TRAILING
 | TRANSLATE
 | TRANSLATE_REGEX
 | TRANSLATION
 | TREAT
 | TRIGGER
 | TRIM
 | TRIM_ARRAY
 | TRUE
 | TRUNCATE
 | UESCAPE
 | UNION
 | UNIQUE
 | UNKNOWN
 | UNNEST
 | UPDATE
 | UPPER
 | USER
 | USING
 | VALUE
 | VALUES
 | VALUE_OF
 | VARBINARY
 | VARCHAR
 | VARYING
 | VAR_POP
 | VAR_SAMP
 | VERSIONING
 | VIEW
 | WHEN
 | WHENEVER
 | WHERE
 | WIDTH_BUCKET
 | WINDOW
 | WITH
 | WITHIN
 | WITHOUT
 | XML
 | XMLAGG
 | XMLATTRIBUTES
 | XMLBINARY
 | XMLCAST
 | XMLCOMMENT
 | XMLCONCAT
 | XMLDOCUMENT
 | XMLELEMENT
 | XMLEXISTS
 | XMLFOREST
 | XMLITERATE
 | XMLNAMESPACES
 | XMLPARSE
 | XMLPI
 | XMLQUERY
 | XMLSERIALIZE
 | XMLTABLE
 | XMLTEXT
 | XMLVALIDATE
 | YEAR
 ;
name
 : any_name
 ;

function_name
 : any_name
 ;

database_name
 : any_name
 ;

table_name
 : any_name
 ;

table_or_index_name
 : any_name
 ;

new_table_name
 : any_name
 ;

column_name
 : any_name
 ;

collation_name
 : any_name
 ;

foreign_table
 : any_name
 ;

index_name
 : any_name
 ;

trigger_name
 : any_name
 ;

view_name
 : any_name
 ;

module_name
 : any_name
 ;

pragma_name
 : any_name
 ;

savepoint_name
 : any_name
 ;

table_alias
 : any_name
 ;

transaction_name
 : any_name
 ;

any_name
 : IDENTIFIER
 | keyword
 | STRING_LITERAL
 | '(' any_name ')'
 ;

//SCOL : ';';
//DOT : '.';
//OPEN_PAR : '(';
//CLOSE_PAR : ')';
//COMMA : ',';
//ASSIGN : '=';
//STAR : '*';
//PLUS : '+';
//MINUS : '-';
//TILDE : '~';
//PIPE2 : '||';
//DIV : '/';
//MOD : '%';
//LT2 : '<<';
//GT2 : '>>';
//AMP : '&';
//PIPE : '|';
//LT : '<';
//LT_EQ : '<=';
//GT : '>';
//GT_EQ : '>=';
//EQ : '==';
//NOT_EQ1 : '!=';
//NOT_EQ2 : '<>';
//

// Reserved keywords from SQL 2011.
// http://www.postgresql.org/docs/9.2/static/sql-keywords-appendix.html

ABS: [Aa][Bb][Ss];
ALL: [Aa][Ll][Ll];
ALLOCATE: [Aa][Ll][Ll][Oo][Cc][Aa][Tt][Ee];
ALTER: [Aa][Ll][Tt][Ee][Rr];
AND: [Aa][Nn][Dd];
ANY: [Aa][Nn][Yy];
ARE: [Aa][Rr][Ee];
ARRAY: [Aa][Rr][Rr][Aa][Yy];
ARRAY_AGG: [Aa][Rr][Rr][Aa][Yy] '_' [Aa][Gg][Gg];
ARRAY_MAX_CARDINALITY: [Aa][Rr][Rr][Aa][Yy] '_' [Mm][Aa][Xx] '_' [Cc][Aa][Rr][Dd][Ii][Nn][Aa][Ll][Ii][Tt][Yy];
AS: [Aa][Ss];
ASC: [Aa][Ss][Cc];
ASENSITIVE: [Aa][Ss][Ee][Nn][Ss][Ii][Tt][Ii][Vv][Ee];
ASYMMETRIC: [Aa][Ss][Yy][Mm][Mm][Ee][Tt][Rr][Ii][Cc];
AT: [Aa][Tt];
ATOMIC: [Aa][Tt][Oo][Mm][Ii][Cc];
AUTHORIZATION: [Aa][Uu][Tt][Hh][Oo][Rr][Ii][Zz][Aa][Tt][Ii][Oo][Nn];
AVG: [Aa][Vv][Gg];
BEGIN: [Bb][Ee][Gg][Ii][Nn];
BEGIN_FRAME: [Bb][Ee][Gg][Ii][Nn] '_' [Ff][Rr][Aa][Mm][Ee];
BEGIN_PARTITION: [Bb][Ee][Gg][Ii][Nn] '_' [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn];
BETWEEN: [Bb][Ee][Tt][Ww][Ee][Ee][Nn];
BIGINT: [Bb][Ii][Gg][Ii][Nn][Tt];
BINARY: [Bb][Ii][Nn][Aa][Rr][Yy];
BLOB: [Bb][Ll][Oo][Bb];
BOOLEAN: [Bb][Oo][Oo][Ll][Ee][Aa][Nn];
BOTH: [Bb][Oo][Tt][Hh];
BY: [Bb][Yy];
CALL: [Cc][Aa][Ll][Ll];
CALLED: [Cc][Aa][Ll][Ll][Ee][Dd];
CARDINALITY: [Cc][Aa][Rr][Dd][Ii][Nn][Aa][Ll][Ii][Tt][Yy];
CASCADED: [Cc][Aa][Ss][Cc][Aa][Dd][Ee][Dd];
CASE: [Cc][Aa][Ss][Ee];
CAST: [Cc][Aa][Ss][Tt];
CEIL: [Cc][Ee][Ii][Ll];
CEILING: [Cc][Ee][Ii][Ll][Ii][Nn][Gg];
CHAR: [Cc][Hh][Aa][Rr];
CHARACTER: [Cc][Hh][Aa][Rr][Aa][Cc][Tt][Ee][Rr];
CHARACTER_LENGTH: [Cc][Hh][Aa][Rr][Aa][Cc][Tt][Ee][Rr] '_' [Ll][Ee][Nn][Gg][Tt][Hh];
CHAR_LENGTH: [Cc][Hh][Aa][Rr] '_' [Ll][Ee][Nn][Gg][Tt][Hh];
CHECK: [Cc][Hh][Ee][Cc][Kk];
CLOB: [Cc][Ll][Oo][Bb];
CLOSE: [Cc][Ll][Oo][Ss][Ee];
COALESCE: [Cc][Oo][Aa][Ll][Ee][Ss][Cc][Ee];
COLLATE: [Cc][Oo][Ll][Ll][Aa][Tt][Ee];
COLLECT: [Cc][Oo][Ll][Ll][Ee][Cc][Tt];
COLUMN: [Cc][Oo][Ll][Uu][Mm][Nn];
COMMIT: [Cc][Oo][Mm][Mm][Ii][Tt];
CONDITION: [Cc][Oo][Nn][Dd][Ii][Tt][Ii][Oo][Nn];
CONNECT: [Cc][Oo][Nn][Nn][Ee][Cc][Tt];
CONSTRAINT: [Cc][Oo][Nn][Ss][Tt][Rr][Aa][Ii][Nn][Tt];
CONTAINS: [Cc][Oo][Nn][Tt][Aa][Ii][Nn][Ss];
CONVERT: [Cc][Oo][Nn][Vv][Ee][Rr][Tt];
CORR: [Cc][Oo][Rr][Rr];
CORRESPONDING: [Cc][Oo][Rr][Rr][Ee][Ss][Pp][Oo][Nn][Dd][Ii][Nn][Gg];
COUNT: [Cc][Oo][Uu][Nn][Tt];
COVAR_POP: [Cc][Oo][Vv][Aa][Rr] '_' [Pp][Oo][Pp];
COVAR_SAMP: [Cc][Oo][Vv][Aa][Rr] '_' [Ss][Aa][Mm][Pp];
CREATE: [Cc][Rr][Ee][Aa][Tt][Ee];
CROSS: [Cc][Rr][Oo][Ss][Ss];
CUBE: [Cc][Uu][Bb][Ee];
CUME_DIST: [Cc][Uu][Mm][Ee] '_' [Dd][Ii][Ss][Tt];
CURRENT: [Cc][Uu][Rr][Rr][Ee][Nn][Tt];
CURRENT_CATALOG: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Cc][Aa][Tt][Aa][Ll][Oo][Gg];
CURRENT_DATE: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Dd][Aa][Tt][Ee];
CURRENT_DEFAULT_TRANSFORM_GROUP: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Dd][Ee][Ff][Aa][Uu][Ll][Tt] '_' [Tt][Rr][Aa][Nn][Ss][Ff][Oo][Rr][Mm] '_' [Gg][Rr][Oo][Uu][Pp];
CURRENT_PATH: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Pp][Aa][Tt][Hh];
CURRENT_ROLE: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Rr][Oo][Ll][Ee];
CURRENT_ROW: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Rr][Oo][Ww];
CURRENT_SCHEMA: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Ss][Cc][Hh][Ee][Mm][Aa];
CURRENT_TIME: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Tt][Ii][Mm][Ee];
CURRENT_TIMESTAMP: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
CURRENT_TRANSFORM_GROUP_FOR_TYPE: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Tt][Rr][Aa][Nn][Ss][Ff][Oo][Rr][Mm] '_' [Gg][Rr][Oo][Uu][Pp] '_' [Ff][Oo][Rr] '_' [Tt][Yy][Pp][Ee];
CURRENT_USER: [Cc][Uu][Rr][Rr][Ee][Nn][Tt] '_' [Uu][Ss][Ee][Rr];
CURSOR: [Cc][Uu][Rr][Ss][Oo][Rr];
CYCLE: [Cc][Yy][Cc][Ll][Ee];
DATALINK: [Dd][Aa][Tt][Aa][Ll][Ii][Nn][Kk];
DATE: [Dd][Aa][Tt][Ee];
DAY: [Dd][Aa][Yy];
DEALLOCATE: [Dd][Ee][Aa][Ll][Ll][Oo][Cc][Aa][Tt][Ee];
DEC: [Dd][Ee][Cc];
DECIMAL: [Dd][Ee][Cc][Ii][Mm][Aa][Ll];
DECLARE: [Dd][Ee][Cc][Ll][Aa][Rr][Ee];
DEFAULT: [Dd][Ee][Ff][Aa][Uu][Ll][Tt];
DELETE: [Dd][Ee][Ll][Ee][Tt][Ee];
DENSE_RANK: [Dd][Ee][Nn][Ss][Ee] '_' [Rr][Aa][Nn][Kk];
DEREF: [Dd][Ee][Rr][Ee][Ff];
DESC: [Dd][Ee][Ss][Cc];
DESCRIBE: [Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee];
DETERMINISTIC: [Dd][Ee][Tt][Ee][Rr][Mm][Ii][Nn][Ii][Ss][Tt][Ii][Cc];
DISCONNECT: [Dd][Ii][Ss][Cc][Oo][Nn][Nn][Ee][Cc][Tt];
DISTINCT: [Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt];
DLNEWCOPY: [Dd][Ll][Nn][Ee][Ww][Cc][Oo][Pp][Yy];
DLPREVIOUSCOPY: [Dd][Ll][Pp][Rr][Ee][Vv][Ii][Oo][Uu][Ss][Cc][Oo][Pp][Yy];
DLURLCOMPLETE: [Dd][Ll][Uu][Rr][Ll][Cc][Oo][Mm][Pp][Ll][Ee][Tt][Ee];
DLURLCOMPLETEONLY: [Dd][Ll][Uu][Rr][Ll][Cc][Oo][Mm][Pp][Ll][Ee][Tt][Ee][Oo][Nn][Ll][Yy];
DLURLCOMPLETEWRITE: [Dd][Ll][Uu][Rr][Ll][Cc][Oo][Mm][Pp][Ll][Ee][Tt][Ee][Ww][Rr][Ii][Tt][Ee];
DLURLPATH: [Dd][Ll][Uu][Rr][Ll][Pp][Aa][Tt][Hh];
DLURLPATHONLY: [Dd][Ll][Uu][Rr][Ll][Pp][Aa][Tt][Hh][Oo][Nn][Ll][Yy];
DLURLPATHWRITE: [Dd][Ll][Uu][Rr][Ll][Pp][Aa][Tt][Hh][Ww][Rr][Ii][Tt][Ee];
DLURLSCHEME: [Dd][Ll][Uu][Rr][Ll][Ss][Cc][Hh][Ee][Mm][Ee];
DLURLSERVER: [Dd][Ll][Uu][Rr][Ll][Ss][Ee][Rr][Vv][Ee][Rr];
DLVALUE: [Dd][Ll][Vv][Aa][Ll][Uu][Ee];
DOUBLE: [Dd][Oo][Uu][Bb][Ll][Ee];
DROP: [Dd][Rr][Oo][Pp];
DYNAMIC: [Dd][Yy][Nn][Aa][Mm][Ii][Cc];
EACH: [Ee][Aa][Cc][Hh];
ELEMENT: [Ee][Ll][Ee][Mm][Ee][Nn][Tt];
ELSE: [Ee][Ll][Ss][Ee];
END: [Ee][Nn][Dd];
END_EXEC: [Ee][Nn][Dd] '-' [Ee][Xx][Ee][Cc];
END_FRAME: [Ee][Nn][Dd] '_' [Ff][Rr][Aa][Mm][Ee];
END_PARTITION: [Ee][Nn][Dd] '_' [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn];
EQUALS: [Ee][Qq][Uu][Aa][Ll][Ss];
ESCAPE: [Ee][Ss][Cc][Aa][Pp][Ee];
EVERY: [Ee][Vv][Ee][Rr][Yy];
EXCEPT: [Ee][Xx][Cc][Ee][Pp][Tt];
EXEC: [Ee][Xx][Ee][Cc];
EXECUTE: [Ee][Xx][Ee][Cc][Uu][Tt][Ee];
EXISTS: [Ee][Xx][Ii][Ss][Tt][Ss];
EXP: [Ee][Xx][Pp];
EXPLAIN: [Ee][Xx][Pp][Ll][Aa][Ii][Nn];
EXTERNAL: [Ee][Xx][Tt][Ee][Rr][Nn][Aa][Ll];
EXTRACT: [Ee][Xx][Tt][Rr][Aa][Cc][Tt];
FAIL: [Ff][Aa][Ii][Ll];
FALSE: [Ff][Aa][Ll][Ss][Ee];
FETCH: [Ff][Ee][Tt][Cc][Hh];
FILTER: [Ff][Ii][Ll][Tt][Ee][Rr];
FIRST_VALUE: [Ff][Ii][Rr][Ss][Tt] '_' [Vv][Aa][Ll][Uu][Ee];
FLOAT: [Ff][Ll][Oo][Aa][Tt];
FLOOR: [Ff][Ll][Oo][Oo][Rr];
FOR: [Ff][Oo][Rr];
FOREIGN: [Ff][Oo][Rr][Ee][Ii][Gg][Nn];
FRAME_ROW: [Ff][Rr][Aa][Mm][Ee] '_' [Rr][Oo][Ww];
FREE: [Ff][Rr][Ee][Ee];
FROM: [Ff][Rr][Oo][Mm];
FULL: [Ff][Uu][Ll][Ll];
FUNCTION: [Ff][Uu][Nn][Cc][Tt][Ii][Oo][Nn];
FUSION: [Ff][Uu][Ss][Ii][Oo][Nn];
GET: [Gg][Ee][Tt];
GLOB: [Gg][Ll][Oo][Bb];
GLOBAL: [Gg][Ll][Oo][Bb][Aa][Ll];
GRANT: [Gg][Rr][Aa][Nn][Tt];
GROUP: [Gg][Rr][Oo][Uu][Pp];
GROUPING: [Gg][Rr][Oo][Uu][Pp][Ii][Nn][Gg];
GROUPS: [Gg][Rr][Oo][Uu][Pp][Ss];
HAVING: [Hh][Aa][Vv][Ii][Nn][Gg];
HOLD: [Hh][Oo][Ll][Dd];
HOUR: [Hh][Oo][Uu][Rr];
IDENTITY: [Ii][Dd][Ee][Nn][Tt][Ii][Tt][Yy];
IF: [Ii][Ff];
IGNORE: [Ii][Gg][Nn][Oo][Rr][Ee];
IMPORT: [Ii][Mm][Pp][Oo][Rr][Tt];
IN: [Ii][Nn];
INDICATOR: [Ii][Nn][Dd][Ii][Cc][Aa][Tt][Oo][Rr];
INNER: [Ii][Nn][Nn][Ee][Rr];
INOUT: [Ii][Nn][Oo][Uu][Tt];
INSENSITIVE: [Ii][Nn][Ss][Ee][Nn][Ss][Ii][Tt][Ii][Vv][Ee];
INSERT: [Ii][Nn][Ss][Ee][Rr][Tt];
INT: [Ii][Nn][Tt];
INTEGER: [Ii][Nn][Tt][Ee][Gg][Ee][Rr];
INTERSECT: [Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt];
INTERSECTION: [Ii][Nn][Tt][Ee][Rr][Ss][Ee][Cc][Tt][Ii][Oo][Nn];
INTERVAL: [Ii][Nn][Tt][Ee][Rr][Vv][Aa][Ll];
INTO: [Ii][Nn][Tt][Oo];
IS: [Ii][Ss];
ISNULL: [Ii][Ss][Nn][Uu][Ll][Ll];
JOIN: [Jj][Oo][Ii][Nn];
KEY: [Kk][Ee][Yy];
LAG: [Ll][Aa][Gg];
LANGUAGE: [Ll][Aa][Nn][Gg][Uu][Aa][Gg][Ee];
LARGE: [Ll][Aa][Rr][Gg][Ee];
LAST_VALUE: [Ll][Aa][Ss][Tt] '_' [Vv][Aa][Ll][Uu][Ee];
LATERAL: [Ll][Aa][Tt][Ee][Rr][Aa][Ll];
LEAD: [Ll][Ee][Aa][Dd];
LEADING: [Ll][Ee][Aa][Dd][Ii][Nn][Gg];
LEFT: [Ll][Ee][Ff][Tt];
LIKE: [Ll][Ii][Kk][Ee];
LIKE_REGEX: [Ll][Ii][Kk][Ee] '_' [Rr][Ee][Gg][Ee][Xx];
LIMIT: [Ll][Ii][Mm][Ii][Tt];
LN: [Ll][Nn];
LOCAL: [Ll][Oo][Cc][Aa][Ll];
LOCALTIME: [Ll][Oo][Cc][Aa][Ll][Tt][Ii][Mm][Ee];
LOCALTIMESTAMP: [Ll][Oo][Cc][Aa][Ll][Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
LOCATION: [Ll][Oo][Cc][Aa][Tt][Ii][Oo][Nn];
LOWER: [Ll][Oo][Ww][Ee][Rr];
MATCH: [Mm][Aa][Tt][Cc][Hh];
MAX: [Mm][Aa][Xx];
MEMBER: [Mm][Ee][Mm][Bb][Ee][Rr];
MERGE: [Mm][Ee][Rr][Gg][Ee];
METHOD: [Mm][Ee][Tt][Hh][Oo][Dd];
MIN: [Mm][Ii][Nn];
MINUTE: [Mm][Ii][Nn][Uu][Tt][Ee];
K_MOD: [Mm][Oo][Dd];
MODIFIES: [Mm][Oo][Dd][Ii][Ff][Ii][Ee][Ss];
MODULE: [Mm][Oo][Dd][Uu][Ll][Ee];
MONTH: [Mm][Oo][Nn][Tt][Hh];
MULTISET: [Mm][Uu][Ll][Tt][Ii][Ss][Ee][Tt];
NATIONAL: [Nn][Aa][Tt][Ii][Oo][Nn][Aa][Ll];
NATURAL: [Nn][Aa][Tt][Uu][Rr][Aa][Ll];
NCHAR: [Nn][Cc][Hh][Aa][Rr];
NCLOB: [Nn][Cc][Ll][Oo][Bb];
NEW: [Nn][Ee][Ww];
NO: [Nn][Oo];
NONE: [Nn][Oo][Nn][Ee];
NORMALIZE: [Nn][Oo][Rr][Mm][Aa][Ll][Ii][Zz][Ee];
NOT: [Nn][Oo][Tt];
NOTNULL: [Nn][Oo][Tt][Nn][Uu][Ll][Ll];
NTH_VALUE: [Nn][Tt][Hh] '_' [Vv][Aa][Ll][Uu][Ee];
NTILE: [Nn][Tt][Ii][Ll][Ee];
NULL: [Nn][Uu][Ll][Ll];
NULLIF: [Nn][Uu][Ll][Ll][Ii][Ff];
NUMERIC: [Nn][Uu][Mm][Ee][Rr][Ii][Cc];
OCCURRENCES_REGEX: [Oo][Cc][Cc][Uu][Rr][Rr][Ee][Nn][Cc][Ee][Ss] '_' [Rr][Ee][Gg][Ee][Xx];
OCTET_LENGTH: [Oo][Cc][Tt][Ee][Tt] '_' [Ll][Ee][Nn][Gg][Tt][Hh];
OF: [Oo][Ff];
OFFSET: [Oo][Ff][Ff][Ss][Ee][Tt];
OLD: [Oo][Ll][Dd];
ON: [Oo][Nn];
ONLY: [Oo][Nn][Ll][Yy];
OPEN: [Oo][Pp][Ee][Nn];
OR: [Oo][Rr];
ORDER: [Oo][Rr][Dd][Ee][Rr];
OUT: [Oo][Uu][Tt];
OUTER: [Oo][Uu][Tt][Ee][Rr];
OVER: [Oo][Vv][Ee][Rr];
OVERLAPS: [Oo][Vv][Ee][Rr][Ll][Aa][Pp][Ss];
OVERLAY: [Oo][Vv][Ee][Rr][Ll][Aa][Yy];
PARAMETER: [Pp][Aa][Rr][Aa][Mm][Ee][Tt][Ee][Rr];
PARTITION: [Pp][Aa][Rr][Tt][Ii][Tt][Ii][Oo][Nn];
PERCENT: [Pp][Ee][Rr][Cc][Ee][Nn][Tt];
PERCENTILE_CONT: [Pp][Ee][Rr][Cc][Ee][Nn][Tt][Ii][Ll][Ee] '_' [Cc][Oo][Nn][Tt];
PERCENTILE_DISC: [Pp][Ee][Rr][Cc][Ee][Nn][Tt][Ii][Ll][Ee] '_' [Dd][Ii][Ss][Cc];
PERCENT_RANK: [Pp][Ee][Rr][Cc][Ee][Nn][Tt] '_' [Rr][Aa][Nn][Kk];
PERIOD: [Pp][Ee][Rr][Ii][Oo][Dd];
PLAN: [Pp][Ll][Aa][Nn];
PORTION: [Pp][Oo][Rr][Tt][Ii][Oo][Nn];
POSITION: [Pp][Oo][Ss][Ii][Tt][Ii][Oo][Nn];
POSITION_REGEX: [Pp][Oo][Ss][Ii][Tt][Ii][Oo][Nn] '_' [Rr][Ee][Gg][Ee][Xx];
POWER: [Pp][Oo][Ww][Ee][Rr];
PRAGMA: [Pp][Rr][Aa][Gg][Mm][Aa];
PRECEDES: [Pp][Rr][Ee][Cc][Ee][Dd][Ee][Ss];
PRECISION: [Pp][Rr][Ee][Cc][Ii][Ss][Ii][Oo][Nn];
PREPARE: [Pp][Rr][Ee][Pp][Aa][Rr][Ee];
PRIMARY: [Pp][Rr][Ii][Mm][Aa][Rr][Yy];
PROCEDURE: [Pp][Rr][Oo][Cc][Ee][Dd][Uu][Rr][Ee];
QUERY: [Qq][Uu][Ee][Rr][Yy];
RAISE: [Rr][Aa][Ii][Ss][Ee];
RANGE: [Rr][Aa][Nn][Gg][Ee];
RANK: [Rr][Aa][Nn][Kk];
READS: [Rr][Ee][Aa][Dd][Ss];
REAL: [Rr][Ee][Aa][Ll];
RECURSIVE: [Rr][Ee][Cc][Uu][Rr][Ss][Ii][Vv][Ee];
REF: [Rr][Ee][Ff];
REFERENCES: [Rr][Ee][Ff][Ee][Rr][Ee][Nn][Cc][Ee][Ss];
REFERENCING: [Rr][Ee][Ff][Ee][Rr][Ee][Nn][Cc][Ii][Nn][Gg];
REGEXP: [Rr][Ee][Gg][Ee][Xx][Pp];
REGR_AVGX: [Rr][Ee][Gg][Rr] '_' [Aa][Vv][Gg][Xx];
REGR_AVGY: [Rr][Ee][Gg][Rr] '_' [Aa][Vv][Gg][Yy];
REGR_COUNT: [Rr][Ee][Gg][Rr] '_' [Cc][Oo][Uu][Nn][Tt];
REGR_INTERCEPT: [Rr][Ee][Gg][Rr] '_' [Ii][Nn][Tt][Ee][Rr][Cc][Ee][Pp][Tt];
REGR_R2: [Rr][Ee][Gg][Rr] '_' [Rr] '2' ;
REGR_SLOPE: [Rr][Ee][Gg][Rr] '_' [Ss][Ll][Oo][Pp][Ee];
REGR_SXX: [Rr][Ee][Gg][Rr] '_' [Ss][Xx][Xx];
REGR_SXY: [Rr][Ee][Gg][Rr] '_' [Ss][Xx][Yy];
REGR_SYY: [Rr][Ee][Gg][Rr] '_' [Ss][Yy][Yy];
RELEASE: [Rr][Ee][Ll][Ee][Aa][Ss][Ee];
RESULT: [Rr][Ee][Ss][Uu][Ll][Tt];
RETURN: [Rr][Ee][Tt][Uu][Rr][Nn];
RETURNS: [Rr][Ee][Tt][Uu][Rr][Nn][Ss];
REVOKE: [Rr][Ee][Vv][Oo][Kk][Ee];
RIGHT: [Rr][Ii][Gg][Hh][Tt];
ROLLBACK: [Rr][Oo][Ll][Ll][Bb][Aa][Cc][Kk];
ROLLUP: [Rr][Oo][Ll][Ll][Uu][Pp];
ROW: [Rr][Oo][Ww];
ROWS: [Rr][Oo][Ww][Ss];
ROW_NUMBER: [Rr][Oo][Ww] '_' [Nn][Uu][Mm][Bb][Ee][Rr];
SAVEPOINT: [Ss][Aa][Vv][Ee][Pp][Oo][Ii][Nn][Tt];
SCOPE: [Ss][Cc][Oo][Pp][Ee];
SCROLL: [Ss][Cc][Rr][Oo][Ll][Ll];
SEARCH: [Ss][Ee][Aa][Rr][Cc][Hh];
SECOND: [Ss][Ee][Cc][Oo][Nn][Dd];
SELECT: [Ss][Ee][Ll][Ee][Cc][Tt];
SENSITIVE: [Ss][Ee][Nn][Ss][Ii][Tt][Ii][Vv][Ee];
SESSION_USER: [Ss][Ee][Ss][Ss][Ii][Oo][Nn] '_' [Uu][Ss][Ee][Rr];
SET: [Ss][Ee][Tt];
SIMILAR: [Ss][Ii][Mm][Ii][Ll][Aa][Rr];
SMALLINT: [Ss][Mm][Aa][Ll][Ll][Ii][Nn][Tt];
SOME: [Ss][Oo][Mm][Ee];
SPECIFIC: [Ss][Pp][Ee][Cc][Ii][Ff][Ii][Cc];
SPECIFICTYPE: [Ss][Pp][Ee][Cc][Ii][Ff][Ii][Cc][Tt][Yy][Pp][Ee];
SQL: [Ss][Qq][Ll];
SQLEXCEPTION: [Ss][Qq][Ll][Ee][Xx][Cc][Ee][Pp][Tt][Ii][Oo][Nn];
SQLSTATE: [Ss][Qq][Ll][Ss][Tt][Aa][Tt][Ee];
SQLWARNING: [Ss][Qq][Ll][Ww][Aa][Rr][Nn][Ii][Nn][Gg];
SQRT: [Ss][Qq][Rr][Tt];
START: [Ss][Tt][Aa][Rr][Tt];
STATIC: [Ss][Tt][Aa][Tt][Ii][Cc];
STDDEV_POP: [Ss][Tt][Dd][Dd][Ee][Vv] '_' [Pp][Oo][Pp];
STDDEV_SAMP: [Ss][Tt][Dd][Dd][Ee][Vv] '_' [Ss][Aa][Mm][Pp];
SUBMULTISET: [Ss][Uu][Bb][Mm][Uu][Ll][Tt][Ii][Ss][Ee][Tt];
SUBSTRING: [Ss][Uu][Bb][Ss][Tt][Rr][Ii][Nn][Gg];
SUBSTRING_REGEX: [Ss][Uu][Bb][Ss][Tt][Rr][Ii][Nn][Gg] '_' [Rr][Ee][Gg][Ee][Xx];
SUCCEEDS: [Ss][Uu][Cc][Cc][Ee][Ee][Dd][Ss];
SUM: [Ss][Uu][Mm];
SYMMETRIC: [Ss][Yy][Mm][Mm][Ee][Tt][Rr][Ii][Cc];
SYSTEM: [Ss][Yy][Ss][Tt][Ee][Mm];
SYSTEM_TIME: [Ss][Yy][Ss][Tt][Ee][Mm] '_' [Tt][Ii][Mm][Ee];
SYSTEM_USER: [Ss][Yy][Ss][Tt][Ee][Mm] '_' [Uu][Ss][Ee][Rr];
TABLE: [Tt][Aa][Bb][Ll][Ee];
TABLESAMPLE: [Tt][Aa][Bb][Ll][Ee][Ss][Aa][Mm][Pp][Ll][Ee];
TEMP: [Tt][Ee][Mm][Pp];
TEMPORARY: [Tt][Ee][Mm][Pp][Oo][Rr][Aa][Rr][Yy];
THEN: [Tt][Hh][Ee][Nn];
TIME: [Tt][Ii][Mm][Ee];
TIMESTAMP: [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp];
TIMEZONE_HOUR: [Tt][Ii][Mm][Ee][Zz][Oo][Nn][Ee] '_' [Hh][Oo][Uu][Rr];
TIMEZONE_MINUTE: [Tt][Ii][Mm][Ee][Zz][Oo][Nn][Ee] '_' [Mm][Ii][Nn][Uu][Tt][Ee];
TINYINT: [Tt][Ii][Nn][Yy][Ii][Nn][Tt];
TO: [Tt][Oo];
TRAILING: [Tt][Rr][Aa][Ii][Ll][Ii][Nn][Gg];
TRANSLATE: [Tt][Rr][Aa][Nn][Ss][Ll][Aa][Tt][Ee];
TRANSLATE_REGEX: [Tt][Rr][Aa][Nn][Ss][Ll][Aa][Tt][Ee] '_' [Rr][Ee][Gg][Ee][Xx];
TRANSLATION: [Tt][Rr][Aa][Nn][Ss][Ll][Aa][Tt][Ii][Oo][Nn];
TREAT: [Tt][Rr][Ee][Aa][Tt];
TRIGGER: [Tt][Rr][Ii][Gg][Gg][Ee][Rr];
TRIM: [Tt][Rr][Ii][Mm];
TRIM_ARRAY: [Tt][Rr][Ii][Mm] '_' [Aa][Rr][Rr][Aa][Yy];
TRUE: [Tt][Rr][Uu][Ee];
TRUNCATE: [Tt][Rr][Uu][Nn][Cc][Aa][Tt][Ee];
UESCAPE: [Uu][Ee][Ss][Cc][Aa][Pp][Ee];
UNION: [Uu][Nn][Ii][Oo][Nn];
UNIQUE: [Uu][Nn][Ii][Qq][Uu][Ee];
UNKNOWN: [Uu][Nn][Kk][Nn][Oo][Ww][Nn];
UNNEST: [Uu][Nn][Nn][Ee][Ss][Tt];
UPDATE: [Uu][Pp][Dd][Aa][Tt][Ee];
UPPER: [Uu][Pp][Pp][Ee][Rr];
USER: [Uu][Ss][Ee][Rr];
USING: [Uu][Ss][Ii][Nn][Gg];
VALUE: [Vv][Aa][Ll][Uu][Ee];
VALUES: [Vv][Aa][Ll][Uu][Ee][Ss];
VALUE_OF: [Vv][Aa][Ll][Uu][Ee] '_' [Oo][Ff];
VARBINARY: [Vv][Aa][Rr][Bb][Ii][Nn][Aa][Rr][Yy];
VARCHAR: [Vv][Aa][Rr][Cc][Hh][Aa][Rr];
VARYING: [Vv][Aa][Rr][Yy][Ii][Nn][Gg];
VAR_POP: [Vv][Aa][Rr] '_' [Pp][Oo][Pp];
VAR_SAMP: [Vv][Aa][Rr] '_' [Ss][Aa][Mm][Pp];
VERSIONING: [Vv][Ee][Rr][Ss][Ii][Oo][Nn][Ii][Nn][Gg];
VIEW: [Vv][Ii][Ee][Ww];
WHEN: [Ww][Hh][Ee][Nn];
WHENEVER: [Ww][Hh][Ee][Nn][Ee][Vv][Ee][Rr];
WHERE: [Ww][Hh][Ee][Rr][Ee];
WIDTH_BUCKET: [Ww][Ii][Dd][Tt][Hh] '_' [Bb][Uu][Cc][Kk][Ee][Tt];
WINDOW: [Ww][Ii][Nn][Dd][Oo][Ww];
WITH: [Ww][Ii][Tt][Hh];
WITHIN: [Ww][Ii][Tt][Hh][Ii][Nn];
WITHOUT: [Ww][Ii][Tt][Hh][Oo][Uu][Tt];
XML: [Xx][Mm][Ll];
XMLAGG: [Xx][Mm][Ll][Aa][Gg][Gg];
XMLATTRIBUTES: [Xx][Mm][Ll][Aa][Tt][Tt][Rr][Ii][Bb][Uu][Tt][Ee][Ss];
XMLBINARY: [Xx][Mm][Ll][Bb][Ii][Nn][Aa][Rr][Yy];
XMLCAST: [Xx][Mm][Ll][Cc][Aa][Ss][Tt];
XMLCOMMENT: [Xx][Mm][Ll][Cc][Oo][Mm][Mm][Ee][Nn][Tt];
XMLCONCAT: [Xx][Mm][Ll][Cc][Oo][Nn][Cc][Aa][Tt];
XMLDOCUMENT: [Xx][Mm][Ll][Dd][Oo][Cc][Uu][Mm][Ee][Nn][Tt];
XMLELEMENT: [Xx][Mm][Ll][Ee][Ll][Ee][Mm][Ee][Nn][Tt];
XMLEXISTS: [Xx][Mm][Ll][Ee][Xx][Ii][Ss][Tt][Ss];
XMLFOREST: [Xx][Mm][Ll][Ff][Oo][Rr][Ee][Ss][Tt];
XMLITERATE: [Xx][Mm][Ll][Ii][Tt][Ee][Rr][Aa][Tt][Ee];
XMLNAMESPACES: [Xx][Mm][Ll][Nn][Aa][Mm][Ee][Ss][Pp][Aa][Cc][Ee][Ss];
XMLPARSE: [Xx][Mm][Ll][Pp][Aa][Rr][Ss][Ee];
XMLPI: [Xx][Mm][Ll][Pp][Ii];
XMLQUERY: [Xx][Mm][Ll][Qq][Uu][Ee][Rr][Yy];
XMLSERIALIZE: [Xx][Mm][Ll][Ss][Ee][Rr][Ii][Aa][Ll][Ii][Zz][Ee];
XMLTABLE: [Xx][Mm][Ll][Tt][Aa][Bb][Ll][Ee];
XMLTEXT: [Xx][Mm][Ll][Tt][Ee][Xx][Tt];
XMLVALIDATE: [Xx][Mm][Ll][Vv][Aa][Ll][Ii][Dd][Aa][Tt][Ee];
YEAR: [Yy][Ee][Aa][Rr];

IDENTIFIER
 : '"' (~'"' | '""')* '"'
 | '`' (~'`' | '``')* '`'
 | '[' ~']'* ']'
 | [a-zA-Z_] [a-zA-Z_0-9]* // TODO check: needs more chars in set
 ;

NUMERIC_LITERAL
 : DIGIT+ ( '.' DIGIT* )? ( [eE] [-+]? DIGIT+ )?
 | '.' DIGIT+ ( [eE] [-+]? DIGIT+ )?
 ;

BIND_PARAMETER
 : '?' DIGIT*
 | [:@$] IDENTIFIER
 ;

STRING_LITERAL
 : '\'' ( ~'\'' | '\'\'' )* '\''
 ;

BLOB_LITERAL
 : [xX] STRING_LITERAL
 ;

SINGLE_LINE_COMMENT
 : '--' ~[\r\n]* -> channel(HIDDEN)
 ;

MULTILINE_COMMENT
 : '/*' .*? ( '*/' | EOF ) -> channel(HIDDEN)
 ;

SPACES
 : [ \u000B\t\r\n] -> channel(HIDDEN)
 ;

UNEXPECTED_CHAR
 : .
 ;

fragment DIGIT : [0-9];