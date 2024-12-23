/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.incremental;

import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import java.util.List;

/** Operator table containing functions and operators supported by the incremental processing. */
public class IncrementalSqlOperatorTable extends ReflectiveSqlOperatorTable {
    /** The table containing supported operators. */
    private static IncrementalSqlOperatorTable instance = null;

    /** Returns the operator table, creating it if necessary. */
    public static synchronized IncrementalSqlOperatorTable instance() {
        if (instance == null) {
            instance = new IncrementalSqlOperatorTable();
            instance.init();
            FlinkSqlOperatorTable.dynamicFunctions(true).forEach(instance::register);
        }
        return instance;
    }

    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher) {
        super.lookupOperatorOverloads(opName, category, syntax, operatorList, nameMatcher);
    }

    // -----------------------------------------------------------------------------
    // Flink specific built-in scalar SQL functions
    // -----------------------------------------------------------------------------

    public static final SqlFunction PROCTIME = FlinkSqlOperatorTable.PROCTIME;

    public static final SqlFunction MATCH_ROWTIME = FlinkSqlOperatorTable.MATCH_ROWTIME;

    public static final SqlFunction MATCH_PROCTIME = FlinkSqlOperatorTable.MATCH_PROCTIME;

    public static final SqlFunction PROCTIME_MATERIALIZE =
            FlinkSqlOperatorTable.PROCTIME_MATERIALIZE;

    public static final SqlFunction STREAMRECORD_TIMESTAMP =
            FlinkSqlOperatorTable.STREAMRECORD_TIMESTAMP;

    public static final SqlFunction E = FlinkSqlOperatorTable.E;

    public static final SqlFunction PI_FUNCTION = FlinkSqlOperatorTable.PI_FUNCTION;

    public static final SqlFunction CONCAT_FUNCTION = FlinkSqlOperatorTable.CONCAT_FUNCTION;

    public static final SqlFunction CONCAT_WS = FlinkSqlOperatorTable.CONCAT_WS;

    public static final SqlFunction LOG = FlinkSqlOperatorTable.LOG;

    public static final SqlFunction LOG2 = FlinkSqlOperatorTable.LOG2;

    public static final SqlFunction ROUND = FlinkSqlOperatorTable.ROUND;

    public static final SqlFunction TRUNCATE = FlinkSqlOperatorTable.TRUNCATE;

    public static final SqlFunction BIN = FlinkSqlOperatorTable.BIN;

    public static final SqlFunction SINH = FlinkSqlOperatorTable.SINH;

    public static final SqlFunction HEX = FlinkSqlOperatorTable.HEX;

    public static final SqlFunction STR_TO_MAP = FlinkSqlOperatorTable.STR_TO_MAP;

    public static final SqlFunction IS_DECIMAL = FlinkSqlOperatorTable.IS_DECIMAL;

    public static final SqlFunction IS_DIGIT = FlinkSqlOperatorTable.IS_DIGIT;

    public static final SqlFunction IS_ALPHA = FlinkSqlOperatorTable.IS_ALPHA;

    public static final SqlFunction COSH = FlinkSqlOperatorTable.COSH;

    public static final SqlFunction TANH = FlinkSqlOperatorTable.TANH;

    public static final SqlFunction CHR = FlinkSqlOperatorTable.CHR;

    public static final SqlFunction LPAD = FlinkSqlOperatorTable.LPAD;

    public static final SqlFunction RPAD = FlinkSqlOperatorTable.RPAD;

    public static final SqlFunction REPEAT = FlinkSqlOperatorTable.REPEAT;

    public static final SqlFunction REVERSE = FlinkSqlOperatorTable.REVERSE;

    public static final SqlFunction REPLACE = FlinkSqlOperatorTable.REPLACE;

    public static final SqlFunction SPLIT_INDEX = FlinkSqlOperatorTable.SPLIT_INDEX;

    public static final SqlFunction REGEXP_REPLACE = FlinkSqlOperatorTable.REGEXP_REPLACE;

    public static final SqlFunction REGEXP_EXTRACT = FlinkSqlOperatorTable.REGEXP_EXTRACT;

    public static final SqlFunction HASH_CODE = FlinkSqlOperatorTable.HASH_CODE;

    public static final SqlFunction MD5 = FlinkSqlOperatorTable.MD5;

    public static final SqlFunction SHA1 = FlinkSqlOperatorTable.SHA1;

    public static final SqlFunction SHA224 = FlinkSqlOperatorTable.SHA224;

    public static final SqlFunction SHA256 = FlinkSqlOperatorTable.SHA256;

    public static final SqlFunction SHA384 = FlinkSqlOperatorTable.SHA384;

    public static final SqlFunction SHA512 = FlinkSqlOperatorTable.SHA512;

    public static final SqlFunction SHA2 = FlinkSqlOperatorTable.SHA2;

    public static final SqlFunction DATE_FORMAT = FlinkSqlOperatorTable.DATE_FORMAT;

    public static final SqlFunction REGEXP = FlinkSqlOperatorTable.REGEXP;

    public static final SqlFunction PARSE_URL = FlinkSqlOperatorTable.PARSE_URL;

    public static final SqlFunction PRINT = FlinkSqlOperatorTable.PRINT;

    public static final SqlFunction CURRENT_ROW_TIMESTAMP =
            FlinkSqlOperatorTable.CURRENT_ROW_TIMESTAMP;

    public static final SqlFunction UNIX_TIMESTAMP = FlinkSqlOperatorTable.UNIX_TIMESTAMP;

    public static final SqlFunction FROM_UNIXTIME = FlinkSqlOperatorTable.FROM_UNIXTIME;

    public static final SqlFunction IF = FlinkSqlOperatorTable.IF;

    public static final SqlFunction TO_BASE64 = FlinkSqlOperatorTable.TO_BASE64;

    public static final SqlFunction FROM_BASE64 = FlinkSqlOperatorTable.FROM_BASE64;

    public static final SqlFunction UUID = FlinkSqlOperatorTable.UUID;

    public static final SqlFunction SUBSTRING = FlinkSqlOperatorTable.SUBSTRING;

    public static final SqlFunction SUBSTR = FlinkSqlOperatorTable.SUBSTR;

    public static final SqlFunction LEFT = FlinkSqlOperatorTable.LEFT;

    public static final SqlFunction RIGHT = FlinkSqlOperatorTable.RIGHT;

    public static final SqlFunction TO_TIMESTAMP = FlinkSqlOperatorTable.TO_TIMESTAMP;

    public static final SqlFunction TO_DATE = FlinkSqlOperatorTable.TO_DATE;

    public static final SqlFunction CONVERT_TZ = FlinkSqlOperatorTable.CONVERT_TZ;

    public static final SqlFunction LOCATE = FlinkSqlOperatorTable.LOCATE;

    public static final SqlFunction ASCII = FlinkSqlOperatorTable.ASCII;

    public static final SqlFunction ENCODE = FlinkSqlOperatorTable.ENCODE;

    public static final SqlFunction DECODE = FlinkSqlOperatorTable.DECODE;

    public static final SqlFunction INSTR = FlinkSqlOperatorTable.INSTR;

    public static final SqlFunction LTRIM = FlinkSqlOperatorTable.LTRIM;

    public static final SqlFunction RTRIM = FlinkSqlOperatorTable.RTRIM;

    public static final SqlFunction TRY_CAST = FlinkSqlOperatorTable.TRY_CAST;

    public static final SqlFunction RAND = FlinkSqlOperatorTable.RAND;

    public static final SqlFunction RAND_INTEGER = FlinkSqlOperatorTable.RAND_INTEGER;

    // -----------------------------------------------------------------------------
    // operators extend from Calcite
    // -----------------------------------------------------------------------------

    // SET OPERATORS
    public static final SqlOperator UNION = FlinkSqlOperatorTable.UNION;
    public static final SqlOperator UNION_ALL = FlinkSqlOperatorTable.UNION_ALL;
    public static final SqlOperator EXCEPT = FlinkSqlOperatorTable.EXCEPT;
    public static final SqlOperator EXCEPT_ALL = FlinkSqlOperatorTable.EXCEPT_ALL;
    public static final SqlOperator INTERSECT = FlinkSqlOperatorTable.INTERSECT;
    public static final SqlOperator INTERSECT_ALL = FlinkSqlOperatorTable.INTERSECT_ALL;

    // BINARY OPERATORS
    public static final SqlOperator AND = FlinkSqlOperatorTable.AND;
    public static final SqlOperator AS = FlinkSqlOperatorTable.AS;
    public static final SqlOperator CONCAT = FlinkSqlOperatorTable.CONCAT;
    public static final SqlOperator DIVIDE = FlinkSqlOperatorTable.DIVIDE;
    public static final SqlOperator DIVIDE_INTEGER = FlinkSqlOperatorTable.DIVIDE_INTEGER;
    public static final SqlOperator DOT = FlinkSqlOperatorTable.DOT;
    public static final SqlOperator EQUALS = FlinkSqlOperatorTable.EQUALS;
    public static final SqlOperator GREATER_THAN = FlinkSqlOperatorTable.GREATER_THAN;
    public static final SqlOperator IS_DISTINCT_FROM = FlinkSqlOperatorTable.IS_DISTINCT_FROM;
    public static final SqlOperator IS_NOT_DISTINCT_FROM =
            FlinkSqlOperatorTable.IS_NOT_DISTINCT_FROM;
    public static final SqlOperator GREATER_THAN_OR_EQUAL =
            FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL;
    public static final SqlOperator LESS_THAN = FlinkSqlOperatorTable.LESS_THAN;
    public static final SqlOperator LESS_THAN_OR_EQUAL = FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL;
    public static final SqlOperator MINUS = FlinkSqlOperatorTable.MINUS;
    public static final SqlOperator MINUS_DATE = FlinkSqlOperatorTable.MINUS_DATE;
    public static final SqlOperator MULTIPLY = FlinkSqlOperatorTable.MULTIPLY;
    public static final SqlOperator NOT_EQUALS = FlinkSqlOperatorTable.NOT_EQUALS;
    public static final SqlOperator OR = FlinkSqlOperatorTable.OR;
    public static final SqlOperator PLUS = FlinkSqlOperatorTable.PLUS;
    public static final SqlOperator DATETIME_PLUS = FlinkSqlOperatorTable.DATETIME_PLUS;
    public static final SqlOperator PERCENT_REMAINDER = FlinkSqlOperatorTable.PERCENT_REMAINDER;

    // POSTFIX OPERATORS
    public static final SqlOperator NULLS_FIRST = FlinkSqlOperatorTable.NULLS_FIRST;
    public static final SqlOperator NULLS_LAST = FlinkSqlOperatorTable.NULLS_LAST;
    public static final SqlOperator IS_NOT_NULL = FlinkSqlOperatorTable.IS_NOT_NULL;
    public static final SqlOperator IS_NULL = FlinkSqlOperatorTable.IS_NULL;
    public static final SqlOperator IS_NOT_TRUE = FlinkSqlOperatorTable.IS_NOT_TRUE;
    public static final SqlOperator IS_TRUE = FlinkSqlOperatorTable.IS_TRUE;
    public static final SqlOperator IS_NOT_FALSE = FlinkSqlOperatorTable.IS_NOT_FALSE;
    public static final SqlOperator IS_FALSE = FlinkSqlOperatorTable.IS_FALSE;
    public static final SqlOperator IS_NOT_UNKNOWN = FlinkSqlOperatorTable.IS_NOT_UNKNOWN;
    public static final SqlOperator IS_UNKNOWN = FlinkSqlOperatorTable.IS_UNKNOWN;

    // PREFIX OPERATORS
    public static final SqlOperator NOT = FlinkSqlOperatorTable.NOT;
    public static final SqlOperator UNARY_MINUS = FlinkSqlOperatorTable.UNARY_MINUS;
    public static final SqlOperator UNARY_PLUS = FlinkSqlOperatorTable.UNARY_PLUS;

    // ARRAY OPERATORS
    public static final SqlOperator ARRAY_VALUE_CONSTRUCTOR =
            FlinkSqlOperatorTable.ARRAY_VALUE_CONSTRUCTOR;
    public static final SqlOperator ELEMENT = FlinkSqlOperatorTable.ELEMENT;

    // MAP OPERATORS
    public static final SqlOperator MAP_VALUE_CONSTRUCTOR =
            FlinkSqlOperatorTable.MAP_VALUE_CONSTRUCTOR;

    // ARRAY MAP SHARED OPERATORS
    public static final SqlOperator ITEM = FlinkSqlOperatorTable.ITEM;
    public static final SqlOperator CARDINALITY = FlinkSqlOperatorTable.CARDINALITY;

    // SPECIAL OPERATORS
    public static final SqlOperator MULTISET_VALUE = FlinkSqlOperatorTable.MULTISET_VALUE;
    public static final SqlOperator ROW = FlinkSqlOperatorTable.ROW;
    public static final SqlOperator OVERLAPS = FlinkSqlOperatorTable.OVERLAPS;
    public static final SqlOperator LITERAL_CHAIN = FlinkSqlOperatorTable.LITERAL_CHAIN;
    public static final SqlOperator BETWEEN = FlinkSqlOperatorTable.BETWEEN;
    public static final SqlOperator SYMMETRIC_BETWEEN = FlinkSqlOperatorTable.SYMMETRIC_BETWEEN;
    public static final SqlOperator NOT_BETWEEN = FlinkSqlOperatorTable.NOT_BETWEEN;
    public static final SqlOperator SYMMETRIC_NOT_BETWEEN =
            FlinkSqlOperatorTable.SYMMETRIC_NOT_BETWEEN;
    public static final SqlOperator NOT_LIKE = FlinkSqlOperatorTable.NOT_LIKE;
    public static final SqlOperator LIKE = FlinkSqlOperatorTable.LIKE;
    public static final SqlOperator NOT_SIMILAR_TO = FlinkSqlOperatorTable.NOT_SIMILAR_TO;
    public static final SqlOperator SIMILAR_TO = FlinkSqlOperatorTable.SIMILAR_TO;
    public static final SqlOperator CASE = FlinkSqlOperatorTable.CASE;
    public static final SqlOperator REINTERPRET = FlinkSqlOperatorTable.REINTERPRET;
    public static final SqlOperator EXTRACT = FlinkSqlOperatorTable.EXTRACT;
    public static final SqlOperator IN = FlinkSqlOperatorTable.IN;
    public static final SqlOperator SEARCH = FlinkSqlOperatorTable.SEARCH;
    public static final SqlOperator NOT_IN = FlinkSqlOperatorTable.NOT_IN;

    // FUNCTIONS
    public static final SqlFunction OVERLAY = FlinkSqlOperatorTable.OVERLAY;
    public static final SqlFunction TRIM = FlinkSqlOperatorTable.TRIM;
    public static final SqlFunction POSITION = FlinkSqlOperatorTable.POSITION;
    public static final SqlFunction CHAR_LENGTH = FlinkSqlOperatorTable.CHAR_LENGTH;
    public static final SqlFunction CHARACTER_LENGTH = FlinkSqlOperatorTable.CHARACTER_LENGTH;
    public static final SqlFunction UPPER = FlinkSqlOperatorTable.UPPER;
    public static final SqlFunction LOWER = FlinkSqlOperatorTable.LOWER;
    public static final SqlFunction INITCAP = FlinkSqlOperatorTable.INITCAP;
    public static final SqlFunction POWER = FlinkSqlOperatorTable.POWER;
    public static final SqlFunction SQRT = FlinkSqlOperatorTable.SQRT;
    public static final SqlFunction MOD = FlinkSqlOperatorTable.MOD;
    public static final SqlFunction LN = FlinkSqlOperatorTable.LN;
    public static final SqlFunction LOG10 = FlinkSqlOperatorTable.LOG10;
    public static final SqlFunction ABS = FlinkSqlOperatorTable.ABS;
    public static final SqlFunction EXP = FlinkSqlOperatorTable.EXP;
    public static final SqlFunction NULLIF = FlinkSqlOperatorTable.NULLIF;
    public static final SqlFunction FLOOR = FlinkSqlOperatorTable.FLOOR;
    public static final SqlFunction CEIL = FlinkSqlOperatorTable.CEIL;
    public static final SqlFunction CAST = FlinkSqlOperatorTable.CAST;
    public static final SqlOperator SCALAR_QUERY = FlinkSqlOperatorTable.SCALAR_QUERY;
    public static final SqlOperator EXISTS = FlinkSqlOperatorTable.EXISTS;
    public static final SqlFunction SIN = FlinkSqlOperatorTable.SIN;
    public static final SqlFunction COS = FlinkSqlOperatorTable.COS;
    public static final SqlFunction TAN = FlinkSqlOperatorTable.TAN;
    public static final SqlFunction COT = FlinkSqlOperatorTable.COT;
    public static final SqlFunction ASIN = FlinkSqlOperatorTable.ASIN;
    public static final SqlFunction ACOS = FlinkSqlOperatorTable.ACOS;
    public static final SqlFunction ATAN = FlinkSqlOperatorTable.ATAN;
    public static final SqlFunction ATAN2 = FlinkSqlOperatorTable.ATAN2;
    public static final SqlFunction DEGREES = FlinkSqlOperatorTable.DEGREES;
    public static final SqlFunction RADIANS = FlinkSqlOperatorTable.RADIANS;
    public static final SqlFunction SIGN = FlinkSqlOperatorTable.SIGN;
    public static final SqlFunction PI = FlinkSqlOperatorTable.PI;

    // TIME FUNCTIONS
    public static final SqlFunction YEAR = FlinkSqlOperatorTable.YEAR;
    public static final SqlFunction QUARTER = FlinkSqlOperatorTable.QUARTER;
    public static final SqlFunction MONTH = FlinkSqlOperatorTable.MONTH;
    public static final SqlFunction WEEK = FlinkSqlOperatorTable.WEEK;
    public static final SqlFunction HOUR = FlinkSqlOperatorTable.HOUR;
    public static final SqlFunction MINUTE = FlinkSqlOperatorTable.MINUTE;
    public static final SqlFunction SECOND = FlinkSqlOperatorTable.SECOND;
    public static final SqlFunction DAYOFYEAR = FlinkSqlOperatorTable.DAYOFYEAR;
    public static final SqlFunction DAYOFMONTH = FlinkSqlOperatorTable.DAYOFMONTH;
    public static final SqlFunction DAYOFWEEK = FlinkSqlOperatorTable.DAYOFWEEK;
    public static final SqlFunction TIMESTAMP_ADD = FlinkSqlOperatorTable.TIMESTAMP_ADD;
    public static final SqlFunction TIMESTAMP_DIFF = FlinkSqlOperatorTable.TIMESTAMP_DIFF;

    // MATCH_RECOGNIZE
    public static final SqlFunction FIRST = FlinkSqlOperatorTable.FIRST;
    public static final SqlFunction LAST = FlinkSqlOperatorTable.LAST;
    public static final SqlFunction PREV = FlinkSqlOperatorTable.PREV;
    public static final SqlFunction NEXT = FlinkSqlOperatorTable.NEXT;
    public static final SqlFunction CLASSIFIER = FlinkSqlOperatorTable.CLASSIFIER;
    public static final SqlOperator FINAL = FlinkSqlOperatorTable.FINAL;
    public static final SqlOperator RUNNING = FlinkSqlOperatorTable.RUNNING;

    // JSON FUNCTIONS
    public static final SqlFunction JSON_EXISTS = FlinkSqlOperatorTable.JSON_EXISTS;
    public static final SqlFunction JSON_VALUE = FlinkSqlOperatorTable.JSON_VALUE;
    public static final SqlFunction JSON_QUERY = FlinkSqlOperatorTable.JSON_QUERY;
    public static final SqlFunction JSON_OBJECT = FlinkSqlOperatorTable.JSON_OBJECT;

    public static final SqlFunction JSON_ARRAY = FlinkSqlOperatorTable.JSON_ARRAY;

    public static final SqlPostfixOperator IS_JSON_VALUE = FlinkSqlOperatorTable.IS_JSON_VALUE;
    public static final SqlPostfixOperator IS_JSON_OBJECT = FlinkSqlOperatorTable.IS_JSON_OBJECT;
    public static final SqlPostfixOperator IS_JSON_ARRAY = FlinkSqlOperatorTable.IS_JSON_ARRAY;
    public static final SqlPostfixOperator IS_JSON_SCALAR = FlinkSqlOperatorTable.IS_JSON_SCALAR;
    public static final SqlPostfixOperator IS_NOT_JSON_VALUE =
            FlinkSqlOperatorTable.IS_NOT_JSON_VALUE;
    public static final SqlPostfixOperator IS_NOT_JSON_OBJECT =
            FlinkSqlOperatorTable.IS_NOT_JSON_OBJECT;
    public static final SqlPostfixOperator IS_NOT_JSON_ARRAY =
            FlinkSqlOperatorTable.IS_NOT_JSON_ARRAY;
    public static final SqlPostfixOperator IS_NOT_JSON_SCALAR =
            FlinkSqlOperatorTable.IS_NOT_JSON_SCALAR;

    // Catalog Functions
    public static final SqlFunction CURRENT_DATABASE = FlinkSqlOperatorTable.CURRENT_DATABASE;
}
