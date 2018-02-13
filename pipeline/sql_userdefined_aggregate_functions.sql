CREATE OR REPLACE FUNCTION _final_mode(anyarray)
  RETURNS anyelement AS
$BODY$
    SELECT a
    FROM unnest($1) a
    GROUP BY 1 
    ORDER BY COUNT(1) DESC, 1
    LIMIT 1;
$BODY$
LANGUAGE SQL IMMUTABLE;
 
-- Tell Postgres how to use our aggregate
CREATE AGGREGATE dsapp_mode(anyelement) (
  SFUNC=array_append, --Function to call for each row. Just builds the array
  STYPE=anyarray,
  FINALFUNC=_final_mode, --Function to call after everything has been added to array
  INITCOND='{}' --Initialize an empty array when starting
);


DROP FUNCTION _final_count_distinct(anyarray) CASCADE;
CREATE OR REPLACE FUNCTION _final_count_distinct(anyarray)
  RETURNS bigint AS
$BODY$
    SELECT COUNT(DISTINCT a)
    FROM unnest($1) a;
$BODY$
LANGUAGE SQL IMMUTABLE;

CREATE AGGREGATE count_distinct(anyelement) (
  SFUNC=array_append,
  STYPE=anyarray,
  FINALFUNC=_final_count_distinct,
  INITCOND='{}' 
);
