/*

Pregunta
===========================================================================

Escriba una consulta que compute la cantidad de registros por letra de la 
columna 2 y clave de la columna 3

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

*/
DROP TABLE IF EXISTS t0; 
DROP TABLE IF EXISTS datos; 
CREATE TABLE t0 ( 
    c1 STRING, 
    c2 ARRAY<CHAR(1)>,  
    c3 MAP<STRING, INT> 
    ) 
    ROW FORMAT DELIMITED  
        FIELDS TERMINATED BY '\t' 
        COLLECTION ITEMS TERMINATED BY ',' 
        MAP KEYS TERMINATED BY '#' 
        LINES TERMINATED BY '\n'; 
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE t0; 
 
WITH A AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY NULL ORDER BY NULL) CONS, * FROM t0
    ),
    B AS (
        SELECT CONS, exp.c2 AS c2 FROM A 
            LATERAL VIEW EXPLODE(c2) exp AS c2
    ),
    C AS (
        SELECT CONS, exp.c3 AS c3 FROM A 
            LATERAL VIEW EXPLODE(MAP_KEYS(c3)) exp AS c3
    )
INSERT OVERWRITE DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT t1.c2, t2.c3, count(1)
    FROM B AS t1
    INNER JOIN C AS t2 ON t1.CONS = t2.CONS
    GROUP BY  t1.c2, t2.c3;