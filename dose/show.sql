show databases ;
use 2302a;
create database 2302a;
CREATE TABLE dept (
                      deptno INT,
                      dname STRING,
                      loc INT
)
-- 指定存储格式为TextFile（默认存储格式），数据按行存储，字段间默认用制表符(\t)分隔
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

INSERT INTO dept (deptno, dname, loc) VALUES
                                          (10, 'AcCOUNTING', 1700),
                                          (20, 'RESEARCH', 1800),
                                          (30, 'SALES', 1900),
                                          (40, 'OPERATIONS', 1700);

select * from dept;
select * from dept;