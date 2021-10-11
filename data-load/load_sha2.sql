CREATE WAREHOUSE BENCHMARK_LOAD_WH_L WITH WAREHOUSE_SIZE = 'LARGE' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD';

use WAREHOUSE BENCHMARK_LOAD_WH_L;

-- ###### TPCH_SF1000_SHA2 - SHA2_BINARY(256) keys - hex-encoded string containing 256-bit message
create schema DV_BENCHMARK.TPCH_SF1000_SHA2;

create table DV_BENCHMARK.TPCH_SF1000_SHA2.NATION as
select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.NATION;

create table DV_BENCHMARK.TPCH_SF1000_SHA2.REGION as
select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.REGION;

create table DV_BENCHMARK.TPCH_SF1000_SHA2.CUSTOMER as
select 
  CAST(SHA2_BINARY(c_custkey) AS BINARY(32)) as c_custkey,
  c_name,
  c_address,
  c_nationkey,
  c_phone,
  c_acctbal,
  c_mktsegment,
  c_comment 
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;

create table DV_BENCHMARK.TPCH_SF1000_SHA2.SUPPLIER as
select
  CAST(SHA2_BINARY(s_suppkey) AS BINARY(32)) as s_suppkey,
  s_name,
  s_address,
  s_nationkey,
  s_phone,
  s_acctbal,
  s_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER;

create table DV_BENCHMARK.TPCH_SF1000_SHA2.PART as
select
  CAST(SHA2_BINARY(p_partkey) AS BINARY(32)) as p_partkey,
  p_name,
  p_mfgr,
  p_brand,
  p_type,
  p_size,
  p_container,
  p_retailprice,
  p_comment    
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PART;


create table DV_BENCHMARK.TPCH_SF1000_SHA2.PARTSUPP as
select
  CAST(SHA2_BINARY(ps_partkey) AS BINARY(32)) as ps_partkey,
  CAST(SHA2_BINARY(ps_suppkey) AS BINARY(32)) as ps_suppkey,
  ps_availqty,
  ps_supplycost,
  ps_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PARTSUPP;


create table DV_BENCHMARK.TPCH_SF1000_SHA2.ORDERS as
select 
  CAST(SHA2_BINARY(o_orderkey) AS BINARY(32)) as o_orderkey,
  CAST(SHA2_BINARY(o_custkey) AS BINARY(32)) as o_custkey,
  o_orderstatus,
  o_totalprice,
  o_orderdate,
  o_orderpriority,
  o_clerk,
  o_shippriority,
  o_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS order by O_ORDERDATE;

create table DV_BENCHMARK.TPCH_SF1000_SHA2.LINEITEM as
select
  CAST(SHA2_BINARY(l_orderkey) AS BINARY(32)) as l_orderkey,
  CAST(SHA2_BINARY(l_partkey) AS BINARY(32)) as l_partkey,
  CAST(SHA2_BINARY(l_suppkey) AS BINARY(32)) as l_suppkey,
  l_linenumber,
  l_quantity,
  l_extendedprice,
  l_discount,
  l_tax,
  l_returnflag,
  l_linestatus,
  l_shipdate,
  l_commitdate,
  l_receiptdate,
  l_shipinstruct,
  l_shipmode,
  l_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.LINEITEM order by L_SHIPDATE;


drop WAREHOUSE BENCHMARK_LOAD_WH_L;
