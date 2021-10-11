CREATE WAREHOUSE BENCHMARK_LOAD_WH_L WITH WAREHOUSE_SIZE = 'LARGE' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD';

use WAREHOUSE BENCHMARK_LOAD_WH_L;

-- ###### TPCH_SF1000_MD5 - MD5 hash keys - 32-character hex-encoded string containing 128-bit message digest
create schema DV_BENCHMARK.TPCH_SF1000_MD5;

create table DV_BENCHMARK.TPCH_SF1000_MD5.NATION as
select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.NATION;

create table DV_BENCHMARK.TPCH_SF1000_MD5.REGION as
select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.REGION;

create table DV_BENCHMARK.TPCH_SF1000_MD5.CUSTOMER as
select 
  MD5_BINARY(c_custkey) as c_custkey,
  c_name,
  c_address,
  c_nationkey,
  c_phone,
  c_acctbal,
  c_mktsegment,
  c_comment 
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;

create table DV_BENCHMARK.TPCH_SF1000_MD5.SUPPLIER as
select
  MD5_BINARY(s_suppkey) as s_suppkey,
  s_name,
  s_address,
  s_nationkey,
  s_phone,
  s_acctbal,
  s_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER;

create table DV_BENCHMARK.TPCH_SF1000_MD5.PART as
select
  MD5_BINARY(p_partkey) as p_partkey,
  p_name,
  p_mfgr,
  p_brand,
  p_type,
  p_size,
  p_container,
  p_retailprice,
  p_comment    
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PART;


create table DV_BENCHMARK.TPCH_SF1000_MD5.PARTSUPP as
select
  MD5_BINARY(ps_partkey) as ps_partkey,
  MD5_BINARY(ps_suppkey) as ps_suppkey,
  ps_availqty,
  ps_supplycost,
  ps_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PARTSUPP;


create table DV_BENCHMARK.TPCH_SF1000_MD5.ORDERS as
select 
  MD5_BINARY(o_orderkey) as o_orderkey,
  MD5_BINARY(o_custkey) as o_custkey,
  o_orderstatus,
  o_totalprice,
  o_orderdate,
  o_orderpriority,
  o_clerk,
  o_shippriority,
  o_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS order by O_ORDERDATE;

create table DV_BENCHMARK.TPCH_SF1000_MD5.LINEITEM as
select
  MD5_BINARY(l_orderkey) as l_orderkey,
  MD5_BINARY(l_partkey) as l_partkey,
  MD5_BINARY(l_suppkey) as l_suppkey,
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
