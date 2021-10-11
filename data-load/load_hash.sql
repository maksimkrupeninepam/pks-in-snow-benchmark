CREATE WAREHOUSE BENCHMARK_LOAD_WH_L WITH WAREHOUSE_SIZE = 'LARGE' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD';

use WAREHOUSE BENCHMARK_LOAD_WH_L;

-- ###### TPCH_SF1000_HASH - hash signed 64-bit hash keys
create schema DV_BENCHMARK.TPCH_SF1000_HASH;


create table DV_BENCHMARK.TPCH_SF1000_HASH.NATION as
select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.NATION;

create table DV_BENCHMARK.TPCH_SF1000_HASH.REGION as
select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.REGION;

create table DV_BENCHMARK.TPCH_SF1000_HASH.CUSTOMER as
select 
  HASH(c_custkey) as c_custkey,
  c_name,
  c_address,
  c_nationkey,
  c_phone,
  c_acctbal,
  c_mktsegment,
  c_comment 
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER;

create table DV_BENCHMARK.TPCH_SF1000_HASH.SUPPLIER as
select
  HASH(s_suppkey) as s_suppkey,
  s_name,
  s_address,
  s_nationkey,
  s_phone,
  s_acctbal,
  s_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.SUPPLIER;

create table DV_BENCHMARK.TPCH_SF1000_HASH.PART as
select
  HASH(p_partkey) as p_partkey,
  p_name,
  p_mfgr,
  p_brand,
  p_type,
  p_size,
  p_container,
  p_retailprice,
  p_comment    
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PART;


create table DV_BENCHMARK.TPCH_SF1000_HASH.PARTSUPP as
select
  HASH(ps_partkey) as ps_partkey,
  HASH(ps_suppkey) as ps_suppkey,
  ps_availqty,
  ps_supplycost,
  ps_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.PARTSUPP;


create table DV_BENCHMARK.TPCH_SF1000_HASH.ORDERS as
select 
  HASH(o_orderkey) as o_orderkey,
  HASH(o_custkey) as o_custkey,
  o_orderstatus,
  o_totalprice,
  o_orderdate,
  o_orderpriority,
  o_clerk,
  o_shippriority,
  o_comment
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS order by O_ORDERDATE;

create table DV_BENCHMARK.TPCH_SF1000_HASH.LINEITEM as
select
  HASH(l_orderkey) as l_orderkey,
  HASH(l_partkey) as l_partkey,
  HASH(l_suppkey) as l_suppkey,
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
