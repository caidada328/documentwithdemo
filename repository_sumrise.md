

# 一、2.0对比1.0的改动

| 改动       | 1.0                | 2.0                        |
| ---------- | ------------------ | -------------------------- |
| 业务数仓   | 7张表              | 24张表                     |
| 数仓分层   | 4层                | 5层，多了DWT(主题明细层)层 |
| 建模方式   |                    | 更规范                     |
| 数据可视化 | springboot+echarts | superset                   |
| 集群监控   |                    | zabbix                     |
| 元数据     |                    | atlas                      |
| 权限管理   |                    | ranger                     |

# 二、电商业务介绍

## 1.术语介绍

sku: 一款商品中的具体某一个型号的产品

spu:一款商品

 uv: user views 用户浏览总量

 pv: page views 页面浏览总量



## 2.电商关键业务表

| 表名                             | 同步方式   | 字段名       | 字段描述                           |
| -------------------------------- | ---------- | ------------ | ---------------------------------- |
| order_info(订单表)               | 新增和变化 | order_status | 订单状态(会被修改)                 |
|                                  |            | create_time  | 创建时间                           |
|                                  |            | operate_time | 操作时间(最后一次修改订单的时间)   |
| order_detail(订单详情表)         | 增量       | create_time  | 创建时间                           |
|                                  |            | order_id     | 订单号可以和order_info.id关联      |
| sku_info(sku商品表)              | 全量       | create_time  | 创建时间                           |
| user_info(用户表)                | 新增和变化 | create_time  | 创建时间                           |
|                                  |            | operate_time | 操作时间(最后一次用户信息的时间)   |
| payment_info(支付流水表)         | 增量       | payment_time | 支付时间                           |
| base_category1(商品一级分类表)   | 全量       |              |                                    |
| base_category2(商品二级分类表)   | 全量       |              |                                    |
| base_category3(商品三级分类表)   | 全量       |              |                                    |
| base_province（省份表）          | 全量导一次 |              |                                    |
| base_region(地区表)              | 全量导一次 |              |                                    |
| base_trademark（品牌表）         | 全量       |              |                                    |
| order_status_log(订单状态表)     | 增量       | operate_time | 操作时间                           |
| spu_info(SPU商品表)              | 全量       |              |                                    |
| comment_info(商品评论表)         | 增量       | create_time  | 创建时间                           |
| order_refund_info（退单表）      | 增量       | create_time  | 创建时间                           |
| cart_info(加购表)                | (特殊)全量 | create_time  | 创建时间                           |
|                                  |            | operate_time | 操作时间(最后一次修改购物车的时间) |
| favor_info（商品收藏表）         | (特殊)全量 | create_time  | 创建时间                           |
|                                  |            | cancel_time  | 取消收藏的最后一次时间             |
| coupon_use（优惠券领用表）       | 增量和变化 | get_time     | 领券时间                           |
|                                  |            | using_time   | 使用时间                           |
|                                  |            | used_time    | 支付时间                           |
| coupon_info（优惠券表）          | 全量       | create_time  | 创建时间                           |
| activity_info（活动表）          | 全量       | create_time  | 创建时间                           |
| activity_order（活动订单关联表） | 增量       | create_time  | 创建日期                           |
| activity_rule（优惠规则表）      | 全量       |              |                                    |
| base_dic（编码字典表）           | 全量       | create_time  | 创建日期                           |
|                                  |            | operate_time | 操作时间(最后一次修改的时间)       |
| date_info（时间表）              | 全量导一次 |              |                                    |
| holiday_info（假期表）           | 全量导一次 |              |                                    |
| holiday_year（假期年表）         | 全量导一次 |              |                                    |



## 3.业务表的同步方式

### 3.1 同步的周期

​		每天同步一次！每天将同步的数据在hive中创建一个分区！

​		一般是次日凌晨0:30(保证用户行为数据已经采集到hdfs)开始同步前一天的数据！



### 3.2 同步的策略

原则： 数据的同步策略只取决于数据量！

​			数据量小或修改频率低的表一般可以进行全量同步(省事)！

​			数据量大或修改频率高的表一般使用增量同步！

增量同步： select * from 表名 where create_time='同步的数据日期'

全量同步： select * from 表名

新增和变化同步：  select * from 表名 where create_time='同步的数据日期' or operate_time='同步的数据日期'



### 3.3 数据的保存周期

​		数仓中的数据需要保留半年的回溯周期！



## 4.数仓中表的分类

事实表： 记录某个发生的事实。一般在记录事实时会参考3W原则，对事实进行描述。记录who ，where, when,do



维度表： 用来描述事实，或描述事实中的某一部分！



事务型事实表：  特点是一旦事实发生，不会改变！表中的记录，一般只会新增！

周期型事实表： 记录事实，只记录这个事实在某个时间周期内最终的状态！重视结果！

累积型快照事实表： 记录事实，记录整个事实在某个时间周期内的累积的变化状态！重视过程！



## 5.数仓中表的分层

ODS层：  原始数据层，将采集的数据原封不动导入！

​					分区表！按照日期进行分区！

DWD层：数据明细层，将ODS层的数据，进行ETL后，展开明细！

​					分区表！按照日期进行分区！

DWS层： 数据服务层，将DWD层数据，每天轻度聚合！每天一个分区！

​					分区表！按照日期进行分区！

DWT层： 数据主题层，围绕某个具体要统计的主题，例如日活，订单，用户等，一张总表，记录了从第一天截至今日的所有的围绕此主题的汇总数据！

​					不是分区表！一张普通表！

ADS层： 数据应用层。可以从DWS层或DWT层取需要的数据！

​					不是分区表！一张普通表！



## 6.数仓中每层表的建模

ODS： 特点是保持原始数据的原貌，不作修改！

​			原始数据怎么建模，ODS就怎么建模！

​			举例：  用户行为数据特征是一条记录就是一行！

​						ODS层表(line string)

​							业务数据，参考Sqoop导入的数据类型进行建模！



DWD层：特点从ODS层，将数据进行ETL（清洗），轻度聚合，再展开明细！

​				 ①在展开明细时，对部分维度表进行降维操作！

​				例如：将商品一二三级分类表，sku商品表，spu商品表，商品品牌表合并汇总为一张维度表！

​				②对事实表，参考星型模型的建模策略，按照**选择业务过程→声明粒度→确认维度→确认事实**思路进行建模

​				选择业务过程： 选择感兴趣的事实表

​				声明粒度： 选择最细的粒度！可以由最细的粒度通过聚合的方式得到粗粒度！

​				确认维度： 根据3w原则确认维度，挑选自己感兴趣的维度

​				确认事实： 挑选感兴趣的度量字段，一般是从事实表中选取！

​				 

DWS层： 根据业务需求进行分主题建模！一般是建宽表！

DWT层：  根据业务需求进行分主题建模！一般是建宽表！

ADS层：  根据业务需求进行建模！

# 三、安装Hive2.3

## 1.安装hive

①解压tar包

②修改/etc/profile中HIVE_HOME=新hive的家目录

③配置hive-site.xml

④拷贝mysql的驱动到HVIE_HOME/lib



## 2.使用Hive

先启动hive metastore的服务！

```
hive --service metastore &
```



## 3.使用JDBC方式连接Hive

配置core-site.xml

```xml
<property>
<name>hadoop.proxyuser.atguigu.hosts</name>
<value>*</value>
</property>
<property>
<name>hadoop.proxyuser.atguigu.groups</name>
<value>*</value>
</property>
```

配置hdfs-site.xml

```xml
<property>
<name>dfs.webhdfs.enabled</name>
<value>true</value>
</property>
```

分发之后重启hdfs!

开启hiveserver2服务

```
hiveserver2
```

# 四、ODS层

## 1.数据的准备

### 1.1 造log数据

例如造05-06,05-07，xxx

①先将集群时间调整到以上日期中最早的一天 dt.sh 最早 的日期

②启动采集通道 onekeyboot.sh start

③造数据 lg 200 300

④去hdfs上查看



⑤将集群的时间，往后调整，此时采集通道不需要重启！  dt.sh xxx

​	之后重复③，④



### 1.2 造db数据

①修改application.properties，重点改数据库连接的参数

```
mock.date=造数据的日期
#第一次，mock.clear=1，之后改为0
mock.clear=1
```

②执行造数据的程序

```
java -jar gmall-mock-db-2020-03-16-SNAPSHOT.jar
```

③去数据库中查看

④执行sqoop的导入脚本

```
mysql_to_hdfs.sh xxx xxxx
```

业务数据，必须造一天，导一天！

mysql_to_hdfs.sh   参数1  参数2

​		参数1：  frist(第一次导数据) |  all(之后每天导) | 表名

​		参数2： 日期，如果缺省，默认选择当前日期的前一天



⑤重复①-④

## 2.向ods层导入数据

### 2.1 导入log数据到ods层

先建表！

```
hdfs_to_ods_log.sh 日期
```

### 2.2 导入db数据到ods层

先建表！

```
hdfs_to_ods_db.sh  first|all 日期
```



# 五、DWD层

## 1.向dwd层导入log数据

先建表！

### 1.1 导入log数据到dwd层

导入启动日志数据

```
ods_to_dwd_log.sh 日期
```

创建两个函数

```
create function base_analizer as 'com.atguigu.udf.MyUDF'
create function flat_analizer as 'com.atguigu.udtf.MyUDTF'
```

将事件日志，拆散到base表中

```
ods_to_dwd_base_log.sh 日期
```

从base表导入各个事件表

```
ods_to_dwd_event_log.sh 日期
```



## 2.向dwd层导入db数据

### 2.1 dwd_dim_sku_info

使用

with

t1 as (),

t2 as (),

t3 as (),

....

select xxx

```sql
with 
t1 as (select * from ods_base_category3 where dt='2020-05-06'),
t2 as (select * from ods_base_category2 where dt='2020-05-06'),
t3 as (select * from ods_base_category1 where dt='2020-05-06'),
t4 as (select * from ods_base_trademark where dt='2020-05-06'),
t5 as (select spu_name,id from ods_spu_info where dt='2020-05-06'),
t6 as (SELECT * from ods_sku_info where dt='2020-05-06')
insert overwrite table dwd_dim_sku_info PARTITION(dt='2020-05-06')
select 
    t6.id, t6.spu_id, t6.price, t6.sku_name,t6. sku_desc, t6.weight,
t6.tm_id,t4.tm_name,
t6.category3_id, t2.id category2_id, t3.id category1_id, 
t1.name category3_name, t2.name category2_name,t3.name category1_name,
t5.spu_name,t6.create_time
from 
t6 left JOIN t5 on t6.spu_id=t5.id
left JOIN t4 on t6.tm_id=t4.tm_id
left join t1 on t6.category3_id=t1.id
left join t2 on t1.category2_id=t2.id
left join t3 on t2.category1_id=t3.id
```

### 2.2 dwd_dim_coupon_info

```sql
set hive.exec.dynamic.partition.mode=nonstrict

insert overwrite table dwd_dim_coupon_info PARTITION(dt)
select *
FROM ods_coupon_info
where dt='2020-05-06'
```

### 2.3  dwd_dim_activity_info

```sql
insert overwrite table dwd_dim_activity_info PARTITION(dt='2020-05-06')
SELECT
    t1.id, t1.activity_name, t1.activity_type,
    t2.condition_amount, t2.condition_num, t2.benefit_amount, 
    t2.benefit_discount, t2.benefit_level,
    t1.start_time, t1.end_time, t1.create_time
FROM
(select * from ods_activity_info where dt='2020-05-06') t1
left join
(SELECT * from ods_activity_rule where dt='2020-05-06') t2
on t1.id=t2.activity_id
```

### 2.4 dwd_dim_base_province

```sql
insert overwrite TABLE dwd_dim_base_province
SELECT
    bp.id, bp.name province_name, bp.area_code, 
    bp.iso_code, bp.region_id, br.region_name
FROM
ods_base_province bp left join ods_base_region br
on bp.region_id=br.id
```

### 2.5 dwd_dim_date_info

```
load data local inpath '/home/atguigu/date_info.txt' into table dwd_dim_date_info;
```



### 2.6 维度表总结

​		将同一类型的多个维度表，进行维度退化，把所有需要的字段退化到一张维度表中！

​		

### 2.7 dwd_fact_order_detail订单明细事实表

选择业务表： ods_order_detail

粒度：一个订单的一款sku商品为一行

选择维度： 用户，时间，地区，商品

选择度量：商品的总额



思路：以ods_order_detail作为核心表，选取表中的大量字段和主键

​			以 用户，时间，地区，商品作为维度，选取四个表中的主键作为外键

​			选择商品的总额作为度量字段！

```sql
insert overwrite table dwd_fact_order_detail PARTITION(dt='2020-05-06')
SELECT 
    t1.id, t1.order_id, t1.user_id, t1.sku_id, t1.sku_name, 
    t1.order_price, t1.sku_num, t1.create_time, t2.province_id, 
    order_price*sku_num total_amount
FROM 
(SELECT * from ods_order_detail where dt='2020-05-06') t1 
join 
(select id,province_id from ods_order_info where dt='2020-05-06') t2
on t2.id=t1.order_id
```

### 2.8 dwd_fact_payment_info 支付事实表

选择业务表：ods_payment_info

粒度：一个订单的一笔付款信息为一条

选择维度： 用户，时间，地区

选择度量：支付金额



思路：选择ods_payment_info为事实表，选取用户，时间，地区作为外键！

​			选择支付金额作为度量！

```sql
insert overwrite table dwd_fact_payment_info PARTITION(dt='2020-05-06')
select
    t1.id, out_trade_no, order_id, user_id, alipay_trade_no, 
total_amount payment_amount, subject, payment_type, payment_time,
province_id
FROM
(select * from ods_payment_info where dt='2020-05-06') t1
join
(select id,province_id from ods_order_info where dt='2020-05-06') t2
on t2.id=t1.order_id
```



### 2.9 dwd_fact_order_refund_info 退款事实表

选择业务表：ods_order_refund_info

粒度：一个订单的一款商品的退款信息为1条

选择维度： 用户，时间，商品

选择度量：退款金额



思路：选择ods_order_refund_info为事实表，选取用户，时间，商品作为外键！

​			选择退款金额作为度量！



```sql
insert overwrite table dwd_fact_order_refund_info PARTITION(dt)
SELECT * from ods_order_refund_info where dt='2020-05-06'
```



### 2.10 dwd_fact_comment_info 评价事实表

选择业务表：ods_order_refund_info

粒度：针对一款sku商品的评价为1条

选择维度： 用户，时间，商品

选择度量：评价类型



思路：选择ods_order_refund_info为事实表，选取用户，时间，商品作为外键！

​			选择评价类型作为度量！

```sql
insert overwrite table dwd_fact_comment_info PARTITION(dt)
select * from ods_comment_info where dt='2020-05-06'
```



### 2.11 事务型事实表总结

​		事务型事实表，一般从ods层拿取当天新增的数据，再按照星型模型，基于3w原则选取的维度列，和对应的维度表进行join，选取字段查询！



### 2.12 周期型快照事实表总结

周期型快照事实表： 统计的是一个事实表在周期内进行变化后，最终状态的快照！

​									比如关心用户在截至到5-6日，购物车里还有什么商品，只需要将购物车表在5-6日的所有数据全部同步即可！



周期型快照事实表，在导入数据上为了方便，是每日全量，但是可以缩短数据的保留周期！



### 2.13  dwd_fact_cart_info 加够表

```sql
insert overwrite table dwd_fact_cart_info PARTITION(dt)
SELECT * from ods_cart_info where dt='2020-05-06'
```



### 2.14  dwd_fact_favor_info 收藏表

```sql
insert overwrite table dwd_fact_favor_info PARTITION(dt)
SELECT * from ods_favor_info where dt='2020-05-06'
```



### 2.15 累积型快照事实表导入数据流程

​		累积型快照事实表一般是按照统计的事实的生命周期的起始时间作为分区字段！

​		优惠券表按照领取时间作为分区字段！

​		订单表按照下单时间作为分区字段！



​		核心： 优惠券和订单表，从mysql 同步到 hdfs时，都是同步新增和变化的数据！

​		目的： 将当日新增的数据，插入到当日的分区，

​					再将当日发送变化的数据，插入(覆盖写)到对应的分区！

​					覆盖写时，需要先查询发送变化数据所对应分区的全部数据，和变化数据，进行结合，结合后，一起覆盖此分区！



​		原则： 没有变化的数据不能丢，变化的数据进行修改，新增的数据进行插入！

​		过程：  ①查询所有变化的老的分区数据

​					②查询所有新增和变化的新数据

​					③新老关联交替，以新换老

​					④按照分区覆盖

​		

### 2.16  dwd_fact_coupon_use

```sql
insert overwrite TABLE dwd_fact_coupon_use PARTITION(dt)
select 
    nvl(new.id,old.id),
    nvl(new.coupon_id,old.coupon_id),
    nvl(new.user_id,old.user_id),
    nvl(new.order_id,old.order_id),
    nvl(new.coupon_status,old.coupon_status),
    nvl(new.get_time,old.get_time),
    nvl(new.using_time,old.using_time),
    nvl(new.used_time,old.used_time),
    date_format(nvl(new.get_time,old.get_time),'yyyy-MM-dd')
from
(SELECT
    *
from dwd_fact_coupon_use
where dt in
(SELECT
    DISTINCT date_format(get_time,'yyyy-MM-dd')
from ods_coupon_use 
where dt='2020-05-06' and get_time<'2020-05-06')) old
full join 
(SELECT * from ods_coupon_use where dt='2020-05-06') new 
on old.id=new.id
```

### 2.17 dwd_fact_order_info

分析：

```sql
--ods_order_info
id, order_status, user_id, out_trade_no, 
create_time, 
--ods_order_status_log
payment_time, cancel_time, finish_time, 
refund_time, refund_finish_time, 
--ods_order_info
province_id,
-- ods_activity_order
activity_id,
--ods_order_info
original_total_amount, benefit_reduce_amount,
feight_fee, final_total_amount,
```

sql:

```sql
insert overwrite table dwd_fact_order_info PARTITION(dt)
SELECT
    nvl(new.id,old.id), 
    nvl(new.order_status,old.order_status),
    nvl(new.user_id,old.user_id),
    nvl(new.out_trade_no,old.out_trade_no),
    nvl(new.create_time,old.create_time),
    nvl(status_map['1002'],old.payment_time),
    nvl(status_map['1003'],old.cancel_time),
    nvl(status_map['1004'],old.finish_time),
    nvl(status_map['1005'],old.refund_time),
    nvl(status_map['1006'],old.refund_finish_time),
    nvl(new.province_id,old.province_id),
    nvl(new.activity_id,old.activity_id),
    nvl(new.original_total_amount,old.original_total_amount),
    nvl(new.benefit_reduce_amount,old.benefit_reduce_amount),
    nvl(new.feight_fee,old.feight_fee),
    nvl(new.final_total_amount,old.final_total_amount),
    date_format(nvl(new.create_time,old.create_time),'yyyy-MM-dd')
from
(SELECT
    *
FROM dwd_fact_order_info
where dt in
(SELECT
    DISTINCT date_format(create_time,'yyyy-MM-dd')
FROM ods_order_info 
where dt='2020-05-06' and create_time<'2020-05-06')) old
full join
(select 
   t1.*,activity_id,status_map
from
(SElect * from ods_order_info where dt='2020-05-06' ) t1
left join 
(select order_id,activity_id from ods_activity_order where dt='2020-05-06') t2
on t1.id=t2.order_id
left join
(select order_id,
str_to_map(concat_ws(',',collect_set(concat(order_status,':',operate_time))),',',':') status_map
from ods_order_status_log where dt='2020-05-06'
GROUP by order_id) t3
on t1.id=t3.order_id) new
on old.id=new.id
```

### 2.18 DWD层SQL导入总结

①知道来龙去脉

​		来龙： 需要从哪些ODS层表中导入数据！

​					维度表：  找和此维度相关的ods层表！

​					事实表：  按照星型模型，基于3w原则进行建模，选择需要的字段！

​									 事实表+N个维度表



​					重点： ODS层这些表的数据，是怎么同步的！

​								ODS层表的数据，是每日全量？还是每日增量？还是每日新增和变化？

​		去脉： dwd层所建的表是一种什么样类型的表！

​					维度表： 从ods层多表join，取字段

​					事实表：  事务型事实表：  从每日增量的事实表中取数据，join 部分维度表取出维度字段即可！

​										周期型快照事实表：  ODS是全量同步，直接从ODS层原封不动导入即可！

​																			ODS层是增量同步，将ODS所有分区的数据汇总，按照user_id，sku_id进行汇总和去重，统计出要统计数据的最新状态！

​									

​									累积型快照事实表：  ①先查老的要覆盖的分区的数据

​																		②查询新导入的新增和变化的数据

​																		③新老交替，以新换旧

​																		④插入覆盖分区



②思考业务流程

​					a)考虑 join的方式

​					b)选择何种函数

​					c) join表之间的粒度



### 2.19 用户维度表(拉链表)

​		场景： 适合缓慢变化维度表！

​					 数据大部分是新增，但是少量数据可能会发送变化，变化的概率很少！



​		拉链表最终是使用一张全量表(不是分区表)来存储全部的用户信息，以及用户信息的生命周期！

​					提供了start_time，end_time来标识信息的生命周期！



​	用户维度表该如何同步：

​				①数据量小，可以全量同步，如果是每日全量，那么需要对用户表进行处理后，才能反映用户数据的历史变化情况

​				②如果数据量大，可以只同步新增和变化的数据

​					问题： 如果对已经变化的之前的数据，进行修改？

​								答：从原始拉链表查询所有的历史数据和当天的新增和变化的数据进行混合，之后导入到一张临时拉链表(为了防止，直接导入到原始拉链表出错，出错后可能会清空原始数据)，再由临时表覆盖导入到原始拉链表！

​								如何链条化记录数据的特征变化？

​								答：为数据提供start_time，end_time



```sql
insert overwrite table dwd_dim_user_info_his_tmp
select 
    old.id,old.name,old.birthday,old.gender,old.email,
    old.user_level,old.create_time,old.operate_time,
    old.start_date,
    if(old.end_date='9999-99-99' and new.id is not null,date_sub(new.dt,1),old.end_date)
from 
(select * from dwd_dim_user_info_his  ) old
LEFT join
(select *,'9999-99-99' from ods_user_info WHERE dt='2020-05-07') new
on OLD.id=new.id
UNION all
select *,'9999-99-99' from ods_user_info WHERE dt='2020-05-07';


insert overwrite table dwd_dim_user_info_his
select * from dwd_dim_user_info_his_tmp;
```

# 六、DWS层

## 1.dws层的概括

①dws层的数据从dwd层来选取！

②在建模上，紧紧贴合业务需求，将业务需要进行主题上的分类，分类后，每个主题都制作一张表，按照dwd层的数据，进行每日的轻度聚合，一般是建宽表！

③要求：  

​				知道来龙去脉；

​				熟悉常见的电商业务术语;



粒度：

| 表名                                                         | 粒度                         |
| ------------------------------------------------------------ | ---------------------------- |
| dws_uv_detail_daycount                                       | 一个设备是一行               |
| dws_user_action_daycount(只统计今天登录的会员)               | 一个会员是一行               |
| dws_sku_action_daycount（只统计被下单或平均或支付或加购或收藏的商品） | 一个商品是一行               |
| dws_coupon_use_daycount（只统计未过期的优惠券）              | 一个优惠券是一行             |
| dws_activity_info_daycount（统计所有活动）                   | 一个活动是一行               |
| dws_sale_detail_daycount（每日购买数据）                     | 一个用户购买的一款商品是一行 |



## 2.dws_uv_detail_daycount(每日设备行为)

### 2.1 分析

```sql
create external table dws_uv_detail_daycount
(
    -- 从启动日志dwd_start_log表取以下字段
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    -- 从启动日志dwd_start_log表按照mid_id进行聚合，之后count(*)取以下字段
    `login_count` bigint COMMENT '活跃次数'
)

```

### 2.2 SQL

```SQL
insert overwrite table dws_uv_detail_daycount PARTITION(dt='2020-05-06')
select 
    mid_id,
    concat_ws('|',collect_set(user_id)),
    concat_ws('|',collect_set(version_code)),
    concat_ws('|',collect_set(version_name)),
    concat_ws('|',collect_set(lang)),
    concat_ws('|',collect_set(source)),
    concat_ws('|',collect_set(os)),
    concat_ws('|',collect_set(area)),
    concat_ws('|',collect_set(model)),
    concat_ws('|',collect_set(brand)),
    concat_ws('|',collect_set(sdk_version)),
    concat_ws('|',collect_set(gmail)),
    concat_ws('|',collect_set(height_width)),
    concat_ws('|',collect_set(app_time)),
    concat_ws('|',collect_set(network)),
    concat_ws('|',collect_set(lng)),
    concat_ws('|',collect_set(lat)),
    count(*)
FROM dwd_start_log where dt='2020-05-06'
GROUP by mid_id
```



## 3.dws_user_action_daycount(每日会员行为)

### 3.1 分析

会员： user_id

```sql
create external table dws_user_action_daycount
(   
    user_id string comment '用户 id',
    -- 从dwd_start_log取 保证user_id不为null
    login_count bigint comment '登录次数',
    -- 从dwd_fact_cart_info取 按照user_id分组，统计次数，使用单价*件数计算金额
    cart_count bigint comment '当日新加入购物车的商品sku数',
    cart_amount double comment '当日购物车的总金额,
    -- 从dwd_fact_order_info取
    order_count bigint comment '下单次数',
    order_amount    decimal(16,2)  comment '下单金额',
    -- 从dwd_fact_payment_info取
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额'
) COMMENT '每日用户行为'

```

### 3.2 SQL

```sql
with
t1 as (select user_id,count(*) login_count  from dwd_start_log where dt='2020-05-06' and user_id is not NULL GROUP BY user_id),
t3 as (select user_id,count(*) order_count, sum(final_total_amount) order_amount from dwd_fact_order_info where dt='2020-05-06' GROUP by user_id ),
t4 as (select user_id,count(*) payment_count,sum(payment_amount) payment_amount from dwd_fact_payment_info where dt='2020-05-06' GROUP by user_id),
t2 as (select user_id,count(*) cart_count,sum(cart_price*sku_num) cart_amount from dwd_fact_cart_info where dt='2020-05-06' and date_format(create_time,'yyyy-MM-dd')='2020-05-06' GROUP by user_id )
insert overwrite TABLE dws_user_action_daycount PARTITION(dt='2020-05-06')
select 
    t1.user_id,login_count,
    nvl(cart_count,0),
    nvl(cart_amount,0),
    nvl(order_count,0),
    nvl(order_amount,0),
    nvl(payment_count,0),
    nvl(payment_amount,0)
from t1 left join t2 on t1.user_id=t2.user_id
left join t3 on t1.user_id=t3.user_id
left join t4 on t1.user_id=t4.user_id
```

## 4.dws_sku_action_daycount(每日商品行为)

### 4.1 分析

```sql
create external table dws_sku_action_daycount 
(   
    sku_id string comment 'sku_id',
    -- 从dwd_fact_order_detail表取
    order_count bigint comment '被下单次数',
    order_num bigint comment '被下单件数',
    order_amount decimal(16,2) comment '被下单金额',
    -- 从dwd_fact_payment_info取发现没有sku_id
    --	因此需要关联dwd_fact_order_detail,因此存在跨天支付的情形，需要关联前一天和当前天的
    --	下单信息，才能找出所有的在今天支付的商品信息
    payment_count bigint  comment '被支付次数',
    payment_num bigint comment '被支付件数',
    -- *无法求出
    payment_amount decimal(16,2) comment '应支付金额',
    -- dwd_fact_order_refund_info
    refund_count bigint  comment '被退款次数',
    refund_num bigint comment '被退款件数',
    refund_amount  decimal(16,2) comment '被退款金额',
    -- dwd_fact_cart_info
    * cart_count bigint comment '商品被加入购物车人数',
    * cart_num bigint comment '被加入购物车件数',
    -- dwd_fact_favor_info 
    * favor_count bigint comment '被收藏人数',
    -- dwd_fact_comment_info 
    appraise_good_count bigint comment '好评数',
    appraise_mid_count bigint comment '中评数',
    appraise_bad_count bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'

```

### 4.2 SQL

```SQL
insert overwrite table dws_sku_action_daycount PARTITION(dt='2020-05-06')
select
    sku_id,sum(order_count),sum(order_num),sum(order_amount),
    sum(payment_count),sum(payment_num),sum(payment_amount),sum(refund_count),
    sum(refund_num),
    sum(refund_amount),sum(cart_count),sum(cart_num),
    sum(favor_count),sum(appraise_good_count),sum(appraise_mid_count),
    sum(appraise_bad_count),sum(appraise_default_count)
from
(select  sku_id,count(*) order_count,sum(sku_num) order_num,
sum(total_amount) order_amount,
0 payment_count ,0 payment_num , 0 payment_amount ,
0 refund_count ,0 refund_num ,0 refund_amount , 
0 cart_count , 0 cart_num , 
0 favor_count , 
0 appraise_good_count , 0 appraise_mid_count ,0 appraise_bad_count ,
0 appraise_default_count 
from dwd_fact_order_detail where dt='2020-05-06'
GROUP by sku_id
UNION all
select 
    sku_id,
    0 order_count, 0 order_num, 0 order_amount,
    sum(sku_num)  payment_num,sum(total_amount) payment_amount,count(*) payment_count,
    0 refund_count ,0 refund_num ,0 refund_amount , 
    0 cart_count , 0 cart_num , 
    0 favor_count , 
    0 appraise_good_count , 0 appraise_mid_count ,0 appraise_bad_count ,
    0 appraise_default_count 
from
(SELECT order_id,sku_id,sku_num,total_amount from dwd_fact_order_detail 
where dt='2020-05-06' or dt=date_sub('2020-05-06',1)) tmp1
 join
(select order_id from dwd_fact_payment_info where dt='2020-05-06') tmp2
on tmp1.order_id=tmp2.order_id
GROUP by sku_id
UNION all
SELECT sku_id,0 order_count, 0 order_num, 0 order_amount,
        0 payment_count ,0 payment_num , 0 payment_amount ,
        count(*) refund_count,sum(refund_num) refund_num,sum(refund_amount) refund_amount,
        0 cart_count , 0 cart_num , 
        0 favor_count , 
        0 appraise_good_count , 0 appraise_mid_count ,0 appraise_bad_count ,
        0 appraise_default_count 
from dwd_fact_order_refund_info where dt='2020-05-06'
GROUP by sku_id
UNION all
select sku_id,0 order_count, 0 order_num, 0 order_amount,
        0 payment_count ,0 payment_num , 0 payment_amount ,
        0 refund_count ,0 refund_num ,0 refund_amount , 
        count(*) cart_count,sum(sku_num) cart_num,
         0 favor_count , 
        0 appraise_good_count , 0 appraise_mid_count ,0 appraise_bad_count ,
        0 appraise_default_count 
from dwd_fact_cart_info where dt='2020-05-06' and sku_num>0
GROUP by sku_id
UNION all
SELECT sku_id,0 order_count, 0 order_num, 0 order_amount,
        0 payment_count ,0 payment_num , 0 payment_amount ,
        0 refund_count ,0 refund_num ,0 refund_amount ,
        0 cart_count , 0 cart_num , 
        count(*) favor_count,
        0 appraise_good_count , 0 appraise_mid_count ,0 appraise_bad_count ,
        0 appraise_default_count 
from dwd_fact_favor_info where dt='2020-05-06' and is_cancel=0
group by sku_id
UNION all
SELECT sku_id,0 order_count, 0 order_num, 0 order_amount,
        0 payment_count ,0 payment_num , 0 payment_amount ,
        0 refund_count ,0 refund_num ,0 refund_amount ,
        0 cart_count , 0 cart_num , 0 favor_count ,
sum(if(appraise='1201',1,0)) appraise_good_count,
sum(if(appraise='1202',1,0)) appraise_mid_count,
sum(if(appraise='1203',1,0)) appraise_bad_count,
sum(if(appraise='1204',1,0)) appraise_default_count
from  dwd_fact_comment_info where dt='2020-05-06'
group by sku_id) tmp
group by sku_id
```

或

```sql
with 
t1 as
(select  sku_id,count(*) order_count,sum(sku_num) order_num,
sum(total_amount) order_amount
from dwd_fact_order_detail where dt='2020-05-06'
GROUP by sku_id),
t2 as
(select 
    sku_id,
    sum(sku_num)  payment_num,sum(total_amount) payment_amount,
    count(*) payment_count
from
(SELECT order_id,sku_id,sku_num,total_amount from dwd_fact_order_detail 
where dt='2020-05-06' or dt=date_sub('2020-05-06',1)) tmp1
 join
(select order_id from dwd_fact_payment_info where dt='2020-05-06') tmp2
on tmp1.order_id=tmp2.order_id
GROUP by sku_id),
t3 as 
(SELECT sku_id,
        count(*) refund_count,sum(refund_num) refund_num,
        sum(refund_amount) refund_amount
from dwd_fact_order_refund_info where dt='2020-05-06'
GROUP by sku_id),
t4 as
(select sku_id,
        count(*) cart_count,sum(sku_num) cart_num
from dwd_fact_cart_info where dt='2020-05-06' and sku_num>0
GROUP by sku_id),
t5 as
(SELECT sku_id,
        count(*) favor_count      
from dwd_fact_favor_info where dt='2020-05-06' and is_cancel=0
group by sku_id),
t6 as
(SELECT sku_id,
sum(if(appraise='1201',1,0)) appraise_good_count,
sum(if(appraise='1202',1,0)) appraise_mid_count,
sum(if(appraise='1203',1,0)) appraise_bad_count,
sum(if(appraise='1204',1,0)) appraise_default_count
from  dwd_fact_comment_info where dt='2020-05-06'
group by sku_id)
insert overwrite table dws_sku_action_daycount partition(dt='2020-05-06')
SELECT
nvl(nvl(nvl(nvl(nvl(t1.sku_id,t2.sku_id),t3.sku_id),t4.sku_id),t5.sku_id),t6.sku_id),
    nvl(order_count,0), 
    nvl(order_num,0),
    nvl(order_amount,0),
    nvl(payment_count,0),
    nvl(payment_num,0),
    nvl(payment_amount,0),
    nvl(refund_count,0),
    nvl(refund_num,0),
    nvl(refund_amount,0),
    nvl(cart_count,0),
    nvl(cart_num,0),
    nvl(favor_count,0),
    nvl(appraise_good_count,0),
    nvl(appraise_mid_count,0),
    nvl(appraise_bad_count,0),
    nvl(appraise_default_count,0)
from t1 
full join t2 on t1.sku_id=t2.sku_id
full join t3 on t1.sku_id=t3.sku_id
full join t4 on t1.sku_id=t4.sku_id
full join t5 on t1.sku_id=t5.sku_id
full join t6 on t1.sku_id=t6.sku_id

```



## 5.dws_coupon_use_daycount（每日优惠券使用行为）

### 5.1 分析

```sql
-- 不统计已经过期的优惠券
create external table dws_coupon_use_daycount
(   
    -- dwd_dim_coupon_info
    `coupon_id` string  COMMENT '优惠券ID',
    `coupon_name` string COMMENT '购物券名称',
    `coupon_type` string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` string COMMENT '满额数',
    `condition_num` string COMMENT '满件数',
    `activity_id` string COMMENT '活动编号',
    `benefit_amount` string COMMENT '减金额',
    `benefit_discount` string COMMENT '折扣',
    `create_time` string COMMENT '创建时间',
    `range_type` string COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `spu_id` string COMMENT '商品id',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `limit_num` string COMMENT '最多领用次数',
    -- dwd_fact_coupon_use 按照领取时间进行分区，需要全表扫描
    `get_count` bigint COMMENT '领用次数',
    `using_count` bigint COMMENT '使用(下单)次数',
    `used_count` bigint COMMENT '使用(支付)次数'
) COMMENT '每日优惠券统计'

```

### 5.2 SQL

```SQL
insert overwrite table dws_coupon_use_daycount PARTITION(dt='2020-05-06')
select 
  t1.id coupon_id,coupon_name, coupon_type, condition_amount, 
condition_num, activity_id, benefit_amount, benefit_discount,
create_time, range_type, spu_id, tm_id, category3_id, limit_num,
    get_count,using_count, used_count
from
(SELECT *
from dwd_dim_coupon_info 
where dt='2020-05-06' and nvl(expire_time,'9999-99-99') >'2020-05-06') t1
left join
(select coupon_id,
sum(if(date_format(get_time,'yyyy-MM-dd')='2020-05-06',1,0)) get_count,
sum(if(date_format(using_time,'yyyy-MM-dd')='2020-05-06',1,0)) using_count,
sum(if(date_format(used_time,'yyyy-MM-dd')='2020-05-06',1,0)) used_count 
from dwd_fact_coupon_use
GROUP by coupon_id) t2
on t1.id=t2.coupon_id

```

## 6.dws_activity_info_daycount(每日活动行为)

### 6.1 分析

```sql
create external table dws_activity_info_daycount(
    // 从dwd_dim_activity_info查询
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    // 从dwd_fact_order_info查询
    `order_count` bigint COMMENT '下单次数',
    // 取当天和前一天下单，在今天支付的参与活动的订单
    `payment_count` bigint COMMENT '支付次数'
) COMMENT '活动信息表'

```

### 6.2 SQL

```sql
with 
 t1 as
 (select 
    id,activity_name,activity_type,
    start_time,end_time,create_time  
 from dwd_dim_activity_info 
 where dt='2020-05-06'
 GROUP by id,activity_name,activity_type,
    start_time,end_time,create_time),
 t2 as 
 (select 
    activity_id,count(*) order_count
 from dwd_fact_order_info 
 where dt='2020-05-06'
 GROUP by activity_id),
  t5 as 
 (SELECT
    activity_id,count(*) payment_count
 from
 (SELECT order_id,id from dwd_fact_payment_info where dt='2020-05-06') t3
  join
 (SELECT id,activity_id from dwd_fact_order_info WHERE dt='2020-05-06' or dt=date_sub('2020-05-06',1)) t4
  on   t3.order_id=t4.id
  GROUP by activity_id)
  insert overwrite table dws_activity_info_daycount partition(dt='2020-05-06')
  SELECT
    t1.id,activity_name, activity_type, 
  start_time, end_time, create_time, 
  nvl(order_count,0),
  nvl(payment_count,0)
  from t1 
  left join t2 on t1.id=t2.activity_id
  left join t5 on t1.id=t5.activity_id
```

## 7.dws_sale_detail_daycount

### 7.1 分析

粒度：一个用户，购买一款商品的信息

```sql
create external table dws_sale_detail_daycount
(   
    -- dwd_dim_user_info_his  where endtime='9999-99-99'
    user_id   string  comment '用户 id',
    user_gender  string comment '用户性别',
    user_age string  comment '用户年龄',
    user_level string comment '用户等级',
    -- dwd_dim_sku_info 
    sku_id    string comment '商品 id',
    order_price decimal(10,2) comment '商品价格',
    sku_name string   comment '商品名称',
    sku_tm_id string   comment '品牌id',
    sku_category3_id string comment '商品三级品类id',
    sku_category2_id string comment '商品二级品类id',
    sku_category1_id string comment '商品一级品类id',
    sku_category3_name string comment '商品三级品类名称',
    sku_category2_name string comment '商品二级品类名称',
    sku_category1_name string comment '商品一级品类名称',
    spu_id  string comment '商品 spu',
    -- 何为购买？ 下单算购买？ 支付算购买？
    -- 取当天支付的订单，且是前一天和今天下单的商品，被商品支付的个数
    sku_num  int comment '购买个数',
    -- dwd_fact_order_detail
    order_count bigint comment '当日下单单数',
    order_amount decimal(16,2) comment '当日下单金额'
) COMMENT '每日购买行为'

```

### 7.2 SQL

```SQL
with
-- 所有用户信息
 t1 as
 (select 
    id user_id,gender user_gender,
    FLOOR(datediff('2020-05-06',birthday)/365) user_age,
    user_level
 FROM dwd_dim_user_info_his
 where end_date='9999-99-99'),
 -- 所有商品信息
 t2 as
 (SELECT 
    id sku_id ,
    price order_price ,
    sku_name ,
    tm_id sku_tm_id ,
    category3_id sku_category3_id ,
    category2_id sku_category2_id ,
    category1_id sku_category1_id ,
    category3_name sku_category3_name ,
    category2_name sku_category2_name ,
    category1_name sku_category1_name ,
    spu_id    
 from dwd_dim_sku_info
 where dt='2020-05-06'),
 -- 2020-05-06 这一天，哪些用户分别下单了哪些商品，下了多少单，下单总金额多少
t3 as
(select 
    user_id,sku_id,
    count(*) order_count,
    sum(total_amount) order_amount
from dwd_fact_order_detail 
where dt='2020-05-06'
GROUP by user_id,sku_id),
-- 2020-05-06这一天，哪些用户分别购买了哪些商品，买了多少个
t4 as 
(SELECT
    user_id,sku_id,sum(sku_num) sku_num
from
(select order_id
from dwd_fact_payment_info where dt='2020-05-06') t5
join
(select order_id,sku_id,sku_num,user_id from dwd_fact_order_detail where dt='2020-05-06' or dt=date_sub('2020-05-06',1)) t6
on t5.order_id=t6.order_id 
GROUP by user_id,sku_id)
insert overwrite table dws_sale_detail_daycount PARTITION(dt='2020-05-06')
SELECT
    t7.user_id, t7.sku_id, user_gender, user_age, 
    user_level, order_price, sku_name, sku_tm_id,
    sku_category3_id, sku_category2_id, sku_category1_id, 
    sku_category3_name, sku_category2_name, 
    sku_category1_name, spu_id, sku_num, order_count,
    order_amount
from
(select 
    nvl(t3.user_id,t4.user_id) user_id,
    nvl(t3.sku_id,t4.sku_id) sku_id,
    nvl(order_count,0) order_count, 
    nvl(order_amount,0) order_amount,
    nvl(sku_num,0) sku_num 
FROM t3 full join t4 on 
t3.user_id=t4.user_id and t3.sku_id=t4.sku_id) t7
 join t1 on t7.user_id=t1.user_id 
 join t2 on t7.sku_id=t2.sku_id
```

# 七、DWT层

## 1.DWT层的概括

DWT层将DWS层每日聚合的数据进行累积！

DWT层不是分区表，是一个累积型全量表！

DWT层的数据来源于DWS层。

累积型全量表：  ①查询要改动的旧数据

​							 ②查询新增和变化的新数据

​							③新旧关联，以新换旧

​							④导入覆盖

## 2.dwt_uv_topic

### 2.1 分析

```sql
create external table dwt_uv_topic
(
    -- dws_uv_detail_daycount
    `mid_id` string COMMENT '设备唯一标识',
    -- 普通字段，需要将old+new表中的数据，进行拼接！
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号',
    `version_name` string COMMENT '程序版本名',
    `lang` string COMMENT '系统语言',
    `source` string COMMENT '渠道号',
    `os` string COMMENT '安卓系统版本',
    `area` string COMMENT '区域',
    `model` string COMMENT '手机型号',
    `brand` string COMMENT '手机品牌',
    `sdk_version` string COMMENT 'sdkVersion',
    `gmail` string COMMENT 'gmail',
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    -- dt,如果是老用户，首次活跃时间取old表中的数据，如果是今天新增的用户，取今天日期作为首次活跃时间
    `login_date_first` string  comment '首次活跃时间',
    -- 如果是今天未登录的老用户，末次活跃时间取old表中的时间，如果是今天活跃的用户，取今天作为末次活跃时间
    `login_date_last` string  comment '末次活跃时间',
    -- login_count 如果是今天未登录的老用户，当日活跃次数=0，如果是今天活跃的用户，当日活跃次数=login_count
    `login_day_count` bigint comment '当日活跃次数',
    -- 累积活跃天数=old表活跃的天数+（if new表活跃，1，0）
    `login_count` bigint comment '累积活跃天数'
)

```

今天未登录的老用户：new.mid_id is null

老用户：old.mid_id is not null

新用户：old.mid_id is null

今天登录的老用户：new.mid_id is not null and old.mid_id is not null

### 2.2 SQL

```SQL
insert overwrite table gmall.dwt_uv_topic
select 
    nvl(old.mid_id,new.mid_id),
    concat_ws('|',old.user_id,new.user_id),
    concat_ws('|',old.version_code,new.version_code),
    concat_ws('|',old.version_name,new.version_name),
    concat_ws('|',old.lang,new.lang),
    concat_ws('|',old.source,new.source),
    concat_ws('|',old.os,new.os),
    concat_ws('|',old.area,new.area),
    concat_ws('|',old.model,new.model),
    concat_ws('|',old.brand,new.brand),
    concat_ws('|',old.sdk_version,new.sdk_version),
    concat_ws('|',old.gmail,new.gmail),
    concat_ws('|',old.height_width,new.height_width),
    concat_ws('|',old.app_time,new.app_time),
    concat_ws('|',old.network,new.network),
    concat_ws('|',old.lng,new.lng),
    concat_ws('|',old.lat,new.lat),
    nvl(old.login_date_first,'2020-05-06') login_date_first,
    IF(new.mid_id is null,old.login_date_last,'2020-05-06') login_date_last, 
    nvl(new.login_count,0) login_day_count, 
    nvl(old.login_count,0)+if(new.login_count is not null,1,0) login_count
from
dwt_uv_topic old
full join
(select * from dws_uv_detail_daycount where dt='2020-05-06') new
on old.mid_id=new.mid_id
```



## 3.dwt_user_topic

### 3.1 分析

```sql
create external table dwt_user_topic
(
    -- dws_user_action_daycount
    user_id string  comment '用户id',
    -- 如果是老用户，就取old表中的时间，否则就取当天
    login_date_first string  comment '首次登录时间',
    order_date_first string  comment '首次下单时间',
     payment_date_first string  comment '首次支付时间',
    -- 如果是今天未登录的老用户，就取old表中的时间，否则就取当天
    login_date_last string  comment '末次登录时间',
    order_date_last string  comment '末次下单时间',
    payment_date_last string  comment '末次支付时间',
    
    -- old full join new,  old + new 
     login_count bigint comment '累积登录天数',
    order_count bigint comment '累积下单次数',
    order_amount decimal(16,2) comment '累积下单金额',
    payment_count decimal(16,2) comment '累积支付次数',
    payment_amount decimal(16,2) comment '累积支付金额',
    
    -- dws_user_action_daycount  30天之前<dt<=今天，使用sum进行求和 
    order_last_30d_count bigint comment '最近30日下单次数',
    order_last_30d_amount bigint comment '最近30日下单金额',
    payment_last_30d_count decimal(16,2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16,2) comment '最近30日支付金额'
     login_last_30d_count bigint comment '最近30日登录天数',
   
 )COMMENT '用户主题宽表'

```

### 3.2 SQL

```sql
insert overwrite table dwt_user_topic
SELECT
    t1.user_id,login_date_first, 
login_date_last, login_count, nvl(login_last_30d_count,0), 
order_date_first, order_date_last, order_count, order_amount,
nvl(order_last_30d_count,0), nvl(order_last_30d_amount,0), payment_date_first, 
payment_date_last, payment_count, payment_amount, nvl(payment_last_30d_count,0), 
nvl(payment_last_30d_amount,0)
from
 (SELECT
    nvl(old.user_id,new.user_id) user_id,
    nvl(old.login_date_first,'2020-05-06') login_date_first,
    nvl(old.order_date_first,if(new.order_count>0,'2020-05-06',null)) order_date_first,
    nvl(old.payment_date_first,if(new.payment_count>0,'2020-05-06',null))  payment_date_first,
    if(new.user_id is null,old.login_date_last,'2020-05-06') login_date_last,
    if(new.order_count>0,'2020-05-06',old.order_date_last) order_date_last,
    if(new.payment_count>0,'2020-05-06',old.payment_date_last) payment_date_last,
    nvl(old.login_count,0)+if(new.user_id is not null,1,0) login_count,
    nvl(old.order_count,0)+nvl(new.order_count,0) order_count,
    nvl(old.order_amount,0)+nvl(new.order_amount,0) order_amount,
    nvl(old.payment_count,0)+nvl(new.payment_count,0) payment_count,
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0) payment_amount 
 from
 dwt_user_topic old
 full join (select * from dws_user_action_daycount where dt='2020-05-06') new
 on old.user_id=new.user_id) t1
 left join
 ( 
  SELECT
    user_id,
    sum(order_count) order_last_30d_count,
    sum(order_amount) order_last_30d_amount,
    sum(payment_count) payment_last_30d_count,
    sum(payment_amount) payment_last_30d_amount,
    count(*) login_last_30d_count
  FROM dws_user_action_daycount
  where dt BETWEEN date_sub('2020-05-06',29) and '2020-05-06'
  GROUP by user_id) t2
  on t1.user_id=t2.user_id
```

## 4.dwt_sku_topic

### 4.1 分析

```sql
create external table dwt_sku_topic
(
    sku_id string comment 'sku_id',
    spu_id string comment 'spu_id',
    -- 从dws_sku_action_daycount  取，where 30天之前<=dt<=今天，sum()
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(16,2)  comment '最近30日被下单金额',
      payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(16,2) comment '最近30日被支付金额',
      refund_last_30d_count bigint comment '最近三十日退款次数',
    refund_last_30d_num bigint comment '最近三十日退款件数',
    refund_last_30d_amount decimal(10,2) comment '最近三十日退款金额',
     appraise_last_30d_good_count bigint comment '最近30日好评数',
    appraise_last_30d_mid_count bigint comment '最近30日中评数',
    appraise_last_30d_bad_count bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
        cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    cart_last_30d_num bigint comment '最近30日被加入购物车件数',
      favor_last_30d_count bigint comment '最近30日被收藏次数',
    
    -- old full join new   old+new
    order_count bigint comment '累积被下单次数',
    order_num bigint comment '累积被下单件数',
    order_amount decimal(16,2) comment '累积被下单金额',
    payment_count   bigint  comment '累积被支付次数',
    payment_num bigint comment '累积被支付件数',
    payment_amount  decimal(16,2) comment '累积被支付金额',
    refund_count bigint comment '累积退款次数',
    refund_num bigint comment '累积退款件数',
    refund_amount decimal(10,2) comment '累积退款金额',
    cart_count bigint comment '累积被加入购物车次数',
    cart_num bigint comment '累积被加入购物车件数',
    favor_count bigint comment '累积被收藏次数',
    appraise_good_count bigint comment '累积好评数',
    appraise_mid_count bigint comment '累积中评数',
    appraise_bad_count bigint comment '累积差评数',
    appraise_default_count bigint comment '累积默认评价数'
 )COMMENT '商品主题宽表'

```

### 4.2 SQL

```sql
insert overwrite TABLE dwt_sku_topic
SELECT
    t2.sku_id, t2.spu_id, 
    nvl(order_last_30d_count,0), 
    nvl(order_last_30d_num,0), 
    nvl(order_last_30d_amount,0), order_count, order_num, order_amount, 
    nvl(payment_last_30d_count,0),
    nvl(payment_last_30d_num,0),
    nvl(payment_last_30d_amount,0),
    payment_count, payment_num, payment_amount, 
    nvl(refund_last_30d_count,0),
    nvl(refund_last_30d_num,0), 
    nvl(refund_last_30d_amount,0), refund_count, refund_num, 
    refund_amount, 
    nvl(cart_last_30d_count,0), 
    nvl(cart_last_30d_num,0), cart_count,
    cart_num, 
    nvl(favor_last_30d_count,0), favor_count, 
    nvl(appraise_last_30d_good_count,0),
    nvl(appraise_last_30d_mid_count,0),
    nvl(appraise_last_30d_bad_count,0),
    nvl(appraise_last_30d_default_count,0), appraise_good_count,
    appraise_mid_count, appraise_bad_count, appraise_default_count
from
(select
    nvl(old.sku_id,new.sku_id) sku_id  ,
    nvl(old.spu_id,new.spu_id) spu_id ,
    nvl(old.order_count,0)+nvl(new.order_count,0) order_count ,
    nvl(old.order_num,0)+nvl(new.order_num,0) order_num ,
    nvl(old.order_amount,0)+nvl(new.order_amount,0) order_amount ,
    nvl(old.payment_count,0)+nvl(new.payment_count,0) payment_count ,
    nvl(old.payment_num,0)+nvl(new.payment_num,0) payment_num ,
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0) payment_amount ,
    nvl(old.refund_count,0)+nvl(new.refund_count,0) refund_count ,
    nvl(old.refund_num,0)+nvl(new.refund_num,0) refund_num ,
    nvl(old.refund_amount,0)+nvl(new.refund_amount,0) refund_amount ,
    nvl(old.cart_count,0)+nvl(new.cart_count,0) cart_count ,
    nvl(old.cart_num,0)+nvl(new.cart_num,0) cart_num ,
    nvl(old.favor_count,0)+nvl(new.favor_count,0) favor_count ,
    nvl(old.appraise_good_count,0)+nvl(new.appraise_good_count,0) appraise_good_count ,
    nvl(old.appraise_mid_count,0)+nvl(new.appraise_mid_count,0) appraise_mid_count ,
    nvl(old.appraise_bad_count,0)+nvl(new.appraise_bad_count,0) appraise_bad_count ,
    nvl(old.appraise_default_count,0)+nvl(new.appraise_default_count,0) appraise_default_count 
from dwt_sku_topic old full join 
(SELECT
    t3.*, sku_dim.spu_id 
from
(select * from dws_sku_action_daycount where dt='2020-05-06') t3
join 
(select id,spu_id from dwd_dim_sku_info where dt='2020-05-06') sku_dim
on t3.sku_id=sku_dim.id) new
on new.sku_id=old.sku_id) t2
left join 
(select
 sku_id  ,
    sum(order_count) order_last_30d_count ,
    sum(order_num) order_last_30d_num ,
    sum(order_amount) order_last_30d_amount ,
    sum(payment_count)  payment_last_30d_count   ,
    sum(payment_num) payment_last_30d_num ,
    sum(payment_amount) payment_last_30d_amount ,
     sum(refund_count) refund_last_30d_count ,
     sum(refund_num) refund_last_30d_num ,
     sum(refund_amount) refund_last_30d_amount ,
     sum(appraise_good_count) appraise_last_30d_good_count ,
     sum(appraise_mid_count) appraise_last_30d_mid_count ,
     sum(appraise_bad_count) appraise_last_30d_bad_count ,
     sum(appraise_default_count) appraise_last_30d_default_count ,
      sum(cart_count)  cart_last_30d_count  ,
    sum(cart_num) cart_last_30d_num ,
     sum(favor_count) favor_last_30d_count 
 FROM dws_sku_action_daycount
 where dt BETWEEN date_sub('2020-05-06',29) and '2020-05-06'
 GROUP by sku_id) t1
on t2.sku_id=t1.sku_id
```



## 5.dwt_coupon_topic

### 5.1 分析

```sql
create external table dwt_coupon_topic
(
    -- dws_coupon_use_daycount
    `coupon_id` string  COMMENT '优惠券ID',
    -- 将当天的数据查询即可
    `get_day_count` bigint COMMENT '当日领用次数',
    `using_day_count` bigint COMMENT '当日使用(下单)次数',
    `used_day_count` bigint COMMENT '当日使用(支付)次数',
    -- old full join new  old+new
    `get_count` bigint COMMENT '累积领用次数',
    `using_count` bigint COMMENT '累积使用(下单)次数',
    `used_count` bigint COMMENT '累积使用(支付)次数'
)COMMENT '购物券主题宽表'
```

### 5.2 SQL

```sql
insert overwrite table dwt_coupon_topic
select 
    nvl(old.coupon_id,new.coupon_id) coupon_id,
    nvl(new.get_count,0) get_day_count,
    nvl(new.using_count,0) using_day_count,
    nvl(new.used_count,0) used_day_count,
    nvl(old.get_count,0)+nvl(new.get_count,0) get_count, 
    nvl(old.get_count,0)+nvl(new.using_count,0) using_count,
    nvl(old.get_count,0)+nvl(new.used_count,0) used_count
from dwt_coupon_topic old 
full join (select * from dws_coupon_use_daycount where dt='2020-05-06')new 
on old.coupon_id=new.coupon_id

```

## 6.dwt_activity_topic

### 6.1 分析

```sql
create external table dwt_activity_topic(
    `id` string COMMENT '活动id',
    `activity_name` string  COMMENT '活动名称',
    `order_day_count` bigint COMMENT '当日日下单次数',
    `payment_day_count` bigint COMMENT '当日支付次数',
    `order_count` bigint COMMENT '累积下单次数',
    `payment_count` bigint COMMENT '累积支付次数'
) COMMENT '活动主题宽表'

```

### 6.2 SQL

```SQL
insert overwrite table dwt_activity_topic
select 
    nvl(old.id,new.id) id,
    nvl(old.activity_name,new.activity_name) activity_name,
    nvl(new.order_count,0) order_day_count,
    nvl(new.payment_count,0) payment_day_count,
    nvl(old.order_count,0)+nvl(new.order_count,0) order_count, 
    nvl(old.payment_count,0)+nvl(new.payment_count,0) payment_count
from dwt_activity_topic old 
full join (select * from dws_activity_info_daycount where dt='2020-05-06')new 
on old.id=new.id
```

# 八、ADS层

## 1.ADS层的概括

ADS的特征是紧紧贴合需求！将需求根据要查询的数据源进行分类！

ADS的建模根据分类后的需求，同一类需求创建一张表进行统计！

ADS层的表都是全量表！

## 2.一键造数据

①各层导数据的脚本

②将集群的时间，调整到要导入数据的前一天

​		例如： 要导入的是从5-8号-5-18日的数据

​						将集群时间先调整到5-7日

③修改循环的次数

④将造业务数据的jar包上传到脚本中指定的路径下

⑤启动采集通道，启动hive

⑥执行脚本

## 3.设备主题

### 3.1 活跃设备数（日、周、月）

设备： 从dws_uv_daycount 或 dwt_uv_topic 表取数据

日活： 在当天活跃至少活跃一次

周活： 在一周至少活跃一次

月活：在一个月至少活跃一次

#### 3.1.1 分析

```sql
create external table ads_uv_count( 
    `dt` string COMMENT '统计日期',
    // 从dws层取当天的，也可以从dwt层取
    `day_count` bigint COMMENT '当日用户数量',
    // 从dws层取当周的，也可以从dwt层取
    `wk_count`  bigint COMMENT '当周用户数量',
    // 从dws层取当月的，也可以从dwt层取
    `mn_count`  bigint COMMENT '当月用户数量',
    // 借助next_day()
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    // 借助last_day()
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果' 
) COMMENT '活跃设备数'

```

#### 3.1.2 SQL

```sql
insert into table ads_uv_count
 SELECT
    '2020-05-06',day_count,wk_count,mn_count,
    if('2020-05-06'=date_sub(next_day('2020-05-06','MO'),1),'Y','N') is_weekend,
    if('2020-05-06'=last_day('2020-05-06'),'Y','N') is_monthend
 from
 (SELECT '2020-05-06' dt,count(*)  day_count
 from dwt_uv_topic 
 WHERE login_date_last='2020-05-06') t1
 join
 (SELECT '2020-05-06' dt,count(*)  wk_count
 from dwt_uv_topic 
 WHERE login_date_last>=date_sub(next_day('2020-05-06','MO'),7)) t2
 on t1.dt=t2.dt
 join
 (SELECT '2020-05-06' dt,count(*)  mn_count
 from dwt_uv_topic 
 WHERE last_day(login_date_last)=last_day('2020-05-06')) t3
 on t1.dt=t3.dt
```

### 3.2 每日新增设备

每日新增设备:  login_date_first=今天

```sql
insert into ads_new_mid_count
SELECT 
    '2020-05-06' create_date, 
    count(*) new_mid_count
FROM dwt_uv_topic
where login_date_first='2020-05-06';
```

### 3.3 沉默用户数

沉默用户：只在安装当天启动过，且启动时间是在7天前

只在安装当天启动过： login_date_first='当天'= login_date_last

启动时间是在7天前:      login_date_last< 今天的7天前

```sql
insert into table ads_silent_count
SELECT
    '2020-05-06',
    count(*)
from dwt_uv_topic
where login_date_first=login_date_last
      and
      login_date_last<date_sub('2020-05-06',7)
```

### 3.4 本周回流用户数

 本周回流用户数：  本周登录过的，没在上周登录过的老用户数！

本周活跃 - 本周新增 - 上周活跃



本周活跃的老用户： login_date_last > = 本周一  and login_date_first< 本周一 

上周活跃的用户： 从dws_uv_daycount 取dt  BETWEEN 上周一 and 上周日

两个结果集取差集：  a left join b on a.xx=b.xx where b.xx is null

```sql
insert into table ads_back_count
select 
    '2020-05-06',
    concat(date_sub(next_day('2020-05-06','MO'),7),'_',date_sub(next_day('2020-05-06','MO'),1)),
    COUNT(*) 
from
(SELECT
    mid_id
from dwt_uv_topic
where  login_date_last >= date_sub(next_day('2020-05-06','MO'),7)
        and login_date_first< date_sub(next_day('2020-05-06','MO'),7) ) t1
left join    
(SELECT
     mid_id
from dws_uv_detail_daycount     
where dt  BETWEEN 
       date_sub(next_day('2020-05-06','MO'),14)
       and
       date_sub(next_day('2020-05-06','MO'),8)
GROUP by mid_id) t2
on t1.mid_id=t2.mid_id
where t2.mid_id is null

```

### 3.5 流失用户数

流失用户数: 连续7天未活跃的设备

​						最后一次登录的时间距离今天已经连续7天！

```sql
insert into table ads_wastage_count
SELECT
    '2020-05-06',
    count(*)
FROM dwt_user_topic
where login_date_last<date_sub('2020-05-06',7)
```

取历史状态： 从dws层取

取当前的快照状态： 从dwt层取

### 3.6 留存率

留存用户：  某天新增的用户中，在n天后继续使用的用户称为留存用户

留存率：     留存用户 占  某天新增用户的 比率



如果要计算留存率，需要有：

①某一天新增的人数

②留存的天数，留存的日期=新增的天数+留存的天数

③取留存日期当天的留存人数



举例：

| 新增日期 | 新增设备数 | 留存天数 | 留存日期 | 留存数量 | 留存率 |
| -------- | ---------- | -------- | -------- | -------- | ------ |
| 2020-5-6 | 100        | 1        | 2020-5-7 | 80       | 0.8    |
| 2020-5-5 | 200        | 2        | 2020-5-7 | 100      | 0.5    |
| 2020-5-4 | 100        | 3        | 2020-5-7 | 30       | 0.3    |

```sql
select 
     '2020-05-09' stat_date,
     date_sub('2020-05-09',1) create_date,
     1 retention_day,
     sum(if(login_date_first=date_sub('2020-05-09',1) and
        login_date_last='2020-05-09',1,0
     )) retention_count,
     sum(if(login_date_first=date_sub('2020-05-09',1),1,0)) new_mid_count,
     
     nvl(cast(sum(if(login_date_first=date_sub('2020-05-09',1) and
        login_date_last='2020-05-09',1,0
     )) / sum(if(login_date_first=date_sub('2020-05-09',1),1,0)) * 100 as decimal(10,2)),0.00) retention_ratio
from dwt_uv_topic
union all
select 
     '2020-05-09' stat_date,
     date_sub('2020-05-09',2) create_date,
     2 retention_day,
     sum(if(login_date_first=date_sub('2020-05-09',2) and
        login_date_last='2020-05-09',1,0
     )) retention_count,
     sum(if(login_date_first=date_sub('2020-05-09',2),1,0)) new_mid_count,
     
     nvl(cast(sum(if(login_date_first=date_sub('2020-05-09',2) and
        login_date_last='2020-05-09',1,0
     )) / sum(if(login_date_first=date_sub('2020-05-09',2),1,0)) * 100 as decimal(10,2)),0.00) retention_ratio
from dwt_uv_topic
union all
select 
     '2020-05-09' stat_date,
     date_sub('2020-05-09',3) create_date,
     3 retention_day,
     sum(if(login_date_first=date_sub('2020-05-09',3) and
        login_date_last='2020-05-09',1,0
     )) retention_count,
     sum(if(login_date_first=date_sub('2020-05-09',3),1,0)) new_mid_count,
     
     nvl(cast(sum(if(login_date_first=date_sub('2020-05-09',3) and
        login_date_last='2020-05-09',1,0
     )) / sum(if(login_date_first=date_sub('2020-05-09',3),1,0)) * 100 as decimal(10,2)),0.00) retention_ratio
from dwt_uv_topic

```

### 3.7 最近连续三周活跃用户数

最近连续三周：求本周，上周和上上周

连续活跃用户数：  用户在这三周中，都至少需要出现一次

```sql
insert into table ads_continuity_wk_count
SELECT
    '2020-05-06',
    concat(date_sub(next_day('2020-05-06','MO'),21),'-',date_sub(next_day('2020-05-06','MO'),1)),
    count(*)
from
(select
    mid_id
from
(select 
    mid_id
from dws_uv_detail_daycount
where dt>=date_sub(next_day('2020-05-06','MO'),7)
GROUP by mid_id
UNION all
select 
    mid_id
from dws_uv_detail_daycount
where dt BETWEEN date_sub(next_day('2020-05-06','MO'),14)
         and  date_sub(next_day('2020-05-06','MO'),8)
GROUP by mid_id
UNION all
select 
    mid_id
from dws_uv_detail_daycount
where dt BETWEEN date_sub(next_day('2020-05-06','MO'),21)
         and  date_sub(next_day('2020-05-06','MO'),15)
GROUP by mid_id) tmp
GROUP by mid_id
having count(*)=3) tmp1
        
```

### 3.8最近七天内连续三天活跃用户数

范围： 最近七天

要求： 连续三天活跃

从dws_uv_daycount表取！



如何判断连续3天活跃？

假设有

A列，以X递增，以a开头！

B列，以Y递增，以b开头！

如果A列和B列都是连续递增，那么连续两列的差值之间的差，也是以Y-X递增！

| A    | B    | 连续两列的差值 |
| ---- | ---- | -------------- |
| a    | b    | b-a            |
| a+X  | b+Y  | b-a+(Y-X)      |
| a+2X | b+2Y | b-a+2(Y-X)     |

如果Y=X，那么连续两列的差值之间的差相等！



类比求连续活跃

| 日期       | 参照列 row_number（） | 做差       |
| ---------- | --------------------- | ---------- |
| 2020-05-06 | 1                     | 2020-05-05 |
| 2020-05-07 | 2                     | 2020-05-05 |
| 2020-05-09 | 3                     | 2020-05-06 |

连续3天活跃，将差值分组，分组后如果组内差值数量>=3，就是连续3天！

```sql
insert into table ads_continuity_uv_count
select
    '2020-05-06',
    concat(date_sub('2020-05-06',6)),'_','2020-05-06'),
    COUNT(DISTINCT mid_id)
from
(select 
    mid_id
from
(SELECT
    dt,mid_id,ROW_NUMBER() over(PARTITION by mid_id order by dt ) rn,
    date_sub(dt,ROW_NUMBER() over(PARTITION by mid_id order by dt )) diff
from dws_uv_detail_daycount
where dt>=date_sub('2020-05-06',6)) tmp
GROUP by mid_id,diff
having count(*)>=3) tmp2
```

## 4.会员主题

### 4.1 会员信息

```sql
create external table ads_user_topic(
    `dt` string COMMENT '统计日期',
    `day_users` string COMMENT '活跃会员数',
    `day_new_users` string COMMENT '新增会员数',
    `day_new_payment_users` string COMMENT '新增消费会员数',
    `payment_users` string COMMENT '总付费会员数',
    `users` string COMMENT '总会员数',
    `day_users2users` decimal(10,2) COMMENT '会员活跃率',
    `payment_users2users` decimal(10,2) COMMENT '总会员付费率',
    `day_new_users2users` decimal(10,2) COMMENT '会员新鲜度'
) COMMENT '会员主题信息表'

```

会员新鲜度：  当日的活跃会员中，新增会员占日活会员的比例

会员活跃率： 当日的日活会员 占 总会员的比率

总会员付费率： 付费会员  占 会员总数的 比率



从dwt_user_topic取

```sql
insert into table ads_user_topic
SELECT
    '2020-05-19' dt,
    sum(if(login_date_last='2020-05-19',1,0)) day_users,
    sum(if(login_date_first='2020-05-19',1,0)) day_new_users,
    sum(if(payment_date_first='2020-05-19',1,0)) day_new_payment_users,
    sum(if(payment_amount>0,1,0)) payment_users,
    count(*) users,
    cast(sum(if(login_date_last='2020-05-19',1,0)) / count(*) * 100 as decimal(10,2)) day_users2users,
    cast(sum(if(payment_amount>0,1,0)) / count(*) * 100 as decimal(10,2)) payment_users2users,
    cast( sum(if(login_date_first='2020-05-19',1,0)) / sum(if(login_date_last='2020-05-19',1,0)) * 100 as decimal(10,2))
from dwt_user_topic
```

### 4.2 转化率

```sql
create external  table ads_user_action_convert_day(
    `dt` string COMMENT '统计日期',
    `total_visitor_m_count`  bigint COMMENT '总访问人数',
    `cart_u_count` bigint COMMENT '加入购物车的人数',
    `visitor2cart_convert_ratio` decimal(10,2) COMMENT '访问到加入购物车转化率',
    `order_u_count` bigint     COMMENT '下单人数',
    `cart2order_convert_ratio`  decimal(10,2) COMMENT '加入购物车到下单转化率',
    `payment_u_count` bigint     COMMENT '支付人数',
    `order2payment_convert_ratio` decimal(10,2) COMMENT '下单到支付的转化率'
 ) COMMENT '用户行为漏斗分析'

```

从 dws_user_action_daycount取， dwt层没有和加购相关的信息！

```sql
insert INTO TABLE ads_user_action_convert_day
 select 
    '2020-05-19',
    count(*) total_visitor_m_count,
    sum(if(cart_count>0,1,0)) cart_u_count,
    cast(sum(if(cart_count>0,1,0)) / count(*) * 100 as decimal(10,2)) visitor2cart_convert_ratio,
    sum(if(order_count>0,1,0)) order_u_count,
    cast( sum(if(order_count>0,1,0)) / sum(if(cart_count>0,1,0)) * 100 as decimal(10,2))  cart2order_convert_ratio,
    sum(if(payment_count>0,1,0)) payment_u_count,
    cast( sum(if(payment_count>0,1,0)) / sum(if(order_count>0,1,0)) * 100 as decimal(10,2))
 from dws_user_action_daycount
 WHERE dt='2020-05-19'
```

## 5.商品主题

### 5.1 商品个数信息

```sql
insert into table ads_product_info
SELECT
    '2020-05-19' dt,
    count(*) sku_num,
    count(DISTINCT spu_id) spu_num
from dwt_sku_topic

```

### 5.2 商品累积销量排名

```sql
insert into table ads_product_sale_top10
SELECT
    '2020-05-19',
    sku_id,
    payment_num
FROM dwt_sku_topic
where payment_num>0
order by payment_num desc
limit 10
```

### 5.3 商品收藏排名

```sql
insert into TABLE ads_product_favor_topn
SELECT
    '2020-05-19',
    sku_id,
    favor_count
from dwt_sku_topic
where favor_count>0
order by favor_count desc
limit 10
```

### 5.4 加入购物车排名

```sql
insert into TABLE ads_product_cart_topn
SELECT
    '2020-05-19',
    sku_id,
    cart_num
from dwt_sku_topic
where cart_num>0
order by cart_num desc
limit 10
```

### 5.5 最近30天退款率

```sql
insert into TABLE ads_product_refund_topn
select
    '2020-05-19',
    sku_id,
    refund_ratio
from
(SELECT
    sku_id,
    nvl(cast(refund_last_30d_count/payment_last_30d_count * 100 as decimal(10,2)),0) refund_ratio 
from dwt_sku_topic) tmp
where refund_ratio>0
order by refund_ratio desc
limit 10
```

### 5.6 差评率排名

```sql
insert into TABLE ads_appraise_bad_topn
select
    '2020-05-19',
    sku_id,
    appraise_bad_ratio
from
(SELECT
    sku_id,
    nvl(cast(appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) * 100 as decimal(10,2)),0) appraise_bad_ratio 
from dwt_sku_topic) tmp
where appraise_bad_ratio>0
order by appraise_bad_ratio desc
limit 10
```

## 6.营销主题

### 6.1 每日下单统计

```sql
insert into TABLE ads_order_daycount
SELECT
    '2020-05-19',
    sum(order_count) order_count,
    sum(order_amount) order_amount,
    count(DISTINCT user_id) order_users
from dws_sale_detail_daycount
where dt='2020-05-19'
```

### 6.2 每日支付统计

```sql
insert into TABLE ads_payment_daycount
select 
    '2020-05-19',order_count,order_amount,payment_user_count,
    payment_sku_count,payment_avg_time
    
from
(SELECT
    '2020-05-19' dt,
    count(*) order_count,
    sum(payment_amount) order_amount,
    count(DISTINCT user_id) payment_user_count
from dwd_fact_payment_info
where dt='2020-05-19') t1
join
(select 
    '2020-05-19' dt,
    sum(payment_num) payment_sku_count
from dws_sku_action_daycount
where dt='2020-05-19') t2
on t1.dt=t2.dt
join
(SELECT
    '2020-05-19' dt,
    cast(sum(unix_timestamp(payment_time)-unix_timestamp(create_time))/ 60 / COUNT(*) as double) payment_avg_time
from dwd_fact_order_info
where dt='2020-05-19' or dt=date_sub('2020-05-19',1)
      and date_format(payment_time,'yyyy-MM-dd')='2020-05-19') t3
on t1.dt=t3.dt


```

### 6.3 品牌的月复购率

```sql
create external table ads_sale_tm_category1_stat_mn
(  
    tm_id string comment '品牌id',
    category1_id string comment '1级品类id ',
    category1_name string comment '1级品类名称 ',
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(10,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(10,2)  comment  '多次复购率',
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期' 
)   COMMENT '复购率统计'

```

复够率：  多次购买人数之间 和 购买过的人数 的比值

单次复购率：  购买两次以上的人数 / 购买过的人数

多次复购率：  购买三次以上的人数 / 购买过的人数



思路： ①从dws_sale_detail_daycount取当月的数据

​			 ②将当月的数据，按照 品牌id 和 用户id  进行聚合，统计出在这个月，每个用户分别买每个品牌

​					各下多少单

​	

​			③将以上数据，按照品牌id进行聚合

​			④根据下单次数，判断人数

```sql
insert into table ads_sale_tm_category1_stat_mn
select
     sku_tm_id,sku_category1_id,sku_category1_name,
     sum(if(sum_order_count>0,1,0)) buycount,
 sum(if(sum_order_count>1,1,0)) buy_twice_last,
 cast(sum(if(sum_order_count>1,1,0))/sum(if(sum_order_count>0,1,0))*100 as decimal(10,2)) buy_twice_last,
 sum(if(sum_order_count>2,1,0)) buy_3times_last,
 cast(sum(if(sum_order_count>2,1,0))/ sum(if(sum_order_count>0,1,0)) * 100 as decimal(10,2)) buy_3times_last_ratio,
 date_format('2020-05-19','yyyy-MM') stat_mn,
 '2020-05-19'
from
(select
     user_id,sku_tm_id,sku_category1_name,sku_category1_id,
     sum(order_count) sum_order_count
from dws_sale_detail_daycount
where date_format(dt,'yyyy-MM')=date_format('2020-05-19','yyyy-MM')
group by user_id,sku_tm_id,sku_category1_name,sku_category1_id) t1
group by sku_tm_id,sku_category1_name,sku_category1_id
```

# 七、总结

## 1.数据来源

|          | 用户行为数据       | Mysql中的电商业务数据 |
| -------- | ------------------ | --------------------- |
| 存储形式 | 日志               | 表                    |
| 格式     | {} ,  xxxx\|{}     | 字段                  |
| 采集方式 | flume              | sqoop                 |
| ETL      | flume中的拦截器    | 通过SQL               |
| 分类     | 启动日志，事件日志 | 维度表+事实表         |
|          |                    |                       |
|          |                    |                       |
|          |                    |                       |



| 数据源      | 建模                                                         | 如何导入数据                                                 | 备注              |
| ----------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------- |
| hdfs        |                                                              |                                                              | 采用lzo压缩的格式 |
| ODS         | 原数据有几个字段是什么类型，就怎么建模                       | 必须指定ODS的表使用能够读取LZO压缩格式的输入格式，为LZO格式创建索引 |                   |
| 用户行为DWD | 用户行为数据根据不同类型数据的字段明细，进行建模             | 启动日志： get_json_object  事件日志： 自定义UDF,UDTF，将事件日志中的每个事件，解析到一个base_event表中，再使用get_json_object展开事件明细。 |                   |
| 业务数据DWD | 维度表：维度退化，将多个同一类型维度的字段合并到一张表中。事实表：采取星型模型，基于3w原则，按照选取业务线---确认粒度---选取维度---选取度量进行建模 | 维度表：多表Join 事实表：选择一张事实表作为主表，通过外键关联维度表，选取维度字段。再选取度量！ |                   |
|             |                                                              | 事务型事实表：选取ods层某一天分区的数据，再关联维度表，选取维度字段，再选取度量！ |                   |
|             |                                                              | 周期型快照事实表：直接从ODS层全量导入(加入购物车，收藏表)    |                   |
|             |                                                              | 累积型快照事实表：  按照事实发生最初的事件作为分区字段！①选择要覆盖的老的分区的所有数据②选取今日新增和变化的新数据③新旧交替，以新换旧④覆盖到指定的分区 |                   |
|             |                                                              | 拉链表(缓慢变化维度)：old left join new ,将old中过期的数据的end_date修改为new中start_date的前一天。 再union all new。导入到临时表，再导入到原表 |                   |
| dws层       | 紧紧贴合需求。将同一类型的需求，汇总，分类，以某个需求的统计目标为主题(设备，用户，商品，优惠券，活动，购买行为)，创建宽表 | 取dwd层每日最新的分区，进行多表关联                          |                   |
| dwt层       | 紧紧贴合需求。将同一类型的需求，汇总，分类，以某个需求的统计目标为主题(设备，用户，商品，优惠券，活动，购买行为)，创建宽表 | dwt full join dws 当日分区的数据①新旧交替，以新换旧②覆盖原表 |                   |
| ads         | 紧紧贴合需求。将同一类型的需求，汇总，分类，以某个需求的统计目标为主题(用户，商品，会员，营销)， | 取某一天的历史切片数据，从dws层取，如果要取当前的数据或累计状态，从dwt层取 |                   |
| 导出mysql   |                                                              | update_mode： allowinsert         update-key： dt            |                   |



​										

​									

​									

​									

​									