# 1. 日志数据
## 1.1 日志数据结构描述
日志数据记录了页面浏览记录、动作记录、曝光记录、启动记录和错误记录。可以分为两大类：一是页面日志，二是启动日志。  
一条完整的页面日志包含，一个页面浏览记录，若干个用户在该页面所做的动作记录，若干个该页面的曝光记录，以及一个在该页面发生的报错记录。
除上述行为信息，页面日志还包含了这些行为所处的各种环境信息，包括用户信息、时间信息、地理位置信息、设备信息、应用信息、渠道信息等。
页面日志的格式如下：
```
{
	"common": {                     -- 环境信息
		"ar": "230000",             -- 地区编码
		"ba": "iPhone",             -- 手机品牌
		"ch": "Appstore",           -- 渠道
		"is_new": "1",              -- 是否首日使用，首次使用的当日，该字段值为1，过了24:00，该字段置为0。
		"md": "iPhone 8",           -- 手机型号
		"mid": "YXfhjAYH6As2z9Iq",  -- 设备id
		"os": "iOS 13.2.9",         -- 操作系统
		"uid": "485",               -- 会员id
		"vc": "v2.1.134"            -- app版本号
	},
	"actions": [{                   -- 动作(事件)
		"action_id": "favor_add",   -- 动作id
		"item": "3",                -- 目标id
		"item_type": "sku_id",      -- 目标类型
		"ts": 1585744376605         -- 动作时间戳
	    }
	],
	"displays": [{                  -- 曝光
			"displayType": "query", -- 曝光类型
			"item": "3",            -- 曝光对象id
			"item_type": "sku_id",  -- 曝光对象类型
			"order": 1,             -- 出现顺序
			"pos_id": 2             -- 曝光位置
		},
		{
			"displayType": "promotion",
			"item": "6",
			"item_type": "sku_id",
			"order": 2,
			"pos_id": 1
		},
		{
			"displayType": "promotion",
			"item": "9",
			"item_type": "sku_id",
			"order": 3,
			"pos_id": 3
		},
		{
			"displayType": "recommend",
			"item": "6",
			"item_type": "sku_id",
			"order": 4,
			"pos_id": 2
		},
		{
			"displayType": "query ",
			"item": "6",
			"item_type": "sku_id",
			"order": 5,
			"pos_id": 1
		}
	],
	"page": {                          -- 页面信息
		"during_time": 7648,           -- 持续时间毫秒
		"item": "3", 	               -- 目标id
		"item_type": "sku_id",         -- 目标类型
		"last_page_id": "login",       -- 上页类型
		"page_id": "good_detail",      -- 页面ID
		"sourceType": "promotion"      -- 来源类型
	},                                 
	"err": {                           --错误
		"error_code": "1234",          --错误码
		"msg": "***********"           --错误信息
	},                                 
	"ts": 1585744374423                --跳入时间戳
}
```
启动日志以启动为单位，每一次启动行为，生成一条启动日志。一条完整的启动日志包括一个启动记录，一个本次启动时的报错记录，
以及启动时所处的环境信息，包括用户信息、时间信息、地理位置信息、设备信息、应用信息、渠道信息等。
启动日志的格式如下：
```
{
  "common": {
    "ar": "370000",
    "ba": "Honor",
    "ch": "wandoujia",
    "is_new": "1",
    "md": "Honor 20s",
    "mid": "eQF5boERMJFOujcp",
    "os": "Android 11.0",
    "uid": "76",
    "vc": "v2.1.134"
  },
  "start": {   
    "entry": "icon",         --icon手机图标  notice 通知   install 安装后启动
    "loading_time": 18803,  --启动加载时间
    "open_ad_id": 7,        --广告页ID
    "open_ad_ms": 3449,    -- 广告总共播放时间
    "open_ad_skip_ms": 1989   --  用户跳过广告时点
  },
"err":{                     --错误
"error_code": "1234",      --错误码
    "msg": "***********"       --错误信息
},
  "ts": 1585744304000
}
```
## 1.2 日志数据采集
在日志服务器启动 flume，将日志数据采集进 kafka 的"topic_log"主题。再使用 flume 将日志数据从 kafka 采集进 hdfs。
# 2. 业务数据
## 2.1 业务表描述
本项目模拟的电商平台共设计了34张业务表。这34个表以订单表、用户表、SKU商品表、活动表和优惠券表为中心，延伸出了优惠券领用表、
支付流水表、活动订单表、订单详情表、订单状态表、商品评论表、编码字典表退单表、SPU商品表等，
用户表提供用户的详细信息，支付流水表提供该订单的支付详情，订单详情表提供订单的商品数量等情况，
商品表给订单详情表提供商品的详细信息。实际项目中，业务数据库中表格远远不止这些。
### 2.1.1 活动信息表（activity_info）
|字段名 |字段说明 |
| :---- | :---- |
| id | 活动id |
|activity_name|活动名称|
|activity_type|	活动类型（1：满减，2：折扣）|
|activity_desc|	活动描述|
|start_time	|开始时间|
|end_time	|结束时间|
|create_time	|创建时间|
### 2.1.2 活动规则表（activity_rule）
|字段名 |字段说明 |
| :---- | :---- |
|id	 |编号|
|activity_id	| 活动ID|
|activity_type	| 活动类型|
|condition_amount	 | 满减金额|
|condition_num	| 满减件数|
|benefit_amount	| 优惠金额|
|benefit_discount	| 优惠折扣|
|benefit_level	| 优惠级别|
### 2.1.3 活动商品关联表（activity_sku）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|activity_id	|活动id |
|sku_id	|sku_id|
|create_time	|创建时间|
### 2.1.4 平台属性表（base_attr_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|attr_name	|属性名称|
|category_id	|分类id|
|category_level	|分类层级|
### 2.1.5 平台属性值表（base_attr_value）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|value_name	|属性值名称|
|attr_id	|属性id|
### 2.1.6 一级分类表（base_category1）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|name	|分类名称|
### 2.1.7 二级分类表（base_category2）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|name	|二级分类名称|
|category1_id	|一级分类编号|
### 2.1.8 三级分类表（base_category3）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|name	|三级分类名称|
|category2_id	|二级分类编号|
### 2.1.9 字典表（base_dic）
|字段名	|字段说明|
| :---- | :---- |
|dic_code	|编号|
|dic_name	|编码名称|
|parent_code	|父编号|
|create_time	|创建日期|
|operate_time	|修改日期|
### 2.1.10 省份表（base_province）
|字段名	|字段说明|
| :---- | :---- |
|id	|id|
|name	|省名称|
|region_id	|大区id|
|area_code	|行政区位码|
|iso_code	|国际编码|
|iso_3166_2	|ISO3166编码|
### 2.1.11 地区表（base_region）
|字段名	|字段说明|
| :---- | :---- |
|id	|大区id|
|region_name	|大区名称|
### 2.1.12 品牌表（base_trademark）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|tm_name	|属性值|
|logo_url	|品牌logo的图片路径|
### 2.1.13 购物车表（cart_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|user_id	|用户id|
|sku_id	|skuid|
|cart_price	|放入购物车时价格|
|sku_num	|数量|
|img_url	|图片文件|
|sku_name	|sku名称 (冗余)|
|is_checked	|是否已经下单|
|create_time	|创建时间|
|operate_time	|修改时间|
|is_ordered	|是否已经下单|
|order_time	|下单时间|
|source_type	|来源类型|
|source_id	|来源编号|
### 2.1.14 评价表（comment_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|user_id	|用户id|
|nick_name	|用户昵称|
|head_img	|图片|
|sku_id	|商品sku_id|
|spu_id	|商品spu_id|
|order_id	|订单编号|
|appraise	|评价 1 好评 2 中评 3 差评|
|comment_txt	|评价内容|
|create_time	|创建时间|
|operate_time	|修改时间|
### 2.1.15 优惠券信息表（coupon_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|购物券编号|
|coupon_name	|购物券名称|
|coupon_type	|购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券|
|condition_amount	|满额数（3）|
|condition_num	|满件数（4）|
|activity_id	|活动编号|
|benefit_amount	|减金额（1 3）|
|benefit_discount	|折扣（2 4）|
|create_time	|创建时间|
|range_type	|范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌|
|limit_num	|最多领用次数|
|taken_count	|已领用次数|
|start_time	|可以领取的开始日期|
|end_time	|可以领取的结束日期|
|operate_time	|修改时间|
|expire_time	|过期时间|
|range_desc	|范围描述|
### 2.1.16 优惠券优惠范围表（coupon_range）
|字段名	|字段说明|
| :---- | :---- |
|id	|购物券编号|
|coupon_id	|优惠券id|
|range_type	|范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌|
|range_id	|范围id|
### 2.1.17 优惠券领用表（coupon_use）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|coupon_id	|购物券id|
|user_id	|用户id|
|order_id	|订单id|
|coupon_status	|购物券状态（1：未使用 2：已使用）|
|get_time	|获取时间|
|using_time	|使用时间|
|used_time	|支付时间|
|expire_time	|过期时间|
### 2.1.18 收藏表（favor_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|user_id	|用户id|
|sku_id	|skuid|
|spu_id	|商品id|
|is_cancel	|是否已取消 0 正常 1 已取消|
|create_time	|创建时间|
|cancel_time	|修改时间|
### 2.1.19 订单明细表（order_detail）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|order_id	|订单编号|
|sku_id	|sku_id|
|sku_name	|sku名称（冗余)|
|img_url	|图片名称（冗余)|
|order_price	|购买价格(下单时sku价格）|
|sku_num	|购买个数|
|create_time	|创建时间|
|source_type	|来源类型|
|source_id	|来源编号|
|split_total_amount	|分摊总金额|
|split_activity_amount	|分摊活动减免金额|
|split_coupon_amount	|分摊优惠券减免金额|
### 2.1.20 订单明细活动关联表（order_detail_activity）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|order_id	|订单id|
|order_detail_id	|订单明细id|
|activity_id	|活动id|
|activity_rule_id	|活动规则|
|sku_id	|skuid|
|create_time	|获取时间|
### 2.1.21 订单明细优惠券关联表（order_detail_coupon）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|order_id	|订单id|
|order_detail_id	|订单明细id|
|coupon_id	|购物券id|
|coupon_use_id	|购物券领用id|
|sku_id	|skuid|
|create_time	|获取时间|
### 2.1.22 订单表(order_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|consignee	|收货人|
|consignee_tel	|收件人电话|
|total_amount	|总金额|
|order_status	|订单状态|
|user_id	|用户id|
|payment_way	|付款方式|
|delivery_address	|送货地址|
|order_comment	|订单备注|
|out_trade_no	|订单交易编号（第三方支付用)|
|trade_body	|订单描述(第三方支付用)|
|create_time	|创建时间|
|operate_time	|操作时间|
|expire_time	|失效时间|
|process_status	|进度状态|
|tracking_no	|物流单编号|
|parent_order_id	|父订单编号|
|img_url	|图片路径|
|province_id	|地区|
|activity_reduce_amount	|促销金额|
|coupon_reduce_amount	|优惠金额|
|original_total_amount	|原价金额|
|feight_fee	|运费|
|feight_fee_reduce	|运费减免|
|refundable_time	|可退款日期（签收后30天）|
### 2.1.23 退单表（order_refund_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|user_id	|用户id|
|order_id	|订单id|
|sku_id	|skuid|
|refund_type	|退款类型|
|refund_num	|退货件数|
|refund_amount	|退款金额|
|refund_reason_type	|原因类型|
|refund_reason_txt	|原因内容|
|refund_status	|退款状态（0：待审批 1：已退款）|
|create_time	|创建时间|
### 2.1.24 订单状态流水表（order_status_log）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|order_id	|订单编号|
|order_status	|订单状态|
|operate_time	|操作时间|
### 2.1.25 支付表（payment_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|out_trade_no	|对外业务编号|
|order_id	|订单编号|
|user_id	|用户id|
|payment_type	|支付类型（微信 支付宝）|
|trade_no	|交易编号|
|total_amount	|支付金额|
|subject	|交易内容|
|payment_status	|支付状态|
|create_time	|创建时间|
|callback_time	|回调时间|
|callback_content	|回调信息|
### 2.1.26 退款表（refund_payment）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|out_trade_no	|对外业务编号|
|order_id	|订单编号|
|sku_id	|商品sku_id|
|payment_type	|支付类型（微信 支付宝）|
|trade_no	|交易编号|
|total_amount	|退款金额|
|subject	|交易内容|
|refund_status	|退款状态|
|create_time	|创建时间|
|callback_time	|回调时间|
|callback_content	|回调信息|
### 2.1.27 SKU平台属性表（sku_attr_value）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|attr_id	|属性id（冗余)|
|value_id	|属性值id|
|sku_id	|skuid|
|attr_name	|属性名称|
|value_name	|属性值名称|
### 2.1.28 SKU信息表（sku_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|库存id(itemID)|
|spu_id	|商品id|
|price	|价格|
|sku_name	|sku名称|
|sku_desc	|商品规格描述|
|weight	|重量|
|tm_id	|品牌(冗余)|
|category3_id	|三级分类id（冗余)|
|sku_default_img	|默认显示图片(冗余)|
|is_sale	|是否销售（1：是 0：否）|
|create_time	|创建时间|
### 2.1.29 SKU销售属性表（sku_sale_attr_value）
|字段名	|字段说明|
| :---- | :---- |
|id	|id|
|sku_id	|库存单元id|
|spu_id	|spu_id（冗余）|
|sale_attr_value_id	|销售属性值id|
|sale_attr_id	|销售属性id|
|sale_attr_name	|销售属性值名称|
|sale_attr_value_name	|销售属性值名称|
### 2.1.30 SPU信息表（spu_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|商品id|
|spu_name	|商品名称|
|description	|商品描述(后台简述）|
|category3_id	|三级分类id|
|tm_id	|品牌id|
### 2.1.31 SPU销售属性表（spu_sale_attr）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号（业务中无关联）|
|spu_id	|商品id|
|base_sale_attr_id	|销售属性id|
|sale_attr_name	|销售属性名称（冗余）|
### 2.1.32 SPU销售属性值表（spu_sale_attr_value）
|字段名	|字段说明|
| :---- | :---- |
|id	|销售属性值编号|
|spu_id	|商品id|
|base_sale_attr_id	|销售属性id|
|sale_attr_value_name	|销售属性值名称|
|sale_attr_name	|销售属性名称（冗余）|
### 2.1.33 用户地址表（user_address）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|user_id	|用户id|
|province_id	|省份id|
|user_address	|用户地址|
|consignee	|收件人|
|phone_num	|联系方式|
|is_default	|是否是默认|
### 2.1.34 用户信息表（user_info）
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|login_name	|用户名称|
|nick_name	|用户昵称|
|passwd	|用户密码|
|name	|用户姓名|
|phone_num	|手机号|
|email	|邮箱|
|head_img	|头像|
|user_level	|用户级别|
|birthday	|用户生日|
|gender	|性别 M男,F女|
|create_time	|创建时间|
|operate_time	|修改时间|
|status	|状态|
## 2.2 业务数据采集
业务数据导入方式可以分为全量导入和增量导入。全量导入每次将业务表所有数据同步到数据仓库，逻辑简单，使用方便，缺点是存在大量数据冗余。
增量导入只同步当日新增及变化的数据，并存入数仓当日对应的分区，适合只有新增数据或原来数据变化较小的场合，缺点是某些场景下使用逻辑比较复杂，
需要进行整合才能使用。  
通常情况下，业务表数据量比较大，优先考虑增量同步，数据量比较小，优先考虑全量同步。  
本项目使用 dataX 实现全量同步，将业务表数据采集进 hdfs；使用 MaxWell 实现增量同步，将业务表新增及变化的数据采集进 kafka，
再使用 flume 将数据采集进 hdfs。  
全量表：活动表（activity_info）、优惠活动规则表（activity_rule）、商品一级分类（base_category1）、商品二级分类（base_category2）、
商品三级分类（base_category3）、编码字典表（base_dic）、省份表（base_province）、地区表（base_region）、
品牌表（base_trademark）、购物车表（cart_info）、优惠券表（coupon_info）、sku 平台属性表（sku_attr_value）、sku 销售属性表（sku_sale_attr_value）、
sku 商品表（sku_info）、spu 商品表（spu_info）  
增量表：购物车表（cart_info）、商品评论表（comment_info）、优惠券领用表（coupon_use）、收藏表（favor_info）、订单明细活动关联表（order_detail_activity）、
订单明细优惠券关联表（order_detail_coupon）、订单详情表（order_detail）、
订单表（order_info）、退单表（order_refund_info）、订单状态表（order_status_log）、支付表（payment_info）、退款表（refund_payment）、用户表（user_info）
## 2.3 ods层建设
ods 层的功能是对原始数据的备份，因此表结构与原始数据结构保持一致，同时由于 ods 层的数据量较大，因此采用压缩比较高的 gzip 压缩。
表命名格式为 ods_表名_增量/全量标识（inc/full）。
### 2.3.1 日志表
```DROP TABLE IF EXISTS ods_log_inc;
   CREATE EXTERNAL TABLE ods_log_inc
   (
       `common`   STRUCT<ar :STRING,ba :STRING,ch :STRING,is_new :STRING,md :STRING,mid :STRING,os :STRING,uid :STRING,vc
                         :STRING> COMMENT '公共信息',
       `page`     STRUCT<during_time :STRING,item :STRING,item_type :STRING,last_page_id :STRING,page_id
                         :STRING,source_type :STRING> COMMENT '页面信息',
       `actions`  ARRAY<STRUCT<action_id:STRING,item:STRING,item_type:STRING,ts:BIGINT>> COMMENT '动作信息',
       `displays` ARRAY<STRUCT<display_type :STRING,item :STRING,item_type :STRING,`order` :STRING,pos_id
                               :STRING>> COMMENT '曝光信息',
       `start`    STRUCT<entry :STRING,loading_time :BIGINT,open_ad_id :BIGINT,open_ad_ms :BIGINT,open_ad_skip_ms
                         :BIGINT> COMMENT '启动信息',
       `err`      STRUCT<error_code:BIGINT,msg:STRING> COMMENT '错误信息',
       `ts`       BIGINT  COMMENT '时间戳'
   ) COMMENT '活动信息表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_log_inc/';
```
### 2.3.2 活动信息表（全量表）
```DROP TABLE IF EXISTS ods_activity_info_full;
   CREATE EXTERNAL TABLE ods_activity_info_full
   (
       `id`            STRING COMMENT '活动id',
       `activity_name` STRING COMMENT '活动名称',
       `activity_type` STRING COMMENT '活动类型',
       `activity_desc` STRING COMMENT '活动描述',
       `start_time`    STRING COMMENT '开始时间',
       `end_time`      STRING COMMENT '结束时间',
       `create_time`   STRING COMMENT '创建时间'
   ) COMMENT '活动信息表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_activity_info_full/';
```
### 2.3.3 活动规则表（全量表）
```DROP TABLE IF EXISTS ods_activity_rule_full;
   CREATE EXTERNAL TABLE ods_activity_rule_full
   (
       `id`               STRING COMMENT '编号',
       `activity_id`      STRING COMMENT '活动id',
       `activity_type`    STRING COMMENT '活动类型',
       `condition_amount` DECIMAL(16, 2) COMMENT '满减金额',
       `condition_num`    BIGINT COMMENT '满减件数',
       `benefit_amount`   DECIMAL(16, 2) COMMENT '优惠金额',
       `benefit_discount` DECIMAL(16, 2) COMMENT '优惠折扣',
       `benefit_level`    STRING COMMENT '优惠级别'
   ) COMMENT '活动规则表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
```
### 2.3.4 一级品类表（全量表）
```DROP TABLE IF EXISTS ods_base_category1_full;
   CREATE EXTERNAL TABLE ods_base_category1_full
   (
       `id`   STRING COMMENT '编号',
       `name` STRING COMMENT '分类名称'
   ) COMMENT '一级品类表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_base_category1_full/';
```
### 2.3.5 二级品类表（全量表）
```DROP TABLE IF EXISTS ods_base_category2_full;
   CREATE EXTERNAL TABLE ods_base_category2_full
   (
       `id`           STRING COMMENT '编号',
       `name`         STRING COMMENT '二级分类名称',
       `category1_id` STRING COMMENT '一级分类编号'
   ) COMMENT '二级品类表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_base_category2_full/';
```
### 2.3.6 三级品类表（全量表）
```DROP TABLE IF EXISTS ods_base_category3_full;
   CREATE EXTERNAL TABLE ods_base_category3_full
   (
       `id`           STRING COMMENT '编号',
       `name`         STRING COMMENT '三级分类名称',
       `category2_id` STRING COMMENT '二级分类编号'
   ) COMMENT '三级品类表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_base_category3_full/';
```
### 2.3.7 编码字典表（全量表）
```DROP TABLE IF EXISTS ods_base_dic_full;
   CREATE EXTERNAL TABLE ods_base_dic_full
   (
       `dic_code`     STRING COMMENT '编号',
       `dic_name`     STRING COMMENT '编码名称',
       `parent_code`  STRING COMMENT '父编号',
       `create_time`  STRING COMMENT '创建日期',
       `operate_time` STRING COMMENT '修改日期'
   ) COMMENT '编码字典表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_base_dic_full/';
```
### 2.3.8 省份表（全量表）
```DROP TABLE IF EXISTS ods_base_province_full;
   CREATE EXTERNAL TABLE ods_base_province_full
   (
       `id`         STRING COMMENT '编号',
       `name`       STRING COMMENT '省份名称',
       `region_id`  STRING COMMENT '地区ID',
       `area_code`  STRING COMMENT '地区编码',
       `iso_code`   STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',
       `iso_3166_2` STRING COMMENT '新版IOS-3166-2编码，供可视化使用'
   ) COMMENT '省份表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_base_province_full/';
```
### 2.3.9 地区表（全量表）
```DROP TABLE IF EXISTS ods_base_region_full;
   CREATE EXTERNAL TABLE ods_base_region_full
   (
       `id`          STRING COMMENT '编号',
       `region_name` STRING COMMENT '地区名称'
   ) COMMENT '地区表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_base_region_full/';
```
### 2.3.10 品牌表（全量表）
```DROP TABLE IF EXISTS ods_base_trademark_full;
   CREATE EXTERNAL TABLE ods_base_trademark_full
   (
       `id`       STRING COMMENT '编号',
       `tm_name`  STRING COMMENT '品牌名称',
       `logo_url` STRING COMMENT '品牌logo的图片路径'
   ) COMMENT '品牌表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_base_trademark_full/';
```
### 2.3.11 购物车表（全量表）
```DROP TABLE IF EXISTS ods_cart_info_full;
   CREATE EXTERNAL TABLE ods_cart_info_full
   (
       `id`           STRING COMMENT '编号',
       `user_id`      STRING COMMENT '用户id',
       `sku_id`       STRING COMMENT 'sku_id',
       `cart_price`   DECIMAL(16, 2) COMMENT '放入购物车时价格',
       `sku_num`      BIGINT COMMENT '数量',
       `img_url`      BIGINT COMMENT '商品图片地址',
       `sku_name`     STRING COMMENT 'sku名称 (冗余)',
       `is_checked`   STRING COMMENT '是否被选中',
       `create_time`  STRING COMMENT '创建时间',
       `operate_time` STRING COMMENT '修改时间',
       `is_ordered`   STRING COMMENT '是否已经下单',
       `order_time`   STRING COMMENT '下单时间',
       `source_type`  STRING COMMENT '来源类型',
       `source_id`    STRING COMMENT '来源编号'
   ) COMMENT '购物车全量表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_cart_info_full/';
```
### 2.3.12 优惠券信息表（全量表）
```DROP TABLE IF EXISTS ods_coupon_info_full;
   CREATE EXTERNAL TABLE ods_coupon_info_full
   (
       `id`               STRING COMMENT '购物券编号',
       `coupon_name`      STRING COMMENT '购物券名称',
       `coupon_type`      STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
       `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
       `condition_num`    BIGINT COMMENT '满件数',
       `activity_id`      STRING COMMENT '活动编号',
       `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
       `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
       `create_time`      STRING COMMENT '创建时间',
       `range_type`       STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
       `limit_num`        BIGINT COMMENT '最多领用次数',
       `taken_count`      BIGINT COMMENT '已领用次数',
       `start_time`       STRING COMMENT '开始领取时间',
       `end_time`         STRING COMMENT '结束领取时间',
       `operate_time`     STRING COMMENT '修改时间',
       `expire_time`      STRING COMMENT '过期时间'
   ) COMMENT '优惠券信息表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_coupon_info_full/';
```
### 2.3.13 商品平台属性表（全量表）
```DROP TABLE IF EXISTS ods_sku_attr_value_full;
   CREATE EXTERNAL TABLE ods_sku_attr_value_full
   (
       `id`         STRING COMMENT '编号',
       `attr_id`    STRING COMMENT '平台属性ID',
       `value_id`   STRING COMMENT '平台属性值ID',
       `sku_id`     STRING COMMENT '商品ID',
       `attr_name`  STRING COMMENT '平台属性名称',
       `value_name` STRING COMMENT '平台属性值名称'
   ) COMMENT 'sku平台属性表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_sku_attr_value_full/';
```
### 2.3.14 商品表（全量表）
```DROP TABLE IF EXISTS ods_sku_info_full;
   CREATE EXTERNAL TABLE ods_sku_info_full
   (
       `id`              STRING COMMENT 'skuId',
       `spu_id`          STRING COMMENT 'spuid',
       `price`           DECIMAL(16, 2) COMMENT '价格',
       `sku_name`        STRING COMMENT '商品名称',
       `sku_desc`        STRING COMMENT '商品描述',
       `weight`          DECIMAL(16, 2) COMMENT '重量',
       `tm_id`           STRING COMMENT '品牌id',
       `category3_id`    STRING COMMENT '品类id',
       `sku_default_igm` STRING COMMENT '商品图片地址',
       `is_sale`         STRING COMMENT '是否在售',
       `create_time`     STRING COMMENT '创建时间'
   ) COMMENT 'SKU商品表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_sku_info_full/';
```
### 2.3.15 商品销售属性值表（全量表）
```DROP TABLE IF EXISTS ods_sku_sale_attr_value_full;
   CREATE EXTERNAL TABLE ods_sku_sale_attr_value_full
   (
       `id`                   STRING COMMENT '编号',
       `sku_id`               STRING COMMENT 'sku_id',
       `spu_id`               STRING COMMENT 'spu_id',
       `sale_attr_value_id`   STRING COMMENT '销售属性值id',
       `sale_attr_id`         STRING COMMENT '销售属性id',
       `sale_attr_name`       STRING COMMENT '销售属性名称',
       `sale_attr_value_name` STRING COMMENT '销售属性值名称'
   ) COMMENT 'sku销售属性名称'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_sku_sale_attr_value_full/';
```
### 2.3.16 SPU表（全量表）
```DROP TABLE IF EXISTS ods_spu_info_full;
   CREATE EXTERNAL TABLE ods_spu_info_full
   (
       `id`           STRING COMMENT 'spu_id',
       `spu_name`     STRING COMMENT 'spu名称',
       `description`  STRING COMMENT '描述信息',
       `category3_id` STRING COMMENT '品类id',
       `tm_id`        STRING COMMENT '品牌id'
   ) COMMENT 'SPU商品表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       NULL DEFINED AS ''
       LOCATION '/warehouse/gmall/ods/ods_spu_info_full/';
```
### 2.3.17 购物车表（增量表）
```DROP TABLE IF EXISTS ods_cart_info_inc;
   CREATE EXTERNAL TABLE ods_cart_info_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,cart_price :DECIMAL(16, 2),sku_num :BIGINT,img_url :STRING,sku_name
                     :STRING,is_checked :STRING,create_time :STRING,operate_time :STRING,is_ordered :STRING,order_time
                     :STRING,source_type :STRING,source_id :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '购物车增量表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_cart_info_inc/';
```
### 2.3.18 评论表（增量表）
```DROP TABLE IF EXISTS ods_comment_info_inc;
   CREATE EXTERNAL TABLE ods_comment_info_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,user_id :STRING,nick_name :STRING,head_img :STRING,sku_id :STRING,spu_id :STRING,order_id
                     :STRING,appraise :STRING,comment_txt :STRING,create_time :STRING,operate_time :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '评价表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_comment_info_inc/';
```
### 2.3.19 优惠券领用表（增量表）
```DROP TABLE IF EXISTS ods_coupon_use_inc;
   CREATE EXTERNAL TABLE ods_coupon_use_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,coupon_id :STRING,user_id :STRING,order_id :STRING,coupon_status :STRING,get_time :STRING,using_time
                     :STRING,used_time :STRING,expire_time :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '优惠券领用表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_coupon_use_inc/';
```
### 2.3.20 收藏表（增量表）
```DROP TABLE IF EXISTS ods_favor_info_inc;
   CREATE EXTERNAL TABLE ods_favor_info_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,user_id :STRING,sku_id :STRING,spu_id :STRING,is_cancel :STRING,create_time :STRING,cancel_time
                     :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '收藏表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_favor_info_inc/';
```
### 2.3.21 订单明细表（增量表）
```DROP TABLE IF EXISTS ods_order_detail_inc;
   CREATE EXTERNAL TABLE ods_order_detail_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,order_id :STRING,sku_id :STRING,sku_name :STRING,img_url :STRING,order_price
                     :DECIMAL(16, 2),sku_num :BIGINT,create_time :STRING,source_type :STRING,source_id :STRING,split_total_amount
                     :DECIMAL(16, 2),split_activity_amount :DECIMAL(16, 2),split_coupon_amount
                     :DECIMAL(16, 2)> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '订单明细表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_order_detail_inc/';
```
### 2.3.22 订单明细活动关联表（增量表）
```DROP TABLE IF EXISTS ods_order_detail_activity_inc;
   CREATE EXTERNAL TABLE ods_order_detail_activity_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,activity_id :STRING,activity_rule_id :STRING,sku_id
                     :STRING,create_time :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '订单明细活动关联表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_order_detail_activity_inc/';
```
### 2.3.23 订单明细优惠券关联表（增量表）
```DROP TABLE IF EXISTS ods_order_detail_coupon_inc;
   CREATE EXTERNAL TABLE ods_order_detail_coupon_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,order_id :STRING,order_detail_id :STRING,coupon_id :STRING,coupon_use_id :STRING,sku_id
                     :STRING,create_time :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '订单明细优惠券关联表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon_inc/';
```
### 2.3.24 订单表（增量表）
```DROP TABLE IF EXISTS ods_order_info_inc;
   CREATE EXTERNAL TABLE ods_order_info_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,consignee :STRING,consignee_tel :STRING,total_amount :DECIMAL(16, 2),order_status :STRING,user_id
                     :STRING,payment_way :STRING,delivery_address :STRING,order_comment :STRING,out_trade_no :STRING,trade_body
                     :STRING,create_time :STRING,operate_time :STRING,expire_time :STRING,process_status :STRING,tracking_no
                     :STRING,parent_order_id :STRING,img_url :STRING,province_id :STRING,activity_reduce_amount
                     :DECIMAL(16, 2),coupon_reduce_amount :DECIMAL(16, 2),original_total_amount :DECIMAL(16, 2),freight_fee
                     :DECIMAL(16, 2),freight_fee_reduce :DECIMAL(16, 2),refundable_time :DECIMAL(16, 2)> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '订单表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_order_info_inc/';
```
### 2.3.25 退单表（增量表）
```DROP TABLE IF EXISTS ods_order_refund_info_inc;
   CREATE EXTERNAL TABLE ods_order_refund_info_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,user_id :STRING,order_id :STRING,sku_id :STRING,refund_type :STRING,refund_num :BIGINT,refund_amount
                     :DECIMAL(16, 2),refund_reason_type :STRING,refund_reason_txt :STRING,refund_status :STRING,create_time
                     :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '退单表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_order_refund_info_inc/';
```
### 2.3.26 订单状态流水表（增量表）
```DROP TABLE IF EXISTS ods_order_status_log_inc;
   CREATE EXTERNAL TABLE ods_order_status_log_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,order_id :STRING,order_status :STRING,operate_time :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '退单表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_order_status_log_inc/';
```
### 2.3.27 支付表（增量表）
```DROP TABLE IF EXISTS ods_payment_info_inc;
   CREATE EXTERNAL TABLE ods_payment_info_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,user_id :STRING,payment_type :STRING,trade_no
                     :STRING,total_amount :DECIMAL(16, 2),subject :STRING,payment_status :STRING,create_time :STRING,callback_time
                     :STRING,callback_content :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '支付表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_payment_info_inc/';
```
### 2.3.28 退款表（增量表）
```DROP TABLE IF EXISTS ods_refund_payment_inc;
   CREATE EXTERNAL TABLE ods_refund_payment_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,out_trade_no :STRING,order_id :STRING,sku_id :STRING,payment_type :STRING,trade_no :STRING,total_amount
                     :DECIMAL(16, 2),subject :STRING,refund_status :STRING,create_time :STRING,callback_time :STRING,callback_content
                     :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '退款表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_refund_payment_inc/';
```
### 2.3.29 用户表（增量表）
```DROP TABLE IF EXISTS ods_user_info_inc;
   CREATE EXTERNAL TABLE ods_user_info_inc
   (
       `type` STRING COMMENT '变动类型',
       `ts`   BIGINT COMMENT '变动时间',
       `data` STRUCT<id :STRING,login_name :STRING,nick_name :STRING,passwd :STRING,name :STRING,phone_num :STRING,email
                     :STRING,head_img :STRING,user_level :STRING,birthday :STRING,gender :STRING,create_time :STRING,operate_time
                     :STRING,status :STRING> COMMENT '数据',
       `old`  MAP<STRING,STRING> COMMENT '旧值'
   ) COMMENT '用户表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
       LOCATION '/warehouse/gmall/ods/ods_user_info_inc/';
```