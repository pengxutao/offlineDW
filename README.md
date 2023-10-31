- [1. 日志数据](#head1)
	- [1.1 日志数据结构描述](#head2)
	- [1.2 日志数据采集](#head3)
- [2. 业务数据](#head4)
	- [2.1 业务表描述](#head5)
		- [2.1.1 活动信息表（activity_info）](#head6)
		- [2.1.2 活动规则表（activity_rule）](#head7)
		- [2.1.3 活动商品关联表（activity_sku）](#head8)
		- [2.1.4 平台属性表（base_attr_info）](#head9)
		- [2.1.5 平台属性值表（base_attr_value）](#head10)
		- [2.1.6 一级分类表（base_category1）](#head11)
		- [2.1.7 二级分类表（base_category2）](#head12)
		- [2.1.8 三级分类表（base_category3）](#head13)
		- [2.1.9 字典表（base_dic）](#head14)
		- [2.1.10 省份表（base_province）](#head15)
		- [2.1.11 地区表（base_region）](#head16)
		- [2.1.12 品牌表（base_trademark）](#head17)
		- [2.1.13 购物车表（cart_info）](#head18)
		- [2.1.14 评价表（comment_info）](#head19)
		- [2.1.15 优惠券信息表（coupon_info）](#head20)
		- [2.1.16 优惠券优惠范围表（coupon_range）](#head21)
		- [2.1.17 优惠券领用表（coupon_use）](#head22)
		- [2.1.18 收藏表（favor_info）](#head23)
		- [2.1.19 订单明细表（order_detail）](#head24)
		- [2.1.20 订单明细活动关联表（order_detail_activity）](#head25)
		- [2.1.21 订单明细优惠券关联表（order_detail_coupon）](#head26)
		- [2.1.22 订单表(order_info）](#head27)
		- [2.1.23 退单表（order_refund_info）](#head28)
		- [2.1.24 订单状态流水表（order_status_log）](#head29)
		- [2.1.25 支付表（payment_info）](#head30)
		- [2.1.26 退款表（refund_payment）](#head31)
		- [2.1.27 SKU平台属性表（sku_attr_value）](#head32)
		- [2.1.28 SKU信息表（sku_info）](#head33)
		- [2.1.29 SKU销售属性表（sku_sale_attr_value）](#head34)
		- [2.1.30 SPU信息表（spu_info）](#head35)
		- [2.1.31 SPU销售属性表（spu_sale_attr）](#head36)
		- [2.1.32 SPU销售属性值表（spu_sale_attr_value）](#head37)
		- [2.1.33 用户地址表（user_address）](#head38)
		- [2.1.34 用户信息表（user_info）](#head39)
	- [2.2 业务数据采集](#head40)
- [3. 离线数仓建设](#head41)
	- [3.1 ods层建设](#head42)
		- [3.1.1 日志表](#head43)
		- [3.1.2 活动信息表（全量表）](#head44)
		- [3.1.3 活动规则表（全量表）](#head45)
		- [3.1.4 一级品类表（全量表）](#head46)
		- [3.1.5 二级品类表（全量表）](#head47)
		- [3.1.6 三级品类表（全量表）](#head48)
		- [3.1.7 编码字典表（全量表）](#head49)
		- [3.1.8 省份表（全量表）](#head50)
		- [3.1.9 地区表（全量表）](#head51)
		- [3.1.10 品牌表（全量表）](#head52)
		- [3.1.11 购物车表（全量表）](#head53)
		- [3.1.12 优惠券信息表（全量表）](#head54)
		- [3.1.13 商品平台属性表（全量表）](#head55)
		- [3.1.14 商品表（全量表）](#head56)
		- [3.1.15 商品销售属性值表（全量表）](#head57)
		- [3.1.16 SPU表（全量表）](#head58)
		- [3.1.17 购物车表（增量表）](#head59)
		- [3.1.18 评论表（增量表）](#head60)
		- [3.1.19 优惠券领用表（增量表）](#head61)
		- [3.1.20 收藏表（增量表）](#head62)
		- [3.1.21 订单明细表（增量表）](#head63)
		- [3.1.22 订单明细活动关联表（增量表）](#head64)
		- [3.1.23 订单明细优惠券关联表（增量表）](#head65)
		- [3.1.24 订单表（增量表）](#head66)
		- [3.1.25 退单表（增量表）](#head67)
		- [3.1.26 订单状态流水表（增量表）](#head68)
		- [3.1.27 支付表（增量表）](#head69)
		- [3.1.28 退款表（增量表）](#head70)
		- [3.1.29 用户表（增量表）](#head71)
	- [3.2 dim层建设](#head72)
		- [3.2.1 商品维度表](#head73)
		- [3.2.2 优惠券维度表](#head74)
		- [3.2.3 活动维度表](#head75)
		- [3.2.4 地区维度表](#head76)
		- [3.2.5 用户维度表](#head77)
	- [3.3 dwd层建设](#head78)
		- [3.3.1 交易域下单事务事实表](#head79)
		- [3.3.2 交易域支付成功事务事实表](#head80)
		- [3.3.3 交易域购物车周期快照事实表](#head81)
		- [3.3.4 交易域交易流程累积快照事实表](#head82)
		- [3.3.5 流量域页面浏览事务事实表](#head83)
		- [3.3.6 用户域用户注册事务事实表](#head84)
		- [3.3.7 用户域用户登录事务事实表](#head85)
	- [3.4 dws层建设](#head86)
		- [3.4.1 近 1 日汇总表](#head87)
			- [3.4.1.1 交易域用户sku粒度下单近 1 日汇总表](#head88)
			- [3.4.1.2 交易域用户粒度下单近 1 日汇总表](#head89)
			- [3.4.1.3 交易域用户粒度支付近 1 日汇总表](#head90)
			- [3.4.1.4 交易域省份粒度下单近 1 日汇总表](#head91)
			- [3.4.1.5 流量域会话粒度页面浏览近 1 日汇总表](#head92)
			- [3.4.1.6 流量域访客页面粒度页面浏览近 1 日汇总表](#head93)
		- [3.4.2 近 n 日汇总表](#head94)
			- [3.4.2.1 交易域用户sku粒度下单近 n 日汇总表](#head95)
			- [3.4.2.2 交易域省份粒度下单近 n 日汇总表](#head96)
		- [3.4.3 历史至今汇总表](#head97)
			- [3.4.3.1 交易域用户粒度下单历史至今汇总表](#head98)
			- [3.4.3.2 用户域用户粒度登录历史至今汇总表](#head99)
	- [3.5 ads层建设](#head100)
		- [3.5.1 流量主题](#head101)
			- [3.5.1.1 各渠道流量统计](#head102)
			- [3.5.1.2 路径分析](#head103)
		- [3.5.2 用户主题](#head104)
			- [3.5.2.1 用户变动统计](#head105)
			- [3.5.2.2 用户留存率](#head106)
			- [3.5.2.3 用户行为漏斗分析](#head107)
		- [3.5.3 商品主题](#head108)
			- [3.5.3.1 最近30日各品牌复购率](#head109)
			- [3.5.3.2 各分类商品购物车存量Top3](#head110)
		- [3.5.4 交易主题](#head111)
			- [3.5.4.1 下单到支付时间间隔平均值](#head112)
			- [3.5.4.2 各省份交易统计](#head113)
# <span id="head1">1. 日志数据</span>
## <span id="head2">1.1 日志数据结构描述</span>
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
## <span id="head3">1.2 日志数据采集</span>
在日志服务器启动 flume，将日志数据采集进 kafka 的"topic_log"主题。再使用 flume 将日志数据从 kafka 采集进 hdfs。
# <span id="head4">2. 业务数据</span>
## <span id="head5">2.1 业务表描述</span>
本项目模拟的电商平台共设计了34张业务表。这34个表以订单表、用户表、SKU商品表、活动表和优惠券表为中心，延伸出了优惠券领用表、
支付流水表、活动订单表、订单详情表、订单状态表、商品评论表、编码字典表退单表、SPU商品表等，
用户表提供用户的详细信息，支付流水表提供该订单的支付详情，订单详情表提供订单的商品数量等情况，
商品表给订单详情表提供商品的详细信息。实际项目中，业务数据库中表格远远不止这些。
### <span id="head6">2.1.1 活动信息表（activity_info）</span>
|字段名 |字段说明 |
| :---- | :---- |
| id | 活动id |
|activity_name|活动名称|
|activity_type|	活动类型（1：满减，2：折扣）|
|activity_desc|	活动描述|
|start_time	|开始时间|
|end_time	|结束时间|
|create_time	|创建时间|
### <span id="head7">2.1.2 活动规则表（activity_rule）</span>
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
### <span id="head8">2.1.3 活动商品关联表（activity_sku）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|activity_id	|活动id |
|sku_id	|sku_id|
|create_time	|创建时间|
### <span id="head9">2.1.4 平台属性表（base_attr_info）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|attr_name	|属性名称|
|category_id	|分类id|
|category_level	|分类层级|
### <span id="head10">2.1.5 平台属性值表（base_attr_value）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|value_name	|属性值名称|
|attr_id	|属性id|
### <span id="head11">2.1.6 一级分类表（base_category1）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|name	|分类名称|
### <span id="head12">2.1.7 二级分类表（base_category2）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|name	|二级分类名称|
|category1_id	|一级分类编号|
### <span id="head13">2.1.8 三级分类表（base_category3）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|name	|三级分类名称|
|category2_id	|二级分类编号|
### <span id="head14">2.1.9 字典表（base_dic）</span>
|字段名	|字段说明|
| :---- | :---- |
|dic_code	|编号|
|dic_name	|编码名称|
|parent_code	|父编号|
|create_time	|创建日期|
|operate_time	|修改日期|
### <span id="head15">2.1.10 省份表（base_province）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|id|
|name	|省名称|
|region_id	|大区id|
|area_code	|行政区位码|
|iso_code	|国际编码|
|iso_3166_2	|ISO3166编码|
### <span id="head16">2.1.11 地区表（base_region）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|大区id|
|region_name	|大区名称|
### <span id="head17">2.1.12 品牌表（base_trademark）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|tm_name	|属性值|
|logo_url	|品牌logo的图片路径|
### <span id="head18">2.1.13 购物车表（cart_info）</span>
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
### <span id="head19">2.1.14 评价表（comment_info）</span>
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
### <span id="head20">2.1.15 优惠券信息表（coupon_info）</span>
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
### <span id="head21">2.1.16 优惠券优惠范围表（coupon_range）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|购物券编号|
|coupon_id	|优惠券id|
|range_type	|范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌|
|range_id	|范围id|
### <span id="head22">2.1.17 优惠券领用表（coupon_use）</span>
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
### <span id="head23">2.1.18 收藏表（favor_info）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|user_id	|用户id|
|sku_id	|skuid|
|spu_id	|商品id|
|is_cancel	|是否已取消 0 正常 1 已取消|
|create_time	|创建时间|
|cancel_time	|修改时间|
### <span id="head24">2.1.19 订单明细表（order_detail）</span>
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
### <span id="head25">2.1.20 订单明细活动关联表（order_detail_activity）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|order_id	|订单id|
|order_detail_id	|订单明细id|
|activity_id	|活动id|
|activity_rule_id	|活动规则|
|sku_id	|skuid|
|create_time	|获取时间|
### <span id="head26">2.1.21 订单明细优惠券关联表（order_detail_coupon）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|order_id	|订单id|
|order_detail_id	|订单明细id|
|coupon_id	|购物券id|
|coupon_use_id	|购物券领用id|
|sku_id	|skuid|
|create_time	|获取时间|
### <span id="head27">2.1.22 订单表(order_info）</span>
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
### <span id="head28">2.1.23 退单表（order_refund_info）</span>
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
### <span id="head29">2.1.24 订单状态流水表（order_status_log）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|order_id	|订单编号|
|order_status	|订单状态|
|operate_time	|操作时间|
### <span id="head30">2.1.25 支付表（payment_info）</span>
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
### <span id="head31">2.1.26 退款表（refund_payment）</span>
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
### <span id="head32">2.1.27 SKU平台属性表（sku_attr_value）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|attr_id	|属性id（冗余)|
|value_id	|属性值id|
|sku_id	|skuid|
|attr_name	|属性名称|
|value_name	|属性值名称|
### <span id="head33">2.1.28 SKU信息表（sku_info）</span>
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
### <span id="head34">2.1.29 SKU销售属性表（sku_sale_attr_value）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|id|
|sku_id	|库存单元id|
|spu_id	|spu_id（冗余）|
|sale_attr_value_id	|销售属性值id|
|sale_attr_id	|销售属性id|
|sale_attr_name	|销售属性值名称|
|sale_attr_value_name	|销售属性值名称|
### <span id="head35">2.1.30 SPU信息表（spu_info）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|商品id|
|spu_name	|商品名称|
|description	|商品描述(后台简述）|
|category3_id	|三级分类id|
|tm_id	|品牌id|
### <span id="head36">2.1.31 SPU销售属性表（spu_sale_attr）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号（业务中无关联）|
|spu_id	|商品id|
|base_sale_attr_id	|销售属性id|
|sale_attr_name	|销售属性名称（冗余）|
### <span id="head37">2.1.32 SPU销售属性值表（spu_sale_attr_value）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|销售属性值编号|
|spu_id	|商品id|
|base_sale_attr_id	|销售属性id|
|sale_attr_value_name	|销售属性值名称|
|sale_attr_name	|销售属性名称（冗余）|
### <span id="head38">2.1.33 用户地址表（user_address）</span>
|字段名	|字段说明|
| :---- | :---- |
|id	|编号|
|user_id	|用户id|
|province_id	|省份id|
|user_address	|用户地址|
|consignee	|收件人|
|phone_num	|联系方式|
|is_default	|是否是默认|
### <span id="head39">2.1.34 用户信息表（user_info）</span>
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
## <span id="head40">2.2 业务数据采集</span>
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
## <span id="head41">3. 离线数仓建设</span>
### <span id="head42">3.1 ods层建设</span>
ods 层的功能是对原始数据的备份，因此表结构与原始数据结构保持一致，同时由于 ods 层的数据量较大，因此采用压缩比较高的 gzip 压缩。
表命名格式为 ods_表名_增量/全量标识（inc/full）。
### <span id="head43">3.1.1 日志表</span>
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
### <span id="head44">3.1.2 活动信息表（全量表）</span>
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
### <span id="head45">3.1.3 活动规则表（全量表）</span>
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
### <span id="head46">3.1.4 一级品类表（全量表）</span>
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
### <span id="head47">3.1.5 二级品类表（全量表）</span>
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
### <span id="head48">3.1.6 三级品类表（全量表）</span>
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
### <span id="head49">3.1.7 编码字典表（全量表）</span>
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
### <span id="head50">3.1.8 省份表（全量表）</span>
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
### <span id="head51">3.1.9 地区表（全量表）</span>
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
### <span id="head52">3.1.10 品牌表（全量表）</span>
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
### <span id="head53">3.1.11 购物车表（全量表）</span>
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
### <span id="head54">3.1.12 优惠券信息表（全量表）</span>
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
### <span id="head55">3.1.13 商品平台属性表（全量表）</span>
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
### <span id="head56">3.1.14 商品表（全量表）</span>
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
### <span id="head57">3.1.15 商品销售属性值表（全量表）</span>
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
### <span id="head58">3.1.16 SPU表（全量表）</span>
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
### <span id="head59">3.1.17 购物车表（增量表）</span>
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
### <span id="head60">3.1.18 评论表（增量表）</span>
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
### <span id="head61">3.1.19 优惠券领用表（增量表）</span>
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
### <span id="head62">3.1.20 收藏表（增量表）</span>
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
### <span id="head63">3.1.21 订单明细表（增量表）</span>
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
### <span id="head64">3.1.22 订单明细活动关联表（增量表）</span>
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
### <span id="head65">3.1.23 订单明细优惠券关联表（增量表）</span>
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
### <span id="head66">3.1.24 订单表（增量表）</span>
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
### <span id="head67">3.1.25 退单表（增量表）</span>
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
### <span id="head68">3.1.26 订单状态流水表（增量表）</span>
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
### <span id="head69">3.1.27 支付表（增量表）</span>
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
### <span id="head70">3.1.28 退款表（增量表）</span>
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
### <span id="head71">3.1.29 用户表（增量表）</span>
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
## <span id="head72">3.2 dim层建设</span>
dim 层存储维度表，使用 orc 列式存储 + snappy 压缩，其中用户维度使用拉链表。命名规则为dim_表名_全量/拉链标识(full/zip)。
### <span id="head73">3.2.1 商品维度表</span>
建表语句：
```DROP TABLE IF EXISTS dim_sku_full;
   CREATE EXTERNAL TABLE dim_sku_full
   (
       `id`                   STRING COMMENT 'sku_id',
       `price`                DECIMAL(16, 2) COMMENT '商品价格',
       `sku_name`             STRING COMMENT '商品名称',
       `sku_desc`             STRING COMMENT '商品描述',
       `weight`               DECIMAL(16, 2) COMMENT '重量',
       `is_sale`              BOOLEAN COMMENT '是否在售',
       `spu_id`               STRING COMMENT 'spu编号',
       `spu_name`             STRING COMMENT 'spu名称',
       `category3_id`         STRING COMMENT '三级分类id',
       `category3_name`       STRING COMMENT '三级分类名称',
       `category2_id`         STRING COMMENT '二级分类id',
       `category2_name`       STRING COMMENT '二级分类名称',
       `category1_id`         STRING COMMENT '一级分类id',
       `category1_name`       STRING COMMENT '一级分类名称',
       `tm_id`                STRING COMMENT '品牌id',
       `tm_name`              STRING COMMENT '品牌名称',
       `sku_attr_values`      ARRAY<STRUCT<attr_id :STRING,value_id :STRING,attr_name :STRING,value_name:STRING>> COMMENT '平台属性',
       `sku_sale_attr_values` ARRAY<STRUCT<sale_attr_id :STRING,sale_attr_value_id :STRING,sale_attr_name :STRING,sale_attr_value_name:STRING>> COMMENT '销售属性',
       `create_time`          STRING COMMENT '创建时间'
   ) COMMENT '商品维度表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dim/dim_sku_full/'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```with
   sku as
   (
       select
           id,
           price,
           sku_name,
           sku_desc,
           weight,
           is_sale,
           spu_id,
           category3_id,
           tm_id,
           create_time
       from ods_sku_info_full
       where dt='2020-06-14'
   ),
   spu as
   (
       select
           id,
           spu_name
       from ods_spu_info_full
       where dt='2020-06-14'
   ),
   c3 as
   (
       select
           id,
           name,
           category2_id
       from ods_base_category3_full
       where dt='2020-06-14'
   ),
   c2 as
   (
       select
           id,
           name,
           category1_id
       from ods_base_category2_full
       where dt='2020-06-14'
   ),
   c1 as
   (
       select
           id,
           name
       from ods_base_category1_full
       where dt='2020-06-14'
   ),
   tm as
   (
       select
           id,
           tm_name
       from ods_base_trademark_full
       where dt='2020-06-14'
   ),
   attr as
   (
       select
           sku_id,
           collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
       from ods_sku_attr_value_full
       where dt='2020-06-14'
       group by sku_id
   ),
   sale_attr as
   (
       select
           sku_id,
           collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
       from ods_sku_sale_attr_value_full
       where dt='2020-06-14'
       group by sku_id
   )
   insert overwrite table dim_sku_full partition(dt='2020-06-14')
   select
       sku.id,
       sku.price,
       sku.sku_name,
       sku.sku_desc,
       sku.weight,
       sku.is_sale,
       sku.spu_id,
       spu.spu_name,
       sku.category3_id,
       c3.name,
       c3.category2_id,
       c2.name,
       c2.category1_id,
       c1.name,
       sku.tm_id,
       tm.tm_name,
       attr.attrs,
       sale_attr.sale_attrs,
       sku.create_time
   from sku
   left join spu on sku.spu_id=spu.id
   left join c3 on sku.category3_id=c3.id
   left join c2 on c3.category2_id=c2.id
   left join c1 on c2.category1_id=c1.id
   left join tm on sku.tm_id=tm.id
   left join attr on sku.id=attr.sku_id
   left join sale_attr on sku.id=sale_attr.sku_id;
```
### <span id="head74">3.2.2 优惠券维度表</span>
建表语句：
```DROP TABLE IF EXISTS dim_coupon_full;
   CREATE EXTERNAL TABLE dim_coupon_full
   (
       `id`               STRING COMMENT '购物券编号',
       `coupon_name`      STRING COMMENT '购物券名称',
       `coupon_type_code` STRING COMMENT '购物券类型编码',
       `coupon_type_name` STRING COMMENT '购物券类型名称',
       `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
       `condition_num`    BIGINT COMMENT '满件数',
       `activity_id`      STRING COMMENT '活动编号',
       `benefit_amount`   DECIMAL(16, 2) COMMENT '减金额',
       `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
       `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',
       `create_time`      STRING COMMENT '创建时间',
       `range_type_code`  STRING COMMENT '优惠范围类型编码',
       `range_type_name`  STRING COMMENT '优惠范围类型名称',
       `limit_num`        BIGINT COMMENT '最多领取次数',
       `taken_count`      BIGINT COMMENT '已领取次数',
       `start_time`       STRING COMMENT '可以领取的开始日期',
       `end_time`         STRING COMMENT '可以领取的结束日期',
       `operate_time`     STRING COMMENT '修改时间',
       `expire_time`      STRING COMMENT '过期时间'
   ) COMMENT '优惠券维度表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dim/dim_coupon_full/'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dim_coupon_full partition(dt='2020-06-14')
   select
       id,
       coupon_name,
       coupon_type,
       coupon_dic.dic_name,
       condition_amount,
       condition_num,
       activity_id,
       benefit_amount,
       benefit_discount,
       case coupon_type
           when '3201' then concat('满',condition_amount,'元减',benefit_amount,'元')
           when '3202' then concat('满',condition_num,'件打',10*(1-benefit_discount),'折')
           when '3203' then concat('减',benefit_amount,'元')
       end benefit_rule,
       create_time,
       range_type,
       range_dic.dic_name,
       limit_num,
       taken_count,
       start_time,
       end_time,
       operate_time,
       expire_time
   from
   (
       select
           id,
           coupon_name,
           coupon_type,
           condition_amount,
           condition_num,
           activity_id,
           benefit_amount,
           benefit_discount,
           create_time,
           range_type,
           limit_num,
           taken_count,
           start_time,
           end_time,
           operate_time,
           expire_time
       from ods_coupon_info_full
       where dt='2020-06-14'
   )ci
   left join
   (
       select
           dic_code,
           dic_name
       from ods_base_dic_full
       where dt='2020-06-14'
       and parent_code='32'
   )coupon_dic
   on ci.coupon_type=coupon_dic.dic_code
   left join
   (
       select
           dic_code,
           dic_name
       from ods_base_dic_full
       where dt='2020-06-14'
       and parent_code='33'
   )range_dic
   on ci.range_type=range_dic.dic_code;
```
### <span id="head75">3.2.3 活动维度表</span>
建表语句：
```DROP TABLE IF EXISTS dim_activity_full;
   CREATE EXTERNAL TABLE dim_activity_full
   (
       `activity_rule_id`   STRING COMMENT '活动规则ID',
       `activity_id`        STRING COMMENT '活动ID',
       `activity_name`      STRING COMMENT '活动名称',
       `activity_type_code` STRING COMMENT '活动类型编码',
       `activity_type_name` STRING COMMENT '活动类型名称',
       `activity_desc`      STRING COMMENT '活动描述',
       `start_time`         STRING COMMENT '开始时间',
       `end_time`           STRING COMMENT '结束时间',
       `create_time`        STRING COMMENT '创建时间',
       `condition_amount`   DECIMAL(16, 2) COMMENT '满减金额',
       `condition_num`      BIGINT COMMENT '满减件数',
       `benefit_amount`     DECIMAL(16, 2) COMMENT '优惠金额',
       `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣',
       `benefit_rule`       STRING COMMENT '优惠规则',
       `benefit_level`      STRING COMMENT '优惠级别'
   ) COMMENT '活动信息表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dim/dim_activity_full/'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dim_activity_full partition(dt='2020-06-14')
   select
       rule.id,
       info.id,
       activity_name,
       rule.activity_type,
       dic.dic_name,
       activity_desc,
       start_time,
       end_time,
       create_time,
       condition_amount,
       condition_num,
       benefit_amount,
       benefit_discount,
       case rule.activity_type
           when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
           when '3102' then concat('满',condition_num,'件打',10*(1-benefit_discount),'折')
           when '3103' then concat('打',10*(1-benefit_discount),'折')
       end benefit_rule,
       benefit_level
   from
   (
       select
           id,
           activity_id,
           activity_type,
           condition_amount,
           condition_num,
           benefit_amount,
           benefit_discount,
           benefit_level
       from ods_activity_rule_full
       where dt='2020-06-14'
   )rule
   left join
   (
       select
           id,
           activity_name,
           activity_type,
           activity_desc,
           start_time,
           end_time,
           create_time
       from ods_activity_info_full
       where dt='2020-06-14'
   )info
   on rule.activity_id=info.id
   left join
   (
       select
           dic_code,
           dic_name
       from ods_base_dic_full
       where dt='2020-06-14'
       and parent_code='31'
   )dic
   on rule.activity_type=dic.dic_code;
```
### <span id="head76">3.2.4 地区维度表</span>
建表语句：
```DROP TABLE IF EXISTS dim_province_full;
   CREATE EXTERNAL TABLE dim_province_full
   (
       `id`            STRING COMMENT 'id',
       `province_name` STRING COMMENT '省市名称',
       `area_code`     STRING COMMENT '地区编码',
       `iso_code`      STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',
       `iso_3166_2`    STRING COMMENT '新版IOS-3166-2编码，供可视化使用',
       `region_id`     STRING COMMENT '地区id',
       `region_name`   STRING COMMENT '地区名称'
   ) COMMENT '地区维度表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dim/dim_province_full/'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dim_province_full partition(dt='2020-06-14')
   select
       province.id,
       province.name,
       province.area_code,
       province.iso_code,
       province.iso_3166_2,
       region_id,
       region_name
   from
   (
       select
           id,
           name,
           region_id,
           area_code,
           iso_code,
           iso_3166_2
       from ods_base_province_full
       where dt='2020-06-14'
   )province
   left join
   (
       select
           id,
           region_name
       from ods_base_region_full
       where dt='2020-06-14'
   )region
   on province.region_id=region.id;
```
### <span id="head77">3.2.5 用户维度表</span>
用户维度表采用拉链表，使用"start_date"和"end_date"两个字段标识数据的生效时间和失效时间，至今有效的数据存在"9999-12-31"分区，
当日失效的数据存进当日对应的分区。数据装载的思路为：9999-12-31分区的数据 union 上今日新增及变化的数据，使用 row_number 函数开窗，
根据 uid 分组，创建时间降序排序，rn 值为 1 的是有效数据，写进 9999-12-31 分区，为 2 的是今日失效数据，写进今日对应的分区。  
建表语句：
```DROP TABLE IF EXISTS dim_user_zip;
   CREATE EXTERNAL TABLE dim_user_zip
   (
       `id`           STRING COMMENT '用户id',
       `login_name`   STRING COMMENT '用户名称',
       `nick_name`    STRING COMMENT '用户昵称',
       `name`         STRING COMMENT '用户姓名',
       `phone_num`    STRING COMMENT '手机号码',
       `email`        STRING COMMENT '邮箱',
       `user_level`   STRING COMMENT '用户等级',
       `birthday`     STRING COMMENT '生日',
       `gender`       STRING COMMENT '性别',
       `create_time`  STRING COMMENT '创建时间',
       `operate_time` STRING COMMENT '操作时间',
       `start_date`   STRING COMMENT '开始日期',
       `end_date`     STRING COMMENT '结束日期'
   ) COMMENT '用户表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dim/dim_user_zip/'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```set hive.exec.dynamic.partition.mode=nonstrict;
   insert overwrite table dim_user_zip partition(dt)
   select
       id,
       login_name,
       nick_name,
       name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       start_date,
       if(rn=2,date_sub('2020-06-15',1),end_date) end_date,
       if(rn=1,'9999-12-31',date_sub('2020-06-15',1)) dt
   from
   (
       select
           id,
           login_name,
           nick_name,
           name,
           phone_num,
           email,
           user_level,
           birthday,
           gender,
           create_time,
           operate_time,
           start_date,
           end_date,
           row_number() over (partition by id order by start_date desc) rn
       from
       (
           select
               id,
               login_name,
               nick_name,
               name,
               phone_num,
               email,
               user_level,
               birthday,
               gender,
               create_time,
               operate_time,
               start_date,
               end_date
           from dim_user_zip
           where dt='9999-12-31'
           union
           select
               id,
               login_name,
               nick_name,
               md5(name) name,
               md5(if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',phone_num,null)) phone_num,
               md5(if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',email,null)) email,
               user_level,
               birthday,
               gender,
               create_time,
               operate_time,
               '2020-06-15' start_date,
               '9999-12-31' end_date
           from
           (
               select
                   data.id,
                   data.login_name,
                   data.nick_name,
                   data.name,
                   data.phone_num,
                   data.email,
                   data.user_level,
                   data.birthday,
                   data.gender,
                   data.create_time,
                   data.operate_time,
                   row_number() over (partition by d d order by ts desc) rn
               from ods_user_info_inc
               where dt='2020-06-15'
           )t1
           where rn=1
       )t2
   )t3;
```
## <span id="head78">3.3 dwd层建设</span>
dwd 层存储事实表，粒度选择最细粒度，存储格式采用 orc 列式存储 + snappy 压缩，事实表命名规范为 dwd_数据域_表名_增量/全量/累积快照标识(inc/full/acc)。
除了事务事实表，我们还设计了购物车周期快照事实表和交易流程累积快照事实表。
### <span id="head79">3.3.1 交易域下单事务事实表</span>
建表语句：
```DROP TABLE IF EXISTS dwd_trade_order_detail_inc;
   CREATE EXTERNAL TABLE dwd_trade_order_detail_inc
   (
       `id`                    STRING COMMENT '编号',
       `order_id`              STRING COMMENT '订单id',
       `user_id`               STRING COMMENT '用户id',
       `sku_id`                STRING COMMENT '商品id',
       `province_id`           STRING COMMENT '省份id',
       `activity_id`           STRING COMMENT '参与活动规则id',
       `activity_rule_id`      STRING COMMENT '参与活动规则id',
       `coupon_id`             STRING COMMENT '使用优惠券id',
       `date_id`               STRING COMMENT '下单日期id',
       `create_time`           STRING COMMENT '下单时间',
       `source_id`             STRING COMMENT '来源编号',
       `source_type_code`      STRING COMMENT '来源类型编码',
       `source_type_name`      STRING COMMENT '来源类型名称',
       `sku_num`               BIGINT COMMENT '商品数量',
       `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
       `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
       `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
       `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
   ) COMMENT '交易域下单明细事务事实表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       STORED AS ORC
       LOCATION '/warehouse/gmall/dwd/dwd_trade_order_detail_inc/'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dwd_trade_order_detail_inc partition (dt='2020-06-15')
   select
       od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_id,
       create_time,
       source_id,
       source_type,
       dic_name,
       sku_num,
       split_original_amount,
       nvl(split_activity_amount,0.0),
       nvl(split_coupon_amount,0.0),
       split_total_amount
   from
   (
       select
           data.id,
           data.order_id,
           data.sku_id,
           date_format(data.create_time, 'yyyy-MM-dd') date_id,
           data.create_time,
           data.source_id,
           data.source_type,
           data.sku_num,
           data.sku_num * data.order_price split_original_amount,
           data.split_total_amount,
           data.split_activity_amount,
           data.split_coupon_amount
       from ods_order_detail_inc
       where dt = '2020-06-15'
       and type = 'insert'
   ) od
   left join
   (
       select
           data.id,
           data.user_id,
           data.province_id
       from ods_order_info_inc
       where dt = '2020-06-15'
       and type = 'insert'
   ) oi
   on od.order_id = oi.id
   left join
   (
       select
           data.order_detail_id,
           data.activity_id,
           data.activity_rule_id
       from ods_order_detail_activity_inc
       where dt = '2020-06-15'
       and type = 'insert'
   ) act
   on od.id = act.order_detail_id
   left join
   (
       select
           data.order_detail_id,
           data.coupon_id
       from ods_order_detail_coupon_inc
       where dt = '2020-06-15'
       and type = 'insert'
   ) cou
   on od.id = cou.order_detail_id
   left join
   (
       select
           dic_code,
           dic_name
       from ods_base_dic_full
       where dt='2020-06-15'
       and parent_code='24'
   )dic
   on od.source_type=dic.dic_code;
```
### <span id="head80">3.3.2 交易域支付成功事务事实表</span>
建表语句：
```DROP TABLE IF EXISTS dwd_trade_pay_detail_suc_inc;
   CREATE EXTERNAL TABLE dwd_trade_pay_detail_suc_inc
   (
       `id`                    STRING COMMENT '编号',
       `order_id`              STRING COMMENT '订单id',
       `user_id`               STRING COMMENT '用户id',
       `sku_id`                STRING COMMENT '商品id',
       `province_id`           STRING COMMENT '省份id',
       `activity_id`           STRING COMMENT '参与活动规则id',
       `activity_rule_id`      STRING COMMENT '参与活动规则id',
       `coupon_id`             STRING COMMENT '使用优惠券id',
       `payment_type_code`     STRING COMMENT '支付类型编码',
       `payment_type_name`     STRING COMMENT '支付类型名称',
       `date_id`               STRING COMMENT '支付日期id',
       `callback_time`         STRING COMMENT '支付成功时间',
       `source_id`             STRING COMMENT '来源编号',
       `source_type_code`      STRING COMMENT '来源类型编码',
       `source_type_name`      STRING COMMENT '来源类型名称',
       `sku_num`               BIGINT COMMENT '商品数量',
       `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
       `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
       `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
       `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
   ) COMMENT '交易域成功支付事务事实表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       STORED AS ORC
       LOCATION '/warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt='2020-06-15')
   select
       od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time,'yyyy-MM-dd') date_id,
       callback_time,
       source_id,
       source_type,
       src_dic.dic_name,
       sku_num,
       split_original_amount,
       nvl(split_activity_amount,0.0),
       nvl(split_coupon_amount,0.0),
       split_total_amount
   from
   (
       select
           data.id,
           data.order_id,
           data.sku_id,
           data.source_id,
           data.source_type,
           data.sku_num,
           data.sku_num * data.order_price split_original_amount,
           data.split_total_amount,
           data.split_activity_amount,
           data.split_coupon_amount
       from ods_order_detail_inc
       where dt = '2020-06-15'
   ) od
   join
   (
       select
           data.user_id,
           data.order_id,
           data.payment_type,
           data.callback_time
       from ods_payment_info_inc
       where dt='2020-06-15'
       and type='update'
       and array_contains(map_keys(old),'payment_status')
       and data.payment_status='1602'
   ) pi
   on od.order_id=pi.order_id
   left join
   (
       select
           data.id,
           data.province_id
       from ods_order_info_inc
       where (dt = '2020-06-15' or dt = date_add('2020-06-15',-1))
       and (type = 'insert' or type = 'bootstrap-insert')
   ) oi
   on od.order_id = oi.id
   left join
   (
       select
           data.order_detail_id,
           data.activity_id,
           data.activity_rule_id
       from ods_order_detail_activity_inc
       where (dt = '2020-06-15' or dt = date_add('2020-06-15',-1))
       and (type = 'insert' or type = 'bootstrap-insert')
   ) act
   on od.id = act.order_detail_id
   left join
   (
       select
           data.order_detail_id,
           data.coupon_id
       from ods_order_detail_coupon_inc
       where (dt = '2020-06-15' or dt = date_add('2020-06-15',-1))
       and (type = 'insert' or type = 'bootstrap-insert')
   ) cou
   on od.id = cou.order_detail_id
   left join
   (
       select
           dic_code,
           dic_name
       from ods_base_dic_full
       where dt='2020-06-15'
       and parent_code='11'
   ) pay_dic
   on pi.payment_type=pay_dic.dic_code
   left join
   (
       select
           dic_code,
           dic_name
       from ods_base_dic_full
       where dt='2020-06-15'
       and parent_code='24'
   ) src_dic
   on od.source_type=src_dic.dic_code;
```
### <span id="head81">3.3.3 交易域购物车周期快照事实表</span>
建表语句：
```DROP TABLE IF EXISTS dwd_trade_cart_full;
   CREATE EXTERNAL TABLE dwd_trade_cart_full
   (
       `id`       STRING COMMENT '编号',
       `user_id`  STRING COMMENT '用户id',
       `sku_id`   STRING COMMENT '商品id',
       `sku_name` STRING COMMENT '商品名称',
       `sku_num`  BIGINT COMMENT '加购物车件数'
   ) COMMENT '交易域购物车周期快照事实表'
       PARTITIONED BY (`dt` STRING)
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       STORED AS ORC
       LOCATION '/warehouse/gmall/dwd/dwd_trade_cart_full/'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dwd_trade_cart_full partition(dt='2020-06-14')
   select
       id,
       user_id,
       sku_id,
       sku_name,
       sku_num
   from ods_cart_info_full
   where dt='2020-06-14'
   and is_ordered='0';
```
### <span id="head82">3.3.4 交易域交易流程累积快照事实表</span>
累计快照事实表是基于一个业务流程中的多个关键业务过程联合处理而构建的事实表，通常具有多个日期字段，每个日期对应业务流程中的一个关键业务过程（里程碑）。
累积型快照事实表主要用于分析业务过程（里程碑）之间的时间间隔等需求，例如用户下单到支付的平均时间间隔，使用累积型快照事实表进行统计，
就能避免两个事务事实表的关联操作，从而变得十分简单高效。  
数据装载思路：9999-12-31分区存放至今未完成的订单，其他分区存放该日完成的订单。数据装载时，将9999-12-31分区数据 union 今日新增下单数据，得到的就是至今为止可能未完成的订单，再 left join 今日
支付的数据，left join 今日完成的订单数据，三个时间字段均有值说明订单今日完成，写入今日对应的分区，否则订单未完成，写入9999-12-31分区。  
建表语句：
```DROP TABLE IF EXISTS dwd_trade_trade_flow_acc;
   CREATE EXTERNAL TABLE dwd_trade_trade_flow_acc
   (
       `order_id`              STRING COMMENT '订单id',
       `user_id`               STRING COMMENT '用户id',
       `province_id`           STRING COMMENT '省份id',
       `order_date_id`         STRING COMMENT '下单日期id',
       `order_time`            STRING COMMENT '下单时间',
       `payment_date_id`       STRING COMMENT '支付日期id',
       `payment_time`          STRING COMMENT '支付时间',
       `finish_date_id`        STRING COMMENT '确认收货日期id',
       `finish_time`           STRING COMMENT '确认收货时间',
       `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始价格',
       `order_activity_amount` DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
       `order_coupon_amount`   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
       `order_total_amount`    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
       `payment_amount`        DECIMAL(16, 2) COMMENT '支付金额'
   ) COMMENT '交易域交易流程累积快照事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dwd/dwd_trade_trade_flow_acc/'
   TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dwd_trade_trade_flow_acc partition(dt)
   select
       oi.order_id,
       user_id,
       province_id,
       order_date_id,
       order_time,
       nvl(oi.payment_date_id,pi.payment_date_id),
       nvl(oi.payment_time,pi.payment_time),
       nvl(oi.finish_date_id,log.finish_date_id),
       nvl(oi.finish_time,log.finish_time),
       order_original_amount,
       order_activity_amount,
       order_coupon_amount,
       order_total_amount,
       nvl(oi.payment_amount,pi.payment_amount),
       nvl(nvl(oi.finish_time,log.finish_time),'9999-12-31')
   from
   (
       select
           order_id,
           user_id,
           province_id,
           order_date_id,
           order_time,
           payment_date_id,
           payment_time,
           finish_date_id,
           finish_time,
           order_original_amount,
           order_activity_amount,
           order_coupon_amount,
           order_total_amount,
           payment_amount
       from dwd_trade_trade_flow_acc
       where dt='9999-12-31'
       union all
       select
           data.id,
           data.user_id,
           data.province_id,
           date_format(data.create_time,'yyyy-MM-dd') order_date_id,
           data.create_time,
           null payment_date_id,
           null payment_time,
           null finish_date_id,
           null finish_time,
           data.original_total_amount,
           data.activity_reduce_amount,
           data.coupon_reduce_amount,
           data.total_amount,
           null payment_amount
       from ods_order_info_inc
       where dt='2020-06-15'
       and type='insert'
   )oi
   left join
   (
       select
           data.order_id,
           date_format(data.callback_time,'yyyy-MM-dd') payment_date_id,
           data.callback_time payment_time,
           data.total_amount payment_amount
       from ods_payment_info_inc
       where dt='2020-06-15'
       and type='update'
       and array_contains(map_keys(old),'payment_status')
       and data.payment_status='1602'
   )pi
   on oi.order_id=pi.order_id
   left join
   (
       select
           data.order_id,
           date_format(data.operate_time,'yyyy-MM-dd') finish_date_id,
           data.operate_time finish_time
       from ods_order_status_log_inc
       where dt='2020-06-15'
       and type='insert'
       and data.order_status='1004'
   )log
   on oi.order_id=log.order_id;
```
### <span id="head83">3.3.5 流量域页面浏览事务事实表</span>
建表语句：
```DROP TABLE IF EXISTS dwd_traffic_page_view_inc;
   CREATE EXTERNAL TABLE dwd_traffic_page_view_inc
   (
       `province_id`    STRING COMMENT '省份id',
       `brand`          STRING COMMENT '手机品牌',
       `channel`        STRING COMMENT '渠道',
       `is_new`         STRING COMMENT '是否首次启动',
       `model`          STRING COMMENT '手机型号',
       `mid_id`         STRING COMMENT '设备id',
       `operate_system` STRING COMMENT '操作系统',
       `user_id`        STRING COMMENT '会员id',
       `version_code`   STRING COMMENT 'app版本号',
       `page_item`      STRING COMMENT '目标id ',
       `page_item_type` STRING COMMENT '目标类型',
       `last_page_id`   STRING COMMENT '上页类型',
       `page_id`        STRING COMMENT '页面ID ',
       `source_type`    STRING COMMENT '来源类型',
       `date_id`        STRING COMMENT '日期id',
       `view_time`      STRING COMMENT '跳入时间',
       `session_id`     STRING COMMENT '所属会话id',
       `during_time`    BIGINT COMMENT '持续时间毫秒'
   ) COMMENT '页面日志表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dwd/dwd_traffic_page_view_inc'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```set hive.cbo.enable=false;
   insert overwrite table dwd_traffic_page_view_inc partition (dt='2020-06-14')
   select
       province_id,
       brand,
       channel,
       is_new,
       model,
       mid_id,
       operate_system,
       user_id,
       version_code,
       page_item,
       page_item_type,
       last_page_id,
       page_id,
       source_type,
       date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
       date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
       concat(mid_id,'-',last_value(session_start_point,true) over (partition by mid_id order by ts)) session_id,
       during_time
   from
   (
       select
           common.ar area_code,
           common.ba brand,
           common.ch channel,
           common.is_new is_new,
           common.md model,
           common.mid mid_id,
           common.os operate_system,
           common.uid user_id,
           common.vc version_code,
           page.during_time,
           page.item page_item,
           page.item_type page_item_type,
           page.last_page_id,
           page.page_id,
           page.source_type,
           ts,
           if(page.last_page_id is null,ts,null) session_start_point
       from ods_log_inc
       where dt='2020-06-14'
       and page is not null
   )log
   left join
   (
       select
           id province_id,
           area_code
       from ods_base_province_full
       where dt='2020-06-14'
   )bp
   on log.area_code=bp.area_code;
```
### <span id="head84">3.3.6 用户域用户注册事务事实表</span>
建表语句：
```DROP TABLE IF EXISTS dwd_user_register_inc;
   CREATE EXTERNAL TABLE dwd_user_register_inc
   (
       `user_id`        STRING COMMENT '用户ID',
       `date_id`        STRING COMMENT '日期ID',
       `create_time`    STRING COMMENT '注册时间',
       `channel`        STRING COMMENT '应用下载渠道',
       `province_id`    STRING COMMENT '省份id',
       `version_code`   STRING COMMENT '应用版本',
       `mid_id`         STRING COMMENT '设备id',
       `brand`          STRING COMMENT '设备品牌',
       `model`          STRING COMMENT '设备型号',
       `operate_system` STRING COMMENT '设备操作系统'
   ) COMMENT '用户域用户注册事务事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dwd/dwd_user_register_inc/'
       TBLPROPERTIES ("orc.compress" = "snappy");
```
数据装载：
```insert overwrite table dwd_user_register_inc partition(dt='2020-06-15')
   select
       ui.user_id,
       date_format(create_time,'yyyy-MM-dd') date_id,
       create_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
   from
   (
       select
           data.id user_id,
           data.create_time
       from ods_user_info_inc
       where dt='2020-06-15'
       and type='insert'
   )ui
   left join
   (
       select
           common.ar area_code,
           common.ba brand,
           common.ch channel,
           common.md model,
           common.mid mid_id,
           common.os operate_system,
           common.uid user_id,
           common.vc version_code
       from ods_log_inc
       where dt='2020-06-15'
       and page.page_id='register'
       and common.uid is not null
   )log
   on ui.user_id=log.user_id
   left join
   (
       select
           id province_id,
           area_code
       from ods_base_province_full
       where dt='2020-06-15'
   )bp
   on log.area_code=bp.area_code;
```
### <span id="head85">3.3.7 用户域用户登录事务事实表</span>
建表语句：
```DROP TABLE IF EXISTS dwd_user_login_inc;
   CREATE EXTERNAL TABLE dwd_user_login_inc
   (
       `user_id`        STRING COMMENT '用户ID',
       `date_id`        STRING COMMENT '日期ID',
       `login_time`     STRING COMMENT '登录时间',
       `channel`        STRING COMMENT '应用下载渠道',
       `province_id`    STRING COMMENT '省份id',
       `version_code`   STRING COMMENT '应用版本',
       `mid_id`         STRING COMMENT '设备id',
       `brand`          STRING COMMENT '设备品牌',
       `model`          STRING COMMENT '设备型号',
       `operate_system` STRING COMMENT '设备操作系统'
   ) COMMENT '用户域用户登录事务事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dwd/dwd_user_login_inc/'
       TBLPROPERTIES ("orc.compress" = "snappy");
```
数据装载：
```insert overwrite table dwd_user_login_inc partition(dt='2020-06-14')
   select
       user_id,
       date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
       date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') login_time,
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
   from
   (
       select
           user_id,
           channel,
           area_code,
           version_code,
           mid_id,
           brand,
           model,
           operate_system,
           ts
       from
       (
           select
               user_id,
               channel,
               area_code,
               version_code,
               mid_id,
               brand,
               model,
               operate_system,
               ts,
               row_number() over (partition by session_id order by ts) rn
           from
           (
               select
                   user_id,
                   channel,
                   area_code,
                   version_code,
                   mid_id,
                   brand,
                   model,
                   operate_system,
                   ts,
                   concat(mid_id,'-',last_value(session_start_point,true) over(partition by mid_id order by ts)) session_id
               from
               (
                   select
                       common.uid user_id,
                       common.ch channel,
                       common.ar area_code,
                       common.vc version_code,
                       common.mid mid_id,
                       common.ba brand,
                       common.md model,
                       common.os operate_system,
                       ts,
                       if(page.last_page_id is null,ts,null) session_start_point
                   from ods_log_inc
                   where dt='2020-06-14'
                   and page is not null
               )t1
           )t2
           where user_id is not null
       )t3
       where rn=1
   )t4
   left join
   (
       select
           id province_id,
           area_code
       from ods_base_province_full
       where dt='2020-06-14'
   )bp
   on t4.area_code=bp.area_code;
```
## <span id="head86">3.4 dws层建设</span>
dws 层存储汇总表，目的是在一张汇总表中统计业务过程相同、统计粒度相同、统计周期相同的多个派生指标，避免重复计算。
本层根据统计周期不同分为近 1 日汇总表，近 n 日汇总表和历史至今汇总表，汇总表命名规范为：dws_数据域_统计粒度_业务过程_统计周期(1d/nd/td)。
存储格式为 orc 列式存储 + snappy 压缩。
### <span id="head87">3.4.1 近 1 日汇总表</span>
#### <span id="head88">3.4.1.1 交易域用户sku粒度下单近 1 日汇总表</span>
统计各用户各 sku 近 1 日的下单次数、件数、总金额。    
建表语句：
```DROP TABLE IF EXISTS dws_trade_user_sku_order_1d;
   CREATE EXTERNAL TABLE dws_trade_user_sku_order_1d
   (
       `user_id`                   STRING COMMENT '用户id',
       `sku_id`                    STRING COMMENT 'sku_id',
       `sku_name`                  STRING COMMENT 'sku名称',
       `category1_id`              STRING COMMENT '一级分类id',
       `category1_name`            STRING COMMENT '一级分类名称',
       `category2_id`              STRING COMMENT '一级分类id',
       `category2_name`            STRING COMMENT '一级分类名称',
       `category3_id`              STRING COMMENT '一级分类id',
       `category3_name`            STRING COMMENT '一级分类名称',
       `tm_id`                     STRING COMMENT '品牌id',
       `tm_name`                   STRING COMMENT '品牌名称',
       `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
       `order_num_1d`              BIGINT COMMENT '最近1日下单件数',
       `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
       `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额',
       `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额',
       `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
   ) COMMENT '交易域用户商品粒度订单最近1日汇总事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_1d'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_trade_user_sku_order_1d partition(dt='2020-06-15')
   select
       user_id,
       id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       order_count,
       order_num,
       order_original_amount,
       activity_reduce_amount,
       coupon_reduce_amount,
       order_total_amount
   from
   (
       select
           user_id,
           sku_id,
           count(*) order_count,
           sum(sku_num) order_num,
           sum(split_original_amount) order_original_amount,
           sum(nvl(split_activity_amount,0)) activity_reduce_amount,
           sum(nvl(split_coupon_amount,0)) coupon_reduce_amount,
           sum(split_total_amount) order_total_amount
       from dwd_trade_order_detail_inc
       where dt='2020-06-15'
       group by user_id,sku_id
   )od
   left join
   (
       select
           id,
           sku_name,
           category1_id,
           category1_name,
           category2_id,
           category2_name,
           category3_id,
           category3_name,
           tm_id,
           tm_name
       from dim_sku_full
       where dt='2020-06-15'
   )sku
   on od.sku_id=sku.id;
```
#### <span id="head89">3.4.1.2 交易域用户粒度下单近 1 日汇总表</span>
统计各用户近 1 日的下单次数、件数和总金额。  
建表语句：
```DROP TABLE IF EXISTS dws_trade_user_order_1d;
   CREATE EXTERNAL TABLE dws_trade_user_order_1d
   (
       `user_id`                   STRING COMMENT '用户id',
       `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
       `order_num_1d`              BIGINT COMMENT '最近1日下单商品件数',
       `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日最近1日下单原始金额',
       `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
       `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '下单优惠券优惠金额',
       `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
   ) COMMENT '交易域用户粒度订单最近1日汇总事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_trade_user_order_1d'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_trade_user_order_1d partition(dt='2020-06-15')
   select
       user_id,
       count(distinct(order_id)),
       sum(sku_num),
       sum(split_original_amount),
       sum(nvl(split_activity_amount,0)),
       sum(nvl(split_coupon_amount,0)),
       sum(split_total_amount)
   from dwd_trade_order_detail_inc
   where dt='2020-06-15'
   group by user_id;
```
#### <span id="head90">3.4.1.3 交易域用户粒度支付近 1 日汇总表</span>
统计各用户近 1 日的支付次数、件数和总金额。  
建表语句：
```DROP TABLE IF EXISTS dws_trade_user_payment_1d;
   CREATE EXTERNAL TABLE dws_trade_user_payment_1d
   (
       `user_id`           STRING COMMENT '用户id',
       `payment_count_1d`  BIGINT COMMENT '最近1日支付次数',
       `payment_num_1d`    BIGINT COMMENT '最近1日支付商品件数',
       `payment_amount_1d` DECIMAL(16, 2) COMMENT '最近1日支付金额'
   ) COMMENT '交易域用户粒度支付最近1日汇总事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_trade_user_payment_1d'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_trade_user_payment_1d partition(dt='2020-06-15')
   select
       user_id,
       count(distinct(order_id)),
       sum(sku_num),
       sum(split_payment_amount)
   from dwd_trade_pay_detail_suc_inc
   where dt='2020-06-15'
   group by user_id;
```
#### <span id="head91">3.4.1.4 交易域省份粒度下单近 1 日汇总表</span>
统计各省份近 1 日的下单次数、总金额。  
建表语句：
```DROP TABLE IF EXISTS dws_trade_province_order_1d;
   CREATE EXTERNAL TABLE dws_trade_province_order_1d
   (
       `province_id`               STRING COMMENT '省份id',
       `province_name`             STRING COMMENT '省份名称',
       `area_code`                 STRING COMMENT '地区编码',
       `iso_code`                  STRING COMMENT '旧版ISO-3166-2编码',
       `iso_3166_2`                STRING COMMENT '新版版ISO-3166-2编码',
       `order_count_1d`            BIGINT COMMENT '最近1日下单次数',
       `order_original_amount_1d`  DECIMAL(16, 2) COMMENT '最近1日下单原始金额',
       `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单活动优惠金额',
       `coupon_reduce_amount_1d`   DECIMAL(16, 2) COMMENT '最近1日下单优惠券优惠金额',
       `order_total_amount_1d`     DECIMAL(16, 2) COMMENT '最近1日下单最终金额'
   ) COMMENT '交易域省份粒度订单最近1日汇总事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_trade_province_order_1d'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_trade_province_order_1d partition(dt='2020-06-15')
   select
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_original_amount_1d,
       activity_reduce_amount_1d,
       coupon_reduce_amount_1d,
       order_total_amount_1d
   from
   (
       select
           province_id,
           count(distinct(order_id)) order_count_1d,
           sum(split_original_amount) order_original_amount_1d,
           sum(nvl(split_activity_amount,0)) activity_reduce_amount_1d,
           sum(nvl(split_coupon_amount,0)) coupon_reduce_amount_1d,
           sum(split_total_amount) order_total_amount_1d
       from dwd_trade_order_detail_inc
       where dt='2020-06-15'
       group by province_id
   )o
   left join
   (
       select
           id,
           province_name,
           area_code,
           iso_code,
           iso_3166_2
       from dim_province_full
       where dt='2020-06-15'
   )p
   on o.province_id=p.id;
```
#### <span id="head92">3.4.1.5 流量域会话粒度页面浏览近 1 日汇总表</span>
统计各会话近 1 日的页面浏览数、浏览时长。  
建表语句：
```DROP TABLE IF EXISTS dws_traffic_session_page_view_1d;
   CREATE EXTERNAL TABLE dws_traffic_session_page_view_1d
   (
       `session_id`     STRING COMMENT '会话id',
       `mid_id`         string comment '设备id',
       `brand`          string comment '手机品牌',
       `model`          string comment '手机型号',
       `operate_system` string comment '操作系统',
       `version_code`   string comment 'app版本号',
       `channel`        string comment '渠道',
       `during_time_1d` BIGINT COMMENT '最近1日访问时长',
       `page_count_1d`  BIGINT COMMENT '最近1日访问页面数'
   ) COMMENT '流量域会话粒度页面浏览最近1日汇总表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_traffic_session_page_view_1d'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_traffic_session_page_view_1d partition(dt='2020-06-14')
   select
       session_id,
       mid_id,
       brand,
       model,
       operate_system,
       version_code,
       channel,
       sum(during_time),
       count(*)
   from dwd_traffic_page_view_inc
   where dt='2020-06-14'
   group by session_id,mid_id,brand,model,operate_system,version_code,channel;
```
#### <span id="head93">3.4.1.6 流量域访客页面粒度页面浏览近 1 日汇总表</span>
统计各访客各页面近 1 日的访问次数、浏览总时长。  
建表语句：
```DROP TABLE IF EXISTS dws_traffic_page_visitor_page_view_1d;
   CREATE EXTERNAL TABLE dws_traffic_page_visitor_page_view_1d
   (
       `mid_id`         STRING COMMENT '访客id',
       `brand`          string comment '手机品牌',
       `model`          string comment '手机型号',
       `operate_system` string comment '操作系统',
       `page_id`        STRING COMMENT '页面id',
       `during_time_1d` BIGINT COMMENT '最近1日浏览时长',
       `view_count_1d`  BIGINT COMMENT '最近1日访问次数'
   ) COMMENT '流量域访客页面粒度页面浏览最近1日汇总事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_traffic_page_visitor_page_view_1d'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_traffic_page_visitor_page_view_1d partition(dt='2020-06-14')
   select
       mid_id,
       brand,
       model,
       operate_system,
       page_id,
       sum(during_time),
       count(*)
   from dwd_traffic_page_view_inc
   where dt='2020-06-14'
   group by mid_id,brand,model,operate_system,page_id;
```
### <span id="head94">3.4.2 近 n 日汇总表</span>
#### <span id="head95">3.4.2.1 交易域用户sku粒度下单近 n 日汇总表</span>
统计各用户各 sku 近 7 天和近 30 天的下单次数、件数、总金额。数据装载时，直接从 dws 层交易域用户 sku 粒度下单近 1 日汇总表中取近 30 天的下单汇总数据，再进行近 7 日汇总
和近 30 日的汇总。  
建表语句：
```DROP TABLE IF EXISTS dws_trade_user_sku_order_nd;
   CREATE EXTERNAL TABLE dws_trade_user_sku_order_nd
   (
       `user_id`                    STRING COMMENT '用户id',
       `sku_id`                     STRING COMMENT 'sku_id',
       `sku_name`                   STRING COMMENT 'sku名称',
       `category1_id`               STRING COMMENT '一级分类id',
       `category1_name`             STRING COMMENT '一级分类名称',
       `category2_id`               STRING COMMENT '一级分类id',
       `category2_name`             STRING COMMENT '一级分类名称',
       `category3_id`               STRING COMMENT '一级分类id',
       `category3_name`             STRING COMMENT '一级分类名称',
       `tm_id`                      STRING COMMENT '品牌id',
       `tm_name`                    STRING COMMENT '品牌名称',
       `order_count_7d`             STRING COMMENT '最近7日下单次数',
       `order_num_7d`               BIGINT COMMENT '最近7日下单件数',
       `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
       `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日活动优惠金额',
       `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额',
       `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
       `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
       `order_num_30d`              BIGINT COMMENT '最近30日下单件数',
       `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
       `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额',
       `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额',
       `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
   ) COMMENT '交易域用户商品粒度订单最近n日汇总事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_trade_user_sku_order_nd'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_trade_user_sku_order_nd partition(dt='2020-06-14')
   select
       user_id,
       sku_id,
       sku_name,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       tm_id,
       tm_name,
       sum(if(dt>=date_add('2020-06-14',-6),order_count_1d,0)),
       sum(if(dt>=date_add('2020-06-14',-6),order_num_1d,0)),
       sum(if(dt>=date_add('2020-06-14',-6),order_original_amount_1d,0)),
       sum(if(dt>=date_add('2020-06-14',-6),activity_reduce_amount_1d,0)),
       sum(if(dt>=date_add('2020-06-14',-6),coupon_reduce_amount_1d,0)),
       sum(if(dt>=date_add('2020-06-14',-6),order_total_amount_1d,0)),
       sum(order_count_1d),
       sum(order_num_1d),
       sum(order_original_amount_1d),
       sum(activity_reduce_amount_1d),
       sum(coupon_reduce_amount_1d),
       sum(order_total_amount_1d)
   from dws_trade_user_sku_order_1d
   where dt>=date_add('2020-06-14',-29)
   group by  user_id,sku_id,sku_name,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name,tm_id,tm_name;
```
#### <span id="head96">3.4.2.2 交易域省份粒度下单近 n 日汇总表</span>
统计各省份近 7 日、30 日下单次数和总金额。数据装载时从 dws 层交易域省份粒度下单近 1 日汇总表取近 30 天数据，再进行近 7 日汇总和近 30 日汇总。  
建表语句：
```DROP TABLE IF EXISTS dws_trade_province_order_nd;
   CREATE EXTERNAL TABLE dws_trade_province_order_nd
   (
       `province_id`                STRING COMMENT '省份id',
       `province_name`              STRING COMMENT '省份名称',
       `area_code`                  STRING COMMENT '地区编码',
       `iso_code`                   STRING COMMENT '旧版ISO-3166-2编码',
       `iso_3166_2`                 STRING COMMENT '新版版ISO-3166-2编码',
       `order_count_7d`             BIGINT COMMENT '最近7日下单次数',
       `order_original_amount_7d`   DECIMAL(16, 2) COMMENT '最近7日下单原始金额',
       `activity_reduce_amount_7d`  DECIMAL(16, 2) COMMENT '最近7日下单活动优惠金额',
       `coupon_reduce_amount_7d`    DECIMAL(16, 2) COMMENT '最近7日下单优惠券优惠金额',
       `order_total_amount_7d`      DECIMAL(16, 2) COMMENT '最近7日下单最终金额',
       `order_count_30d`            BIGINT COMMENT '最近30日下单次数',
       `order_original_amount_30d`  DECIMAL(16, 2) COMMENT '最近30日下单原始金额',
       `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单活动优惠金额',
       `coupon_reduce_amount_30d`   DECIMAL(16, 2) COMMENT '最近30日下单优惠券优惠金额',
       `order_total_amount_30d`     DECIMAL(16, 2) COMMENT '最近30日下单最终金额'
   ) COMMENT '交易域省份粒度订单最近n日汇总事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_trade_province_order_nd'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_trade_province_order_nd partition(dt='2020-06-14')
   select
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       sum(if(dt>=date_add('2020-06-14',-6),order_count_1d,0)),
       sum(if(dt>=date_add('2020-06-14',-6),order_original_amount_1d,0)),
       sum(if(dt>=date_add('2020-06-14',-6),activity_reduce_amount_1d,0)),
       sum(if(dt>=date_add('2020-06-14',-6),coupon_reduce_amount_1d,0)),
       sum(if(dt>=date_add('2020-06-14',-6),order_total_amount_1d,0)),
       sum(order_count_1d),
       sum(order_original_amount_1d),
       sum(activity_reduce_amount_1d),
       sum(coupon_reduce_amount_1d),
       sum(order_total_amount_1d)
   from dws_trade_province_order_1d
   where dt>=date_add('2020-06-14',-29)
   and dt<='2020-06-14'
   group by province_id,province_name,area_code,iso_code,iso_3166_2;
```
### <span id="head97">3.4.3 历史至今汇总表</span>
#### <span id="head98">3.4.3.1 交易域用户粒度下单历史至今汇总表</span>
统计各用户首次下单日期、末次下单日期、累计下单次数、购买商品件数、总金额。  数据装载思路：从 dws 层交易域用户粒度下单历史至今汇总表取昨天的数据，
得到截止到昨天各统计指标的结果（old 表）， full join dws层交易域用户粒度下单近 1 日汇总表今日的数据（new 表）。若 old.user_id is not null，
那么该用户之前下过单，首次下单日期是 old 表记录的首次下单日期，否则首次下单日期是今天。若 new.user_id is not null，说明该用户今天下过单，
末次下单日期是今天，否则末次下单日期为 new 表记录的末次下单日期。  
建表语句：
```DROP TABLE IF EXISTS dws_trade_user_order_td;
   CREATE EXTERNAL TABLE dws_trade_user_order_td
   (
       `user_id`                   STRING COMMENT '用户id',
       `order_date_first`          STRING COMMENT '首次下单日期',
       `order_date_last`           STRING COMMENT '末次下单日期',
       `order_count_td`            BIGINT COMMENT '下单次数',
       `order_num_td`              BIGINT COMMENT '购买商品件数',
       `original_amount_td`        DECIMAL(16, 2) COMMENT '原始金额',
       `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT '活动优惠金额',
       `coupon_reduce_amount_td`   DECIMAL(16, 2) COMMENT '优惠券优惠金额',
       `total_amount_td`           DECIMAL(16, 2) COMMENT '最终金额'
   ) COMMENT '交易域用户粒度订单历史至今汇总事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_trade_user_order_td'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_trade_user_order_td partition(dt='2020-06-15')
   select
       nvl(old.user_id,new.user_id),
       if(old.user_id is not null,old.order_date_first,'2020-06-15'),
       if(new.user_id is not null,'2020-06-15',old.order_date_last),
       nvl(old.order_count_td,0)+nvl(new.order_count_1d,0),
       nvl(old.order_num_td,0)+nvl(new.order_num_1d,0),
       nvl(old.original_amount_td,0)+nvl(new.order_original_amount_1d,0),
       nvl(old.activity_reduce_amount_td,0)+nvl(new.activity_reduce_amount_1d,0),
       nvl(old.coupon_reduce_amount_td,0)+nvl(new.coupon_reduce_amount_1d,0),
       nvl(old.total_amount_td,0)+nvl(new.order_total_amount_1d,0)
   from
   (
       select
           user_id,
           order_date_first,
           order_date_last,
           order_count_td,
           order_num_td,
           original_amount_td,
           activity_reduce_amount_td,
           coupon_reduce_amount_td,
           total_amount_td
       from dws_trade_user_order_td
       where dt=date_add('2020-06-15',-1)
   )old
   full outer join
   (
       select
           user_id,
           order_count_1d,
           order_num_1d,
           order_original_amount_1d,
           activity_reduce_amount_1d,
           coupon_reduce_amount_1d,
           order_total_amount_1d
       from dws_trade_user_order_1d
       where dt='2020-06-15'
   )new
   on old.user_id=new.user_id;
```
#### <span id="head99">3.4.3.2 用户域用户粒度登录历史至今汇总表</span>
统计各用户末次登录日期，累计登录次数。  数据装载思路：从 dws 层用户域用户粒度登录历史至今汇总表取昨天的数据（old 表），full join dwd 层
用户登录事务事实表今日的数据（要先做一次汇总，new 表），若 new.user_id is null，该用户今天没有登录，末次登录日期是 old 表记录的末次登录
日期，否则末次登录日期是今天。  
建表语句：
```DROP TABLE IF EXISTS dws_user_user_login_td;
   CREATE EXTERNAL TABLE dws_user_user_login_td
   (
       `user_id`         STRING COMMENT '用户id',
       `login_date_last` STRING COMMENT '末次登录日期',
       `login_count_td`  BIGINT COMMENT '累计登录次数'
   ) COMMENT '用户域用户粒度登录历史至今汇总事实表'
       PARTITIONED BY (`dt` STRING)
       STORED AS ORC
       LOCATION '/warehouse/gmall/dws/dws_user_user_login_td'
       TBLPROPERTIES ('orc.compress' = 'snappy');
```
数据装载：
```insert overwrite table dws_user_user_login_td partition(dt='2020-06-15')
   select
       nvl(old.user_id,new.user_id),
       if(new.user_id is null,old.login_date_last,'2020-06-15'),
       nvl(old.login_count_td,0)+nvl(new.login_count_1d,0)
   from
   (
       select
           user_id,
           login_date_last,
           login_count_td
       from dws_user_user_login_td
       where dt=date_add('2020-06-15',-1)
   )old
   full outer join
   (
       select
           user_id,
           count(*) login_count_1d
       from dwd_user_login_inc
       where dt='2020-06-15'
       group by user_id
   )new
   on old.user_id=new.user_id;
```
## <span id="head100">3.5 ads层建设</span>
ads 层存放我们最终的统计指标，不分区，使用一个时间字段标识是哪一天的统计结果，不压缩。由于从 hive 查询比较慢，一般 ads 层数据装载完成后，会使用 dataX 等数据同步
工具将应用层的数据同步到 MySQL 等关系型数据库，方便查询。
### <span id="head101">3.5.1 流量主题</span>
#### <span id="head102">3.5.1.1 各渠道流量统计</span>
统计近 1/7/30 日各渠道访客数、会话数、各会话平均访问时长、各会话平均浏览页面数、跳出率。跳出指的是会话只访问了一个页面。  数据装载思路：
从 dws 层流量域会话粒度页面浏览近 1 日汇总表取数据，再根据 1/7/30 天汇总统计。  
建表语句：
```DROP TABLE IF EXISTS ads_traffic_stats_by_channel;
   CREATE EXTERNAL TABLE ads_traffic_stats_by_channel
   (
       `dt`               STRING COMMENT '统计日期',
       `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
       `channel`          STRING COMMENT '渠道',
       `uv_count`         BIGINT COMMENT '访客人数',
       `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
       `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
       `sv_count`         BIGINT COMMENT '会话数',
       `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
   ) COMMENT '各渠道流量统计'
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       LOCATION '/warehouse/gmall/ads/ads_traffic_stats_by_channel/';
```
数据装载：
```insert overwrite table ads_traffic_stats_by_channel
   select * from ads_traffic_stats_by_channel
   union
   select
       '2020-06-14' dt,
       recent_days,
       channel,
       cast(count(distinct(mid_id)) as bigint) uv_count,
       cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
       cast(avg(page_count_1d) as bigint) avg_page_count,
       cast(count(*) as bigint) sv_count,
       cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
   from dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
   where dt>=date_add('2020-06-14',-recent_days+1)
   group by recent_days,channel;
```
#### <span id="head103">3.5.1.2 路径分析</span>
统计从 source 页面跳到 target 页面的跳转次数，页面格式为"step-n:page_id"，n 为该会话的第几跳。  数据装载思路：首先从 dwd 层页面浏览事务事实表
计算出本页面 id 和下一跳页面 id 及该页面在会话中是第几跳，再进行拼接得到 source 和 target，最后统计 source 到 target 的总次数。  
建表语句：
```DROP TABLE IF EXISTS ads_page_path;
   CREATE EXTERNAL TABLE ads_page_path
   (
       `dt`          STRING COMMENT '统计日期',
       `source`      STRING COMMENT '跳转起始页面ID',
       `target`      STRING COMMENT '跳转终到页面ID',
       `path_count`  BIGINT COMMENT '跳转次数'
   ) COMMENT '页面浏览路径分析'
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       LOCATION '/warehouse/gmall/ads/ads_page_path/';
   DROP TABLE IF EXISTS ads_user_change;
```
数据装载：
```insert overwrite table ads_page_path
   select * from ads_page_path
   union
   select
       '2020-06-14' dt,
       source,
       nvl(target,'null'),
       count(*) path_count
   from
   (
       select
           concat('step-',rn,':',page_id) source,
           concat('step-',rn+1,':',next_page_id) target
       from
       (
           select
               page_id,
               lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
               row_number() over (partition by session_id order by view_time) rn
           from dwd_traffic_page_view_inc
           where dt='2020-06-14'
       )t1
   )t2
   group by source,target;
```
### <span id="head104">3.5.2 用户主题</span>
#### <span id="head105">3.5.2.1 用户变动统计</span>
统计流失用户数和回流用户数。流失用户指该用户 7 日前登录，但近 7 日未登录；回流用户指该用户曾流失过，但今日又登录了。  
数据装载思路：从 dws 层用户域用户粒度登录历史至今汇总表计算末次登录日期是 7 天前的用户数，就是流失用户数。
从 dws 层用户域用户粒度登录历史至今汇总表取昨天的数据和今日的数据，求两个表记录的末次登录日期相差超过 8 天的用户数，就是
回流用户数。  
建表语句：
```DROP TABLE IF EXISTS ads_user_change;
   CREATE EXTERNAL TABLE ads_user_change
   (
       `dt`               STRING COMMENT '统计日期',
       `user_churn_count` BIGINT COMMENT '流失用户数',
       `user_back_count`  BIGINT COMMENT '回流用户数'
   ) COMMENT '用户变动统计'
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       LOCATION '/warehouse/gmall/ads/ads_user_change/';
```
数据装载：
```insert overwrite table ads_user_change
   select * from ads_user_change
   union
   select
       churn.dt,
       user_churn_count,
       user_back_count
   from
   (
       select
           '2020-06-14' dt,
           count(*) user_churn_count
       from dws_user_user_login_td
       where dt='2020-06-14'
       and login_date_last=date_add('2020-06-14',-7)
   )churn
   join
   (
       select
           '2020-06-14' dt,
           count(*) user_back_count
       from
       (
           select
               user_id,
               login_date_last
           from dws_user_user_login_td
           where dt='2020-06-14'
       )t1
       join
       (
           select
               user_id,
               login_date_last login_date_previous
           from dws_user_user_login_td
           where dt=date_add('2020-06-14',-1) // 昨天统计的最后一次登录日期
       )t2
       on t1.user_id=t2.user_id
       where datediff(login_date_last,login_date_previous)>=8
   )back
   on churn.dt=back.dt;
```
#### <span id="head106">3.5.2.2 用户留存率</span>
统计 1-7 天的用户留存和新增用户数。n 日留存指的是 n 天前注册的用户中，今天登录过的用户数。  
数据装载思路：从 dwd 层用户域用户注册事务事实表取 1-7 天前的用户注册数据，从 dws 层取用户域用户粒度登录历史至今汇总表，两者 join，
末次登录日期是今天的是留存用户。  
建表语句：
```DROP TABLE IF EXISTS ads_user_retention;
   CREATE EXTERNAL TABLE ads_user_retention
   (
       `dt`              STRING COMMENT '统计日期',
       `create_date`     STRING COMMENT '用户新增日期',
       `retention_day`   INT COMMENT '截至当前日期留存天数',
       `retention_count` BIGINT COMMENT '留存用户数量',
       `new_user_count`  BIGINT COMMENT '新增用户数量',
       `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
   ) COMMENT '用户留存率'
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       LOCATION '/warehouse/gmall/ads/ads_user_retention/';
```
数据装载：
```insert overwrite table ads_user_retention
   select * from ads_user_retention
   union
   select
       '2020-06-14' dt,
       login_date_first create_date,
       datediff('2020-06-14',login_date_first) retention_day,
       sum(if(login_date_last='2020-06-14',1,0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last='2020-06-14',1,0))/count(*)*100 as decimal(16,2)) retention_rate
   from
   (
       select
           user_id,
           date_id login_date_first
       from dwd_user_register_inc
       where dt>=date_add('2020-06-14',-7)
       and dt<'2020-06-14'
   )t1
   join
   (
       select
           user_id,
           login_date_last
       from dws_user_user_login_td
       where dt='2020-06-14'
   )t2
   on t1.user_id=t2.user_id
   group by login_date_first;
```
#### <span id="head107">3.5.2.3 用户行为漏斗分析</span>
漏斗分析是一个数据分析模型，它能够科学反映一个业务流程从起点到终点各阶段用户转化情况。由于其能将各阶段环节都展示出来，
故哪个阶段存在问题，就能一目了然。该需求要求统计最近 1 日首页浏览人数、商品详情页浏览人数、下单人数、支付人数。  
数据装载思路：首页浏览人数、商品详情页浏览人数从 dws 层流量域访客页面粒度页面浏览汇总表统计得到，下单人数从 dws 层交易域用户粒度下单汇总表得到，
支付人数从 dws 层交易域用户粒度支付汇总表得到。  
建表语句：
```DROP TABLE IF EXISTS ads_user_action;
   CREATE EXTERNAL TABLE ads_user_action
   (
       `dt`                STRING COMMENT '统计日期',
       `home_count`        BIGINT COMMENT '浏览首页人数',
       `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
       `cart_count`        BIGINT COMMENT '加入购物车人数',
       `order_count`       BIGINT COMMENT '下单人数',
       `payment_count`     BIGINT COMMENT '支付人数'
   ) COMMENT '漏斗分析'
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       LOCATION '/warehouse/gmall/ads/ads_user_action/';
```
数据装载：
```insert overwrite table ads_user_action
   select * from ads_user_action
   union
   select
       '2020-06-14' dt,
       home_count,
       good_detail_count,
       cart_count,
       order_count,
       payment_count
   from
   (
       select
           1 recent_days,
           sum(if(page_id='home',1,0)) home_count,
           sum(if(page_id='good_detail',1,0)) good_detail_count
       from dws_traffic_page_visitor_page_view_1d
       where dt='2020-06-14'
       and page_id in ('home','good_detail')
   )page
   join
   (
       select
           1 recent_days,
           count(*) cart_count
       from dws_trade_user_cart_add_1d
       where dt='2020-06-14'
   )cart
   on page.recent_days=cart.recent_days
   join
   (
       select
           1 recent_days,
           count(*) order_count
       from dws_trade_user_order_1d
       where dt='2020-06-14'
   )ord
   on page.recent_days=ord.recent_days
   join
   (
       select
           1 recent_days,
           count(*) payment_count
       from dws_trade_user_payment_1d
       where dt='2020-06-14'
   )pay
   on page.recent_days=pay.recent_days;
```
### <span id="head108">3.5.3 商品主题</span>
#### <span id="head109">3.5.3.1 最近30日各品牌复购率</span>
统计近 30 日各品牌的复购率。从 dws 层用户 sku 粒度下单汇总表得到（先做一次聚合），下单次数大于等于 2 的就是复购用户。  
建表语句：
```DROP TABLE IF EXISTS ads_repeat_purchase_by_tm;
   CREATE EXTERNAL TABLE ads_repeat_purchase_by_tm
   (
       `dt`                STRING COMMENT '统计日期',
       `recent_days`       BIGINT COMMENT '最近天数,30:最近30天',
       `tm_id`             STRING COMMENT '品牌ID',
       `tm_name`           STRING COMMENT '品牌名称',
       `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
   ) COMMENT '各品牌复购率统计'
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       LOCATION '/warehouse/gmall/ads/ads_repeat_purchase_by_tm/';
```
数据装载：
```insert overwrite table ads_repeat_purchase_by_tm
   select * from ads_repeat_purchase_by_tm
   union
   select
       '2020-06-14',
       30,
       tm_id,
       tm_name,
       cast(sum(if(order_count>=2,1,0))/count(*) as decimal(16,2))
   from
   (
       select
           user_id,
           tm_id,
           tm_name,
           sum(order_count_30d) order_count
       from dws_trade_user_sku_order_nd
       where dt='2020-06-14'
       group by user_id, tm_id,tm_name
   )t1
   group by tm_id,tm_name;
```
#### <span id="head110">3.5.3.2 各分类商品购物车存量Top3</span>
统计各分类商品购物车存量 top3。从购物车周期快照事实表统计得到，根据品类分组，按件数降序排列取前三。  
建表语句：
```DROP TABLE IF EXISTS ads_sku_cart_num_top3_by_cate;
   CREATE EXTERNAL TABLE ads_sku_cart_num_top3_by_cate
   (
       `dt`             STRING COMMENT '统计日期',
       `category1_id`   STRING COMMENT '一级分类ID',
       `category1_name` STRING COMMENT '一级分类名称',
       `category2_id`   STRING COMMENT '二级分类ID',
       `category2_name` STRING COMMENT '二级分类名称',
       `category3_id`   STRING COMMENT '三级分类ID',
       `category3_name` STRING COMMENT '三级分类名称',
       `sku_id`         STRING COMMENT '商品id',
       `sku_name`       STRING COMMENT '商品名称',
       `cart_num`       BIGINT COMMENT '购物车中商品数量',
       `rk`             BIGINT COMMENT '排名'
   ) COMMENT '各分类商品购物车存量Top10'
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       LOCATION '/warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate/';
```
数据装载：
```insert overwrite table ads_sku_cart_num_top3_by_cate
   select * from ads_sku_cart_num_top3_by_cate
   union
   select
       '2020-06-14' dt,
       category1_id,
       category1_name,
       category2_id,
       category2_name,
       category3_id,
       category3_name,
       sku_id,
       sku_name,
       cart_num,
       rk
   from
   (
       select
           sku_id,
           sku_name,
           category1_id,
           category1_name,
           category2_id,
           category2_name,
           category3_id,
           category3_name,
           cart_num,
           rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
       from
       (
           select
               sku_id,
               sum(sku_num) cart_num
           from dwd_trade_cart_full
           where dt='2020-06-14'
           group by sku_id
       )cart
       join
       (
           select
               id,
               sku_name,
               category1_id,
               category1_name,
               category2_id,
               category2_name,
               category3_id,
               category3_name
           from dim_sku_full
           where dt='2020-06-14'
       )sku
       on cart.sku_id=sku.id
   )t1
   where rk<=3;
```
### <span id="head111">3.5.4 交易主题</span>
#### <span id="head112">3.5.4.1 下单到支付时间间隔平均值</span>
统计最近1日完成支付的订单的下单时间到支付时间的平均时间间隔。从 dwd 层交易流程累积快照事实表统计得到。  
建表语句：
```DROP TABLE IF EXISTS ads_order_to_pay_interval_avg;
   CREATE EXTERNAL TABLE ads_order_to_pay_interval_avg
   (
       `dt`                        STRING COMMENT '统计日期',
       `order_to_pay_interval_avg` BIGINT COMMENT '下单到支付时间间隔平均值,单位为秒'
   ) COMMENT '各品牌商品收藏次数Top3'
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       LOCATION '/warehouse/gmall/ads/ads_order_to_pay_interval_avg/';
```
数据装载：
```insert overwrite table ads_order_to_pay_interval_avg
   select * from ads_order_to_pay_interval_avg
   union
   select
       '2020-06-14',
       cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
   from dwd_trade_trade_flow_acc
   where dt in ('9999-12-31','2020-06-14')
   and payment_date_id='2020-06-14';
```
#### <span id="head113">3.5.4.2 各省份交易统计</span>
统计各省份近 1/7/30 日的订单数和订单金额。  
建表语句：
```DROP TABLE IF EXISTS ads_order_by_province;
   CREATE EXTERNAL TABLE ads_order_by_province
   (
       `dt`                 STRING COMMENT '统计日期',
       `recent_days`        BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
       `province_id`        STRING COMMENT '省份ID',
       `province_name`      STRING COMMENT '省份名称',
       `area_code`          STRING COMMENT '地区编码',
       `iso_code`           STRING COMMENT '国际标准地区编码',
       `iso_code_3166_2`    STRING COMMENT '国际标准地区编码',
       `order_count`        BIGINT COMMENT '订单数',
       `order_total_amount` DECIMAL(16, 2) COMMENT '订单金额'
   ) COMMENT '各地区订单统计'
       ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
       LOCATION '/warehouse/gmall/ads/ads_order_by_province/';
    ```
数据装载：
```insert overwrite table ads_order_by_province
   select * from ads_order_by_province
   union
   select
       '2020-06-14' dt,
       1 recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       order_count_1d,
       order_total_amount_1d
   from dws_trade_province_order_1d
   where dt='2020-06-14'
   union
   select
       '2020-06-14' dt,
       recent_days,
       province_id,
       province_name,
       area_code,
       iso_code,
       iso_3166_2,
       case recent_days
           when 7 then order_count_7d
           when 30 then order_count_30d
       end order_count,
       case recent_days
           when 7 then order_total_amount_7d
           when 30 then order_total_amount_30d
       end order_total_amount
   from dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
   where dt='2020-06-14';
```