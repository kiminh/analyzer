# README

## 基本介绍
这里存放：
1. ocpc的通用代码模板
2. 基于ocpc的通用代码做的升级代码
3. 通用化基础数据表

## data目录
存放ocpc的一部分新版的基础数据表，以前开发的基础数据表（尤其是转化数据）集成度太高，不利于后期取数口径调整后的代码调整个，因此，新版的基础数据表将做一下两个方面：
1. 尽可能集成ocpc需要的unionlog字段
2. 在降低集成度的情况下严格按照正确的取数口径重新集成转化标签字段

## model目录
用于计算k值和产出pb文件的代码文件

## model_v2目录
升级版ocpc通用代码，利用ocpc.conf提高代码封装性与通用性

## CreateTable目录
存放用于创建hive表的sql脚本

## model_qtt目录
用于存放趣头条的新版ocpc代码

## model_novel目录
用于存放米读小说的新版ocpc代码

## report目录
ocpc报表系统的通用代码，利用ocpc.conf提高代码封装性与通用性

## report_qtt目录
report目录的趣头条版本

## experiment目录
1. 新版ab实验代码
2. 新版cpasuggest代码
3. 测试阶段k值计算代码

## experiment目录下代码的实验配置（json文件）

## model_hottopic_v3
热点段子明投新版代码

## suggest_cpa
按转化目标过滤后计算推荐cpa

## suggest_cpa_v1
不按转化目标过滤计算推荐cpa
