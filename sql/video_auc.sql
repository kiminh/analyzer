create temporary function auc as 'hivemall.evaluation.AUCUDAF' using jar "/home/cpc/anal/lib/hivemall-all-0.5.2-incubating.jar";
insert overwrite table dl_cpc.cpc_model_auc partitions (day = "2019-08-19", type = "video")
select
ctr_model_name,
count(*) as show_cnt,
auc(A.exp_ctr/1000000, coalesce(A.isclick,0)) as auc,
sum(A.exp_ctr)/1000000/sum(A.isclick) as pcoc
from
(
  select
  ctr_model_name, isshow, isclick, exp_ctr
  from
  dl_cpc.cpc_basedata_union_events
  where
  day in ('2019-08-19')
  and media_appsid in ('80000001','80000002')
  and adsrc = 1
--  and adtype in (8, 10)
  and isshow = 1
  ----and length(uid) in (14,15,36)
  distribute by ctr_model_name sort by exp_ctr DESC
) A
group by ctr_model_name;
