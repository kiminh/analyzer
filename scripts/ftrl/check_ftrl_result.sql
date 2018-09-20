-- check v19 ftrl model
SELECT
    case 
        when exptags like "%ctr2model=1-v15%" then "v15"
        when exptags like "%ctr2model=1-v19%" then "v19"
        ELSE "noctr"
    END AS ctrtag,
    case 
        when round(ext["adclass"].int_value/1000) == 110110 then 'wz'
        else 'other'
    end as adclass,
    round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
    round(sum(case WHEN isclick == 1 then price else 0 end)*10/count(distinct uid),3) as arpu,
    round(sum(isclick)*100 / sum(isshow),3) as ctr,
    round(sum(iscvr)*100 / sum(isclick),3) click_cvr_rate,
    round(sum(iscvr)*100 / sum(isshow),3) as show_cvr_rate,
    sum(iscvr)/sum(case WHEN isclick == 1 then price else 0 end)*1000 as roi,
    sum(case WHEN isclick == 1 then price else 0 end)/sum(iscvr) as customer_cost
FROM 
    (
        select 
            * 
        from
            dl_cpc.cpc_union_log 
        WHERE 
            `date` = "2018-09-19" 
        and
            `hour`="13"
        and adslot_type!=3
    ) a
    left outer join
    (
        select 
            searchid, 
            label as iscvr 
        from dl_cpc.ml_cvr_feature_v1
        WHERE `date` = "2018-09-19" and `hour`="13"
    ) b on a.searchid = b.searchid
GROUP BY 
case 
    when exptags like "%ctr2model=1-v15%" then "v15"
    when exptags like "%ctr2model=1-v19%" then "v19"
    ELSE "noctr"  
END,
case 
    when round(ext["adclass"].int_value/1000) == 110110 then 'wz'
    else 'other'
end;



-- result
-- 2018-09-19 09
noctr   wz      12.528  90.127  1.253   2.368   0.03    0.23690817855157967     4221.044651619234
v19     other   53.517  310.768 2.037   13.851  0.282   0.5272572695204443      1896.6073258876615
noctr   other   54.19   295.422 2.085   14.23   0.297   0.5476088198584178      1826.1210625835909
v19     wz      13.135  88.307  1.306   2.494   0.033   0.24792635401597435     4033.4558380008616
v15     wz      12.751  92.849  1.28    2.38    0.03    0.23891977799997477     4185.5053121642595
v15     other   57.206  308.191 2.099   14.208  0.298   0.5213512224626157      1918.0927499823915

-- 2018-09-19 10
noctr   wz      13.613  99.056  1.227   2.47    0.03    0.2225419535283993      4493.534743202417
v19     other   78.694  448.574 1.969   13.576  0.267   0.3396333876978717      2944.351280591918
noctr   other   82.534  444.159 2.006   13.509  0.271   0.3282893976585628      3046.0928897863746
v19     wz      14.079  97.111  1.295   2.534   0.033   0.23307048455300947     4290.547565118913
v15     wz      13.84   100.96  1.263   2.529   0.032   0.23079518744923397     4332.845979381444
v15     other   83.023  443.344 2.036   13.787  0.281   0.3380390615104446      2958.238008151322


-- 2018-09-19 11
noctr   wz      14.563  100.6   1.268   2.817   0.036   0.24522854838013655     4077.8286484405085
v19     other   98.178  544.305 1.909   12.651  0.241   0.24593181808332787     4066.167638630536
noctr   other   101.59  531.698 1.937   12.734  0.247   0.24277015282609296     4119.1225048012575
v19     wz      15.233  99.31   1.339   3.014   0.04    0.2649151437770642      3774.793640493186
v15     wz      14.878  102.704 1.308   3.017   0.039   0.2652995989303908      3769.323451794511
v15     other   101.166 527.966 1.948   12.978  0.253   0.24989032503492536     4001.755569609337

-- 2018-09-19 12
noctr   wz      15.557  110.457 1.276   2.651   0.034   0.2174063957717239      4599.680687637162
v19     other   72.756  457.22  1.432   7.372   0.106   0.14513594867130925     6890.091732302033
noctr   other   73.198  440.282 1.409   6.883   0.097   0.1324430921836949      7550.4126603524755
v19     wz      16.096  109.126 1.344   2.613   0.035   0.21815350007202533     4583.928287512422
v15     wz      15.822  112.081 1.334   2.64    0.035   0.22254517204758997     4493.469756271126
v15     other   74.84   450.258 1.43    7.139   0.102   0.13637487631352807     7332.728923625079

-- 2018-09-19 13
