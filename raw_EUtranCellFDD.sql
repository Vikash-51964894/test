

{{ config(materialized='table') }}


with
    abc as (
        select
            start_time,
            date,
            hour,
            minute,
            eutrancellfdd as cellid,
            nename,
            (
                select max(cast(abc as numeric))
                from unnest(split(cast(pmpdcplattimedlqci as string))) abc
            ) as pmpdcplattimedlqci,
            (
                select max(cast(abc as numeric))
                from unnest(split(cast(pmpdcplatpkttransdlqci as string))) abc
            ) as pmpdcplatpkttransdlqci,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 1
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(0)]
                else pmradiouerepcqidistr
            end pmradiouerepcqidistr_0,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 2
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(1)]
                else pmradiouerepcqidistr
            end pmradiouerepcqidistr_1,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 3
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(2)]
                else pmradiouerepcqidistr
            end pmradiouerepcqidistr_2,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 4
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(3)]
                else '0'
            end pmradiouerepcqidistr_3,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 5
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(4)]
                else '0'
            end pmradiouerepcqidistr_4,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 6
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(5)]
                else '0'
            end pmradiouerepcqidistr_5,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 7
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(6)]
                else '0'
            end pmradiouerepcqidistr_6,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 8
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(7)]
                else '0'
            end pmradiouerepcqidistr_7,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 9
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(8)]
                else '0'
            end pmradiouerepcqidistr_8,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 10
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(9)]
                else '0'
            end pmradiouerepcqidistr_9,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 11
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(10)]
                else '0'
            end pmradiouerepcqidistr_10,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 12
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(11)]
                else '0'
            end pmradiouerepcqidistr_11,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 13
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(12)]
                else '0'
            end pmradiouerepcqidistr_12,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 14
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(13)]
                else '0'
            end pmradiouerepcqidistr_13,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 15
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(14)]
                else '0'
            end pmradiouerepcqidistr_14,
            case
                when
                    (select count(abc) from unnest(split(pmradiouerepcqidistr)) abc)
                    >= 16
                    and pmradiouerepcqidistr <> '0'
                then split(pmradiouerepcqidistr, ',')[offset(15)]
                else '0'
            end pmradiouerepcqidistr_15,
            pmcelldowntimeauto,
            pmcelldowntimeman,
            pmpdcpvoldldrb,
            pmschedactivitycelldl,
            pmpdcpvoluldrb,
            pmrrcconnestabsucc,
            pms1sigconnestabsucc,
            pmerabestabsuccinit,
            pmrrcconnestabatt,
            pmrrcconnestabattreatt,
            pms1sigconnestabatt,
            pmerabestabsuccadded,
            pmerabestabattinit,
            pmerabestabattadded,
            pmerabrelabnormalenbact,
            pmerabrelabnormalmmeact,
            pmerabrelnormalenb,
            pmerabrelabnormalenb,
            pmerabrelmme,
            pmerabrelabnormalenbacthpr,
            pmerabrelabnormalenbacttnfail,
            (
                select sum(cast(abc as numeric))
                from unnest(split(cast(pmerabrelnormalenbqci as string))) abc
            ) as pmerabrelnormalenbqci,
            pmerabrelnormalenbactqci,
            pmerabrelabnormalenbqci
        from `gebu-data-ml-day0-01-333910.Ericsson_4G_PmGroups_latest.raw_EUtranCellFDD`
    )

select
    date,
    hour,
    minute,
    '15' intervals,
    nename,
    cellid,
    sum(pmcelldowntimeauto) as pmcelldowntimeauto,
    sum(pmcelldowntimeman) as pmcelldowntimeman,
    sum(pmpdcpvoldldrb) as pmpdcpvoldldrb,
    sum(pmschedactivitycelldl) as pmschedactivitycelldl,
    sum(pmpdcpvoluldrb) as pmpdcpvoluldrb,
    100 * (
        1 - (sum(pmcelldowntimeauto + pmcelldowntimeman) * count(distinct cellid) / 900)
    ) as nwa,
    (
        case
            when
                (
                    coalesce(
                        100 * sum(
                            nullif(
                                (
                                    pmrrcconnestabsucc
                                    * pms1sigconnestabsucc
                                    * pmerabestabsuccinit
                                ),
                                0
                            )
                        )
                        / sum(
                            (pmrrcconnestabatt - pmrrcconnestabattreatt)
                            * pmrrcconnestabsucc
                            * pmerabestabattinit
                        ),
                        0
                    )
                )
                > 100
            then 100
            else
                (
                    coalesce(
                        100 * sum(
                            nullif(
                                (
                                    pmrrcconnestabsucc
                                    * pms1sigconnestabsucc
                                    * pmerabestabsuccinit
                                ),
                                0
                            )
                        )
                        / sum(
                            (pmrrcconnestabatt - pmrrcconnestabattreatt)
                            * pmrrcconnestabsucc
                            * pmerabestabattinit
                        ),
                        0
                    )
                )
        end
    ) as session_success_rate,
    (
        case
            when
                (
                    coalesce(
                        100 * (
                            sum(nullif(pmrrcconnestabsucc, 0))
                            / (sum(pmrrcconnestabatt) - sum(pmrrcconnestabattreatt))
                        ),
                        0
                    )
                )
                > 100
            then 100
            else
                (
                    coalesce(
                        100 * (
                            sum(nullif(pmrrcconnestabsucc, 0))
                            / (sum(pmrrcconnestabatt) - sum(pmrrcconnestabattreatt))
                        ),
                        0
                    )
                )
        end
    ) as rrc_success_rate,
    sum(pmrrcconnestabatt - pmrrcconnestabattreatt) as rrc_attempts,
    sum(pmrrcconnestabsucc) as rrc_success,
    (
        case
            when
                (
                    coalesce(
                        100
                        * sum(nullif(pms1sigconnestabsucc, 0))
                        / sum(pmrrcconnestabsucc),
                        0
                    )
                )
                > 100
            then 100
            else
                coalesce(
                    100
                    * sum(nullif(pms1sigconnestabsucc, 0))
                    / sum(pmrrcconnestabsucc),
                    0
                )
        end
    ) as s1_success_rate,
    sum(pms1sigconnestabatt) as s1_attempts,
    (
        case
            when
                (
                    coalesce(
                        100
                        * sum(nullif((pmerabestabsuccinit + pmerabestabsuccadded), 0))
                        / sum(pmerabestabattinit + pmerabestabattadded),
                        0
                    )
                )
                > 100
            then 100
            else
                coalesce(
                    100
                    * sum(nullif((pmerabestabsuccinit + pmerabestabsuccadded), 0))
                    / sum(pmerabestabattinit + pmerabestabattadded),
                    0
                )
        end
    ) as erab_success_rate,
    (
        case
            when
                (
                    coalesce(
                        100 * sum(
                            nullif(
                                (pmerabrelabnormalenbact + pmerabrelabnormalmmeact), 0
                            )
                        )
                        / sum(pmerabrelnormalenb + pmerabrelabnormalenb + pmerabrelmme),
                        0
                    )
                )
                > 100
            then 100
            else
                coalesce(
                    100 * sum(
                        nullif((pmerabrelabnormalenbact + pmerabrelabnormalmmeact), 0)
                    )
                    / sum(pmerabrelnormalenb + pmerabrelabnormalenb + pmerabrelmme),
                    0
                )
        end
    ) as erab_drop_active,
    (
        case
            when
                (
                    coalesce(
                        100
                        * sum(nullif((pmerabrelabnormalenb + pmerabrelmme), 0))
                        / sum(pmerabrelnormalenb + pmerabrelabnormalenb + pmerabrelmme),
                        0
                    )
                )
                > 100
            then 100
            else
                coalesce(
                    100
                    * sum(nullif((pmerabrelabnormalenb + pmerabrelmme), 0))
                    / sum(pmerabrelnormalenb + pmerabrelabnormalenb + pmerabrelmme),
                    0
                )
        end
    ) as erab_drop_all,
    sum(pmerabrelabnormalenbacthpr) as enbrel_fail_acthpr,
    sum(pmerabrelabnormalenbacttnfail) as enbrel_fail_acttnfail,
    sum(pmerabrelnormalenbqci) as erabrel_nor_enbqci,
    sum(pmerabrelnormalenbactqci) as erabrel_nor_enbactqci,
    sum(pmerabrelabnormalenbqci) as erabrel_abn_enbqci,
    sum(
        (cast(pmradiouerepcqidistr_0 as numeric) * 0)
        + (cast(pmradiouerepcqidistr_1 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_2 as numeric) * 2)
        + (cast(pmradiouerepcqidistr_3 as numeric) * 3)
        + (cast(pmradiouerepcqidistr_4 as numeric) * 4)
        + (cast(pmradiouerepcqidistr_5 as numeric) * 5)
        + (cast(pmradiouerepcqidistr_6 as numeric) * 6)
        + (cast(pmradiouerepcqidistr_7 as numeric) * 7)
        + (cast(pmradiouerepcqidistr_8 as numeric) * 8)
        + (cast(pmradiouerepcqidistr_9 as numeric) * 9)
        + (cast(pmradiouerepcqidistr_10 as numeric) * 10)
        + (cast(pmradiouerepcqidistr_11 as numeric) * 11)
        + (cast(pmradiouerepcqidistr_12 as numeric) * 12)
        + (cast(pmradiouerepcqidistr_13 as numeric) * 13)
        + (cast(pmradiouerepcqidistr_14 as numeric) * 14)
        + (cast(pmradiouerepcqidistr_15 as numeric) * 15)
    ) / sum(
        (cast(pmradiouerepcqidistr_0 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_1 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_2 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_3 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_4 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_5 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_6 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_7 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_8 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_9 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_10 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_11 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_12 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_13 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_14 as numeric) * 1)
        + (cast(pmradiouerepcqidistr_15 as numeric) * 1)
    ) as reported_cqi,
    sum(pmpdcplattimedlqci) / sum(nullif(pmpdcplatpkttransdlqci, 0)) as dl_latency_qci
from abc
group by date, hour, minute, nename, cellid
order by date, hour, minute, nename, cellid
limit
    500
    /* limit added automatically by dbt cloud */
    
