INSERT OVERWRITE TABLE s_trade_demand_df PARTITION(ds='{ds}')
SELECT  month_bucket
        ,trade
        ,search_frequency
FROM    (
            SELECT  month_bucket
                    ,trade
                    ,search_frequency
                    ,ROW_NUMBER() OVER( PARTITION BY month_bucket ORDER BY is_new DESC ) AS rank_num
            FROM    (
                        SELECT  month_bucket
                                ,trade
                                ,search_frequency
                                ,1 is_new
                        FROM    trade_demand
                        UNION ALL
                        SELECT  month_bucket
                                ,trade
                                ,search_frequency
                                ,0 is_new
                        FROM    s_trade_demand_df
                        WHERE   ds = '{y_ds}'
                    ) fina
        ) s  WHERE   rank_num = 1

;
