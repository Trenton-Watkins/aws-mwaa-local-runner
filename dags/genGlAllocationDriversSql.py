getPeriod = """
            SELECT *
            FROM ods.CAL_LU 
            WHERE FULLDATE = (
                                SELECT MONTH_START_DATE - 1
                                FROM ods.CAL_LU 
                                WHERE FULLDATE = current_date)
			"""
getRun = """
        SELECT COALESCE(max(run_id),1) as run_id FROM ods.GL_ALLOCATION_TEST
         """

getRules = """
            SELECT *
            FROM ods.gl_allocation_rules
            """


insertAllHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'all' AS level1, 
                'all' AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt 
                FROM ods.transactions trn
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                GROUP BY period_end_date 
                """

insertLocHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'location' as level1,
                    trn.LOCATION_ID AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt 
                    FROM ods.transactions trn
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, trn.LOCATION_ID 
                    """

insertCatHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt)
                    SELECT {ruleId} AS rule_id,  '{endDate}' AS period_end_date, 'category' as level1,
                    gpgx.CATEGORY_NAME AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt 
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, gpgx.CATEGORY_NAME 
                    """

insertVendorHeader = """
                        INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt)
                        SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'vendor' AS level1, 
                        ci.PRIMARY_VENDOR_ID AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                        sum(trn.{tranColumn})  AS total_amt 
                        FROM ods.transactions trn
                        LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                         WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                        AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                        GROUP BY period_end_date, ci.PRIMARY_VENDOR_ID 
                        """

insertBrandHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'brand' AS level1, 
                    ci.brand_id AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt 
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                     WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, ci.brand_id 
                    """

insertOrderHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'order' AS level1, 
                    trn.order_id AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt 
                    FROM ods.transactions trn
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, trn.order_id 
                    """


insertOrderLocHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'order' AS level1, 
                    trn.order_id AS level1_name, 'location' as level2, trn.LOCATION_ID as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt 
                    FROM ods.transactions trn
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, trn.order_id, trn.LOCATION_ID
                    """

insertOrderCatHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'order' AS level1, 
                    trn.order_id AS level1_name, 'category' as level2,  gpgx.CATEGORY_NAME as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt 
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, trn.order_id,  gpgx.CATEGORY_NAME
                    """

insertRefundsHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'credit' AS level1, cmi.CREDIT_ID  AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at,
                    sum(cmi.ROW_TOTAL) AS tran_amt  FROM ods.CREDIT_MEMO_ITEMS cmi 
                    WHERE cmi.SKU NOT IN ('membership-product','ice_pack_product') AND 
                    convert_timezone('UTC', 'America/Los_Angeles', cmi.INSERT_UTC_DATETIME)::date BETWEEN '{startDate}' and '{endDate}'
                    GROUP BY credit_id,period_end_date
                    HAVING tran_amt <>0
                    """

insertAllDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt
                    """

insertLocDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.location_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt
                    """

insertCatDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON gpgx.CATEGORY_NAME = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt
                    """

insertOrderDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.order_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt
                    """

insertOrderLocDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.order_id::varchar(100) = gadh.level1_name and trn.location_id::varchar(100) = gadh.level2_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt
                    """

insertOrderCatDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.order_id::varchar(100) = gadh.level1_name and  gpgx.CATEGORY_NAME::varchar(100) = gadh.level2_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt
                    """


insertVendorDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON ci.PRIMARY_VENDOR_ID::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt
                    """

insertBrandDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON ci.brand_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku, gadh.total_amt
                    """

insertRefundsDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(cmi.ROW_TOTAL) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.CREDIT_MEMO_ITEMS cmi
                    INNER JOIN ods.gl_alloc_driver_header gadh ON cmi.credit_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE convert_timezone('UTC', 'America/Los_Angeles', cmi.INSERT_UTC_DATETIME)::date BETWEEN '{startDate}' and '{endDate}'
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, cmi.sku,  gadh.total_amt
                    """
getMap ="""
        SELECT * FROM ods.GL_ALLOCATION_MAP 
        """

createTranAlloc = """
                CREATE  TEMP TABLE gl_alloc as 
                SELECT dje.BATCH_ID,dje.MAP_ID,dje.TRAN_ID,dje.ACCT_NUMBER,dje.TRAN_DATE,dje.GROUP_CODE,dje.TRAN_TYPE,dje.TRAN_SUB_TYPE,dje.TRAN_TIME,dje.ORDER_ID,dje.SALE_DATE,dje.INCREMENT_ID,mp.ALLOC_RULE_ID,mp.CATEGORY,mp.LOCATION,dd.SKU,dd.SKU_AMT,dd.ALLOC_PCT, COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0) AS total_amt, round(total_amt * dd.ALLOC_PCT,2) AS tran_amt FROM ods.DETAIL_JOURNAL_ENTRIES dje
                INNER JOIN NETSUITE_ACCOUNTS na ON na.ACCOUNT_ID  = dje.account_id
                INNER JOIN GL_ALLOCATION_MAP mp ON mp.DJE_MAP_ID = dje.map_id
                INNER JOIN GL_ALLOC_DRIVER_DETAIL dd ON dd.RULE_ID = mp.alloc_rule_id AND dd.LEVEL1 = dje.ORDER_ID {join}
                WHERE dje.TRAN_DATE BETWEEN '{startDate}' and '{endDate}' AND  na.TYPE_NAME IN ('Cost of Goods Sold','Income')
                and dje.map_id = '{mapId}' and dd.rule_id = '{ruleId}'
                """


createTranOob = """
                    CREATE  TEMP TABLE oob as
                    SELECT tran_id ,total_amt, sum(tran_amt) AS total_alloc, total_amt- total_alloc AS total_var
                    FROM GL_ALLOC 
                    GROUP BY tran_id ,total_amt
                    """

updateTranAlloc = """
                update gl_alloc
                set tran_amt = new_alloc_amt
                from (
                       select det.tran_id,det.sku, det.tran_amt + case when total_var > 0 then .01 else -.01 end as new_alloc_amt, det.tran_amt
                        from (
                                select tran_id,sku, tran_amt,
                                row_number() OVER  (PARTITION by tran_id order by tran_amt desc) as rn
                                from  gl_alloc tab
                                where tab.tran_amt <> 0 ) det
                        inner join oob on  rn <= abs(oob.total_var * 100) AND oob.tran_id = det.tran_id
                        order by  rn) upd
                where gl_alloc.sku = upd.sku AND gl_alloc.tran_id = upd.tran_id
                """

insertToGlAllocation = """
                    INSERT into ods.gl_allocation_test(CREATED_AT,RULE_ID,RUN_ID,TRAN_DATE,SALE_DATE,LOCATION_ID,GL_PRODUCT_CATEGORY,ACCT_NUMEBR,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE,SKU,ALLOC_AMT)
                    SELECT SYSDATE() AS created_date, alloc_rule_id AS rule_id, {runid} AS run_id, tran_date, sale_date,LOCATION, CATEGORY, ACCT_NUMBER,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE,SKU,sum(TRAN_AMT) AS tran_amt
                    FROM gl_alloc
                    GROUP BY created_date, rule_id, run_id, tran_date, sale_date,LOCATION, CATEGORY, ACCT_NUMBER,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE,SKU
                        """

# dropTranAlloc = """
#                 drop table gl_alloc;
#                 drop table oob;
#                 """