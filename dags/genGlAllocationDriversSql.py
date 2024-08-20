getPeriod = """
            SELECT *
            FROM ods.CAL_LU 
            WHERE FULLDATE = (
                                SELECT MONTH_START_DATE - 1
                                FROM ods.CAL_LU 
                                WHERE FULLDATE = current_date)
			"""


getRules = """
            SELECT *
            FROM ods.gl_allocation_rules
            --where is_active = TRUE AND id IN (
            --                                    SELECT DISTINCT RULE_ID 
            --                                    FROM ods.gl_allocation_accounts
            --                                    WHERE IS_ACTIVE = TRUE)
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

insertAllDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.level1, gadh.LEVEL2, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.level1, gadh.LEVEL2, trn.sku,  gadh.total_amt
                    """

insertLocDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.level1, gadh.level2, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.location_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.level1, gadh.LEVEL2, trn.sku,  gadh.total_amt
                    """

insertCatDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.level1, gadh.LEVEL2, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON gpgx.CATEGORY_NAME = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.level1, gadh.LEVEL2, trn.sku,  gadh.total_amt
                    """

insertVendorDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.level1, gadh.LEVEL2, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON ci.PRIMARY_VENDOR_ID::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.level1, gadh.LEVEL2, trn.sku,  gadh.total_amt
                    """

insertBrandDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.level1, gadh.LEVEL2, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON ci.brand_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.level1, gadh.LEVEL2, trn.sku, gadh.total_amt
                    """