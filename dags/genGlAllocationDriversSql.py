getPeriod = """
            SELECT *,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext
            FROM ods.CAL_LU 
            WHERE FULLDATE = (
                                SELECT MONTH_START_DATE - 1
                                FROM ods.CAL_LU 
                                WHERE FULLDATE = '2023-07-01')
			"""

getRun = """
        SELECT COALESCE(max(run_id),1) as run_id FROM ods.GL_ALLOCATION_TEST
         """

getRules = """
            SELECT *
            FROM ods.gl_allocation_rules
            """


insertAllHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'all' AS level1, 
                'all' AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.transactions trn
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                GROUP BY period_end_date 
                """

insertLocHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'location' as level1,
                    trn.LOCATION_ID AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.transactions trn
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, trn.LOCATION_ID 
                    """

insertCatHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id,  '{endDate}' AS period_end_date, 'category' as level1,
                    gpgx.CATEGORY_NAME AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, gpgx.CATEGORY_NAME 
                    """

insertVendorHeader = """
                        INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                        SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'vendor' AS level1, 
                        ci.PRIMARY_VENDOR_ID AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                        sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                        FROM ods.transactions trn
                        LEFT OUTER JOIN ods.NS_FC_XREF fc ON fc.NS_FC_ID = trn.LOCATION_ID 
                        LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND COALESCE(fc.ODS_FC_ID,2) = ci.fc_id 
                         WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                        AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                        GROUP BY period_end_date, ci.PRIMARY_VENDOR_ID 
                        """

insertBrandHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'brand' AS level1, 
                    ci.brand_id AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.transactions trn
                        LEFT OUTER JOIN ods.NS_FC_XREF fc ON fc.NS_FC_ID = trn.LOCATION_ID 
                        LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND COALESCE(fc.ODS_FC_ID,2) = ci.fc_id 
                     WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, ci.brand_id 
                    """

insertOrderHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'order' AS level1, 
                    trn.order_id AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.transactions trn
                    WHERE  TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, trn.order_id 
                    """


insertOrderLocHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'order' AS level1, 
                    trn.order_id AS level1_name, 'location' as level2, trn.LOCATION_ID as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.transactions trn
                    WHERE TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, trn.order_id, trn.LOCATION_ID
                    """

insertOrderCatHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'order' AS level1, 
                    trn.order_id AS level1_name, 'category' as level2,  gpgx.CATEGORY_NAME as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    WHERE  TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, trn.order_id,  gpgx.CATEGORY_NAME
                    """

insertRefundsHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'credit' AS level1, cmi.ORDER_ID  AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at,
                    sum(cmi.ROW_TOTAL) AS tran_amt ,sum(cmi.qty_refunded) as total_qty  FROM ods.CREDIT_MEMO_ITEMS cmi 
                    WHERE cmi.SKU NOT IN ('membership-product','ice_pack_product') 
                    GROUP BY order_id,period_end_date
                    HAVING tran_amt <>0 or total_qty <>0
                    """
insertCMSOrderHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'credit' AS level1, od.ORDER_ID  AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at,
                    sum(od.ROW_TOTAL) AS tran_amt ,sum(od.qty_ordered) as total_qty  FROM ods.order_detail od 
                    WHERE od.SKU NOT IN ('membership-product','ice_pack_product') 
                    GROUP BY order_id,period_end_date
                    HAVING tran_amt <>0 or total_qty <>0
                    """



insertAccountHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'account' AS level1, 
                CASE
	        WHEN na.type_name IN ('Income') 
                THEN na.accountnumber 
                ELSE na2.ACCOUNTNUMBER 
                END AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.transactions trn
                INNER JOIN ods.V_GL_MAP_DETAIL md ON md.MAP_ID = trn.JE_MAP_ID 
                INNER JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER =md.DEBIT_ACCOUNT_NUMBER 
                INNER JOIN ods.NETSUITE_ACCOUNTS na2 ON na2.ACCOUNTNUMBER =md.CREDIT_ACCOUNT_NUMBER 
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' and (na.type_name = 'Income' OR na2.TYPE_NAME = 'Income')
                GROUP BY period_end_date ,na.type_name ,na.accountnumber,na2.ACCOUNTNUMBER 
                """


insertAllDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertLocDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.transactions trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.location_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertCatDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON gpgx.CATEGORY_NAME = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertOrderDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.transactions trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.order_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    where TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertOrderLocDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.transactions trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.order_id::varchar(100) = gadh.level1_name and trn.location_id::varchar(100) = gadh.level2_name AND {ruleId} = gadh.rule_id 
                    WHERE  TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertOrderCatDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.order_id::varchar(100) = gadh.level1_name and  gpgx.CATEGORY_NAME::varchar(100) = gadh.level2_name AND {ruleId} = gadh.rule_id 
                    WHERE  TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """


insertVendorDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.NS_FC_XREF fc ON fc.NS_FC_ID = trn.LOCATION_ID 
                        LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND COALESCE(fc.ODS_FC_ID,2) = ci.fc_id  
                    INNER JOIN ods.gl_alloc_driver_header gadh ON ci.PRIMARY_VENDOR_ID::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertBrandDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.NS_FC_XREF fc ON fc.NS_FC_ID = trn.LOCATION_ID 
                        LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND COALESCE(fc.ODS_FC_ID,2) = ci.fc_id  
                    INNER JOIN ods.gl_alloc_driver_header gadh ON ci.brand_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku, gadh.total_amt,gadh.total_qty
                    """

insertRefundsDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(cmi.ROW_TOTAL) AS sku_amt,sum(cmi.qty_refunded) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.CREDIT_MEMO_ITEMS cmi
                    INNER JOIN ods.gl_alloc_driver_header gadh ON cmi.order_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, cmi.sku,  gadh.total_amt,gadh.total_qty
                    """


insertCMSOrderDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(od.ROW_TOTAL) AS sku_amt,sum(od.qty_ordered) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.order_detail od
                    INNER JOIN ods.gl_alloc_driver_header gadh ON od.order_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, od.sku,  gadh.total_amt,gadh.total_qty
                    """

insertAccountDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT gadh.id,gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.tran_amt) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.transactions trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.V_GL_MAP_DETAIL md ON md.MAP_ID = trn.JE_MAP_ID 
                    INNER JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER =md.DEBIT_ACCOUNT_NUMBER 
                    INNER JOIN ods.NETSUITE_ACCOUNTS na2 ON na2.ACCOUNTNUMBER =md.CREDIT_ACCOUNT_NUMBER 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON '{ruleId}'= gadh.rule_id AND CASE
	            WHEN na.type_name IN ('Income')
                    THEN na.ACCOUNTNUMBER
                    ELSE na2.ACCOUNTNUMBER
                    END = gadh.level1_name
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' and (na.type_name = 'Income' OR na2.TYPE_NAME = 'Income') AND trn.sku Is NOT null
                    GROUP BY gadh.id,gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku, gadh.total_amt,gadh.total_qty
                    """

getMapODS ="""
        SELECT * FROM ods.GL_ALLOCATION_MAP where type ='ODS'
        """


getMapNS ="""
        SELECT * FROM ods.GL_ALLOCATION_MAP where type ='NS' 
        """

createTranAlloc = """
                insert into  ods.gl_alloc  (
                BATCH_ID, MAP_ID, TRAN_ID, ACCT_NUMBER, TRAN_DATE, 
                GROUP_CODE, TRAN_TYPE, TRAN_SUB_TYPE_ID ,TRAN_SUB_TYPE, TRAN_TIME, ORDER_ID, 
                SALE_DATE, INCREMENT_ID, ALLOC_RULE_ID, CATEGORY, 
                LOCATION, SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME, SKU_AMT, ALLOC_PCT, TOTAL_AMT, TRAN_AMT
                ) 
                SELECT dje.BATCH_ID,dje.MAP_ID,dje.TRAN_ID,dje.ACCT_NUMBER,dje.TRAN_DATE,dje.GROUP_CODE,dje.TRAN_TYPE,tr.TRAN_SUB_TYPE_ID ,dje.TRAN_SUB_TYPE,dje.TRAN_TIME,dje.ORDER_ID,dje.SALE_DATE,dje.INCREMENT_ID,mp.ALLOC_RULE_ID,mp.CATEGORY,mp.LOCATION
                ,dd.SKU
                ,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                ,dd.SKU_AMT,
                CASE 
                WHEN SUM(dd.SKU_AMT) OVER (PARTITION BY dje.ORDER_ID) = 0 THEN dd.qty_alloc_pct
                ELSE dd.alloc_pct 
                END AS allocation_pct, COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0) AS total_amt, round((COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0)) * allocation_pct,2) AS tran_amt 
                FROM  ods.DETAIL_JOURNAL_ENTRIES dje
                INNER JOIN NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER  = dje.ACCT_NUMBER 
                INNER JOIN GL_ALLOCATION_MAP mp ON mp.DJE_MAP_ID = dje.map_id   {join2}
                INNER JOIN GL_ALLOC_DRIVER_DETAIL dd ON dd.RULE_ID = mp.alloc_rule_id AND dd.LEVEL1 = dje.ORDER_ID::varchar {join}
                INNER JOIN "TRANSACTIONS" tr ON tr.TRAN_ID  =dje.TRAN_ID 
                LEFT JOIN ods.CURR_ITEMS ci ON ci.item_name = dd.sku AND ci.FC_ID = 2
                WHERE dje.TRAN_DATE BETWEEN '{startDate}' and '{endDate}' AND  na.TYPE_NAME IN ('Cost of Goods Sold','Income')
                and dje.map_id = '{mapId}' and dd.rule_id = '{ruleId}'
                """

createTranErrors ="""
                insert into  ods.gl_alloc  (
                BATCH_ID, MAP_ID, TRAN_ID, ACCT_NUMBER, TRAN_DATE, 
                GROUP_CODE, TRAN_TYPE, TRAN_SUB_TYPE_ID ,TRAN_SUB_TYPE, TRAN_TIME, ORDER_ID, 
                SALE_DATE, INCREMENT_ID, ALLOC_RULE_ID, CATEGORY, 
                LOCATION, SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME, SKU_AMT, ALLOC_PCT, TOTAL_AMT, TRAN_AMT
                )
                WITH failures AS (SELECT dje.tran_id
                FROM  ods.DETAIL_JOURNAL_ENTRIES dje
                LEFT JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER  = dje.ACCT_NUMBER 
                LEFT JOIN ods.GL_ALLOCATION_MAP mp ON mp.DJE_MAP_ID = dje.map_id {join2}
                LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.RULE_ID = mp.alloc_rule_id AND dd.LEVEL1 = dje.ORDER_ID::varchar {join}
                WHERE dje.TRAN_DATE BETWEEN '{startDate}' and '{endDate}' AND  na.TYPE_NAME IN ('Cost of Goods Sold','Income')
                and dje.map_id ='{mapId}'  AND dd.SKU IS NULL)
                SELECT dje.BATCH_ID,dje.MAP_ID,dje.TRAN_ID,dje.ACCT_NUMBER,dje.TRAN_DATE,dje.GROUP_CODE,dje.TRAN_TYPE,tr.TRAN_SUB_TYPE_ID ,dje.TRAN_SUB_TYPE,dje.TRAN_TIME,dje.ORDER_ID,dje.SALE_DATE,dje.INCREMENT_ID,2002,null,null
  				,dd.SKU
  				,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
  				,dd.SKU_AMT,
                CASE 
                WHEN SUM(dd.SKU_AMT) OVER (PARTITION BY dje.ORDER_ID) = 0 THEN dd.qty_alloc_pct
                ELSE dd.alloc_pct 
                END AS allocation_pct, COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0) AS total_amt, round((COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0)) * allocation_pct,2) AS tran_amt 
                FROM failures fl
                INNER JOIN ods.DETAIL_JOURNAL_ENTRIES dje ON dje.TRAN_ID = fl.tran_id  
                inner JOIN  ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER  = dje.ACCT_NUMBER 
                INNER JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.RULE_ID = 2002 AND dd.LEVEL1 = dje.ORDER_ID::varchar
                INNER JOIN "TRANSACTIONS" tr ON tr.TRAN_ID  =dje.TRAN_ID 
                LEFT JOIN ods.CURR_ITEMS ci ON ci.item_name = dd.sku AND ci.FC_ID = 2
                WHERE dje.TRAN_DATE BETWEEN '{startDate}' and '{endDate}' AND  na.TYPE_NAME IN ('Cost of Goods Sold','Income')

                """


createTranOob = """
                    CREATE  TEMP TABLE oob as
                    SELECT ACCT_NUMBER ,sum(total_var) AS total_var FROM (SELECT tran_id,ACCT_NUMBER ,total_amt , sum(tran_amt) ,total_amt- sum(tran_amt)AS total_var
                    FROM ods.GL_ALLOC WHERE TRAN_DATE  BETWEEN '{startDate}' and '{endDate}'
                    GROUP BY tran_id,ACCT_NUMBER ,total_amt) GROUP BY acct_number
                    """

updateTranAlloc = """
                update ods.gl_alloc
                set tran_amt = new_alloc_amt
                FROM(
                select det.tran_id,det.acct_number ,det.sku, det.tran_amt + case when total_var > 0 then .01 else -.01 end as new_alloc_amt, det.tran_amt FROM(select tran_id,ACCT_NUMBER ,sku, tran_amt,
                                row_number() OVER  (PARTITION by ACCT_NUMBER order by tran_amt desc) as rn
                                from  ods.gl_alloc tab
                                where tab.tran_amt <> 0 ) det
                                INNER JOIN oob ON rn<abs(oob.total_var *100) AND oob.acct_number = det.acct_number) upd
                        WHERE gl_alloc.TRAN_ID =upd.tran_id AND gl_alloc.SKU =upd.sku
                """

insertToGlAllocation = """
                    INSERT into ods.gl_allocation_test(CREATED_AT,RULE_ID,RUN_ID,TRAN_DATE,SALE_DATE,LOCATION_ID,GL_PRODUCT_CATEGORY,ACCT_NUMEBR,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU,
                    GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,ALLOC_AMT)
                    SELECT SYSDATE() AS created_date, alloc_rule_id AS rule_id, {runid} AS run_id, tran_date, sale_date,LOCATION, CATEGORY, ACCT_NUMBER,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU,
                    GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,sum(TRAN_AMT) AS tran_amt
                    FROM gl_alloc
                    GROUP BY created_date, rule_id, run_id, tran_date, sale_date,LOCATION, CATEGORY, ACCT_NUMBER,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME
                    """


catchAllocation = """
                 insert into  ods.gl_alloc  (  BATCH_ID, MAP_ID, TRAN_ID, ACCT_NUMBER, TRAN_DATE, 
                GROUP_CODE, TRAN_TYPE, TRAN_SUB_TYPE_ID ,TRAN_SUB_TYPE, TRAN_TIME, ORDER_ID, 
                SALE_DATE, INCREMENT_ID, ALLOC_RULE_ID, CATEGORY, 
                LOCATION, SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME, SKU_AMT, ALLOC_PCT, TOTAL_AMT, TRAN_AMT
                )
                WITH missing AS (WITH journals AS (
                SELECT dje.ACCT_NUMBER AS acco, dje.tran_id AS tran,  
                        SUM(CASE WHEN dje.DEBIT_AMT IS NULL THEN 0 ELSE dje.DEBIT_AMT END - 
                        CASE WHEN dje.CREDIT_AMT IS NULL THEN 0 ELSE dje.CREDIT_AMT END) AS net  
                FROM ods.DETAIL_JOURNAL_ENTRIES dje
                INNER JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER = dje.ACCT_NUMBER 
                WHERE dje.TRAN_DATE BETWEEN '{startDate}' and '{endDate}'
                AND na.type_name IN ('Cost of Goods Sold', 'Income') 
                AND dje.sku IS NULL
                GROUP BY dje.ACCT_NUMBER, dje.tran_id
                ),
                allocation AS (
                SELECT gl.tran_id, gl.acct_number, SUM(gl.tran_amt) AS alloc_amt 
                FROM ods.GL_ALLOC gl
                GROUP BY gl.tran_id, gl.acct_number
                )
                SELECT  DISTINCT je.tran
                FROM journals je
                LEFT JOIN allocation al ON je.tran = al.tran_id AND je.acco = al.acct_number
                WHERE je.acco NOT IN ('41221', '40105', '41106', '41112') 
                AND al.alloc_amt IS NULL)
                SELECT dje.BATCH_ID,dje.MAP_ID,dje.TRAN_ID,dje.ACCT_NUMBER,dje.TRAN_DATE,dje.GROUP_CODE,dje.TRAN_TYPE,tr.TRAN_SUB_TYPE_ID ,dje.TRAN_SUB_TYPE,dje.TRAN_TIME,dje.ORDER_ID,dje.SALE_DATE,dje.INCREMENT_ID,
                '1',null,null,dd.SKU,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME,
                dd.SKU_AMT,CASE 
                                WHEN SUM(dd.SKU_AMT) OVER (PARTITION BY dje.ORDER_ID) = 0 THEN dd.qty_alloc_pct
                                ELSE dd.alloc_pct 
                                END AS  allocation_pct, COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0) AS total_amt, round((COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0)) * allocation_pct,2) AS tran_amt 
                                FROM ods.DETAIL_JOURNAL_ENTRIES dje 
                INNER JOIN ods.NETSUITE_ACCOUNTS na ON na.accountnumber = dje.acct_number
                LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id =1
                                INNER JOIN "TRANSACTIONS" tr ON tr.TRAN_ID  =dje.TRAN_ID 
                LEFT JOIN ods.CURR_ITEMS ci ON ci.item_name = dd.sku AND ci.FC_ID = 2
                WHERE dje.tran_date between '{startDate}' and '{endDate}' and dje.tran_id IN (SELECT * FROM missing ) AND na.type_name IN ('Cost of Goods Sold', 'Income') 
                        """

# createNSalloc ="""
#                 insert into ods.ns_alloc(
#                         ACCOUNTNUMBER ,
#                         NS_AMT ,
#                         ID ,
#                         GAH_ID ,
#                         RULE_ID ,
#                         LEVEL1 ,
#                         LEVEL2 ,
#                         SKU ,
#                         CREATED_AT ,
#                         SKU_AMT,
#                         ALLOC_PCT ,
#                         SKU_QTY ,
#                         QTY_ALLOC_PCT ,
#                         ALLOCATED_AMT )  
#                 WITH netsuite AS (SELECT ACCOUNTNUMBER,sum(AMOUNT) AS ns_amt FROM  ods.NS_CM_GL_ACTIVITY where periodname ='Jun 2023'
#                 GROUP BY ACCOUNTNUMBER HAVING  sum(amount) <>0)
#                 SELECT ACCOUNTNUMBER ,
#                 NS_AMT ,
#                 ID ,
#                 GAH_ID ,
#                 RULE_ID ,
#                 LEVEL1 ,
#                 LEVEL2 ,
#                 SKU ,
#                 CREATED_AT ,
#                 SKU_AMT,
#                 ALLOC_PCT ,
#                 SKU_QTY ,
#                 QTY_ALLOC_PCT,round(ns_amt * coalesce(dd.ALLOC_PCT,dd.QTY_ALLOC_PCT),2) AS allocated_amt FROM netsuite  ns
#                 LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.RULE_ID =1
#                 """

createNSAlloc = {"0 - Netsuite COGS Transactions  With Inventory items":
                 """
                insert into ods.gl_allocation_test (CREATED_AT,RULE_ID,RUN_ID,TRAN_DATE,SALE_DATE,LOCATION_ID,GL_PRODUCT_CATEGORY,ACCT_NUMEBR,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU
                ,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,ALLOC_AMT)
                SELECT sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,201,'NS Allocation',ci.ITEM_NAME,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME,
                (cm.amount *inv.relative_amount) AS alloc_amt 
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                INNER JOIN (
                SELECT transaction_id ,item_id,amount,
                amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
                WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null
                ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
                LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                WHERE inv.transaction_id IS NOT NULL and cm.PERIODNAME = '{period}' and accountnumber = '{accountnumber}'
                 """,
                 "1 - Netsuite Transaction with Item Level Detatil - Non VFI":
                 """
                insert into ods.gl_allocation_test (CREATED_AT,RULE_ID,RUN_ID,TRAN_DATE,SALE_DATE,LOCATION_ID,GL_PRODUCT_CATEGORY,ACCT_NUMEBR,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU
                ,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,ALLOC_AMT)
                SELECT sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,201,'NS Allocation',ci.ITEM_NAME,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME,
                cm.amount
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (46805,150000,31184,149999,31185,150001,31091,149998) and cm.PERIODNAME = '{period}' and accountnumber = '{accountnumber}'
                """,
                "2 - Netsuite Transaction with Item Level Detatil - VFI":
                 """
                insert into ods.gl_allocation_test (CREATED_AT,RULE_ID,RUN_ID,TRAN_DATE,SALE_DATE,LOCATION_ID,GL_PRODUCT_CATEGORY,ACCT_NUMEBR,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU
                ,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,ALLOC_AMT)
                SELECT sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,201,'NS Allocation',trim(SPLIT_PART(cm.SKU,'-',1)),ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME,cm.amount
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                WHERE cm.item_id IS NOT NULL AND cm.item_id IN (46805,150000,31184,149999,31185,150001,31091,149998) and cm.PERIODNAME = '{period}' and accountnumber = '{accountnumber}'
                """,
                "3 - Netsuite Transaction allocated by vendor":
                """
                insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT)
                SELECT cm.ACCOUNTNUMBER,cm.AMOUNT,cm.unique_key,dd.GAH_ID,dd.RULE_ID,dd.LEVEL1,dd.LEVEL2,dd.sku,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                ,sysdate(),dd.SKU_AMT,dd.ALLOC_PCT,dd.SKU_QTY,dd.QTY_ALLOC_PCT,
                case 
                        when gadh.total_amt <>0 then round(cm.amount*dd.ALLOC_PCT,2)
                        else round(cm.amount*dd.QTY_ALLOC_PCT,2) end as allocamt FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                INNER JOIN ods.gl_alloc_driver_header gadh ON gadh.id = dd.gah_id
                left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2
                WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL  and cm.PERIODNAME = '{period}' and accountnumber = '{accountnumber}' """
                }

createNoSkuAlloc= """
                        insert into ods.gl_allocation_test (CREATED_AT,RULE_ID,RUN_ID,TRAN_DATE,SALE_DATE,LOCATION_ID,GL_PRODUCT_CATEGORY,ACCT_NUMEBR,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE,SKU,ALLOC_AMT)
                        SELECT sysdate(),3001,{runid},'{endDate}'::date,'{endDate}'::date,NULL, NULL ,acct_number,'NS Allocation',3001,'NS Allocation','GL Activity', sum(amount) AS net FROM (WITH netsuiteallocations  AS (WITH itemlevelns AS (SELECT DISTINCT transaction_id FROM (SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,(cm.amount *inv.relative_amount) AS alloc_amt FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                        LEFT JOIN (
                        SELECT transaction_id ,item_id,amount,
                        amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
                        FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
                        WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null
                        ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
                        LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
                        LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                        WHERE inv.transaction_id IS NOT NULL and accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
                        UNION ALL
                        SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,cm.amount
                        FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                        LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
                        LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                        WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (46805,150000,31184,149999,31185,150001,31091,149998) and accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
                        UNION ALL
                        SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,'NS Allocation',trim(SPLIT_PART(cm.SKU,'-',1)),cm.amount
                        FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                        LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
                        LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                        WHERE cm.item_id IS NOT NULL AND cm.item_id IN (46805,150000,31184,149999,31185,150001,31091,149998) and accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
                        UNION ALL
                        SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,'NS Allocation',dd.sku,(cm.amount*COALESCE(dd.ALLOC_PCT,dd.QTY_ALLOC_PCT)) AS amount  
                        FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                        INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
                        LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                        WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL  and accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'))
                        SELECT *  FROM ods.NS_CM_GL_ACTIVITY cm
                        LEFT JOIN itemlevelns it ON it.transaction_id = cm.transaction_id 
                        WHERE cm.accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME =  '{period}' AND it.transaction_id IS null)
                        SELECT * FROM netsuiteallocations na
                        LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON na.accountnumber::varchar = mp.accountnumber::varchar )
                        GROUP BY acct_number
                     """

createDefaultAlloc ="""
                        insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT)
                        WITH netsuiteallocations  AS (WITH itemlevelns AS (SELECT DISTINCT transaction_id FROM (SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,(cm.amount *inv.relative_amount) AS alloc_amt FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                        LEFT JOIN (
                        SELECT transaction_id ,item_id,amount,
                        amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
                        FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
                        WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null 
                        ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
                        LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
                        LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                        WHERE inv.transaction_id IS NOT NULL and  accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
                        UNION ALL
                        SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,cm.amount
                        FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                        LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
                        LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                        WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (46805,150000,31184,149999,31185,150001,31091,149998) and accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
                        UNION ALL
                        SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,'NS Allocation',trim(SPLIT_PART(cm.SKU,'-',1)),cm.amount
                        FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                        LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
                        LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                        WHERE cm.item_id IS NOT NULL AND cm.item_id IN (46805,150000,31184,149999,31185,150001,31091,149998) and accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
                        UNION ALL
                        SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNUMBER,'NS Allocation',3001,'NS Allocation',dd.sku,(cm.amount*COALESCE(dd.ALLOC_PCT,dd.QTY_ALLOC_PCT)) AS amount  
                        FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                        INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
                        LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                        WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL and accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}' ))
                        SELECT *  FROM ods.NS_CM_GL_ACTIVITY cm
                        LEFT JOIN itemlevelns it ON it.transaction_id = cm.transaction_id 
                        WHERE cm.accountnumber = '{accountnumber}'::varchar and cm.PERIODNAME =  '{period}' AND it.transaction_id IS null)
                        SELECT acct_number,amount,unique_key,GAH_ID,3000,dd.LEVEL1,dd.LEVEL2,dd.sku,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                        ,sysdate(),SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,
                        case 
                                when gah.total_amt <>0 then round(amount * alloc_pct,2)
                                 else round(amount * qty_alloc_pct,2) end AS allocated_amt FROM netsuiteallocations na
                        LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON na.accountnumber::varchar = mp.accountnumber::varchar 
                         LEFT JOIN   (SELECT *
								     FROM ods.GL_ALLOC_DRIVER_DETAIL
								     WHERE rule_id = 3000
								       AND level1 IN ('{accountmap}', '40100')) dd  
								    ON dd.rule_id = 3000 
								    AND dd.level1 = COALESCE(
								        (SELECT level1 
								         FROM ods.GL_ALLOC_DRIVER_DETAIL 
								         WHERE rule_id = 3000 
								           AND level1 = '{accountmap}'), 
								        '40100') 
                        inner join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id
                        left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
                    """

createNSOob = """

                CREATE TEMP TABLE oobb as
                    SELECT accountnumber,"ID" ,sum(total_var) AS total_var FROM (SELECT accountnumber,ns."ID" ,ns_amt, sum(allocated_amt) ,ns_amt- sum(allocated_amt) AS total_var
                    FROM ods.ns_alloc ns
                    GROUP BY accountnumber,ns."ID",ns_amt) GROUP BY accountnumber,"ID"
                    """

updateNSAlloc = """
            
               update ods.ns_alloc
                set allocated_amt = new_alloc_amt
                FROM(
                select det.id,det.ACCOUNTNUMBER ,det.sku, det.allocated_amt+ case when total_var > 0 then .01 else -.01 end as new_alloc_amt, det.allocated_amt FROM(select id,ACCOUNTNUMBER ,sku, allocated_amt,
                                row_number() OVER  (PARTITION by accountnumber order by allocated_amt desc) as rn
                                from  ods.ns_alloc tab
                                where tab.allocated_amt <> 0 ) det
                                INNER JOIN oobb ON rn<abs(oobb.total_var *100) AND oobb.accountnumber = det.accountnumber AND oobb.accountnumber = det.accountnumber) upd
                        WHERE ns_alloc.ID =upd.id AND ns_alloc.SKU =upd.sku AND ns_alloc.ACCOUNTNUMBER = upd.accountnumber
                     
                                    """

insertNSToGlAllocation = """
                    INSERT into ods.gl_allocation_test(CREATED_AT,RULE_ID,RUN_ID,TRAN_DATE,SALE_DATE,LOCATION_ID,GL_PRODUCT_CATEGORY,ACCT_NUMEBR,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU
                  ,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,ALLOC_AMT)
                    SELECT SYSDATE() AS created_date, rule_id, {runid} AS run_id, '{endDate}' as tran_date, '{endDate}' as sale_date,null as LOCATION, null as CATEGORY, accountnumber
                    ,'NS Allocation' as GROUP_CODE,'2000' as TRAN_TYPE,'201' as TRAN_SUB_TYPE_ID,'2000' as TRAN_SUB_TYPE,SKU ,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME
                    ,sum(allocated_amt) AS tran_amt
                    FROM ods.ns_alloc
                    GROUP BY created_date, rule_id, run_id, tran_date, sale_date,LOCATION, CATEGORY, accountnumber,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME
                        """