
getPeriod = """
            SELECT *,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext
            FROM ods.CAL_LU 
            WHERE FULLDATE = (
                                SELECT MONTH_START_DATE - 1
                                FROM ods.CAL_LU 
                                WHERE FULLDATE = '{current_date}')
			"""

# getPeriod = """
#             SELECT *,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext
#             FROM ods.CAL_LU 
#             WHERE FULLDATE = (
#                                 SELECT MONTH_START_DATE - 1
#                                 FROM ods.CAL_LU 
#                                 WHERE FULLDATE = '2023-07-01')
# 			"""

getRun = """
        SELECT COALESCE(max(run_id),1) as run_id FROM ods.GL_ALLOCATION_TEST
         """

getRules = """
            SELECT *
            FROM ods.gl_allocation_rules
            """

insertNSHeader =""""""

insertNSDetail =""""""


insertAllHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'all' AS level1, 
                'all' AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.V_PROD_TRANSACTIONS trn
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' 
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                GROUP BY period_end_date 
                """
insertVfiTprHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, '{rule}' AS level1, 
                'all' AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.V_PROD_TRANSACTIONS trn
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' 
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                GROUP BY period_end_date 
                """
insertVfiAstprHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, '{rule}' AS level1, 
                'all' AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.V_PROD_TRANSACTIONS trn
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' 
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                GROUP BY period_end_date 
                """
insertVfiRebatesHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, '{rule}' AS level1, 
                'all' AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.V_PROD_TRANSACTIONS trn
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType} and tran_amt >0
                 GROUP BY period_end_date 
                """
insertVfiColdHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, '{rule}' AS level1, 
                'all' AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.V_PROD_TRANSACTIONS trn
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' 
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                GROUP BY period_end_date 
                """
insertVfiFreeFillHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, '{rule}' AS level1, 
                'all' AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.V_PROD_TRANSACTIONS trn
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' 
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                GROUP BY period_end_date 
                """
insertVfiBrandMarketingHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, '{rule}' AS level1, 
                ff.brand_id AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.V_PROD_TRANSACTIONS trn
                left join ods.BRAND_MARKETING ff ON ff.VENDOR_FUNDING__BRAND_MARKE_ID = trn.brand_marketing_id
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' 
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                GROUP BY period_end_date ,ff.brand_id
                """


insertMakeGoodHeader = """
                INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, '{rule}' AS level1, 
                'all' AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                FROM ods.V_PROD_TRANSACTIONS trn
                WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' and trn.tran_amt =0
                AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                GROUP BY period_end_date 
                """

insertLocHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'location' as level1,
                    trn.LOCATION_ID AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.V_PROD_TRANSACTIONS trn
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, trn.LOCATION_ID 
                    """

insertDeptHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'department' as level1,
                    dp.dept_id AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.V_PROD_TRANSACTIONS trn
                    INNER JOIN ods.GL_DEPARTMENT_XREF dp ON dp.LOCATION_ID =trn.location_id AND dp.GROUP_ID = trn.group_id
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY period_end_date, dp.dept_id
                    """
insertCatHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id,  '{endDate}' AS period_end_date, 'category' as level1,
                    gpgx.CATEGORY_NAME AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.V_PROD_TRANSACTIONS trn
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
                        FROM ods.V_PROD_TRANSACTIONS trn
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
                    FROM ods.V_PROD_TRANSACTIONS trn
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
                    FROM ods.V_PROD_TRANSACTIONS trn
                    WHERE  TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType} and trn.tran_gl_date between '{startDate}' and '{endDate}'
                    GROUP BY period_end_date, trn.order_id 
                    """


insertOrderLocHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'order' AS level1, 
                    trn.order_id AS level1_name, 'location' as level2, trn.LOCATION_ID as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.V_PROD_TRANSACTIONS trn
                    WHERE TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType} and trn.tran_gl_date between '{startDate}' and '{endDate}'
                    GROUP BY period_end_date, trn.order_id, trn.LOCATION_ID
                    """

insertOrderCatHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'order' AS level1, 
                    trn.order_id AS level1_name, 'category' as level2,  gpgx.CATEGORY_NAME as level2_name, SYSDATE() AS created_at, 
                    sum(trn.{tranColumn})  AS total_amt ,sum(trn.tran_qty) as total_qty
                    FROM ods.V_PROD_TRANSACTIONS trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    WHERE  TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType} and trn.tran_gl_date between '{startDate}' and '{endDate}'
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
                dje.ACCT_NUMBER as level1_name, null as level2, null as level2_name, SYSDATE() AS created_at, 
                sum(COALESCE(dje.debit_amt,0) - COALESCE(dje.credit_amt,0))  AS total_amt ,0 as total_qty FROM ods.V_DJE  dje 
                INNER JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER = dje.ACCT_NUMBER 
                WHERE dje.TRAN_DATE BETWEEN '{startDate}' AND '{endDate}'  AND na.type_name = 'Income' AND dje.sku IS NOT NULL 
                GROUP BY dje.ACCT_NUMBER                        
                """
# insertShippingHeader = """
#                     INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
#                     SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'shipping' AS level1, sp.ORDER_NUMBER   AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at,
#                     sum(sp.FREIGHT_COST) AS cost ,null as total_qty  FROM staging.STG_T_AL_HOST_SHIPMENT_INFO sp
#                     WHERE sp.ACTUAL_SHIP_DATE::date BETWEEN '{startDate}' AND '{endDate}'
#                     GROUP BY sp.ORDER_NUMBER 
#                     """

insertShippingHeader = """
                    INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'shipping' AS level1, NULL   AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at,
                    sum(sp.FREIGHT_COST) AS cost ,null as total_qty  FROM hj.T_AL_HOST_SHIPMENT_INFO  sp
                    WHERE sp.ACTUAL_SHIP_DATE::date BETWEEN '{startDate}' AND '{endDate}' 
                    """

insertInboundHeader = """
                        INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                    SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'inbound' AS level1, NULL   AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at,
                    sum(freight_costs) AS cost ,sum(shipped_qty) as total_qty  FROM (SELECT tr.sku,ib.inbound_freight_per_unit, sum(tr.tran_qty)AS shipped_qty, sum(tr.tran_qty * COALESCE(ib.inbound_freight_per_unit,0))  AS freight_costs
                        FROM TM_IGLOO_ODS_STG.ods.V_PROD_TRANSACTIONS tr 
                        LEFT JOIN ods.curr_items ci ON ci.fc_id =2  AND ci.item_name = tr.sku
                        LEFT JOIN ods.inbound_freight_costs ib ON ib.sku = tr.SKU 
                        WHERE tr.tran_sub_type_id = 16 and tr.tran_gl_date BETWEEN '{startDate}' AND '{endDate}' 
                        AND ci.del_pickup ='Pickup'
                        GROUP BY tr.sku,ib.inbound_freight_per_unit)

                      """      


insertPackagingHeader = """
                        INSERT INTO ods.gl_alloc_driver_header(rule_id, period_end_date, level1 , level1_name , level2, level2_name, created_at, total_amt,total_qty)
                        SELECT {ruleId} AS rule_id, '{endDate}' AS period_end_date, 'shipping' AS level1, NULL   AS level1_name, null as level2, null as level2_name, SYSDATE() AS created_at,
                        sum(amount) AS cost ,null as total_qty  
                        FROM(SELECT increment_id,sku,sum(round(totalskupackagingcost,2)) AS amount
                        FROM (WITH skucosts AS (WITH geami AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'Geami')
                        ,wat AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'WAT')
                        ,poly AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'Polybag')
                        SELECT item_number, wh_id,
                        sum(COALESCE(gm.bv,0)) AS bv_geami
                        ,sum(COALESCE(gm.rn,0)) AS rn_geami,
                        sum(COALESCE(gm.hn,0)) AS hn_geami,
                        sum(COALESCE(wt.bv,0)) AS bv_wat,
                        sum(COALESCE(wt.rn,0)) AS rn_wat,
                        sum(COALESCE(wt.hn,0)) AS hn_wat,
                        sum(COALESCE(pl.bv,0)) AS bv_poly,
                        sum(COALESCE(pl.rn,0)) AS rn_poly,
                        sum(COALESCE(pl.hn,0)) AS hn_poly
                        FROM hj.T_ITEM_COMMENT ic 
                        LEFT JOIN geami gm ON 'Geami'= ic.COMMENT_TEXT 
                        LEFT JOIN wat wt  ON 'Strapping Tape'= ic.COMMENT_TEXT 
                        LEFT JOIN poly pl ON 'Polybagged'= ic.COMMENT_TEXT 
                        GROUP BY item_number, wh_id)
                        ,fulfillments AS 
                        (SELECT tr.increment_id, tr.sku, sum(tr.tran_qty) AS tran_qty, sum(tran_amt) AS tran_amt, tr.magento_location_id FROM ODS.V_PROD_TRANSACTIONS TR 
                        WHERE TR.tran_sub_type_id = 16
                        AND tr.magento_location_id IN (2,3,11) AND tr.tran_gl_date BETWEEN '{startDate}' and '{endDate}'
                        GROUP BY tr.increment_id, tr.sku,tr.magento_location_id )
                        , boxcosts AS (SELECT order_number,
                        sum(CASE 
                                WHEN shipper = 'BATESVILLE' THEN BV 
                                WHEN shipper = 'SPARKS' THEN RN
                                WHEN shipper = 'HANOVER' THEN HN 
                        END) AS box_cost
                        FROM hj.T_AL_HOST_SHIPMENT_INFO  shp
                        LEFT JOIN STAGING.STG_PACKAGING_COSTS pk ON pk.ITEM =shp.BOX_TYPE 
                        GROUP BY ORDER_NUMBER ),
                        dryice AS (WITH freshfrozenorders AS (
                        SELECT  DISTINCT tr.INCREMENT_ID,'FROZEN' AS class FROM TM_IGLOO_ODS_STG.ods.V_PROD_TRANSACTIONS tr 
                        LEFT JOIN TM_IGLOO_ODS_STG.staging.STG_HJ_ITEM_DATA_RN it ON it.ITEM_NUMBER = tr.SKU 
                        WHERE tr.TRAN_SUB_TYPE_ID  = 16 AND it.CLASS_ID IN ('FROZEN','DEEPFREEZE') AND tr.magento_location_id IN (2,3,11)
                        )
                        SELECT order_number,CASE WHEN container_class = 'FROZEN' THEN COOLANT_WEIGHT /5 ELSE 0 END AS dryiceqty	
                        FROM TM_IGLOO_ODS_STG.hj.T_AL_HOST_SHIPMENT_INFO  shp
                        LEFT JOIN TM_IGLOO_ODS_STG.ods.ORDER_HEADER oh ON oh.INCREMENT_ID =  shp.ORDER_NUMBER 
                        LEFT JOIN (SELECT DISTINCT INCREMENT_ID ,location_id,magento_location_id FROM TM_IGLOO_ODS_STG.ods.V_PROD_TRANSACTIONS WHERE tran_sub_type_id =16 AND magento_location_id IN (2,3,11) ) tr ON tr.increment_id = shp.ORDER_NUMBER 
                        LEFT JOIN TM_IGLOO_ODS_STG.staging.STG_TRANSPORTATION_TRANSIT_TIMES tt ON tt.DESTINATION_ZIP5 = oh.SHIPPING_POSTAL_CODE AND tt.WAREHOUSE_ID = tr.magento_location_id AND LEFT(shp.SERVICE_LEVEL,4) = LEFT(tt.SERVICE_LEVEL,4)
                        LEFT JOIN freshfrozenorders frz ON frz.increment_id = shp.ORDER_NUMBER 
                        LEFT JOIN TM_IGLOO_ODS_STG.ods.NS_FC_XREF ns ON ns.FC_ID =tt.WAREHOUSE_ID 
                        LEFT JOIN TM_IGLOO_ODS_STG.hj.T_THRIVE_INSULATED_TRANSIT_REF re ON re.WH_ID = ns.ods_fc_id AND re.TRANSIT_DAYS = tt.TRANSIT_TIME_IN_DAYS AND frz.class = re.CONTAINER_CLASS 
                        WHERE frz.increment_id IS NOT NULL)
                        ,icecosts AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item ='Dry Ice')
                        , liners AS ( SELECT item,bv,rn,hn,order_number 
                        FROM staging.STG_PACKAGING_COSTS lin
                        INNER JOIN hj.T_AL_HOST_SHIPMENT_INFO  sh ON sh.BOX_TYPE = trim(split_part(item,'-',1))
                        WHERE lin.item LIKE '%MP%'),
                        dividers AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'Divider')
                        , cold AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'Cold Care')
                        ,thermo AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item LIKE '%Thermo%')
                        ,void AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item  = 'Void Fill')
                        SELECT sc.item_number,
                        COALESCE(bv_geami,0)+COALESCE(bv_wat,0) +COALESCE(bv_poly,0) AS bv_cost,
                        COALESCE(rn_geami,0) +COALESCE(rn_wat,0) +COALESCE(rn_poly,0) AS rn_cost,
                        COALESCE(hn_geami,0)+COALESCE(hn_wat,0) +COALESCE(hn_poly,0) AS hn_cost,
                        fl.*, fl.tran_amt/ NULLIF(sum(fl.tran_amt) OVER (PARTITION BY increment_id),0) AS pdgprallocation
                        ,CASE 
                                WHEN magento_location_id = 2 THEN bv_cost * fl.tran_qty
                                WHEN magento_location_id = 3 THEN rn_cost * fl.tran_qty
                                WHEN magento_location_id = 11 THEN rn_cost * fl.tran_qty
                        END AS sku_packaging_cost
                        ,bc.box_cost *pdgprallocation as box_cost_allocated,
                        di.dryiceqty,ic.*,
                        CASE 
                                WHEN magento_location_id = 2 THEN dryiceqty *ic.bv
                                WHEN magento_location_id = 3 THEN dryiceqty *ic.rn
                                WHEN  magento_location_id = 11  THEN dryiceqty * ic.hn
                        END AS dryicecost,
                        it.class_id, 
                        CASE 
                                WHEN it.class_id  IN ('FROZEN','DEEPFREEZE') 
                                THEN fl.tran_amt / nullif(sum(fl.tran_amt) OVER (PARTITION BY increment_id),0) 
                        END AS dryicealloc,
                        COALESCE(dryicecost,0) * COALESCE(dryicealloc,0) AS dryiceskucost,
                        lin.item,lin.bv,lin.rn,lin.hn,cld.*,thm.*,vd.*,
                        CASE 
                                WHEN magento_location_id = 2 THEN cld.bv
                                WHEN magento_location_id = 3 THEN cld.rn
                                WHEN  magento_location_id = 11  THEN cld.hn
                        END AS coldcarecost,
                        CASE 
                                WHEN magento_location_id = 2 THEN thm.bv
                                WHEN magento_location_id = 3 THEN thm.rn
                                WHEN  magento_location_id = 11  THEN thm.hn
                        END AS thermocost,
                        CASE 
                                WHEN magento_location_id = 2 THEN vd.bv
                                WHEN magento_location_id = 3 THEN vd.rn
                                WHEN  magento_location_id = 11  THEN vd.hn
                        END AS voidcost,
                        voidcost *pdgprallocation AS voidcostallocated,
                        COALESCE(dryiceskucost,0) + COALESCE(box_cost_allocated,0) +COALESCE(sku_packaging_cost,0) 
                        +coalesce(coldcarecost,0)+coalesce(thermocost,0)+coalesce(voidcostallocated,0)
                        AS totalskupackagingcost
                        FROM fulfillments fl
                        LEFT JOIN skucosts sc ON fl.sku = sc.item_number
                        LEFT JOIN boxcosts bc ON bc.ordeR_number::varchar = fl.increment_id::varchar
                        LEFT JOIN dryice di ON di.order_number::varchar = fl.increment_id::varchar
                        LEFT JOIN icecosts ic ON di.order_number IS NOT NULL
                        LEFT JOIN TM_IGLOO_ODS_STG.staging.STG_HJ_ITEM_DATA_RN it ON it.ITEM_NUMBER = fl.SKU
                        LEFT JOIN liners lin ON lin.order_number = fl.increment_id::varchar AND it.class_id IN ('FROZEN','DEEPFREEZE')
                        LEFT JOIN cold cld ON it.class_id = 'COLD'
                        LEFT JOIN thermo thm ON it.class_id = 'COLD'
                        LEFT JOIN void vd ON fl.increment_id IS NOT NULL 
                        ORDER BY increment_id)
                        GROUP BY  increment_id,sku
                        HAVING sum(round(totalskupackagingcost,2)) <>0
                        )
                      """                                          

insertAllDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertVfiTprDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """
insertVfiAstprDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertVfiRebatesDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType} and tran_amt >0
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertVfiColdDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertVfiFreeFillDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, ci.item_name, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    left join ods.FREE_FILL ff ON ff.VENDOR_FUNDING__FREE_FILL_B_ID = trn.brand_marketing_id
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON ff.UPC_CODE_ID  = ci.item_id  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, ci.item_name,  gadh.total_amt,gadh.total_qty
                    """





insertMakeGoodDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND 2 = ci.fc_id 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}' and trn.tran_amt = 0  
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertLocDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.location_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertDeptDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    INNER JOIN ods.GL_DEPARTMENT_XREF dp ON dp.LOCATION_ID =trn.location_id AND dp.GROUP_ID = trn.group_id
                    INNER JOIN ods.gl_alloc_driver_header gadh ON dp.dept_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
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
                    FROM ods.V_PROD_TRANSACTIONS trn
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
                    FROM ods.V_PROD_TRANSACTIONS trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.order_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    where TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType} and trn.tran_gl_date between '{startDate}' and '{endDate}'
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertOrderLocDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.order_id::varchar(100) = gadh.level1_name and trn.location_id::varchar(100) = gadh.level2_name AND {ruleId} = gadh.rule_id 
                    WHERE  TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType} and trn.tran_gl_date between '{startDate}' and '{endDate}'
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """

insertOrderCatDetail =   """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
                    LEFT OUTER JOIN ods.GL_PRODUCT_group_XREF gpgx ON trn.GROUP_ID  = gpgx.GROUP_ID  AND trn.LOCATION_ID = GPGX.LOCATION_ID 
                    INNER JOIN ods.gl_alloc_driver_header gadh ON trn.order_id::varchar(100) = gadh.level1_name and  gpgx.CATEGORY_NAME::varchar(100) = gadh.level2_name AND {ruleId} = gadh.rule_id 
                    WHERE  TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType} and trn.tran_gl_date between '{startDate}' and '{endDate}'
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku,  gadh.total_amt,gadh.total_qty
                    """


insertVendorDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                    SELECT   gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, sku, 
                    SYSDATE() AS created_at, sum(trn.{tranColumn}) AS sku_amt,sum(trn.tran_qty) as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_PROD_TRANSACTIONS trn
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
                    FROM ods.V_PROD_TRANSACTIONS trn
                    LEFT OUTER JOIN ods.NS_FC_XREF fc ON fc.NS_FC_ID = trn.LOCATION_ID 
                        LEFT OUTER JOIN ods.CURR_ITEMS ci ON trn.sku  = ci.item_name  AND COALESCE(fc.ODS_FC_ID,2) = ci.fc_id  
                    INNER JOIN ods.gl_alloc_driver_header gadh ON ci.brand_id::varchar(100) = gadh.level1_name AND {ruleId} = gadh.rule_id 
                    WHERE trn.tran_gl_date between '{startDate}' and '{endDate}'
                    AND TRAN_TYPE = {tranType} AND TRAN_SUB_TYPE_ID = {tranSubType}
                    GROUP BY  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, trn.sku, gadh.total_amt,gadh.total_qty
                    """

insertVfiBrandMarketingDetail = """
                    INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)     
                    SELECT gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME,dd.sku, sysdate() AS created_at,sum(tr.tran_amt) * dd.ALLOC_PCT AS sku_amt ,0 AS sku_qty, dd.ALLOC_PCT / sum(dd.alloc_pct) over () as alloc,0 AS qty_pct  FROM ods.V_PROD_TRANSACTIONS tr
                    left join ods.BRAND_MARKETING ff ON ff.VENDOR_FUNDING__BRAND_MARKE_ID = tr.brand_marketing_id
                    inner JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.LEVEL1::varchar =ff.BRAND_ID::varchar AND dd.rule_id = '202' 
                    inner JOIN ods.gl_alloc_driver_header gadh ON  {ruleId} = gadh.rule_id AND gadh.LEVEL1_NAME::varchar =ff.BRAND_ID ::varchar
                    WHERE TRAN_TYPE ={tranType}  AND TRAN_SUB_TYPE_ID = {tranSubType} AND tran_gl_date between '{startDate}' and '{endDate}'
                    GROUP BY ff.BRAND_ID ,dd.sku, dd.ALLOC_PCT ,gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME                
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
                    SELECT gadh.id,gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, dje.sku, 
                    SYSDATE() AS created_at, sum(COALESCE(dje.debit_amt,0) - COALESCE(dje.credit_amt,0))  AS sku_amt,0 as sku_qty,
                    case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                    case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                    FROM ods.V_DJE dje
                    INNER JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER = dje.ACCT_NUMBER  
                    INNER JOIN ods.gl_alloc_driver_header gadh ON '{ruleId}'= gadh.rule_id AND dje.acct_number = gadh.level1_name
                    WHERE dje.TRAN_DATE BETWEEN '{startDate}' and '{endDate}' AND na.type_name = 'Income' AND dje.sku IS NOT NULL 
                    GROUP BY gadh.id,gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, dje.sku, gadh.total_amt,gadh.total_qty
                    """

# insertShippingDetail =   """
#                         INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
#                         WITH skuweight AS(SELECT tr.sku,tr.increment_id,sum(costperlb*(unit_weight*tr.tran_qty)) AS weightcost 
#                         FROM ods."TRANSACTIONS" tr
#                                 LEFT JOIN (
#                         SELECT ORDER_NUMBER,sum(RATED_WEIGHT) AS rated_wegith, sum(ACTUAL_WEIGHT) AS actweight, sum(FREIGHT_COST) AS freight ,sum(FREIGHT_COST)/sum(ACTUAL_WEIGHT) AS costperlb FROM STAGING.STG_T_AL_HOST_SHIPMENT_INFO 
#                         WHERE ACTUAL_SHIP_DATE::date BETWEEN '{startDate}' AND '{endDate}'
#                           GROUP BY ORDER_NUMBER )fpp ON fpp.order_number::varchar = tr.INCREMENT_ID::varchar 
#                                  LEFT JOIN (SELECT item_number ,unit_weight FROM hj.T_ITEM_MASTER )im ON im.item_number = tr.sku
#                         WHERE tr.TRAN_SUB_TYPE_ID =16 
#                         AND fpp.order_number IS NOT NULL AND tr.GROUP_ID NOT IN ('170')
#                         GROUP BY tr.sku,tr.increment_id)
#                                 SELECT gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.level2_NAME, sku, 
#                         SYSDATE() AS created_at,sk.weightcost AS sku_amt,0 as sku_qty,
#                         case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
#                         case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct FROM skuweight sk 
#                                 LEFT JOIN ods.GL_ALLOC_DRIVER_HEADER gadh ON gadh.rule_id = {ruleId} and gadh.level1_name = sk.increment_id

#                     """

insertShippingDetail =   """
                        INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt, sku_qty, alloc_pct,qty_alloc_pct)
                        SELECT ID,RULE_ID,ORDER_NUMBER,LEVEL2_NAME,SKU,CREATED_AT,SKU_AMT,SKU_QTY,alloc/ sum(alloc) over() AS allocation,QTY_ALLOC_PCT FROM
                        (WITH shippinginfo AS (SELECT ORDER_NUMBER,sum(ACTUAL_WEIGHT) AS weight, sum(FREIGHT_COST) AS cost, cost/weight AS costperlb  FROM hj.T_AL_HOST_SHIPMENT_INFO  WHERE ACTUAL_SHIP_DATE ::date BETWEEN '{startDate}' AND '{endDate}'
                        GROUP BY ORDER_NUMBER )
                        SELECT  gadh.id, gadh.rule_id,shp.order_number,gadh.level2_NAME,tr.SKU,SYSDATE() AS created_at,
                        (((tr.tran_qty * uom_weight) * (weight / sum((tr.tran_qty * uom_weight)) OVER (PARTITION BY order_number) ) )/weight) * cost AS sku_amt,
                        0 as sku_qty,
                        (((tr.tran_qty * uom_weight) * (weight / sum((tr.tran_qty * uom_weight)) OVER (PARTITION BY order_number) ) )/weight) * cost / (SELECT sum(cost) FROM shippinginfo) AS alloc
                        ,0 AS qty_alloc_pct FROM shippinginfo shp
                        LEFT JOIN ods.V_PROD_TRANSACTIONS tr ON shp.order_number::varchar = tr.INCREMENT_ID::varchar 
                        LEFT JOIN (SELECT item_number,max(UOM_WEIGHT) AS uom_weight ,CLASS_ID FROM (SELECT * FROM staging.STG_HJ_ITEM_DATA_BV 
                        UNION ALL
                        SELECT * FROM staging.STG_HJ_ITEM_DATA_HA 
                        UNION ALL 
                        SELECT * FROM STAGING.STG_HJ_ITEM_DATA_RN
                        UNION ALL 
                        SELECT DISTINCT item_master_id ,ITEM_NUMBER, NULL AS wh_id,uom,UOM_WEIGHT,CLASS_ID FROM staging.STG_T_ITEM_UOM 
                        WHERE uom ='EA')GROUP BY item_number,class_id)im ON im.item_number = tr.sku
                        LEFT JOIN ods.GL_ALLOC_DRIVER_HEADER gadh ON gadh.rule_id = {ruleId}
                        WHERE tr.TRAN_SUB_TYPE_ID =16 AND tr.LOCATION_ID IN (3,4,21)   )               """

insertInboundDetail =   """
                INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                SELECT  gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, tr.sku,
                                SYSDATE() AS created_at,sum(tr.tran_qty * COALESCE(ib.inbound_freight_per_unit,0))  AS sku_amt,
                                sum(tr.tran_qty) as sku_qty,
                                case when coalesce(gadh.total_amt, 0) <> 0 then sku_amt/gadh.total_amt else 0 end AS alloc_pct,
                                case when coalesce(gadh.total_qty, 0) <> 0 then sku_qty/gadh.total_qty else 0 end AS qty_alloc_pct
                FROM TM_IGLOO_ODS_STG.ods.V_PROD_TRANSACTIONS tr 
                LEFT JOIN ods.curr_items ci ON ci.fc_id =2  AND ci.item_name = tr.sku
                LEFT JOIN ods.inbound_freight_costs ib ON ib.sku = tr.SKU 
                INNER JOIN ods.GL_ALLOC_DRIVER_HEADER gadh ON gadh.RULE_ID ='{ruleId}'
                WHERE tr.tran_sub_type_id = 16 and tr.tran_gl_date BETWEEN '{startDate}' AND '{endDate}' 
                AND ci.del_pickup ='Pickup'
                GROUP BY gadh.id, gadh.rule_id, gadh.LEVEL1_NAME, gadh.LEVEL2_NAME, tr.sku,  gadh.total_amt,gadh.total_qty
                                """


insertPackagingDetail =   """
                INSERT INTO ods.gl_alloc_driver_detail(gah_id, rule_id, level1, level2, sku, created_at, sku_amt,sku_qty, alloc_pct,qty_alloc_pct)
                WITH packaging AS ((SELECT increment_id,sku,sum(round(totalskupackagingcost,2)) AS amount
                FROM (WITH skucosts AS (WITH geami AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'Geami')
                ,wat AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'WAT')
                ,poly AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'Polybag')
                SELECT item_number, wh_id,
                sum(COALESCE(gm.bv,0)) AS bv_geami
                ,sum(COALESCE(gm.rn,0)) AS rn_geami,
                sum(COALESCE(gm.hn,0)) AS hn_geami,
                sum(COALESCE(wt.bv,0)) AS bv_wat,
                sum(COALESCE(wt.rn,0)) AS rn_wat,
                sum(COALESCE(wt.hn,0)) AS hn_wat,
                sum(COALESCE(pl.bv,0)) AS bv_poly,
                sum(COALESCE(pl.rn,0)) AS rn_poly,
                sum(COALESCE(pl.hn,0)) AS hn_poly
                FROM hj.T_ITEM_COMMENT ic 
                LEFT JOIN geami gm ON 'Geami'= ic.COMMENT_TEXT 
                LEFT JOIN wat wt  ON 'Strapping Tape'= ic.COMMENT_TEXT 
                LEFT JOIN poly pl ON 'Polybagged'= ic.COMMENT_TEXT 
                GROUP BY item_number, wh_id)
                ,fulfillments AS 
                (SELECT tr.increment_id, tr.sku, sum(tr.tran_qty) AS tran_qty, sum(tran_amt) AS tran_amt, tr.magento_location_id FROM ODS.V_PROD_TRANSACTIONSTR 
                WHERE TR.tran_sub_type_id = 16
                AND tr.magento_location_id IN (2,3,11) AND tr.tran_gl_date BETWEEN '{startDate}' AND '{endDate}' 
                GROUP BY tr.increment_id, tr.sku,tr.magento_location_id )
                , boxcosts AS (SELECT order_number,
                sum(CASE 
                        WHEN shipper = 'BATESVILLE' THEN BV 
                        WHEN shipper = 'SPARKS' THEN RN
                        WHEN shipper = 'HANOVER' THEN HN 
                END) AS box_cost
                FROM hj.T_AL_HOST_SHIPMENT_INFO  shp
                LEFT JOIN STAGING.STG_PACKAGING_COSTS pk ON pk.ITEM =shp.BOX_TYPE 
                GROUP BY ORDER_NUMBER ),
                dryice AS (WITH freshfrozenorders AS (
                SELECT  DISTINCT tr.INCREMENT_ID,'FROZEN' AS class FROM TM_IGLOO_ODS_STG.ods.V_PROD_TRANSACTIONS tr 
                LEFT JOIN TM_IGLOO_ODS_STG.staging.STG_HJ_ITEM_DATA_RN it ON it.ITEM_NUMBER = tr.SKU 
                WHERE tr.TRAN_SUB_TYPE_ID  = 16 AND it.CLASS_ID IN ('FROZEN','DEEPFREEZE') AND tr.magento_location_id IN (2,3,11)
                )
                SELECT order_number,CASE WHEN container_class = 'FROZEN' THEN COOLANT_WEIGHT /5 ELSE 0 END AS dryiceqty	
                FROM TM_IGLOO_ODS_STG.hj.T_AL_HOST_SHIPMENT_INFO  shp
                LEFT JOIN TM_IGLOO_ODS_STG.ods.ORDER_HEADER oh ON oh.INCREMENT_ID =  shp.ORDER_NUMBER 
                LEFT JOIN (SELECT DISTINCT INCREMENT_ID ,location_id,magento_location_id FROM TM_IGLOO_ODS_STG.ods.V_PROD_TRANSACTIONS WHERE tran_sub_type_id =16 AND magento_location_id IN (2,3,11) ) tr ON tr.increment_id = shp.ORDER_NUMBER 
                LEFT JOIN TM_IGLOO_ODS_STG.staging.STG_TRANSPORTATION_TRANSIT_TIMES tt ON tt.DESTINATION_ZIP5 = oh.SHIPPING_POSTAL_CODE AND tt.WAREHOUSE_ID = tr.magento_location_id AND LEFT(shp.SERVICE_LEVEL,4) = LEFT(tt.SERVICE_LEVEL,4)
                LEFT JOIN freshfrozenorders frz ON frz.increment_id = shp.ORDER_NUMBER 
                LEFT JOIN TM_IGLOO_ODS_STG.ods.NS_FC_XREF ns ON ns.FC_ID =tt.WAREHOUSE_ID 
                LEFT JOIN TM_IGLOO_ODS_STG.hj.T_THRIVE_INSULATED_TRANSIT_REF re ON re.WH_ID = ns.ods_fc_id AND re.TRANSIT_DAYS = tt.TRANSIT_TIME_IN_DAYS AND frz.class = re.CONTAINER_CLASS 
                WHERE frz.increment_id IS NOT NULL)
                ,icecosts AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item ='Dry Ice')
                , liners AS ( SELECT item,bv,rn,hn,order_number 
                FROM staging.STG_PACKAGING_COSTS lin
                INNER JOIN hj.T_AL_HOST_SHIPMENT_INFO  sh ON sh.BOX_TYPE = trim(split_part(item,'-',1))
                WHERE lin.item LIKE '%MP%'),
                dividers AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'Divider')
                , cold AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item = 'Cold Care')
                ,thermo AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item LIKE '%Thermo%')
                ,void AS (SELECT * FROM staging.STG_PACKAGING_COSTS WHERE item  = 'Void Fill')
                SELECT sc.item_number,
                COALESCE(bv_geami,0)+COALESCE(bv_wat,0) +COALESCE(bv_poly,0) AS bv_cost,
                COALESCE(rn_geami,0) +COALESCE(rn_wat,0) +COALESCE(rn_poly,0) AS rn_cost,
                COALESCE(hn_geami,0)+COALESCE(hn_wat,0) +COALESCE(hn_poly,0) AS hn_cost,
                fl.*, fl.tran_amt/ NULLIF(sum(fl.tran_amt) OVER (PARTITION BY increment_id),0) AS pdgprallocation
                ,CASE 
                        WHEN magento_location_id = 2 THEN bv_cost * fl.tran_qty
                        WHEN magento_location_id = 3 THEN rn_cost * fl.tran_qty
                        WHEN magento_location_id = 11 THEN rn_cost * fl.tran_qty
                END AS sku_packaging_cost
                ,bc.box_cost *pdgprallocation as box_cost_allocated,
                di.dryiceqty,ic.*,
                CASE 
                        WHEN magento_location_id = 2 THEN dryiceqty *ic.bv
                        WHEN magento_location_id = 3 THEN dryiceqty *ic.rn
                        WHEN  magento_location_id = 11  THEN dryiceqty * ic.hn
                END AS dryicecost,
                it.class_id, 
                CASE 
                        WHEN it.class_id  IN ('FROZEN','DEEPFREEZE') 
                        THEN fl.tran_amt / nullif(sum(fl.tran_amt) OVER (PARTITION BY increment_id),0) 
                END AS dryicealloc,
                COALESCE(dryicecost,0) * COALESCE(dryicealloc,0) AS dryiceskucost,
                lin.item,lin.bv,lin.rn,lin.hn,cld.*,thm.*,vd.*,
                CASE 
                        WHEN magento_location_id = 2 THEN cld.bv
                        WHEN magento_location_id = 3 THEN cld.rn
                        WHEN  magento_location_id = 11  THEN cld.hn
                END AS coldcarecost,
                CASE 
                        WHEN magento_location_id = 2 THEN thm.bv
                        WHEN magento_location_id = 3 THEN thm.rn
                        WHEN  magento_location_id = 11  THEN thm.hn
                END AS thermocost,
                CASE 
                        WHEN magento_location_id = 2 THEN vd.bv
                        WHEN magento_location_id = 3 THEN vd.rn
                        WHEN  magento_location_id = 11  THEN vd.hn
                END AS voidcost,
                voidcost *pdgprallocation AS voidcostallocated,
                COALESCE(dryiceskucost,0) + COALESCE(box_cost_allocated,0) +COALESCE(sku_packaging_cost,0) 
                +coalesce(coldcarecost,0)+coalesce(thermocost,0)+coalesce(voidcostallocated,0)
                AS totalskupackagingcost
                FROM fulfillments fl
                LEFT JOIN skucosts sc ON fl.sku = sc.item_number
                LEFT JOIN boxcosts bc ON bc.ordeR_number::varchar = fl.increment_id::varchar
                LEFT JOIN dryice di ON di.order_number::varchar = fl.increment_id::varchar
                LEFT JOIN icecosts ic ON di.order_number IS NOT NULL
                LEFT JOIN TM_IGLOO_ODS_STG.staging.STG_HJ_ITEM_DATA_RN it ON it.ITEM_NUMBER = fl.SKU
                LEFT JOIN liners lin ON lin.order_number = fl.increment_id::varchar AND it.class_id IN ('FROZEN','DEEPFREEZE')
                LEFT JOIN cold cld ON it.class_id = 'COLD'
                LEFT JOIN thermo thm ON it.class_id = 'COLD'
                LEFT JOIN void vd ON fl.increment_id IS NOT NULL 
                ORDER BY increment_id)
                GROUP BY  increment_id,sku
                HAVING sum(round(totalskupackagingcost,2)) <>0
                ))
                SELECT  gadh.id, gadh.rule_id,increment_id,gadh.level2_NAME,sku,SYSDATE() AS created_at,  amount AS sku_amt, NULL AS sku_qty,
                amount / gadh.total_amt AS alloc_pct,NULL AS qtyalloc
                FROM packaging pkg 
                LEFT JOIN ods.GL_ALLOC_DRIVER_HEADER gadh ON gadh.rule_id = {ruleId}
                                """                                

getMapODS ="""
        SELECT * FROM ods.GL_ALLOCATION_MAP where type ='ODS'
        """


getMapNS ="""
        SELECT * FROM ods.GL_ALLOCATION_MAP where type ='NS'
        """

createTranAlloc = """
                insert into ods.gl_alloc  (
                BATCH_ID, MAP_ID, TRAN_ID, ACCT_NUMBER, TRAN_DATE, 
                GROUP_CODE, TRAN_TYPE, TRAN_SUB_TYPE_ID ,TRAN_SUB_TYPE, TRAN_TIME, ORDER_ID,ORDER_LINE_ID, 
                SALE_DATE, INCREMENT_ID, ALLOC_RULE_ID, CATEGORY, 
                LOCATION, SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME, SKU_AMT, ALLOC_PCT, TOTAL_AMT, TRAN_AMT
                ) 
                SELECT dje.BATCH_ID,dje.MAP_ID,dje.TRAN_ID,dje.ACCT_NUMBER,dje.TRAN_DATE,dje.GROUP_CODE,{tran_type},{tran_sub_type_id} ,'{tran_sub_type}',dje.TRAN_TIME,dje.ORDER_ID,null,dje.SALE_DATE,dje.INCREMENT_ID,mp.ALLOC_RULE_ID,mp.CATEGORY,mp.LOCATION
                ,dd.SKU
                ,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                ,dd.SKU_AMT,
                CASE 
                WHEN SUM(dd.SKU_AMT) OVER (PARTITION BY dje.ORDER_ID) = 0 THEN dd.qty_alloc_pct
                ELSE dd.alloc_pct 
                END AS allocation_pct, COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0) AS total_amt, round((COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0)) * allocation_pct,2) AS tran_amt 
                FROM  ods.V_DJE dje
                INNER JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER  = dje.ACCT_NUMBER 
                INNER JOIN ods.GL_ALLOCATION_MAP mp ON mp.DJE_MAP_ID = dje.map_id   {join2}
                INNER JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.RULE_ID = mp.alloc_rule_id AND dd.LEVEL1 = dje.ORDER_ID::varchar {join}
                INNER JOIN ods.V_PROD_TRANSACTIONS tr ON tr.TRAN_ID  =dje.TRAN_ID 
                LEFT JOIN ods.CURR_ITEMS ci ON ci.item_name = dd.sku AND ci.FC_ID = 2
                WHERE dje.TRAN_DATE BETWEEN '{startDate}' and '{endDate}' AND  na.TYPE_NAME IN ('Cost of Goods Sold','Income')
                and dje.map_id = '{mapId}' and dd.rule_id = '{ruleId}'
                """

createTranErrors ="""
                insert into  ods.gl_alloc  (
                BATCH_ID, MAP_ID, TRAN_ID, ACCT_NUMBER, TRAN_DATE, 
                GROUP_CODE, TRAN_TYPE, TRAN_SUB_TYPE_ID ,TRAN_SUB_TYPE, TRAN_TIME, ORDER_ID, ORDER_LINE_ID,
                SALE_DATE, INCREMENT_ID, ALLOC_RULE_ID, CATEGORY, 
                LOCATION, SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME, SKU_AMT, ALLOC_PCT, TOTAL_AMT, TRAN_AMT
                )
                WITH failures AS (SELECT dje.tran_id
                FROM  ods.V_DJE dje
                LEFT JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER  = dje.ACCT_NUMBER 
                LEFT JOIN ods.GL_ALLOCATION_MAP mp ON mp.DJE_MAP_ID = dje.map_id {join2}
                LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.RULE_ID = mp.alloc_rule_id AND dd.LEVEL1 = dje.ORDER_ID::varchar {join}
                WHERE dje.TRAN_DATE BETWEEN '{startDate}' and '{endDate}' AND  na.TYPE_NAME IN ('Cost of Goods Sold','Income')
                and dje.map_id ='{mapId}'  AND dd.SKU IS NULL)
                SELECT dje.BATCH_ID,dje.MAP_ID,dje.TRAN_ID,dje.ACCT_NUMBER,dje.TRAN_DATE,dje.GROUP_CODE,{tran_type},{tran_sub_type_id} ,'{tran_sub_type}',dje.TRAN_TIME,dje.ORDER_ID,null,dje.SALE_DATE,dje.INCREMENT_ID,2002,null,null
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
                INNER JOIN ods.V_PROD_TRANSACTIONS tr ON tr.TRAN_ID  =dje.TRAN_ID 
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
                                where tab.tran_amt <> 0 and TRAN_DATE  BETWEEN '{startDate}' and '{endDate}' ) det
                                INNER JOIN oob ON rn<abs(oob.total_var *100) AND oob.acct_number = det.acct_number) upd
                        WHERE gl_alloc.TRAN_ID =upd.tran_id AND gl_alloc.SKU =upd.sku
                """

# insertToGlAllocation = """
#                     INSERT into ods.gl_allocation_test(CREATED_AT,RULE_ID,RUN_ID,TRAN_DATE,SALE_DATE,LOCATION_ID,GL_PRODUCT_CATEGORY,ACCT_NUMEBR,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU,
#                     GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,ALLOC_AMT)
#                     SELECT SYSDATE() AS created_date, alloc_rule_id AS rule_id, {runid} AS run_id, tran_date, sale_date,LOCATION, CATEGORY, ACCT_NUMBER,GROUP_CODE,{tran_type} as tran_type,{tran_sub_type_id} as tran_sub_type_id ,'{tran_sub_type}' as tran_sub_type,SKU,
#                     GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,sum(TRAN_AMT) AS tran_amt
#                     FROM gl_alloc where tran_date between '{startDate}' and '{endDate}'
#                     GROUP BY created_date, rule_id, run_id, tran_date, sale_date,LOCATION, CATEGORY, ACCT_NUMBER,GROUP_CODE,tran_type,tran_sub_type_id ,tran_sub_type,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME
#                     """


catchAllocation = """
                 insert into  ods.gl_alloc  (  BATCH_ID, MAP_ID, TRAN_ID, ACCT_NUMBER, TRAN_DATE, 
                GROUP_CODE, TRAN_TYPE, TRAN_SUB_TYPE_ID ,TRAN_SUB_TYPE, TRAN_TIME, ORDER_ID, ORDER_LINE_ID,
                SALE_DATE, INCREMENT_ID, ALLOC_RULE_ID, CATEGORY, 
                LOCATION, SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME, SKU_AMT, ALLOC_PCT, TOTAL_AMT, TRAN_AMT
                )
                WITH missing AS (WITH journals AS (
                SELECT dje.ACCT_NUMBER AS acco, dje.tran_id AS tran,  
                        SUM(CASE WHEN dje.DEBIT_AMT IS NULL THEN 0 ELSE dje.DEBIT_AMT END - 
                        CASE WHEN dje.CREDIT_AMT IS NULL THEN 0 ELSE dje.CREDIT_AMT END) AS net  
                FROM ods.V_DJE dje
                INNER JOIN ods.NETSUITE_ACCOUNTS na ON na.ACCOUNTNUMBER = dje.ACCT_NUMBER 
                WHERE dje.TRAN_DATE BETWEEN '{startDate}' and '{endDate}'
                AND na.type_name IN ('Cost of Goods Sold', 'Income') 
                AND dje.sku IS NULL
                GROUP BY dje.ACCT_NUMBER, dje.tran_id
                ),
                allocation AS (
                SELECT gl.tran_id, gl.acct_number, SUM(gl.tran_amt) AS alloc_amt 
                FROM ods.GL_ALLOC gl where gl.tran_date between '{startDate}' and '{endDate}'
                GROUP BY gl.tran_id, gl.acct_number
                )
                SELECT  DISTINCT je.tran
                FROM journals je
                LEFT JOIN allocation al ON je.tran = al.tran_id AND je.acco = al.acct_number
                WHERE je.acco NOT IN ('41221', '40105', '41106', '41112') 
                AND al.alloc_amt IS NULL)
                SELECT dje.BATCH_ID,dje.MAP_ID,dje.TRAN_ID,dje.ACCT_NUMBER,dje.TRAN_DATE,dje.GROUP_CODE,'300','119' ,'Catch Allocation',dje.TRAN_TIME,dje.ORDER_ID,null,dje.SALE_DATE,dje.INCREMENT_ID,
                '1',null,null,dd.SKU,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME,
                dd.SKU_AMT,CASE 
                                WHEN SUM(dd.SKU_AMT) OVER (PARTITION BY dje.ORDER_ID) = 0 THEN dd.qty_alloc_pct
                                ELSE dd.alloc_pct 
                                END AS  allocation_pct, COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0) AS total_amt, round((COALESCE(dje.DEBIT_AMT,0)- COALESCE(dje.CREDIT_AMT,0)) * allocation_pct,2) AS tran_amt 
                                FROM ods.V_DJE dje 
                INNER JOIN ods.NETSUITE_ACCOUNTS na ON na.accountnumber = dje.acct_number
                LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id =1
                INNER JOIN ods.V_PROD_TRANSACTIONS tr ON tr.TRAN_ID  =dje.TRAN_ID 
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
                insert into ods.ns_alloc(ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE, TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                SELECT ACCOUNTNAME,0,cm.unique_key,null,3001,NULL,NULL,ci.ITEM_NAME,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME,sysdate(),cm.amount,inv.relative_amount,0,0,
               (cm.amount *inv.relative_amount) AS alloc_amt ,'{tran_type}','{tran_sub_type}','{tran_sub_type_id}'
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                INNER JOIN (
                SELECT transaction_id ,item_id,amount,
                amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
                WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null 
                ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
                LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                WHERE inv.transaction_id IS NOT NULL and cm.PERIODNAME = '{period}' and ACCOUNTNAME::varchar = '{accountnumber}' and tran_allocated = 'N'
                 """,
                 "0a - Update table to mark allocated transaction":
                 """
                update ods.ns_cm_gl_activity cm
                set cm.tran_allocated = 'Y'
                from( SELECT distinct cm.unique_key
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                INNER JOIN (
                SELECT transaction_id ,item_id,amount,
                amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
                WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null
                ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
                LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                WHERE inv.transaction_id IS NOT NULL and cm.PERIODNAME = '{period}' and ACCOUNTNAME::varchar = '{accountnumber}' and tran_allocated = 'N') upd
                where upd.unique_key  = cm.unique_key 
                 """,
                 "1 - Netsuite Transaction with Item Level Detatil - Non VFI":
                 """
                insert into ods.ns_alloc(ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE, TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                SELECT ACCOUNTNAME,0,cm.unique_key,null,3001,NULL,NULL,ci.ITEM_NAME,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME,sysdate(),cm.amount,1,0,0,cm.amount
                ,'{tran_type}','{tran_sub_type}','{tran_sub_type_id}'
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (SELECT DISTINCT ns.ITEM_ID FROM ods.NS_CM_GL_ACTIVITY ns 
                INNER JOIN ods.CURR_ITEMS ci ON ci.ITEM_ID ::varchar = ns.ITEM_ID::varchar AND ci.FC_ID =2 AND ci.ITEM_TYPE ='Other Charge'
                ) and cm.PERIODNAME = '{period}' and ACCOUNTNAME::varchar = '{accountnumber}' and '{tran_sub_type_id}' not in ('219') and tran_allocated = 'N'
                """,
                 "1a - Netsuite Transaction with Item Level Detatil - Non VFI (Mark Transaction)":
                 """
                                 update ods.ns_cm_gl_activity cm
                set cm.tran_allocated = 'Y'
                from(
                SELECT distinct cm.unique_key
                FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (SELECT DISTINCT ns.ITEM_ID FROM ods.NS_CM_GL_ACTIVITY ns 
                INNER JOIN ods.CURR_ITEMS ci ON ci.ITEM_ID ::varchar = ns.ITEM_ID::varchar AND ci.FC_ID =2 AND ci.ITEM_TYPE ='Other Charge'
                ) and cm.PERIODNAME = '{period}' and ACCOUNTNAME::varchar = '{accountnumber}' and tran_allocated = 'N') upd
                where upd.unique_key  = cm.unique_key 
                """,
                # "2 - Netsuite Transaction with Item Level Detatil - VFI":
                #  """
                # insert into ods.ns_alloc (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE, TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                # SELECT ACCOUNTNAME,0,cm.unique_key,null,3001,NULL,NULL,trim(SPLIT_PART(cm.SKU,'-',1)),ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME, sysdate() ,cm.amount,1,0,1,cm.amount
                # ,'{tran_type}','{tran_sub_type}','{tran_sub_type_id}'
                # FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                # LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.item_name = trim(SPLIT_PART(cm.SKU,'-',1))::varchar AND ci.FC_ID =2
                # LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                # WHERE cm.item_id IS NOT NULL AND cm.item_id IN (167942,23431,46805,150000,31184,149999,31185,150001,31091,149998) and cm.PERIODNAME = '{period}' and ACCOUNTNAME::varchar = '{accountnumber}' and tran_allocated = 'N'
                # ""","2a - Netsuite Transaction with Item Level Detatil - VFI(Mark Transaction)":
                #  """
                # update ods.ns_cm_gl_activity cm
                # set cm.tran_allocated = 'Y'
                # from(SELECT cm.unique_key
                # FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                # LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.item_name = trim(SPLIT_PART(cm.SKU,'-',1))::varchar AND ci.FC_ID =2
                # LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                # WHERE cm.item_id IS NOT NULL AND cm.item_id IN (167942,23431,46805,150000,31184,149999,31185,150001,31091,149998) and cm.PERIODNAME = '{period}' and ACCOUNTNAME::varchar = '{accountnumber}' and tran_allocated = 'N') upd
                # where upd.unique_key  = cm.unique_key """,
                "3 - Netsuite Transaction allocated by vendor":
                """
                insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE, TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                SELECT cm.ACCOUNTNAME,cm.AMOUNT,cm.unique_key,dd.GAH_ID,dd.RULE_ID,dd.LEVEL1,dd.LEVEL2,dd.sku,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                ,sysdate(),dd.SKU_AMT,dd.ALLOC_PCT,dd.SKU_QTY,dd.QTY_ALLOC_PCT,
                case 
                        when gadh.total_amt <>0 then cm.amount*dd.ALLOC_PCT
                        else cm.amount*dd.QTY_ALLOC_PCT end as allocamt 
                        ,'{tran_type}','{tran_sub_type}','{tran_sub_type_id}'
                        FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                INNER JOIN ods.gl_alloc_driver_header gadh ON gadh.id = dd.gah_id
                left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2
                WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL  and cm.PERIODNAME = '{period}' and ACCOUNTNAME::varchar = '{accountnumber}' and tran_allocated = 'N' """,
                "3a - Netsuite Transaction allocated by vendor":
                 """
                update ods.ns_cm_gl_activity cm
                set cm.tran_allocated = 'Y'
                from( SELECT cm.unique_key FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
                INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
                LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                INNER JOIN ods.gl_alloc_driver_header gadh ON gadh.id = dd.gah_id
                left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2
                WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL  and cm.PERIODNAME = '{period}' and ACCOUNTNAME::varchar = '{accountnumber}' and tran_allocated = 'N') upd
                where upd.unique_key  = cm.unique_key """
                # ,
                # "4 - Outbound Freight Allocation":
                # """
                # insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE, TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                # WITH outbound AS (SELECT periodname ,ACCOUNTNUMBER, sum(AMOUNT) AS AMOUNT FROM ods.NS_CM_GL_ACTIVITY GROUP BY periodname, accountnumber
                # having accountnumber = {accountnumber})
                # SELECT cm.ACCOUNTNUMBER,cm.AMOUNT,NULL,dd.GAH_ID,dd.RULE_ID,dd.LEVEL1,dd.LEVEL2,dd.sku,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                # ,sysdate(),dd.SKU_AMT,dd.ALLOC_PCT,dd.SKU_QTY,dd.QTY_ALLOC_PCT,
                # case 
                #         when gadh.total_amt <>0 then cm.amount*dd.ALLOC_PCT
                #         else cm.amount*dd.QTY_ALLOC_PCT end as allocamt 
                # ,'{tran_type}','{tran_sub_type}','{tran_sub_type_id}'
                # FROM outbound cm
                # INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 4000 
                # LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
                # INNER JOIN ods.gl_alloc_driver_header gadh ON gadh.id = dd.gah_id
                # left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2
                # WHERE dd.sku IS NOT NULL  and cm.PERIODNAME = '{period}' and accountnumber in (SELECT DISTINCT accountnumber FROM ods.GL_ALLOCATION_MAP WHERE alloc_rule_id = 4000 )
                # """
                }

# createNoSkuAlloc= """
#                         insert into ods.ns_alloc(CREATED_AT,RULE_ID,accountnumber,SKU,ALLOCATED_AMT,TRAN_TYPE, TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
#                         SELECT sysdate(),9999,acct_number,'GL Activity', sum(amount) AS net, '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM (WITH netsuiteallocations  AS (WITH itemlevelns AS (SELECT DISTINCT transaction_id FROM (SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,(cm.amount *inv.relative_amount) AS alloc_amt FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN (
#                         SELECT transaction_id ,item_id,amount,
#                         amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
#                         WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null
#                         ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE inv.transaction_id IS NOT NULL and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),9999,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),9999,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',trim(SPLIT_PART(cm.SKU,'-',1)),cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.item_name = trim(SPLIT_PART(cm.SKU,'-',1))::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),9999,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',dd.sku,(cm.amount*COALESCE(dd.ALLOC_PCT,dd.QTY_ALLOC_PCT)) AS amount  
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL  and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'))
#                         SELECT *  FROM ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN itemlevelns it ON it.transaction_id = cm.transaction_id 
#                         WHERE cm.ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME =  '{period}' AND it.transaction_id IS null)
#                         SELECT * FROM netsuiteallocations na
#                         LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON na.accountname::varchar = mp.accountnumber::varchar 
#                         where mp.ns_trans_allocation not in ('N') or  mp.tran_sub_type_id is null)
#                         GROUP BY acct_number
#                      """
createNoSkuAlloc=       {"0 - allocate":
                         """insert into ods.ns_alloc(CREATED_AT,RULE_ID,accountnumber,SKU,ALLOCATED_AMT,TRAN_TYPE, TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                                SELECT sysdate(),9999,acct_number,'GL Activity', sum(amount) AS net, '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM ods.NS_CM_GL_ACTIVITY  cm
                                LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON cm.accountname::varchar = mp.accountnumber::varchar 
                                where (mp.ns_trans_allocation not in ('N') or  mp.tran_sub_type_id is NULL) AND cm.tran_allocated = 'N' AND cm.PERIODNAME = '{period}'
                                GROUP BY acct_number
                                having acct_number ='{accountnumber}'
                                """,
                        "1- update ns allocation table ":"""
                        update ods.ns_cm_gl_activity cm
                        set cm.tran_allocated = 'Y'
                        from( SELECT distinct cm.unique_key FROM ods.NS_CM_GL_ACTIVITY  cm
                        LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON cm.accountname::varchar = mp.accountnumber::varchar 
                        where (mp.ns_trans_allocation not in ('N') or  mp.tran_sub_type_id is NULL) AND cm.tran_allocated = 'N' AND cm.PERIODNAME = '{period}'
                        and acct_number ='{accountnumber}' ) upd
                        where upd.unique_key  = cm.unique_key
                        """}


# createDefaultAlloc ="""
#                         insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE,TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
#                         WITH netsuiteallocations  AS (WITH itemlevelns AS (SELECT DISTINCT transaction_id FROM (SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,(cm.amount *inv.relative_amount) AS alloc_amt FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN (
#                         SELECT transaction_id ,item_id,amount,
#                         amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
#                         WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null 
#                         ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE inv.transaction_id IS NOT NULL and  ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',trim(SPLIT_PART(cm.SKU,'-',1)),cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',dd.sku,(cm.amount*COALESCE(dd.ALLOC_PCT,dd.QTY_ALLOC_PCT)) AS amount  
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}' ))
#                         SELECT *  FROM ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN itemlevelns it ON it.transaction_id = cm.transaction_id 
#                         WHERE cm.ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME =  '{period}' AND it.transaction_id IS null)
#                         SELECT acct_number,amount,unique_key,GAH_ID,{ruleId},dd.LEVEL1,dd.LEVEL2,dd.sku,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
#                         ,sysdate(),SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,
#                         case 
#                                 when gah.total_amt <>0 then amount * alloc_pct
#                                  else amount * qty_alloc_pct end AS allocated_amt,
#                                   '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM netsuiteallocations na
#                         LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON '{accountnumber}' = mp.accountnumber::varchar {join}
#                         inner join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' {join2}
#                         left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
#                     """

createDefaultAlloc ={"0-allocate":"""
                        insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE,TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                        SELECT acct_number,amount,unique_key,GAH_ID,{ruleId},dd.LEVEL1,dd.LEVEL2,dd.sku,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                        ,sysdate(),SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,
                        case 
                                when gah.total_amt <>0 then amount * alloc_pct
                                 else amount * qty_alloc_pct end AS allocated_amt,
                                  '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM ods.NS_CM_GL_ACTIVITY na
                        LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON  mp.accountnumber::varchar = '{accountnumber}'  {join}
                        inner join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' {join2}
                        left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
                        where na.PERIODNAME = '{period}' and na.ACCOUNTNAME::varchar = '{accountnumber}' and na.tran_allocated ='N'
                    """,
                        "1- update ns allocation table ":"""
                        update ods.ns_cm_gl_activity cm
                        set cm.tran_allocated = 'Y'
                        from(  SELECT distinct unique_key FROM ods.NS_CM_GL_ACTIVITY na
                        LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON mp.accountnumber::varchar = '{accountnumber}'  {join}
                        inner join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' {join2}
                        left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
                        where na.PERIODNAME = '{period}' and na.ACCOUNTNAME::varchar = '{accountnumber}' and na.tran_allocated ='N' ) upd
                        where upd.unique_key  = cm.unique_key
                        """}

createDefaultNoSkuAlloc ={"0 - allocate":"""
                        insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE,TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                        SELECT acct_number,amount,unique_key,GAH_ID,{ruleId},dd.LEVEL1,dd.LEVEL2,'GL Activity',ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                        ,sysdate(),SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,
                        amount AS allocated_amt,
                                  '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM ods.ns_cm_gl_activity na
                        LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON '{accountnumber}' = mp.accountnumber::varchar {join}
                        left join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' {join2}
                        left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
                        where gah.id is null and na.PERIODNAME = '{period}' and na.ACCOUNTNAME::varchar = '{accountnumber}' and na.tran_allocated ='N'
                    """,
                    "1- update ns allocation table ":"""
                        update ods.ns_cm_gl_activity cm
                        set cm.tran_allocated = 'Y'
                        from(  SELECT distinct unique_key FROM ods.ns_cm_gl_activity na
                        LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON '{accountnumber}' = mp.accountnumber::varchar {join}
                        left join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' {join2}
                        left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
                        where gah.id is null and na.PERIODNAME = '{period}' and na.ACCOUNTNAME::varchar = '{accountnumber}' and na.tran_allocated ='N') upd
                        where upd.unique_key  = cm.unique_key"""}



# createDefaultNoSkuAlloc ="""
#                         insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE,TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
#                         WITH netsuiteallocations  AS (WITH itemlevelns AS (SELECT DISTINCT transaction_id FROM (SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,(cm.amount *inv.relative_amount) AS alloc_amt FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN (
#                         SELECT transaction_id ,item_id,amount,
#                         amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
#                         WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null 
#                         ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE inv.transaction_id IS NOT NULL and  ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',trim(SPLIT_PART(cm.SKU,'-',1)),cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',dd.sku,(cm.amount*COALESCE(dd.ALLOC_PCT,dd.QTY_ALLOC_PCT)) AS amount  
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL and ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME = '{period}' ))
#                         SELECT *  FROM ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN itemlevelns it ON it.transaction_id = cm.transaction_id 
#                         WHERE cm.ACCOUNTNAME = '{accountnumber}'::varchar and cm.PERIODNAME =  '{period}' AND it.transaction_id IS null)
#                         SELECT acct_number,amount,unique_key,GAH_ID,{ruleId},dd.LEVEL1,dd.LEVEL2,'GL Activity',ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
#                         ,sysdate(),SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,
#                         amount AS allocated_amt,
#                                   '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM netsuiteallocations na
#                         LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON '{accountnumber}' = mp.accountnumber::varchar {join}
#                         left join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' {join2}
#                         left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
#                         where gah.id is null
#                     """

# createDefaultAccountAlloc ="""
#                 insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE,TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
#                 WITH test AS ( WITH netsuiteallocations  AS (WITH itemlevelns AS (SELECT DISTINCT transaction_id FROM (SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,(cm.amount *inv.relative_amount) AS alloc_amt FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN (
#                         SELECT transaction_id ,item_id,amount,
#                         amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
#                         WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null 
#                         ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE inv.transaction_id IS NOT NULL and  ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME ='{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',trim(SPLIT_PART(cm.SKU,'-',1)),cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',dd.sku,(cm.amount*COALESCE(dd.ALLOC_PCT,dd.QTY_ALLOC_PCT)) AS amount  
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL and ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME = '{period}' ))
#                         SELECT *  FROM ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN itemlevelns it ON it.transaction_id = cm.transaction_id 
#                         WHERE cm.ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME =  '{period}' AND it.transaction_id IS null)
#                         SELECT ACCOUNTNAME, sum(amount) AS ns_amt FROM netsuiteallocations na
#                                       WHERE na.ACCOUNTNAME =  '{accountnumber}'
#                                       GROUP BY ACCOUNTNAME)
#                         SELECT  ACCOUNTNAME,ns_amt,NULL AS unique_key,GAH_ID,{ruleId},dd.LEVEL1,dd.LEVEL2,dd.sku,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
#                         ,sysdate(),SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,
#                         case 
#                                 when gah.total_amt <>0 THEN ns_amt * alloc_pct
#                                  else ns_amt * qty_alloc_pct end AS allocated_amt,
#                                   '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM test 
#                         LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id ='{ruleId}' 
#                         inner join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' 
#                         left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
#                     """

createDefaultAccountAlloc ={"0 - allocate":"""
                        insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE,TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                        WITH test AS (
                        SELECT ACCOUNTNAME, sum(amount) AS ns_amt FROM ods.ns_cm_gl_activity na
                                      WHERE  na.PERIODNAME = '{period}' and na.ACCOUNTNAME::varchar = '{accountnumber}' and na.tran_allocated ='N'
                                      GROUP BY ACCOUNTNAME)
                        SELECT  ACCOUNTNAME,ns_amt,NULL AS unique_key,GAH_ID,{ruleId},dd.LEVEL1,dd.LEVEL2,dd.sku,ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                        ,sysdate(),SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,
                        case 
                                when gah.total_amt <>0 THEN ns_amt * alloc_pct
                                 else ns_amt * qty_alloc_pct end AS allocated_amt,
                                  '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM test 
                                  LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON '{accountnumber}' = mp.accountnumber::varchar {join}
                        inner join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}'  {join2}
                        left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
                    """,
                        
                        "1- update ns allocation table ":"""
                        update ods.ns_cm_gl_activity cm
                        set cm.tran_allocated = 'Y'
                        from(   WITH test AS (
                        SELECT ACCOUNTNAME, unique_key, sum(amount) AS ns_amt FROM ods.ns_cm_gl_activity na
                                      WHERE  na.PERIODNAME = '{period}' and na.ACCOUNTNAME::varchar = '{accountnumber}' and na.tran_allocated ='N'
                                      GROUP BY ACCOUNTNAME, unique_key)
                        SELECT  distinct unique_key FROM test 
                        LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id ='{ruleId}' 
                        inner join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' 
                        left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 ) upd
                        where upd.unique_key  = cm.unique_key"""
                    }

createDefaultNoSkuAccountAlloc ={"0 -allocate" :"""
                        insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE,TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
                        WITH test AS ( 
                        SELECT ACCOUNTNAME, sum(amount) AS ns_amt FROM ods.ns_cm_gl_activity na
                                      WHERE na.PERIODNAME = '{period}' and na.ACCOUNTNAME::varchar = '{accountnumber}' and na.tran_allocated ='N'
                                      GROUP BY ACCOUNTNAME)
                        SELECT  ACCOUNTNAME,ns_amt,NULL AS unique_key,GAH_ID,{ruleId},dd.LEVEL1,dd.LEVEL2,'GL Activity',ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
                        ,sysdate(),SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,
                        ns_amt AS allocated_amt,
                                  '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM test 
                        LEFT JOIN (SELECT *, accountnumber AS acct_number FROM ods.GL_ALLOCATION_MAP) mp ON '{accountnumber}' = mp.accountnumber::varchar {join}          
                        left join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' 
                        left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
                        where gah.id is null 
                    """,
                        "1- update ns allocation table ":"""
                        update ods.ns_cm_gl_activity cm
                        set cm.tran_allocated = 'Y'
                        from(           WITH test AS ( 
                        SELECT ACCOUNTNAME, unique_key,sum(amount) AS ns_amt FROM ods.ns_cm_gl_activity na
                                      WHERE na.PERIODNAME = '{period}' and na.ACCOUNTNAME::varchar = '{accountnumber}' and na.tran_allocated ='N'
                                      GROUP BY ACCOUNTNAME, unique_key)
                        SELECT distinct unique_key FROM test 
                        LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id ='{ruleId}' 
                        left join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' 
                        left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
                        where gah.id is null ) upd
                        where upd.unique_key  = cm.unique_key"""
                    }


# createDefaultNoSkuAccountAlloc ="""
#                 insert into ods.NS_ALLOC (ACCOUNTNUMBER,NS_AMT,ID,GAH_ID,RULE_ID,LEVEL1,LEVEL2,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,CREATED_AT,SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,ALLOCATED_AMT,TRAN_TYPE,TRAN_SUB_TYPE,TRAN_SUB_TYPE_ID)
#                 WITH test AS ( WITH netsuiteallocations  AS (WITH itemlevelns AS (SELECT DISTINCT transaction_id FROM (SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,(cm.amount *inv.relative_amount) AS alloc_amt FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN (
#                         SELECT transaction_id ,item_id,amount,
#                         amount / NULLIF(sum(amount) OVER (PARTITION BY transaction_id),0) AS relative_amount 
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY_inv
#                         WHERE item_id IS NOT NULL  AND ITEM_UNIT_PRICE IS NOT null 
#                         ) inv ON inv.transaction_id =cm.TRANSACTION_ID AND cm.ITEM_ID IS NULL
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =inv.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE inv.transaction_id IS NOT NULL and  ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME ='{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',ci.ITEM_NAME,cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id NOT IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',trim(SPLIT_PART(cm.SKU,'-',1)),cm.amount
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN TM_IGLOO_ODS_STG.ods.CURR_ITEMS ci ON ci.ITEM_ID =cm.ITEM_ID::varchar AND ci.FC_ID =2
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NOT NULL AND cm.item_id IN (46805,150000,31184,149999,31185,150001,31091,149998) and ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME = '{period}'
#                         UNION ALL
#                         SELECT cm.transaction_id,sysdate(),3001,2,cl.month_end_date,trandate,NULL, NULL , ACCOUNTNAME,'NS Allocation',3001,'NS Allocation',dd.sku,(cm.amount*COALESCE(dd.ALLOC_PCT,dd.QTY_ALLOC_PCT)) AS amount  
#                         FROM TM_IGLOO_ODS_STG.ods.NS_CM_GL_ACTIVITY cm
#                         INNER JOIN TM_IGLOO_ODS_STG.ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id = 601 AND dd.LEVEL1=cm.VENDOR_ID::varchar 
#                         LEFT JOIN (SELECT DISTINCT MONTH_END_DATE,MONTHNAME(month_end_date) || ' ' || SUBSTRING(CALENDARYEARQTR,1,4) AS quartertext FROM TM_IGLOO_ODS_STG.ods.CAL_LU) cl ON cl.quartertext = cm.PERIODNAME 
#                         WHERE cm.item_id IS NULL AND cm.VENDOR_ID IS NOT NULL AND dd.sku IS NOT NULL and ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME = '{period}' ))
#                         SELECT *  FROM ods.NS_CM_GL_ACTIVITY cm
#                         LEFT JOIN itemlevelns it ON it.transaction_id = cm.transaction_id 
#                         WHERE cm.ACCOUNTNAME =  '{accountnumber}'::varchar and cm.PERIODNAME =  '{period}' AND it.transaction_id IS null)
#                         SELECT ACCOUNTNAME, sum(amount) AS ns_amt FROM netsuiteallocations na
#                                       WHERE na.ACCOUNTNAME =  '{accountnumber}'
#                                       GROUP BY ACCOUNTNAME)
#                         SELECT  ACCOUNTNAME,ns_amt,NULL AS unique_key,GAH_ID,{ruleId},dd.LEVEL1,dd.LEVEL2,'GL Activity',ci.GROUP_ID,ci.GROUP_NAME,ci.ITEM_CATEGORY_ID,ci.ITEM_CATEGORY_NAME,ci.SUBCATEGORY_ID,ci.SUBCATEGORY_NAME,ci.CLASS_ID,ci.CLASS_NAME,ci.SUBCLASS_ID,ci.SUBCLASS_NAME
#                         ,sysdate(),SKU_AMT,ALLOC_PCT,SKU_QTY,QTY_ALLOC_PCT,
#                         ns_amt AS allocated_amt,
#                                   '{tran_type}','{tran_sub_type}','{tran_sub_type_id}' FROM test 
#                         LEFT JOIN ods.GL_ALLOC_DRIVER_DETAIL dd ON dd.rule_id ='{ruleId}' 
#                         left join ods.gl_alloc_driver_header gah on gah.id = dd.gah_id and gah.rule_id ='{ruleId}' 
#                         left join ods.curr_items ci on ci.item_name = dd.sku and ci.fc_id = 2 
#                         where gah.id is null 
#                     """
# createNSOob = """

#                 CREATE or REPLACE TEMP TABLE ods.oobb as
#                     SELECT accountnumber,ident ,sum(total_var) AS total_var FROM (SELECT accountnumber,ns."ID" as ident ,ns_amt, sum(allocated_amt) ,ns_amt- sum(allocated_amt) AS total_var
#                     FROM ods.ns_alloc ns 
#                     INNER JOIN (SELECT  id, period_end_date FROM ods.GL_ALLOC_DRIVER_HEADER) dh ON dh.ID = ns.GAH_ID 
#                     where dh.period_end_date = '{endDate}' and ns.accountnumber = '{nsaccount}'
#                     GROUP BY accountnumber,ident,ns_amt) GROUP BY accountnumber,ident
#                     """

createNSOob = """

                CREATE or REPLACE TEMP TABLE ods.oobb as
                     SELECT accountnumber,sum(total_var) AS total_var FROM (SELECT accountnumber,netsuite_amt, sum(allocated_amt) ,netsuite_amt- sum(allocated_amt) AS total_var
                    FROM ods.ns_alloc ns 
                    INNER JOIN (SELECT  accountname, sum(amount) AS netsuite_amt FROM ods.NS_CM_GL_ACTIVITY WHERE periodname = '{period}'
                    GROUP BY accountname) dh ON dh.accountname::varchar = ns.accountnumber::varchar
                    where  ns.accountnumber::varchar = '{nsaccount}' and ns.period_end_date = '{endDate}'
                    GROUP BY accountnumber,netsuite_amt) GROUP BY accountnumber       
     
                    """


# updateNSAlloc = """
            
#                update ods.ns_alloc
#                 set allocated_amt = new_alloc_amt
#                 FROM(
#                 select det.id,det.ACCOUNTNUMBER ,det.sku, det.allocated_amt+ case when total_var > 0 then .01 else -.01 end as new_alloc_amt, det.allocated_amt FROM(select id,ACCOUNTNUMBER ,sku, allocated_amt,
#                                 row_number() OVER  (PARTITION by accountnumber order by allocated_amt desc) as rn
#                                 from  ods.ns_alloc tab
#                                 INNER JOIN (SELECT  id as ident, period_end_date FROM ods.GL_ALLOC_DRIVER_HEADER) dh ON dh.ident = tab.GAH_ID 
#                                 where dh.period_end_date = '{endDate}'
#                                 and tab.allocated_amt <> 0 ) det
#                                 INNER JOIN ods.oobb ON rn<abs(oobb.total_var *100) AND oobb.accountnumber = det.accountnumber AND oobb.accountnumber = det.accountnumber) upd
#                                  WHERE ns_alloc.ID =upd.id AND ns_alloc.SKU =upd.sku AND ns_alloc.ACCOUNTNUMBER = upd.accountnumber and ns_alloc.accountnumber ='{nsaccount}'
                     
#                                     """

updateNSAlloc = """
                update ods.ns_alloc
                set allocated_amt = new_alloc_amt
                FROM(
                select det.id,det.ACCOUNTNUMBER ,det.sku, det.allocated_amt + case when total_var > 0 then .01 else -.01 end as new_alloc_amt, det.allocated_amt FROM(select id,ACCOUNTNUMBER ,sku, allocated_amt,
                                row_number() OVER  (PARTITION by accountnumber order by allocated_amt desc) as rn
                                from  ods.ns_alloc tab
                                INNER JOIN (SELECT  accountname, sum(amount) AS netsuite_amt FROM ods.NS_CM_GL_ACTIVITY WHERE periodname = '{period}'
                    			GROUP BY accountname) dh ON  dh.accountname::varchar = tab.accountnumber::varchar
                   				 where  tab.accountnumber::varchar = '{nsaccount}' and tab.allocated_amt <> 0 and tab.period_end_date = '{endDate}' ) det
                                INNER JOIN ods.oobb ON rn<abs(oobb.total_var *100) AND oobb.accountnumber = det.accountnumber
                                 ) upd
                                 WHERE ns_alloc.ID =upd.id AND ns_alloc.SKU =upd.sku AND ns_alloc.ACCOUNTNUMBER = upd.accountnumber and ns_alloc.accountnumber ='{nsaccount}' 
                                    """

insertNSToGlAllocation = """
                    INSERT into ods.gl_allocation_test(CREATED_AT,RULE_ID,RUN_ID,TRAN_DATE,SALE_DATE,LOCATION_ID,GL_PRODUCT_CATEGORY,ACCT_NUMEBR,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU
                  ,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME,ALLOC_AMT)
                    SELECT SYSDATE() AS created_date, rule_id, {runid} AS run_id, '{endDate}' as tran_date, '{endDate}' as sale_date,null as LOCATION, null as CATEGORY, accountnumber
                    ,'NS Allocation' as GROUP_CODE,'2000' as TRAN_TYPE,'201' as TRAN_SUB_TYPE_ID,'2000' as TRAN_SUB_TYPE,SKU ,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME
                    ,sum(allocated_amt) AS tran_amt
                    FROM ods.ns_alloc ns
                    INNER JOIN (SELECT  id, period_end_date FROM ods.GL_ALLOC_DRIVER_HEADER) dh ON dh.ID = ns.GAH_ID 
                    where dh.period_end_date = '{endDate}'
                    GROUP BY created_date, rule_id, run_id, tran_date, sale_date,LOCATION, CATEGORY, accountnumber,GROUP_CODE,TRAN_TYPE,TRAN_SUB_TYPE_ID,TRAN_SUB_TYPE,SKU,GROUP_ID,GROUP_NAME,ITEM_CATEGORY_ID,ITEM_CATEGORY_NAME,SUBCATEGORY_ID,SUBCATEGORY_NAME,CLASS_ID,CLASS_NAME,SUBCLASS_ID,SUBCLASS_NAME
                        """


