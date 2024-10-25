getChargeDetails =  """

                    SELECT BATCHSEQ, MESSAGE_CREATED_AT, MESSAGES, SHIPPER_ORDER_PLAN_CREATED_AT, ORDER_NUMBER, LEAN_LOAD_ID, LOAD_NUMBER, PRO_NUMBER, CARRIER_NAME, CARRIER_SCAC, CHARGE_TYPE, CHARGE_AMOUNT, CHARG_CATEGORY
                    FROM PRD_DW.TMS.CHARGE_DETAILS;

                """

postChargeDetails =  """
                    insert into ods.tms_charge_details
                    SELECT stcd.BATCHSEQ, stcd.MESSAGE_CREATED_AT, stcd.MESSAGES, stcd.SHIPPER_ORDER_PLAN_CREATED_AT, stcd.ORDER_NUMBER, stcd.LEAN_LOAD_ID, stcd.LOAD_NUMBER, stcd.PRO_NUMBER, stcd.CARRIER_NAME, stcd.CARRIER_SCAC, stcd.CHARGE_TYPE, stcd.CHARGE_AMOUNT, stcd.CHARG_CATEGORY,current_timestamp()
                    FROM staging.tms_charge_details stcd
                    left join ods.tms_charge_details tcd on tcd.order_number = stcd.order_number and  tcd.LOAD_NUMBER = stcd.LOAD_NUMBER and   tcd.CHARGE_AMOUNT = stcd.CHARGE_AMOUNT  and   tcd.CHARGE_TYPE = stcd.CHARGE_TYPE and  stcd.CARRIER_NAME =  tcd.CARRIER_NAME
                    where tcd.order_number is null;

                """                



getOrderPLan =  """

                    SELECT ORDERNUMBER ,
                            LOADNUMBER ,
                            ORDERGROUP ,
                            SERVICELEVEL ,
                            pickupdeparturedate  ,
                            deliveryarrivaldate ,
                            pallets
                    FROM PRD_DW.TMS.TMS_ORDERPLAN;

                """




postOrderPlan =  """
                    insert into ods.TMS_ORDERPLAN
                    SELECT stcd.ORDERNUMBER ,
                            stcd.LOADNUMBER ,
                            stcd.ORDERGROUP ,
                            stcd.SERVICELEVEL ,
                            stcd.pickupdeparturedate  ,
                            stcd.deliveryarrivaldate ,
                            stcd.pallets,current_timestamp()
                    FROM staging.tms_orderplan stcd
                    left join ods.tms_orderplan tcd on tcd.ordernumber = stcd.ordernumber and  tcd.LOADNUMBER = stcd.LOADNUMBER and   tcd.SERVICELEVEL = stcd.SERVICELEVEL 
                    where tcd.ordernumber is null;

                """                
