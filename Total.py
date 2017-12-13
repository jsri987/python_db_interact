# Program to extract all staging data from BPP Prod for BPC Commercial
# Jeetesh Srivastava , 2016/12/31
# Curs - ET RB Projects


import os
import csv
import cx_Oracle

Report_dt_def  = '31-DEC-2016'
Report_dt_fmt1 = '12/31/2016'
Report_dt_fmt2 = '20161231'


os.chdir("C:\\Users\\16184\\Desktop\\Python scripts\\2016Q4\\Total")
connection = "sa_bppadm/i9#lSd_8@BPPPRD"
orcl = cx_Oracle.connect(connection)
curs = orcl.cursor()

printHeader = True # include column headers in each table output
# Query to Extract et_rb_projects data from BPP 
et_rb_projects = """With temp1 as
                    (
                    select sum(AMOUNT) as EXPENSE_DOLLARS
                    ,c.NAME_LEVEL5 as LOB_LVL_5
                    ,d.MKT_LEVEL3 as MKT_LVL_3
                    ,e.NAME_LEVEL7 as PD_LVL_7
                    ,ENTRY_DATE as  ENTRY_DATE
                    ,DEPENDS_NAME
                    from BPP_REPADM.LEDGER_ENTRY a
                    inner join 
                        (select * from BPP_REPADM.ACCOUNT_BINARY_DIM where PARENT_NAME like 'ET Bank Projects') b
                        on a.ACCOUNT_ID = b.DEPENDS
                    inner join 
                       BPP_REPADM.LOB_DIM c
                       on a.ORGANIZATION_ID = c.LEVEL6_KEY
                    inner join 
                        BPP_REPADM.MKT_DIM d
                      on a.ORGANIZATION_ID = d.MKT_LEVEL6_KEY
                    inner join 
                        BPP_REPADM.PROD_DIM e
                      on a.PRODUCT_ID = e.LEVEL7_KEY
                    where ENTRY_DATE =  """ + "'" + Report_dt_def + "'" + """ 
                       group by DEPENDS_NAME,c.NAME_LEVEL5 ,d.MKT_LEVEL3 ,e.NAME_LEVEL7 ,ENTRY_DATE 
                       order by PD_LVL_7,MKT_LVL_3,LOB_LVL_5
                     )
                    	Select sum(EXPENSE_DOLLARS) as EXPENSE_DOLLARS
                    	 ,LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME
                    	from( 
                    			(Select * from temp1)
                    		) 
                    	 group by 
                    	  LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME""" 
# Query to Extract rb_deposit_ops data from BPP                       
rb_deposit_ops =   """With temp1 as
                    (
                    select sum(AMOUNT) as EXPENSE_DOLLARS
                    ,c.NAME_LEVEL5 as LOB_LVL_5
                    ,d.MKT_LEVEL3 as MKT_LVL_3
                    ,e.NAME_LEVEL7 as PD_LVL_7
                    ,ENTRY_DATE as  ENTRY_DATE
                    ,DEPENDS_NAME
                    from BPP_REPADM.LEDGER_ENTRY a
                    inner join 
                        (select * from BPP_REPADM.ACCOUNT_BINARY_DIM where PARENT_NAME like 'Bank Ops Deposits') b
                        on a.ACCOUNT_ID = b.DEPENDS
                    inner join 
                       BPP_REPADM.LOB_DIM c
                       on a.ORGANIZATION_ID = c.LEVEL6_KEY
                    inner join 
                        BPP_REPADM.MKT_DIM d
                      on a.ORGANIZATION_ID = d.MKT_LEVEL6_KEY
                    inner join 
                        BPP_REPADM.PROD_DIM e
                      on a.PRODUCT_ID = e.LEVEL7_KEY
                    where ENTRY_DATE =  """ + "'" + Report_dt_def + "'" + """ 
                       group by DEPENDS_NAME,c.NAME_LEVEL5 ,d.MKT_LEVEL3 ,e.NAME_LEVEL7 ,ENTRY_DATE 
                       order by PD_LVL_7,MKT_LVL_3,LOB_LVL_5
                     )
                    	Select sum(EXPENSE_DOLLARS) as EXPENSE_DOLLARS
                    	 ,LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME
                    	from( 
                    			(Select * from temp1)
                    		) 
                    	 group by 
                    	  LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME"""                       
# Query to Extract rb_loan_ops data from BPP                        
rb_loan_ops =   """With temp1 as
                    (
                    select sum(AMOUNT) as EXPENSE_DOLLARS
                    ,c.NAME_LEVEL5 as LOB_LVL_5
                    ,d.MKT_LEVEL3 as MKT_LVL_3
                    ,e.NAME_LEVEL7 as PD_LVL_7
                    ,ENTRY_DATE as  ENTRY_DATE
                    ,DEPENDS_NAME
                    from BPP_REPADM.LEDGER_ENTRY a
                    inner join 
                        (select * from BPP_REPADM.ACCOUNT_BINARY_DIM where PARENT_NAME like 'RB Loan Ops') b
                        on a.ACCOUNT_ID = b.DEPENDS
                    inner join 
                       BPP_REPADM.LOB_DIM c
                       on a.ORGANIZATION_ID = c.LEVEL6_KEY
                    inner join 
                        BPP_REPADM.MKT_DIM d
                      on a.ORGANIZATION_ID = d.MKT_LEVEL6_KEY
                    inner join 
                        BPP_REPADM.PROD_DIM e
                      on a.PRODUCT_ID = e.LEVEL7_KEY
                    where ENTRY_DATE =  """ + "'" + Report_dt_def + "'" + """ 
                       group by DEPENDS_NAME,c.NAME_LEVEL5 ,d.MKT_LEVEL3 ,e.NAME_LEVEL7 ,ENTRY_DATE 
                       order by PD_LVL_7,MKT_LVL_3,LOB_LVL_5
                     )
                    	Select sum(EXPENSE_DOLLARS) as EXPENSE_DOLLARS
                    	 ,LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME
                    	from( 
                    			(Select * from temp1)
                    		) 
                    	 group by 
                    	  LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME"""                                            
# Query to Extract bank_ops data from BPP                        
bank_ops       =   """With temp1 as
                    (
                    select sum(AMOUNT) as EXPENSE_DOLLARS
                    ,c.NAME_LEVEL5 as LOB_LVL_5
                    ,d.MKT_LEVEL3 as MKT_LVL_3
                    ,e.NAME_LEVEL7 as PD_LVL_7
                    ,ENTRY_DATE as  ENTRY_DATE
                    ,DEPENDS_NAME
                    from BPP_REPADM.LEDGER_ENTRY a
                    inner join 
                        (select * from BPP_REPADM.ACCOUNT_BINARY_DIM where PARENT_NAME = 'Bank Ops Customer Contact') b
                        on a.ACCOUNT_ID = b.DEPENDS
                    inner join 
                       BPP_REPADM.LOB_DIM c
                       on a.ORGANIZATION_ID = c.LEVEL6_KEY
                    inner join 
                        BPP_REPADM.MKT_DIM d
                      on a.ORGANIZATION_ID = d.MKT_LEVEL6_KEY
                    inner join 
                        BPP_REPADM.PROD_DIM e
                      on a.PRODUCT_ID = e.LEVEL7_KEY
                    where ENTRY_DATE =  """ + "'" + Report_dt_def + "'" + """ 
                       group by DEPENDS_NAME,c.NAME_LEVEL5 ,d.MKT_LEVEL3 ,e.NAME_LEVEL7 ,ENTRY_DATE 
                       order by PD_LVL_7,MKT_LVL_3,LOB_LVL_5
                     )
                    	Select sum(EXPENSE_DOLLARS) as EXPENSE_DOLLARS
                    	 ,LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME
                    	from( 
                    			(Select * from temp1)
                    		) 
                    	 group by 
                    	  LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME"""                        

# Query to Extract bank_ops data from BPP                        
lock_box       =   """With temp1 as
                    (
                    select sum(AMOUNT) as EXPENSE_DOLLARS
                    ,c.NAME_LEVEL5 as LOB_LVL_5
                    ,d.MKT_LEVEL3 as MKT_LVL_3
                    ,e.NAME_LEVEL7 as PD_LVL_7
                    ,ENTRY_DATE as  ENTRY_DATE
                    ,DEPENDS_NAME
                    from BPP_REPADM.LEDGER_ENTRY a
                    inner join 
                        (select * from BPP_REPADM.ACCOUNT_BINARY_DIM where PARENT_NAME = 'Bank Ops Lockbox') b
                        on a.ACCOUNT_ID = b.DEPENDS
                    inner join 
                       BPP_REPADM.LOB_DIM c
                       on a.ORGANIZATION_ID = c.LEVEL6_KEY
                    inner join 
                        BPP_REPADM.MKT_DIM d
                      on a.ORGANIZATION_ID = d.MKT_LEVEL6_KEY
                    inner join 
                        BPP_REPADM.PROD_DIM e
                      on a.PRODUCT_ID = e.LEVEL7_KEY
                    where ENTRY_DATE =  """ + "'" + Report_dt_def + "'" + """ 
                       group by DEPENDS_NAME,c.NAME_LEVEL5 ,d.MKT_LEVEL3 ,e.NAME_LEVEL7 ,ENTRY_DATE 
                       order by PD_LVL_7,MKT_LVL_3,LOB_LVL_5
                     )
                    	Select sum(EXPENSE_DOLLARS) as EXPENSE_DOLLARS
                    	 ,LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME
                    	from( 
                    			(Select * from temp1)
                    		) 
                    	 group by 
                    	  LOB_LVL_5
                    	 ,MKT_LVL_3
                    	 ,PD_LVL_7
                    	 ,DEPENDS_NAME"""  
                      
# Query to Extract Expense breakdown data  from BPP                        
bpp_expn_brkdwn       =   """  SELECT 0 levelkey,
                                 V_DIM_PROD.CUSTOMPRODUCTGROUPNAME ProductGroupkey,
                                 V_DIM_PROD.SORT_KEY SortKey_ProductGroup,
                                 V_DIM_PROD.NAME_LEVEL7 PD_LVL_7,
                                 i.NAME_LEVEL5 as LOB_LVL_5,
                                 j.MKT_LEVEL3 as MKT_LVL_3,
                                 V_DIM_PROD.LEVEL7_SORT_KEY Product_SK_DB,
                                 
                                 SUM ( MV_LEDGER_ENTRY_YR_FACT.BALANCE) Balance,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.C55) Direct_NII,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.ECONOMIC_EQUITY_NII) Economic_Equity_NII,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.ALLOC_NII) Allocated_NII,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.TOTAL_NII) Total_NII,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.TOTAL_NIOI) Total_NIOI,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.TOTAL_REVENUE) Total_Revenue,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.REMAINING_DIRECT_EXPENSE)
                                    Remaining_Direct_Expense,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.TOTAL_FDIC) FDIC,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.CORPORATE_ALLOCATIONS)
                                    Corporate_Allocations,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.PENSION) Pension,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.HR) HR,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.CREAS) CREAS,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.ET_LOANS) ET_Loans,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.ET_GENERAL) ET_General,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.ET_LEGACY_FIDELITY) ET_Legacy_Fidelity,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.ET_APPLICATION_DEV_SUPPORT)
                                    ET_Application_Dev_Support,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.ET_BANK_PROJECTS) ET_Bank_Projects,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.BANK_OPS_DEPOSITS) Bank_Ops_Deposits,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.BANK_OPS_GENERAL) Bank_Ops_General,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.BANK_OPS_COMPLIANCE) Bank_Ops_Compliance,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.BANK_OPS_LOCKBOX) Bank_Ops_Lockbox,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.BANK_OPS_VAULT) Bank_Ops_Vault,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.BANK_OPS_ELECTRONIC_BANKING)
                                    Bank_Ops_Electronic_Banking,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.BANK_OPS_CUSTOMER_SUPPORT)
                                    Bank_Ops_Customer_Contact,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.BANK_OPS_FULFILLMENT)
                                    Bank_Ops_Fulfillment,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.CREDIT_RISK_GROUP) Credit_Risk,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.OTHER_RISK) Other_Risk,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.RB_LOAN_OPS) RB_Loans_Ops,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.RB_RETAIL_SUPPORT) RB_Retail_Support,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.RB_SAFEKEEPING) RB_Safekeeping,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.RB_PRODUCT_SUPPORT) RB_Product_Support,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.RB_SALES_SUPPORT) RB_Sales_Support,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.RB_MARKETING) RB_Marketing,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.RB_ADMIN) RB_Admin,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.RB_LOB_ADMIN) RB_LOB_Admin,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.RB_MORTGAGE) RB_Mortgage,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.BRANCH_ALLOCATION) Branch_Allocation,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.OTHER) Other,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.ALLOCATED_EXPENSE) Allocated_Expense,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.OUTSIDE_EXPENSE) Outside_Expense,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.TOTAL_EXPENSES) Total_Expenses,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.PRETAX_PRE_PROVISION)
                                    Pre_Tax_Pre_Provision,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.EXPECTED_LOSS) Expected_Loss,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.FULLY_LOADED_PRETAX) Fully_Loaded_Pretax,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.C49) Taxes,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.C50) Net_Income,
                                 SUM (MV_LEDGER_ENTRY_YR_FACT.ECONOMIC_EQUITY_BAL)
                                    Economic_Equity_Balance,
                                 SUM ( (MV_LEDGER_ENTRY_YR_FACT.ECONOMIC_EQUITY_BAL) * -0.1)
                                    Cap_Charge,
                                 SUM (
                                      MV_LEDGER_ENTRY_YR_FACT.C50
                                    + (MV_LEDGER_ENTRY_YR_FACT.ECONOMIC_EQUITY_BAL) * -0.1)
                                    Economic_Profit
                            FROM (((BPP_REPADM.LOB_DIM V_DIM_LOB
                                    LEFT OUTER JOIN
                                    BPP_REPADM.MV_LEDGER_ENTRY_YR_FACT MV_LEDGER_ENTRY_YR_FACT
                                       ON V_DIM_LOB.LEVEL6_KEY =
                                             MV_LEDGER_ENTRY_YR_FACT.ORGANIZATION_ID)
                                   FULL OUTER JOIN BPP_REPADM.MKT_DIM V_DIM_MKT
                                      ON V_DIM_MKT.MKT_LEVEL6_KEY =
                                            MV_LEDGER_ENTRY_YR_FACT.ORGANIZATION_ID)
                                  FULL OUTER JOIN BPP_REPADM.PROD_DIM V_DIM_PROD
                                     ON V_DIM_PROD.LEVEL7_KEY = MV_LEDGER_ENTRY_YR_FACT.PRODUCT_ID)
                                 FULL OUTER JOIN BPP_REPADM.MV_DATE_DIM MV_DATE_DIM
                                    ON     MV_DATE_DIM.ENTRY_DATE = MV_LEDGER_ENTRY_YR_FACT.ENTRY_DATE
                                       AND MV_DATE_DIM.LEDGER_ID = MV_LEDGER_ENTRY_YR_FACT.LEDGER_ID
                                 LEFT OUTER JOIN BPP_REPADM.LOB_DIM i
                                    on   i.LEVEL6_KEY =  MV_LEDGER_ENTRY_YR_FACT.ORGANIZATION_ID     
                                 LEFT OUTER JOIN BPP_REPADM.MKT_DIM j
                                    on j.MKT_LEVEL6_KEY = MV_LEDGER_ENTRY_YR_FACT.ORGANIZATION_ID    
                           WHERE     V_DIM_PROD.LEVEL7_SORT_KEY <> 99
                                 AND V_DIM_PROD.CUSTOMPRODUCTGROUPNAME <> 'Others'
                                 AND NOT V_DIM_LOB.NAME_LEVEL5 IS NULL
                                 AND NOT V_DIM_MKT.MKT_LEVEL5 IS NULL
                                 AND MV_DATE_DIM.YEAR_MONTH_DAY_KEY =  """ +  Report_dt_fmt2  + """ 
                        GROUP BY V_DIM_PROD.CUSTOMPRODUCTGROUPNAME,
                                 V_DIM_PROD.SORT_KEY,
                                 V_DIM_PROD.NAME_LEVEL7,
                                 V_DIM_PROD.LEVEL7_SORT_KEY,
                                 i.NAME_LEVEL5 ,
                                 j.MKT_LEVEL3"""  

# Query to Extract STG_CAPITAL_AND_EXPSENSE  from BPP                        
stg_capital_and_exp       =   """ with temp1 as
(
             Select 
                 Sum (case when TM_DIM_ID = 12 then (365/365) * TOTAL_BALANCE
                    end) as TOTAL_BALANCE,
                 sum(TOTAL_EXPN_AMT) as TOTAL_EXPN_AMT,
                 sum(REMAIN_DIR_EXPN_AMT) as REMAIN_DIR_EXPN_AMT,
                 sum(OUT_EXPN_AMT) as OUT_EXPN_AMT,
                 sum(ALLOC_AMT) as ALLOC_AMT,
                 Sum (case  when TM_DIM_ID = 12 then (365/365) * CAP_CHARGE
                      end) as CAP_CHARGE,
                       100* case
                            when Sum (CAP_CHARGE)/Sum(case when TOTAL_BALANCE = 0 then 1 else TOTAL_BALANCE end) > 100 then 0
                            else
                               Sum (CAP_CHARGE)/Sum (case when TOTAL_BALANCE = 0 then 1 else TOTAL_BALANCE end)     
                            end
                        as OP_RISK_OTH_ASSET_RISK, 
                     case when (sum(REMAIN_DIR_EXPN_MF)) > 100 then 0 else nvl(sum(REMAIN_DIR_EXPN_MF),0) end as REMAIN_DIR_EXPN_MF,
                     case when (sum(OUT_AMT_EXPN_MF)) > 100 then 0 else nvl(sum(OUT_AMT_EXPN_MF),0) end as OUT_AMT_EXPN_MF,    
                     case when (sum(ALLOC_AMT_EXPN_MF)) > 100 then 0 else nvl(sum(ALLOC_AMT_EXPN_MF),0) end as ALLOC_AMT_EXPN_MF,
                 
                PROD_LEVEL7,
                MKT_LEVEL3,
                LOB_LEVEL5
                from             (
            
              Select TOTAL_BALANCE,TOTAL_EXPN_AMT,REMAIN_DIR_EXPN_AMT,OUT_EXPN_AMT,ALLOC_AMT,CAP_CHARGE
              /*, (TOTAL_EXPENSE_AMOUNT_YTD/(CASE WHEN TOTAL_BALANCE = 0 then 1
                                                                                                   else TOTAL_BALANCE 
                                                                                                   End))*100 as "EXPN_PCNT_MF" */
                                                                                                   
            ,  (REMAIN_DIR_EXPN_AMT/(CASE WHEN TOTAL_BALANCE = 0 then 1
                  else TOTAL_BALANCE 
                  End))*100 as "REMAIN_DIR_EXPN_MF" 
            ,  (OUT_EXPN_AMT/(CASE WHEN TOTAL_BALANCE = 0 then 1
                  else TOTAL_BALANCE 
                  End))*100 as "OUT_AMT_EXPN_MF"       
            ,  (ALLOC_AMT/(CASE WHEN TOTAL_BALANCE = 0 then 1
                  else TOTAL_BALANCE 
                  End))*100 as "ALLOC_AMT_EXPN_MF"         
                                                                                                     
                                                                                                   
              ,(CAP_CHARGE/(CASE WHEN TOTAL_BALANCE = 0 then 1 
                                        else TOTAL_BALANCE 
                                            End))*100 as "OP_RISK_OTH_ASSET_RISK"
                    ,case 
                        when BAL_PROD_LEVEL7 is NULL then 
                            case WHEN EXP_PROD_LEVEL7 is NULL then 
                                 CAP_PROD_LEVEL7
                            Else EXP_PROD_LEVEL7
                            End 
                     Else BAL_PROD_LEVEL7               
                     end as "PROD_LEVEL7"
                             
                    ,case
                        when BAL_MKT_LEVEL3 is NULL then
                            case WHEN EXP_MKT_LEVEL3 is NULL then
                                 CAP_MKT_LEVEL3
                            Else EXP_MKT_LEVEL3
                            End
                        Else BAL_MKT_LEVEL3         
                     end as "MKT_LEVEL3"
                    ,case
                        when BAL_LOB_LEVEL5 is NULL then
                            case WHEN EXP_LOB_LEVEL5 is NULL then
                                 CAP_LOB_LEVEL5
                            Else EXP_LOB_LEVEL5
                            END
                        Else BAL_LOB_LEVEL5         
                     end as "LOB_LEVEL5"
                     ,12 as "TM_DIM_ID"  
                from (       Select TOTAL_BALANCE,(REMAIN_DIR_EXPN_AMT+OUT_EXPN_AMT+ALLOC_AMT) as TOTAL_EXPN_AMT,REMAIN_DIR_EXPN_AMT,OUT_EXPN_AMT,ALLOC_AMT,CAP_CHARGE,
                              a.PROD_LEVEL7 as "BAL_PROD_LEVEL7",a.MKT_LEVEL3 as "BAL_MKT_LEVEL3",a.LOB_LEVEL5 as "BAL_LOB_LEVEL5"
                             ,b.PROD_LEVEL7 as "EXP_PROD_LEVEL7",b.MKT_LEVEL3 as "EXP_MKT_LEVEL3",b.LOB_LEVEL5 as "EXP_LOB_LEVEL5"
                             ,c.PROD_LEVEL7 as "CAP_PROD_LEVEL7",c.MKT_LEVEL3 as "CAP_MKT_LEVEL3",c.LOB_LEVEL5 as "CAP_LOB_LEVEL5"
                                from 
                                
                                (SELECT SUM (TOTAL_BALANCE) AS "TOTAL_BALANCE",
                              x.NAME_LEVEL2 AS "PROD_LEVEL2",
                              x.NAME_LEVEL3 AS "PROD_LEVEL3",
                              x.NAME_LEVEL4 AS "PROD_LEVEL4",
                              x.NAME_LEVEL5 AS "PROD_LEVEL5",
                              x.NAME_LEVEL6 AS "PROD_LEVEL6",
                              x.NAME_LEVEL7 AS "PROD_LEVEL7",
                              z.MKT_LEVEL3  AS "MKT_LEVEL3",
                              i.NAME_LEVEL5 AS "LOB_LEVEL5"         
                         FROM BPP_REPADM.PROD_DIM x
                              LEFT OUTER JOIN
                              (  SELECT SUM (amount) AS "TOTAL_BALANCE",
                                        PRODUCT_ID,
                                        ORGANIZATION_ID
                                   FROM (SELECT DEPENDS
                                           FROM BPP_REPADM.ACCOUNT_BINARY_DIM
                                          WHERE Parent in( 36528401,36528409)) a
                                        LEFT OUTER JOIN BPP_REPADM.LEDGER_ENTRY b
                                           ON a.DEPENDS = B.ACCOUNT_ID
                                  WHERE TO_CHAR (b.ENTRY_DATE, 'mm/dd/yyyy') =  """ + "'" + Report_dt_fmt1 + "'" + """ 
                               GROUP BY PRODUCT_ID, ORGANIZATION_ID) y
                                 ON X.LEVEL7_KEY = y.PRODUCT_ID
                              LEFT OUTER JOIN BPP_REPADM.MKT_DIM z
                                 ON z.MKT_LEVEL6_KEY = y.ORGANIZATION_ID
                              LEFT OUTER JOIN BPP_REPADM.LOB_DIM i
                                 ON i.LEVEL6_KEY = y.ORGANIZATION_ID
                        WHERE z.MKT_LEVEL6_KEY IS NOT NULL AND I.LEVEL6_KEY IS NOT NULL
                                and UPPER(z.MKT_LEVEL3) not like '%EXCLUDE%' 
                                        GROUP BY x.NAME_LEVEL2,
                              x.NAME_LEVEL3,
                              x.NAME_LEVEL4,
                              x.NAME_LEVEL5,
                              x.NAME_LEVEL6,
                              x.NAME_LEVEL7,
                              z.MKT_LEVEL3,
                              i.NAME_LEVEL5
                     ORDER BY MKT_LEVEL3, LOB_LEVEL5, PROD_LEVEL7) a 
                     full outer join (
                                        SELECT 
                                        SUM (TOTAL_EXPN_AMT) AS "TOTAL_EXPN_AMT",
                                        sum(REMAIN_DIR_EXPN_AMT) as "REMAIN_DIR_EXPN_AMT",
                                        sum(OUT_EXPN_AMT) as "OUT_EXPN_AMT",
                                        sum(ALLOC_AMT) as "ALLOC_AMT",
                              x.NAME_LEVEL2 AS "PROD_LEVEL2",
                              x.NAME_LEVEL3 AS "PROD_LEVEL3",
                              x.NAME_LEVEL4 AS "PROD_LEVEL4",
                              x.NAME_LEVEL5 AS "PROD_LEVEL5",
                              x.NAME_LEVEL6 AS "PROD_LEVEL6",
                              x.NAME_LEVEL7 AS "PROD_LEVEL7",
                              z.MKT_LEVEL3  AS "MKT_LEVEL3",
                              i.NAME_LEVEL5 AS "LOB_LEVEL5"
                         
                         FROM BPP_REPADM.PROD_DIM x
                              LEFT OUTER JOIN
                                               (  SELECT  case when PARENT = 38046991 then sum(Amount) 
                                                      else 0
                                                      end "TOTAL_EXPN_AMT",
                                                     case when PARENT = 31299375 then sum(Amount)
                                                              else 0
                                                               end  "REMAIN_DIR_EXPN_AMT",
                                                     case when PARENT = 38686060 then sum(Amount)
                                                              else 0
                                                              end  "OUT_EXPN_AMT",             
                                                     case when PARENT = 31298638 then sum(Amount)
                                                              else 0
                                                              end "ALLOC_AMT",
                                                     PRODUCT_ID,
                                                     ORGANIZATION_ID,
                                                     PARENT
                                              FROM (  Select PARENT,PRODUCT_ID,ORGANIZATION_ID,sum(amount) as amount from 
                                                        (Select PARENT,DEPENDS from bpp_repadm.ACCOUNT_BINARY_DIM where DEPENDS = 38046991 and PARENT = 38046991) x
                                                        left outer join BPP_REPADM.ledger_entry y
                                                        on   x.DEPENDS = y.account_id
                                                      where to_char(ENTRY_DATE,'mm/dd/yyyy') =  """ + "'" + Report_dt_fmt1 + "'" + """ 
                                                      group by PARENT,PRODUCT_ID,ORGANIZATION_ID
                                                      UNION ALL
                                                      Select PARENT,PRODUCT_ID,ORGANIZATION_ID,sum(amount) as amount from 
                                                      (Select PARENT,DEPENDS from bpp_repadm.ACCOUNT_BINARY_DIM where PARENT = 31299375) x
                                                      left outer join BPP_REPADM.ledger_entry y
                                                        on  x.DEPENDS = y.ACCOUNT_ID
                                                      where to_char(ENTRY_DATE,'mm/dd/yyyy') =  """ + "'" + Report_dt_fmt1 + "'" + """ 
                                                      group by PARENT,PRODUCT_ID,ORGANIZATION_ID  
                                                      UNION ALL
                                                      Select PARENT,PRODUCT_ID,ORGANIZATION_ID,sum(amount) as amount from 
                                                      (Select PARENT,DEPENDS from bpp_repadm.ACCOUNT_BINARY_DIM where PARENT = 38686060) x
                                                      left outer join BPP_REPADM.ledger_entry y
                                                      on  x.DEPENDS = y.ACCOUNT_ID
                                                      where to_char(ENTRY_DATE,'mm/dd/yyyy') =  """ + "'" + Report_dt_fmt1 + "'" + """ 
                                                      group by PARENT,PRODUCT_ID,ORGANIZATION_ID  
                                                        UNION ALL
                                                      Select PARENT,PRODUCT_ID,ORGANIZATION_ID,sum(amount) as amount from 
                                                      (Select PARENT,DEPENDS from bpp_repadm.ACCOUNT_BINARY_DIM where PARENT = 31298638) x
                                                      Left outer join BPP_REPADM.ledger_entry y
                                                      on  x.DEPENDS = y.ACCOUNT_ID
                                                      where to_char(ENTRY_DATE,'mm/dd/yyyy') =  """ + "'" + Report_dt_fmt1 + "'" + """ 
                                                      group by PARENT,PRODUCT_ID,ORGANIZATION_ID  )
                                                      
                                   group by PARENT,PRODUCT_ID,ORGANIZATION_ID                   
                                                      
                                                       ) y
                                 ON X.LEVEL7_KEY = y.PRODUCT_ID
                              LEFT OUTER JOIN BPP_REPADM.MKT_DIM z
                                 ON z.MKT_LEVEL6_KEY = y.ORGANIZATION_ID
                              LEFT OUTER JOIN BPP_REPADM.LOB_DIM i
                                 ON i.LEVEL6_KEY = y.ORGANIZATION_ID
                        WHERE z.MKT_LEVEL6_KEY IS NOT NULL AND I.LEVEL6_KEY IS NOT NULL
                            and UPPER(z.MKT_LEVEL3) not like '%EXCLUDE%' 
                     GROUP BY x.NAME_LEVEL2,
                              x.NAME_LEVEL3,
                              x.NAME_LEVEL4,
                              x.NAME_LEVEL5,
                              x.NAME_LEVEL6,
                              x.NAME_LEVEL7,
                              z.MKT_LEVEL3,
                              i.NAME_LEVEL5) b
                              on UPPER(a.MKT_LEVEL3) = UPPER(b.MKT_LEVEL3)
                              and UPPER(a.LOB_LEVEL5) = UPPER(b.LOB_LEVEL5)
                              and UPPER(a.PROD_LEVEL7) = UPPER(b.PROD_LEVEL7)
                     full outer join (SELECT SUM (CAP_CHARGE) AS "CAP_CHARGE",
                              x.NAME_LEVEL2 AS "PROD_LEVEL2",
                              x.NAME_LEVEL3 AS "PROD_LEVEL3",
                              x.NAME_LEVEL4 AS "PROD_LEVEL4",
                              x.NAME_LEVEL5 AS "PROD_LEVEL5",
                              x.NAME_LEVEL6 AS "PROD_LEVEL6",
                              x.NAME_LEVEL7 AS "PROD_LEVEL7",
                              z.MKT_LEVEL3 AS "MKT_LEVEL3",
                              i.NAME_LEVEL5 AS "LOB_LEVEL5"
                         FROM BPP_REPADM.PROD_DIM x
                              LEFT OUTER JOIN
                              (  SELECT SUM (amount) AS "CAP_CHARGE",
                                        PRODUCT_ID,
                                        ORGANIZATION_ID
                                   FROM (SELECT DEPENDS
                                           FROM BPP_REPADM.ACCOUNT_BINARY_DIM
                                          WHERE PARENT_NAME IN ('Operational Risk Bal',
                                                                'Other Assets Risk Bal')) a
                                        LEFT OUTER JOIN BPP_REPADM.LEDGER_ENTRY b
                                           ON a.DEPENDS = B.ACCOUNT_ID
                                  WHERE TO_CHAR (b.ENTRY_DATE, 'mm/dd/yyyy') =  """ + "'" + Report_dt_fmt1 + "'" + """ 
                               GROUP BY PRODUCT_ID, ORGANIZATION_ID) y
                                 ON X.LEVEL7_KEY = y.PRODUCT_ID
                              LEFT OUTER JOIN BPP_REPADM.MKT_DIM z
                                 ON z.MKT_LEVEL6_KEY = y.ORGANIZATION_ID
                              LEFT OUTER JOIN BPP_REPADM.LOB_DIM i
                                 ON i.LEVEL6_KEY = y.ORGANIZATION_ID
                        WHERE z.MKT_LEVEL6_KEY IS NOT NULL AND I.LEVEL6_KEY IS NOT NULL
                         and UPPER(z.MKT_LEVEL3) not like '%EXCLUDE%'
                     GROUP BY x.NAME_LEVEL2,
                              x.NAME_LEVEL3,
                              x.NAME_LEVEL4,
                              x.NAME_LEVEL5,
                              x.NAME_LEVEL6,
                              x.NAME_LEVEL7,
                              z.MKT_LEVEL3,
                              i.NAME_LEVEL5
                     ORDER BY MKT_LEVEL3, LOB_LEVEL5, PROD_LEVEL7  ) c
                              on UPPER(a.MKT_LEVEL3) = UPPER(c.MKT_LEVEL3)
                              and UPPER(a.LOB_LEVEL5) = UPPER(c.LOB_LEVEL5)
                              and UPPER(a.PROD_LEVEL7) = UPPER(c.PROD_LEVEL7)
                        order by a.MKT_LEVEL3,b.MKT_LEVEL3,c.MKT_LEVEL3)
                --order by PROD_LEVEL7
                ) 
                --where OP_RISK_OTH_ASSET_RISK > 100
                group by 
                PROD_LEVEL7,MKT_LEVEL3,LOB_LEVEL5
                order by PROD_LEVEL7
        )
        
        Select 
        nvl(TOTAL_BALANCE, 0) TOTAL_BALANCE
        ,nvl( TOTAL_EXPN_AMT, 0) as TOTAL_EXPN_AMT
        ,nvl( REMAIN_DIR_EXPN_AMT, 0) as REMAIN_DIR_EXPN_AMT
        ,nvl( OUT_EXPN_AMT, 0) as OUT_EXPN_AMT
        ,nvl( ALLOC_AMT, 0) as ALLOC_AMT
        ,nvl(CAP_CHARGE,0)  as CAP_CHARGE
        ,case when OP_RISK_OTH_ASSET_RISK < -100 then 0 else  cast(nvl(OP_RISK_OTH_ASSET_RISK,0) as decimal(15,10)) end as OP_RISK_OTH_ASSET_RISK
        ,case when REMAIN_DIR_EXPN_MF < -100 then 0 else cast(nvl(REMAIN_DIR_EXPN_MF,0) as decimal(15,10)) end as REMAIN_DIR_EXPN_MF
        ,case when cast(OUT_AMT_EXPN_MF as decimal(15,10)) < -100 then 0 else cast(nvl(OUT_AMT_EXPN_MF,0) as decimal(15,10)) end as OUT_AMT_EXPN_MF
        ,case when ALLOC_AMT_EXPN_MF < -100 then 0 else cast(nvl(ALLOC_AMT_EXPN_MF,0) as decimal(15,10)) end as ALLOC_AMT_EXPN_MF
        ,PROD_LEVEL7
        ,MKT_LEVEL3
        ,LOB_LEVEL5
        from temp1"""
     
#Execute ET_RB_PROJECTS and load data into a CSV file                      
curs.execute(et_rb_projects)
for row_data in curs:
        csv_file_dest='et_rb_projects.csv' # Define the csv file which needs to contain data
        outputFile = open(csv_file_dest,'w', newline='') # 'wb'
        output = csv.writer(outputFile,delimiter='|', dialect='excel')

        if printHeader: # add column headers if requested
            cols = []
            for col in curs.description:
                cols.append(col[0])
            output.writerow(cols)

        for row_data in curs: # add table rows
            output.writerow(row_data)

        outputFile.close()
        
        
#Execute rb_deposit_ops and load data into a CSV file for a table:STG_BPP_EXPN_ET_RB_PRJ_BRK                      
curs.execute(rb_deposit_ops)
for row_data in curs:
        csv_file_dest='STG_BPP_EXPN_ET_RB_PRJ_BRK.csv' # Define the csv file which needs to contain data
        outputFile = open(csv_file_dest,'w', newline='') # 'wb'
        output = csv.writer(outputFile,delimiter='|', dialect='excel')

        if printHeader: # add column headers if requested
            cols = []
            for col in curs.description:
                cols.append(col[0])
            output.writerow(cols)

        for row_data in curs: # add table rows
            output.writerow(row_data)

        outputFile.close()
        
#Execute rb_loan_ops and load data into a CSV file for a table:STG_BPP_RB_LOAN_OPS_BRK                      
curs.execute(rb_loan_ops)
for row_data in curs:
        csv_file_dest='STG_BPP_RB_LOAN_OPS_BRK.csv' # Define the csv file which needs to contain data
        outputFile = open(csv_file_dest,'w', newline='') # 'wb'
        output = csv.writer(outputFile,delimiter='|', dialect='excel')

        if printHeader: # add column headers if requested
            cols = []
            for col in curs.description:
                cols.append(col[0])
            output.writerow(cols)

        for row_data in curs: # add table rows
            output.writerow(row_data)

        outputFile.close()
        
#Execute rb_bank_ops and load data into a CSV file for a table:STG_BPP_BANK_OPS_CUST_BRK                  
curs.execute(bank_ops)
for row_data in curs:
        csv_file_dest='STG_BPP_BANK_OPS_CUST_BRK.csv' # Define the csv file which needs to contain data
        outputFile = open(csv_file_dest,'w', newline='') # 'wb'
        output = csv.writer(outputFile,delimiter='|', dialect='excel')

        if printHeader: # add column headers if requested
            cols = []
            for col in curs.description:
                cols.append(col[0])
            output.writerow(cols)

        for row_data in curs: # add table rows
            output.writerow(row_data)

        outputFile.close()        
		
#Execute rb_bank_ops and load data into a CSV file for a table: STG_BPP_EXPN_BANK_OPS_DEP_BRK                     
curs.execute(bank_ops)
for row_data in curs:
        csv_file_dest='STG_BPP_EXPN_BANK_OPS_DEP_BRK.csv' # Define the csv file which needs to contain data
        outputFile = open(csv_file_dest,'w', newline='') # 'wb'
        output = csv.writer(outputFile,delimiter='|', dialect='excel')

        if printHeader: # add column headers if requested
            cols = []
            for col in curs.description:
                cols.append(col[0])
            output.writerow(cols)

        for row_data in curs: # add table rows
            output.writerow(row_data)

        outputFile.close() 		
        
#Execute lock_box and load data into a CSV file for a table: STG_BPP_EXPN_BANK_OPS_LBOX                    
curs.execute(lock_box)
for row_data in curs:
        csv_file_dest='STG_BPP_EXPN_BANK_OPS_LBOX.csv' # Define the csv file which needs to contain data
        outputFile = open(csv_file_dest,'w', newline='') # 'wb'
        output = csv.writer(outputFile,delimiter='|', dialect='excel')

        if printHeader: # add column headers if requested
            cols = []
            for col in curs.description:
                cols.append(col[0])
            output.writerow(cols)

        for row_data in curs: # add table rows
            output.writerow(row_data)

        outputFile.close()          
        
#Execute bpp_expn_brkdwn and load data into a CSV file for a table: STG_BPP_EXPN_BRK_DWN                  
curs.execute(bpp_expn_brkdwn)
for row_data in curs:
        csv_file_dest='STG_BPP_EXPN_BRK_DWN.csv' # Define the csv file which needs to contain data
        outputFile = open(csv_file_dest,'w', newline='') # 'wb'
        output = csv.writer(outputFile,delimiter='|', dialect='excel')
        if printHeader: # add column headers if requested
            cols = []
            for col in curs.description:
                cols.append(col[0])
            output.writerow(cols)
        for row_data in curs: # add table rows
            output.writerow(row_data)
        outputFile.close()          
        
        
#Execute stg_capital_and_expense_bpp and load data into a CSV file for a table:STG_CAPITAL_AND_EXPENSE_BPP                     
curs.execute(stg_capital_and_exp)
for row_data in curs:
        csv_file_dest='STG_CAPITAL_AND_EXPENSE_BPP.csv' # Define the csv file which needs to contain data
        outputFile = open(csv_file_dest,'w', newline='') # 'wb'
        output = csv.writer(outputFile,delimiter='|', dialect='excel')

        if printHeader: # add column headers if requested
            cols = []
            for col in curs.description:
                cols.append(col[0])
            output.writerow(cols)

        for row_data in curs: # add table rows
            output.writerow(row_data)

        outputFile.close()         
