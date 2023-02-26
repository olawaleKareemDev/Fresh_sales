import pandas as pd
import numpy as np
import mysql.connector
from datetime import datetime, timedelta
import os
from email_validator import validate_email, EmailNotValidError
import re
import sys


from contacts import dummy_data
from contacts import utils as contacts_utils


class DataExtraction:

    def __init__(self) -> None:
        pass


    def cleanData(self, size):


        Mifosdb = mysql.connector.connect(
            host=os.environ.get("MIFOS_DB_HOST"),
            user=os.environ.get("MIFOS_DB_USER"),
            passwd=os.environ.get("MIFOS_DB_PASSWORD"),
            database = os.environ.get("MIFOS_DB_NAME") 
        )

        Dumpdb = mysql.connector.connect(
            host=os.environ.get("DUMP_DB_HOST"),
            user=os.environ.get("MIFOS_DB_USER"),
            passwd=os.environ.get("MIFOS_DB_PASSWORD"),
            database = os.environ.get("DUMP_DB_NAME") 
        )

        # creates empty dataframe with the required fields
        corp_temp = pd.DataFrame(columns = ['client_name', 'phone_no', 'email', 'contact_address', 
                                            'nature_of_business', 'industry/sector', 'date_of_incorporation', 'director_date_of_birth',
                                            'director_gender', 'account_officer', 'account_type', 'account_no', 'product_name', 'account_balance', 
                                            'loan_value', 'loan_repayment_date', 'total_balance_all_accounts', 
                                            'debit_balance', 'loan_maturity_date', 'total_credit_amount'])

        #ndic monthly report for coopeerate accounts
        ndic =  '''
            SELECT 
            ifnull(mc.id, mg.id) AS client_id,
            -- ifnull(mc.lastname, mg.display_name) AS last_name,
            -- ifnull(mc.middlename, mg.display_name) AS middle_name,
            -- ifnull(mc.firstname, mg.display_name) AS first_name,
            ifnull(mc.display_name, mg.display_name) as client_name,
            replace(ai.`Mobile Number`, '-', '') AS Phone_no,
            replace(ai.`Email Address`, '-', '') as email,
            replace(ai.`Mailing Address`, '-', '') AS customer_contact_address,
            
            mcnp.incorp_no as rc_number,
            
            case when length(left(ob.BVN, 11)) = 11 then left(ob.BVN, 11) else null end as director_bvn,

            'N/a on Mifos' as 'nature_of_business',
            'N/a on Mifos' as 'industry/sector',
                mc.date_of_birth as 'date_of_incorporation',
            
            s.display_name as 'account_officer',
            msa.account_no AS account_no,
            case  msa.account_type_enum 
                when '1' then 'Single entity account'
                when '2' then 'Joint account'
                when '3' then 'JLG account' end AS account_type,
            msp.name as 'product_name',
            msa.account_balance_derived AS account_balance,
            
            l.amount_disbursed as loan_value,
            l.loan_outstanding as outstanding_loan_amount,
            l.last_repayment_date,

            d.aggregated_deposit as total_balance_all_accounts,
            
            (l.aggregated_loan_balance) * (-1) as debit_balance,
            
            -- l.maturity_date,

            case when msa.account_balance_derived < -1000 then msa.overdraft_closedon_date else l.maturity_date end as maturity_date,
            
            msa.total_deposits_derived as total_credit_amount       

            -- left(odb.BVN, 11) AS client_bvn,
            -- Null as account_category,
            
            -- case when isnull(msa.closedon_date) then 'Active' 
                -- ELSE 'Dormant' END AS account_status,
            -- NULL AS 'portion_of_deposit_as_collateral',
            -- case when l.collateral_description like '%deposit%' or l.collateral_description like '%Deposit%' then l.collateral_value
                -- else NULL end as portion_of_deposit_pledged_as_collateral,
                
            
            -- l.loan_type,
            -- l.date_granted,
            -- l.loan_outstanding,
            -- l.principal,
            -- l.interest,
            -- l.write_off_or_waiver,
            -- l.cash_backed, 
            -- l.cash_amount_if_yes,
            -- l.secured,
            -- l.collateral_type,
            -- l.collateral_value,
            -- l.collateral_description,
            -- l.collateral_full_address,
            -- l.collateral_status,
            -- l.guarantor_name,
            -- l.guarantor_BVN,
            -- l.guarantor_ID,
            -- l.guarantor_address,
            -- l.guarantor_phone_no,
            
            -- (d.aggregated_deposit) - (l.aggregated_loan_balance) as "Net_depositor's_balance"
            
            FROM m_client mc
            left join `Other Bank Details` ob on ob.client_id = mc.id
            left JOIN m_savings_account msa ON msa.client_id = mc.id
            left JOIN m_savings_product msp ON msp.id = msa.product_id
            left JOIN `Address Info` ai ON ai.client_id = mc.id
            left JOIN m_group mg ON mg.id = msa.group_id
            left join m_client_non_person mcnp on mcnp.client_id = mc.id
            LEFT JOIN `Other Bank Details` odb ON odb.client_id = mc.id
            left join 
                    (select 
                    ms.id,
                    ms.display_name
                    from m_staff ms
                    where ms.is_active = 1) s on s.id = mc.staff_id

            LEFT JOIN 

                (select
                    msa.client_id,
                    SUM(msa.account_balance_derived) AS aggregated_deposit
                FROM m_savings_account msa
                JOIN m_client c ON c.id = msa.client_id
                where msa.status_enum = 300
                GROUP BY msa.client_id) d ON d.client_id = mc.id

            LEFT JOIN

                (select
                ml.id as loan_id,
                ml.client_id AS client_id,
                mpl.name AS loan_type,
                ml.disbursedon_date AS date_granted,
                ml.principal_disbursed_derived AS amount_disbursed,
                lt.last_repayment_date,
                lt.maturity_date,
                ml.total_outstanding_derived AS loan_outstanding,
                ml.principal_outstanding_derived AS principal,
                ml.interest_outstanding_derived AS interest,
                case when ml.total_waived_derived = 0 then ml.total_writtenoff_derived 
                    ELSE ml.total_waived_derived END AS write_off_or_waiver,
                case when mpl.name like '%CASH-BACKED%' then 'YES'
                    ELSE 'NO' end AS cash_backed,
                case when  mpl.name like '%CASH-BACKED%' then ml.principal_disbursed_derived 
                    else NULL end AS cash_amount_if_yes,
                case when !isnull(mlc.type_cv_id) then 'YES'
                    ELSE 'NO' END AS 'secured',
                mlc.description AS collateral_type,
                mlc.value AS collateral_value,
                mlc.description AS collateral_description,
                NULL AS collateral_full_address,
                'Perfected' AS collateral_status,
                NULL AS "guarantor_name",
                NULL AS "guarantor_BVN",
                NULL AS "guarantor_ID",
                NULL AS "guarantor_address",
                NULL AS "guarantor_phone_no",
                e.total_loan_balance as aggregated_loan_balance
                from m_loan ml
                left JOIN m_product_loan mpl ON mpl.id = ml.product_id
                LEFT JOIN m_loan_collateral mlc ON mlc.loan_id = ml.id
                left join 
                    (SELECT
                    ml.client_id,
                    mlt.loan_id,
                    mlt.amount,
                    MAX(mlt.transaction_date) as last_repayment_date,
                    ml.disbursedon_date AS disbursement_date,
                    ml.expected_maturedon_date AS maturity_date,
                    ml.loan_status_id AS loan_status
                    FROM m_loan_transaction mlt
                    JOIN m_loan ml ON ml.id = mlt.loan_id
                    WHERE mlt.transaction_type_enum IN (2,5)
                    AND mlt.is_reversed = 0
                    AND ml.loan_status_id = 300
                    GROUP BY mlt.loan_id) lt on lt.loan_id = ml.id
                left join
                    (select
                    lo.client_id,
                    sum(lo.total_outstanding_derived) as 'total_loan_balance'
                    FROM m_loan lo
                    where lo.loan_status_id = 300
                    GROUP BY lo.client_id) e ON e.client_id = ml.client_id
                    
                where ml.loan_status_id = 300
                GROUP BY ml.account_no) l ON l.client_id = mc.id

            WHERE mc.id in (SELECT distinct
                            a.id
                            FROM
                            (SELECT
                            cl.id,
                            sa.account_no,
                            sa.product_id,
                            sp.name AS product_name
                            FROM m_client cl
                            left JOIN m_savings_account sa ON sa.client_id = cl.id
                            left JOIN m_savings_product sp ON sp.id = sa.product_id 
                            WHERE sa.product_id = 26
                            AND sa.status_enum = 300) a)

            and msa.account_type_enum in (1,2,3)
            and msa.status_enum = 300
            GROUP BY msa.account_no
            '''

        try:
             corp = pd.read_sql_query(ndic, Mifosdb)
        except Exception as e:
            print(e)
            print('moving on mifoddb connection')

        corp['client_id'] = corp['client_id'].astype(str)


        corp['total_balance_all_accounts'] = corp['total_balance_all_accounts'].fillna(0).astype(int)
        corp['debit_balance']= corp['debit_balance'].fillna(0).astype(int)
        corp['account_balance']= corp['account_balance'].fillna(0).astype(int)


        # spooling for the CEO details using BVN
        corp['director_bvn'] = corp['director_bvn'].astype(str)
        strbvn = "'" + corp['director_bvn'] + "'"
        bvn = ','.join([str(x) for x in strbvn.tolist()]) 

        ceo = f'''
            SELECT
            avb.BVN_REGNO AS director_bvn,
            concat(avb.BVN_FIRSTNAME, ' ', IFNULL(avb.BVN_MIDDLENAME, ' '), ' ', avb.BVN_LASTNAME) AS director_name,
            ifnull(avb.BVN_PHONE, '') as director_phone_no,
            ifnull(avb.BVN_EMAIL, '') as director_email,
            avb.BVN_GENDER as director_gender,
            avb.BVN_DOB as director_DOB
            
            FROM AM_VERIFIED_BVN avb
            WHERE avb.BVN_REGNO IN ({bvn})
            '''
        
        try:
             df_ceo = pd.read_sql_query(ceo, Dumpdb)
        except Exception as e:
            print(e)
            print('moving on dumpdb connection')

        dfj = corp.merge(df_ceo, on = 'director_bvn', how = 'left')


        # filling the missing bvn data with zero (0)
        dfj['director_bvn'] = dfj['director_bvn'].fillna('')

        # cleaning the date columns
        new_date = []
        for date in pd.to_datetime(dfj['date_of_incorporation']):
            try:
                new_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                new_date.append('01/01/1900')
                
        dfj['date_of_incorporation'] = new_date

        new_date = [] 
        for date in dfj['director_DOB']:
            if date == '28-May--1991':
                date = date.replace('28-May--1991', '28-May-1991')
                new_date.append(date)
            else:
                new_date.append(date)

        clean_date = []
        for date in pd.to_datetime(new_date):
            try:
                clean_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                clean_date.append('01/01/1900')
                
        dfj['director_DOB'] = clean_date


        new_date = [] 
        for date in pd.to_datetime(dfj['last_repayment_date']):
            try:
                new_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                new_date.append('01/01/1900')
                
        dfj['last_repayment_date'] = new_date


        new_date = [] 
        for date in pd.to_datetime(dfj['maturity_date']):
            try:
                new_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                new_date.append('01/01/1900')
                
        dfj['maturity_date'] = new_date


        # cleaning the RC number column
        print('at rc cleaning')
        import re 
        new_rc = []
        for txt in dfj['rc_number']:
            try:
                x = re.findall("[0-9]", txt)
                rc = ''.join([str(i) for i in x])
                new_rc.append(rc)
            except TypeError:
                new_rc.append('0')
        dfj['rc_number'] = new_rc

        # cleaning phone number column
        new_ph = []
        for txt in dfj['Phone_no']:
            try:
                x = re.findall("[0-9]", txt)
                rc = ''.join([str(i) for i in x])
                new_ph.append(rc)
            except TypeError:
                new_ph.append('0')

        dfj['Phone_no'] = new_ph

        new_ph = []
        for no in dfj['Phone_no']:
            if len(str(no)) > 11:
                no = float(str(no)[:11] + '.' + str(no)[11:])
                new_ph.append(no)
            else:
                new_ph.append(no)
        dfj['Phone_no'] = new_ph


        new_ph = []
        for txt in dfj['director_phone_no']:
            try:
                x = re.findall("[0-9]", txt)
                rc = ''.join([str(i) for i in x])
                new_ph.append(rc)
            except TypeError:
                new_ph.append('0')

        dfj['director_phone_no'] = new_ph

        new_ph = []
        for no in dfj['director_phone_no']:
            if len(str(no)) > 11:
                no = float(str(no)[:11] + '.' + str(no)[11:])
                new_ph.append(no)
            else:
                new_ph.append(no)
        dfj['director_phone_no'] = new_ph
                


        new_bv = []
        for txt in dfj['director_bvn']:
            try:
                x = re.findall("[0-9]", txt)
                rc = ''.join([str(i) for i in x])
                new_bv.append(rc)
            except TypeError:
                new_bv.append('0')

        dfj['director_bvn'] = new_bv


        # re-cleaning
        print('at recleaning')
        reclean = []
        for date in dfj['director_DOB']:
            if date == '01/01/1900':
                date = date.replace('01/01/1900', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['director_DOB'] = reclean


        reclean = []
        for date in dfj['date_of_incorporation']:
            if date == '01/01/1900':
                date = date.replace('01/01/1900', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['date_of_incorporation'] = reclean


        reclean = []
        for date in dfj['last_repayment_date']:
            if date == '01/01/1900':
                date = date.replace('01/01/1900', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['last_repayment_date'] = reclean


        reclean = []
        for date in dfj['maturity_date']:
            if date == '01/01/1900':
                date = date.replace('01/01/1900', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['maturity_date'] = reclean


        reclean = []
        for date in dfj['rc_number']:
            if date == '0':
                date = date.replace('0', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['rc_number'] = reclean

        reclean = []
        for date in dfj['director_bvn']:
            if date == 'None':
                date = date.replace('None', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['director_bvn'] = reclean


        reclean = []
        for date in dfj['Phone_no']:
            if date == '0':
                date = date.replace('0', '')
                reclean.append(date)
            else:
                reclean.append(date)
        dfj['Phone_no'] = reclean



        new_ph = []
        for no in dfj['Phone_no']:
            if len(str(no)) <= 5:
                no = ''
                new_ph.append(no)
            else:
                new_ph.append(no)
        dfj['Phone_no'] = new_ph


        reclean = []
        for date in dfj['director_bvn']:
            if date == '0':
                date = date.replace('0', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['director_bvn'] = reclean

        # email validation 
        # from email_validator import validate_email, EmailNotValidError
        print('at email validation')
        val = []
        counter = 1
        for email in dfj['email']:
            try:
                validate_email(email)
                val.append(email)
            except Exception as e:
                val.append(f'clienthasnoemail{counter}@gmail.com')
                counter += 1
                
        dfj['email'] = val


        final_corp = dfj[['client_id', 'client_name', 'Phone_no', 'email',
            'customer_contact_address', 'rc_number', 'director_bvn',
                'director_name', 'director_phone_no', 'director_email',
                'director_gender', 'director_DOB',
            'nature_of_business', 'industry/sector', 'date_of_incorporation',
            'account_officer', 'account_no', 'account_type', 'product_name',
            'account_balance', 'loan_value', 'outstanding_loan_amount',
            'last_repayment_date', 'total_balance_all_accounts', 'debit_balance',
            'maturity_date', 'total_credit_amount']]



        # test_data = final_corp[:1]

        print('checking data upload size')
        if size == 500:
            print('data within 500 df')
            test_data = final_corp[:500]
            starting_point =1
        else:
            print('data beyond 500 df')
            if  size <= len(final_corp):
                val, rem = divmod(size, 500)
                if val == 1:
                    test_data = final_corp[500:size]
                    starting_point = 500
                if val > 1:
                    starting_point =500 * (val-1)
                    test_data = final_corp[starting_point:size]

            else:
                return False, f"data out of scope. This is the number of data currently available {len(final_corp)}", len(final_corp)


        # test_data = final_corp
        # print(test_data, len(test_data))

        return_data = []
        for ind in test_data.index:
            return_data.append({
                
                'first_name': str(test_data.get("client_name")[ind]),  # client_name is system first_name
                'last_name': ' ',
                'mobile_number':str(test_data.get("Phone_no")[ind]),
                'emails':str(test_data.get("email")[ind]),

                'custom_field':{
                    'cf_client_id': str(test_data.get("client_id")[ind]), 
                    'cf_customer_contact_address':str(test_data.get("customer_contact_address")[ind]),
                    'cf_rc_number':str(test_data.get("rc_number")[ind]),
                    'cf_director_bvn':str(test_data.get("director_bvn")[ind]),
                    'cf_director_name':str(test_data.get("director_name")[ind]),
                    'cf_director_phone_no':str(test_data.get("director_phone_no")[ind]),
                    'cf_director_email':str(test_data.get("director_email")[ind]),
                    'cf_director_gender':str(test_data.get("director_gender")[ind]),
                    'cf_director_DOB':str(test_data.get("director_DOB")[ind]),
                    'cf_nature_of_business':str(test_data.get("nature_of_business")[ind]),
                    'cf_industry/sector':str(test_data.get("industry/sector")[ind]),
                    'cf_date_of_incorporation':str(test_data.get("date_of_incorporation")[ind]),
                    'cf_account_officer':str(test_data.get("account_officer")[ind]), 
                    'cf_account_no':str(test_data.get("account_no")[ind]),
                    'cf_account_type':str(test_data.get("account_type")[ind]),
                    'cf_product_name':str(test_data.get("product_name")[ind]),
                    'cf_account_balance':str(test_data.get("account_balance")[ind]),
                    'cf_loan_value':str(test_data.get("loan_value")[ind]),
                    'cf_outstanding_loan_amount':str(test_data.get("outstanding_loan_amount")[ind]),
                    'cf_last_repayment_date':str(test_data.get("last_repayment_date")[ind]),
                    'cf_total_balance_all_accounts':str(test_data.get("total_balance_all_accounts")[ind]),
                    'cf_debit_balance':str(test_data.get("debit_balance")[ind]),
                    'cf_maturity_date':str(test_data.get("maturity_date")[ind]),
                    'cf_total_credit_amount':str(test_data.get("total_credit_amount")[ind])
                }

            })

        # print('sample data to check loan maturity')
        # print(return_data[:3])


        return True, return_data, len(final_corp), starting_point


    def get_cooperate_data(self, batch):


        Mifosdb = mysql.connector.connect(
            host=os.environ.get("MIFOS_DB_HOST"),
            user=os.environ.get("MIFOS_DB_USER"),
            passwd=os.environ.get("MIFOS_DB_PASSWORD"),
            database = os.environ.get("MIFOS_DB_NAME") 
        )

        Dumpdb = mysql.connector.connect(
            host=os.environ.get("DUMP_DB_HOST"),
            user=os.environ.get("MIFOS_DB_USER"),
            passwd=os.environ.get("MIFOS_DB_PASSWORD"),
            database = os.environ.get("DUMP_DB_NAME") 
        )

        # creates empty dataframe with the required fields
        corp_temp = pd.DataFrame(columns = ['client_name', 'phone_no', 'email', 'contact_address', 
                                            'nature_of_business', 'industry/sector', 'date_of_incorporation', 'director_date_of_birth',
                                            'director_gender', 'account_officer', 'account_type', 'account_no', 'product_name', 'account_balance', 
                                            'loan_value', 'loan_repayment_date', 'total_balance_all_accounts', 
                                            'debit_balance', 'loan_maturity_date', 'total_credit_amount'])

        #ndic monthly report for coopeerate accounts
        ndic =  '''
            SELECT 
            ifnull(mc.id, mg.id) AS client_id,
            -- ifnull(mc.lastname, mg.display_name) AS last_name,
            -- ifnull(mc.middlename, mg.display_name) AS middle_name,
            -- ifnull(mc.firstname, mg.display_name) AS first_name,
            ifnull(mc.display_name, mg.display_name) as client_name,
            replace(ai.`Mobile Number`, '-', '') AS Phone_no,
            replace(ai.`Email Address`, '-', '') as email,
            replace(ai.`Mailing Address`, '-', '') AS customer_contact_address,
            
            mcnp.incorp_no as rc_number,
            
            case when length(left(ob.BVN, 11)) = 11 then left(ob.BVN, 11) else null end as director_bvn,

            'N/a on Mifos' as 'nature_of_business',
            'N/a on Mifos' as 'industry/sector',
                mc.date_of_birth as 'date_of_incorporation',
            
            s.display_name as 'account_officer',
            msa.account_no AS account_no,
            case  msa.account_type_enum 
                when '1' then 'Single entity account'
                when '2' then 'Joint account'
                when '3' then 'JLG account' end AS account_type,
            msp.name as 'product_name',
            msa.account_balance_derived AS account_balance,
            
            l.amount_disbursed as loan_value,
            l.loan_outstanding as outstanding_loan_amount,
            l.last_repayment_date,

            d.aggregated_deposit as total_balance_all_accounts,
            
            (l.aggregated_loan_balance) * (-1) as debit_balance,
            
            -- l.maturity_date,

            case when msa.account_balance_derived < -1000 then msa.overdraft_closedon_date else l.maturity_date end as maturity_date,
            
            msa.total_deposits_derived as total_credit_amount       

            -- left(odb.BVN, 11) AS client_bvn,
            -- Null as account_category,
            
            -- case when isnull(msa.closedon_date) then 'Active' 
                -- ELSE 'Dormant' END AS account_status,
            -- NULL AS 'portion_of_deposit_as_collateral',
            -- case when l.collateral_description like '%deposit%' or l.collateral_description like '%Deposit%' then l.collateral_value
                -- else NULL end as portion_of_deposit_pledged_as_collateral,
                
            
            -- l.loan_type,
            -- l.date_granted,
            -- l.loan_outstanding,
            -- l.principal,
            -- l.interest,
            -- l.write_off_or_waiver,
            -- l.cash_backed, 
            -- l.cash_amount_if_yes,
            -- l.secured,
            -- l.collateral_type,
            -- l.collateral_value,
            -- l.collateral_description,
            -- l.collateral_full_address,
            -- l.collateral_status,
            -- l.guarantor_name,
            -- l.guarantor_BVN,
            -- l.guarantor_ID,
            -- l.guarantor_address,
            -- l.guarantor_phone_no,
            
            -- (d.aggregated_deposit) - (l.aggregated_loan_balance) as "Net_depositor's_balance"
            
            FROM m_client mc
            left join `Other Bank Details` ob on ob.client_id = mc.id
            left JOIN m_savings_account msa ON msa.client_id = mc.id
            left JOIN m_savings_product msp ON msp.id = msa.product_id
            left JOIN `Address Info` ai ON ai.client_id = mc.id
            left JOIN m_group mg ON mg.id = msa.group_id
            left join m_client_non_person mcnp on mcnp.client_id = mc.id
            LEFT JOIN `Other Bank Details` odb ON odb.client_id = mc.id
            left join 
                    (select 
                    ms.id,
                    ms.display_name
                    from m_staff ms
                    where ms.is_active = 1) s on s.id = mc.staff_id

            LEFT JOIN 

                (select
                    msa.client_id,
                    SUM(msa.account_balance_derived) AS aggregated_deposit
                FROM m_savings_account msa
                JOIN m_client c ON c.id = msa.client_id
                where msa.status_enum = 300
                GROUP BY msa.client_id) d ON d.client_id = mc.id

            LEFT JOIN

                (select
                ml.id as loan_id,
                ml.client_id AS client_id,
                mpl.name AS loan_type,
                ml.disbursedon_date AS date_granted,
                ml.principal_disbursed_derived AS amount_disbursed,
                lt.last_repayment_date,
                lt.maturity_date,
                ml.total_outstanding_derived AS loan_outstanding,
                ml.principal_outstanding_derived AS principal,
                ml.interest_outstanding_derived AS interest,
                case when ml.total_waived_derived = 0 then ml.total_writtenoff_derived 
                    ELSE ml.total_waived_derived END AS write_off_or_waiver,
                case when mpl.name like '%CASH-BACKED%' then 'YES'
                    ELSE 'NO' end AS cash_backed,
                case when  mpl.name like '%CASH-BACKED%' then ml.principal_disbursed_derived 
                    else NULL end AS cash_amount_if_yes,
                case when !isnull(mlc.type_cv_id) then 'YES'
                    ELSE 'NO' END AS 'secured',
                mlc.description AS collateral_type,
                mlc.value AS collateral_value,
                mlc.description AS collateral_description,
                NULL AS collateral_full_address,
                'Perfected' AS collateral_status,
                NULL AS "guarantor_name",
                NULL AS "guarantor_BVN",
                NULL AS "guarantor_ID",
                NULL AS "guarantor_address",
                NULL AS "guarantor_phone_no",
                e.total_loan_balance as aggregated_loan_balance
                from m_loan ml
                left JOIN m_product_loan mpl ON mpl.id = ml.product_id
                LEFT JOIN m_loan_collateral mlc ON mlc.loan_id = ml.id
                left join 
                    (SELECT
                    ml.client_id,
                    mlt.loan_id,
                    mlt.amount,
                    MAX(mlt.transaction_date) as last_repayment_date,
                    ml.disbursedon_date AS disbursement_date,
                    ml.expected_maturedon_date AS maturity_date,
                    ml.loan_status_id AS loan_status
                    FROM m_loan_transaction mlt
                    JOIN m_loan ml ON ml.id = mlt.loan_id
                    WHERE mlt.transaction_type_enum IN (2,5)
                    AND mlt.is_reversed = 0
                    AND ml.loan_status_id = 300
                    GROUP BY mlt.loan_id) lt on lt.loan_id = ml.id
                left join
                    (select
                    lo.client_id,
                    sum(lo.total_outstanding_derived) as 'total_loan_balance'
                    FROM m_loan lo
                    where lo.loan_status_id = 300
                    GROUP BY lo.client_id) e ON e.client_id = ml.client_id
                    
                where ml.loan_status_id = 300
                GROUP BY ml.account_no) l ON l.client_id = mc.id

            WHERE mc.id in (SELECT distinct
                            a.id
                            FROM
                            (SELECT
                            cl.id,
                            sa.account_no,
                            sa.product_id,
                            sp.name AS product_name
                            FROM m_client cl
                            left JOIN m_savings_account sa ON sa.client_id = cl.id
                            left JOIN m_savings_product sp ON sp.id = sa.product_id 
                            WHERE sa.product_id = 26
                            AND sa.status_enum = 300) a)

            and msa.account_type_enum in (1,2,3)
            and msa.status_enum = 300
            GROUP BY msa.account_no
            '''

        try:
             corp = pd.read_sql_query(ndic, Mifosdb)
        except Exception as e:
            print(e)
            print('moving on mifoddb connection')

        corp['client_id'] = corp['client_id'].astype(str)


        corp['total_balance_all_accounts'] = corp['total_balance_all_accounts'].fillna(0).astype(int)
        corp['debit_balance']= corp['debit_balance'].fillna(0).astype(int)
        corp['account_balance']= corp['account_balance'].fillna(0).astype(int)


        # spooling for the CEO details using BVN
        corp['director_bvn'] = corp['director_bvn'].astype(str)
        strbvn = "'" + corp['director_bvn'] + "'"
        bvn = ','.join([str(x) for x in strbvn.tolist()]) 

        ceo = f'''
            SELECT
            avb.BVN_REGNO AS director_bvn,
            concat(avb.BVN_FIRSTNAME, ' ', IFNULL(avb.BVN_MIDDLENAME, ' '), ' ', avb.BVN_LASTNAME) AS director_name,
            ifnull(avb.BVN_PHONE, '') as director_phone_no,
            ifnull(avb.BVN_EMAIL, '') as director_email,
            avb.BVN_GENDER as director_gender,
            avb.BVN_DOB as director_DOB
            
            FROM AM_VERIFIED_BVN avb
            WHERE avb.BVN_REGNO IN ({bvn})
            '''
        
        try:
             df_ceo = pd.read_sql_query(ceo, Dumpdb)
        except Exception as e:
            print(e)
            print('moving on dumpdb connection')

        dfj = corp.merge(df_ceo, on = 'director_bvn', how = 'left')


        # filling the missing bvn data with zero (0)
        dfj['director_bvn'] = dfj['director_bvn'].fillna('')

        # cleaning the date columns
        new_date = []
        for date in pd.to_datetime(dfj['date_of_incorporation']):
            try:
                new_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                new_date.append('01/01/1900')
                
        dfj['date_of_incorporation'] = new_date

        new_date = [] 
        for date in dfj['director_DOB']:
            if date == '28-May--1991':
                date = date.replace('28-May--1991', '28-May-1991')
                new_date.append(date)
            else:
                new_date.append(date)

        clean_date = []
        for date in pd.to_datetime(new_date):
            try:
                clean_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                clean_date.append('01/01/1900')
                
        dfj['director_DOB'] = clean_date


        new_date = [] 
        for date in pd.to_datetime(dfj['last_repayment_date']):
            try:
                new_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                new_date.append('01/01/1900')
                
        dfj['last_repayment_date'] = new_date


        new_date = [] 
        for date in pd.to_datetime(dfj['maturity_date']):
            try:
                new_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                new_date.append('01/01/1900')
                
        dfj['maturity_date'] = new_date


        # cleaning the RC number column
        print('at rc cleaning')
        import re 
        new_rc = []
        for txt in dfj['rc_number']:
            try:
                x = re.findall("[0-9]", txt)
                rc = ''.join([str(i) for i in x])
                new_rc.append(rc)
            except TypeError:
                new_rc.append('0')
        dfj['rc_number'] = new_rc

        # cleaning phone number column
        new_ph = []
        for txt in dfj['Phone_no']:
            try:
                x = re.findall("[0-9]", txt)
                rc = ''.join([str(i) for i in x])
                new_ph.append(rc)
            except TypeError:
                new_ph.append('0')

        dfj['Phone_no'] = new_ph

        new_ph = []
        for no in dfj['Phone_no']:
            if len(str(no)) > 11:
                no = float(str(no)[:11] + '.' + str(no)[11:])
                new_ph.append(no)
            else:
                new_ph.append(no)
        dfj['Phone_no'] = new_ph


        new_ph = []
        for txt in dfj['director_phone_no']:
            try:
                x = re.findall("[0-9]", txt)
                rc = ''.join([str(i) for i in x])
                new_ph.append(rc)
            except TypeError:
                new_ph.append('0')

        dfj['director_phone_no'] = new_ph

        new_ph = []
        for no in dfj['director_phone_no']:
            if len(str(no)) > 11:
                no = float(str(no)[:11] + '.' + str(no)[11:])
                new_ph.append(no)
            else:
                new_ph.append(no)
        dfj['director_phone_no'] = new_ph
                


        new_bv = []
        for txt in dfj['director_bvn']:
            try:
                x = re.findall("[0-9]", txt)
                rc = ''.join([str(i) for i in x])
                new_bv.append(rc)
            except TypeError:
                new_bv.append('0')

        dfj['director_bvn'] = new_bv


        # re-cleaning
        print('at recleaning')
        reclean = []
        for date in dfj['director_DOB']:
            if date == '01/01/1900':
                date = date.replace('01/01/1900', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['director_DOB'] = reclean


        reclean = []
        for date in dfj['date_of_incorporation']:
            if date == '01/01/1900':
                date = date.replace('01/01/1900', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['date_of_incorporation'] = reclean


        reclean = []
        for date in dfj['last_repayment_date']:
            if date == '01/01/1900':
                date = date.replace('01/01/1900', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['last_repayment_date'] = reclean


        reclean = []
        for date in dfj['maturity_date']:
            if date == '01/01/1900':
                date = date.replace('01/01/1900', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['maturity_date'] = reclean


        reclean = []
        for date in dfj['rc_number']:
            if date == '0':
                date = date.replace('0', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['rc_number'] = reclean

        reclean = []
        for date in dfj['director_bvn']:
            if date == 'None':
                date = date.replace('None', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['director_bvn'] = reclean


        reclean = []
        for date in dfj['Phone_no']:
            if date == '0':
                date = date.replace('0', '')
                reclean.append(date)
            else:
                reclean.append(date)
        dfj['Phone_no'] = reclean



        new_ph = []
        for no in dfj['Phone_no']:
            if len(str(no)) <= 5:
                no = ''
                new_ph.append(no)
            else:
                new_ph.append(no)
        dfj['Phone_no'] = new_ph


        reclean = []
        for date in dfj['director_bvn']: 
            if date == '0':
                date = date.replace('0', '')
                reclean.append(date)
            else:
                reclean.append(date)
                
        dfj['director_bvn'] = reclean

        # email validation 
        # from email_validator import validate_email, EmailNotValidError
        print('at email validation')
        val = []
        counter = 1
        for email in dfj['email']: #runs 458k+ loop
            try:
                validate_email(email)
                val.append(email)
            except Exception as e:
                val.append(f'clienthasnoemail{counter}@gmail.com')
                counter += 1
                
        dfj['email'] = val


        final_corp = dfj[['client_id', 'client_name', 'Phone_no', 'email',
            'customer_contact_address', 'rc_number', 'director_bvn',
                'director_name', 'director_phone_no', 'director_email',
                'director_gender', 'director_DOB',
            'nature_of_business', 'industry/sector', 'date_of_incorporation',
            'account_officer', 'account_no', 'account_type', 'product_name',
            'account_balance', 'loan_value', 'outstanding_loan_amount',
            'last_repayment_date', 'total_balance_all_accounts', 'debit_balance',
            'maturity_date', 'total_credit_amount']]



        try:
            # call batch utils to figure things out.
            batch_data = contacts_utils.time_checker.batcher(batch_no=batch)
            test_data = final_corp[batch_data.get('start'): batch_data.get('end')]
            # test_data = final_corp
            # print(test_data, len(test_data))

            return_data = []
            for ind in test_data.index:
                return_data.append({
                    
                    'first_name': str(test_data.get("client_name")[ind]),  # client_name is system first_name
                    'last_name': ' ',
                    'mobile_number':str(test_data.get("Phone_no")[ind]),
                    'emails':str(test_data.get("email")[ind]),

                    'custom_field':{
                        'cf_client_id': str(test_data.get("client_id")[ind]), 
                        'cf_customer_contact_address':str(test_data.get("customer_contact_address")[ind]),
                        'cf_rc_number':str(test_data.get("rc_number")[ind]),
                        'cf_director_bvn':str(test_data.get("director_bvn")[ind]),
                        'cf_director_name':str(test_data.get("director_name")[ind]),
                        'cf_director_phone_no':str(test_data.get("director_phone_no")[ind]),
                        'cf_director_email':str(test_data.get("director_email")[ind]),
                        'cf_director_gender':str(test_data.get("director_gender")[ind]),
                        'cf_director_DOB':str(test_data.get("director_DOB")[ind]),
                        'cf_nature_of_business':str(test_data.get("nature_of_business")[ind]),
                        'cf_industry/sector':str(test_data.get("industry/sector")[ind]),
                        'cf_date_of_incorporation':str(test_data.get("date_of_incorporation")[ind]),
                        'cf_account_officer':str(test_data.get("account_officer")[ind]), 
                        'cf_account_no':str(test_data.get("account_no")[ind]),
                        'cf_account_type':str(test_data.get("account_type")[ind]),
                        'cf_product_name':str(test_data.get("product_name")[ind]),
                        'cf_account_balance':str(test_data.get("account_balance")[ind]),
                        'cf_loan_value':str(test_data.get("loan_value")[ind]),
                        'cf_outstanding_loan_amount':str(test_data.get("outstanding_loan_amount")[ind]),
                        'cf_last_repayment_date':str(test_data.get("last_repayment_date")[ind]),
                        'cf_total_balance_all_accounts':str(test_data.get("total_balance_all_accounts")[ind]),
                        'cf_debit_balance':str(test_data.get("debit_balance")[ind]),
                        'cf_maturity_date':str(test_data.get("maturity_date")[ind]),
                        'cf_total_credit_amount':str(test_data.get("total_credit_amount")[ind])
                    }

                })



            return True, return_data
        except Exception as e:
            return False , { 'status':'False', 'message':'Data cleaning error'}
            

    def get_individual_data(self, batch):


        Mifosdb = mysql.connector.connect(
            host=os.environ.get("MIFOS_DB_HOST"),
            user=os.environ.get("MIFOS_DB_USER"),
            passwd=os.environ.get("MIFOS_DB_PASSWORD"),
            database = os.environ.get("MIFOS_DB_NAME") 
        )


        ndic =  '''
                SELECT 
                ifnull(mc.id, mg.id) AS client_id,
                -- ifnull(mc.lastname, mg.display_name) AS last_name,
                -- ifnull(mc.middlename, mg.display_name) AS middle_name,
                -- ifnull(mc.firstname, mg.display_name) AS first_name,
                ifnull(mc.display_name, mg.display_name) as client_name,
                ai.`Mobile Number` AS Phone_no,
                ai.`Email Address` as email,
                ifnull(ai.`Mailing Address`, 'Not stated') AS customer_contact_address,
                
                'N/a on Mifos' as 'occupation',
                mc.date_of_birth AS 'D.O.B',
                ifnull(s.display_name, 'Not assigned') as 'account_officer',
                msa.account_no AS account_no,
                case  msa.account_type_enum 
                    when '1' then 'Individual account'
                    when '2' then 'Group account'
                    when '3' then 'JLG account' end AS account_type,
                msp.name as 'product_name',
                msa.account_balance_derived AS account_balance,
                
                ifnull(l.amount_disbursed, 0) as loan_value,
                ifnull(l.loan_outstanding, 0) as outstanding_loan_amount,
                l.last_repayment_date,

                d.aggregated_deposit as total_balance_all_accounts,
                
                ifnull((l.aggregated_loan_balance) * (-1), 0) as debit_balance,
                
                l.maturity_date,
                
                ifnull(msa.total_deposits_derived, 0) as total_credit_amount
                
            
                -- left(odb.BVN, 11) AS client_bvn,
                -- Null as account_category,
                
                -- case when isnull(msa.closedon_date) then 'Active' 
                -- ELSE 'Dormant' END AS account_status,
                -- NULL AS 'portion_of_deposit_as_collateral',
                -- case when l.collateral_description like '%deposit%' or l.collateral_description like '%Deposit%' then l.collateral_value
                -- else NULL end as portion_of_deposit_pledged_as_collateral,
                    
                

                -- l.loan_type,
                -- l.date_granted,
                -- l.loan_outstanding,
                -- l.principal,
                -- l.interest,
                -- l.write_off_or_waiver,
                -- l.cash_backed, 
                -- l.cash_amount_if_yes,
                -- l.secured,
                -- l.collateral_type,
                -- l.collateral_value,
                -- l.collateral_description,
                -- l.collateral_full_address,
                -- l.collateral_status,
                -- l.guarantor_name,
                -- l.guarantor_BVN,
                -- l.guarantor_ID,
                -- l.guarantor_address,
                -- l.guarantor_phone_no,
                
                -- (d.aggregated_deposit) - (l.aggregated_loan_balance) as "Net_depositor's_balance"
                
                FROM m_client mc
                left JOIN m_savings_account msa ON msa.client_id = mc.id
                left JOIN m_savings_product msp ON msp.id = msa.product_id
                left JOIN `Address Info` ai ON ai.client_id = mc.id
                left JOIN m_group mg ON mg.id = msa.group_id
                LEFT JOIN `Other Bank Details` odb ON odb.client_id = mc.id
                left join 
                        (select 
                        ms.id,
                        ms.display_name
                        from m_staff ms
                        where ms.is_active = 1) s on s.id = mc.staff_id

                LEFT JOIN 

                    (select
                    msa.client_id,
                    SUM(msa.account_balance_derived) AS aggregated_deposit
                    FROM m_savings_account msa
                    JOIN m_client c ON c.id = msa.client_id
                    where !isnull(c.image_id)
                    and msa.status_enum = 300
                    GROUP BY msa.client_id) d ON d.client_id = mc.id

                LEFT JOIN

                    (select
                    ml.id as loan_id,
                    ml.client_id AS client_id,
                    mpl.name AS loan_type,
                    ml.disbursedon_date AS date_granted,
                    ml.principal_disbursed_derived AS amount_disbursed,
                    lt.last_repayment_date,
                    lt.maturity_date,
                    ml.total_outstanding_derived AS loan_outstanding,
                    ml.principal_outstanding_derived AS principal,
                    ml.interest_outstanding_derived AS interest,
                    case when ml.total_waived_derived = 0 then ml.total_writtenoff_derived 
                        ELSE ml.total_waived_derived END AS write_off_or_waiver,
                    case when mpl.name like '%CASH-BACKED%' then 'YES'
                        ELSE 'NO' end AS cash_backed,
                    case when  mpl.name like '%CASH-BACKED%' then ml.principal_disbursed_derived 
                        else NULL end AS cash_amount_if_yes,
                    case when !isnull(mlc.type_cv_id) then 'YES'
                        ELSE 'NO' END AS 'secured',
                    mlc.description AS collateral_type,
                    mlc.value AS collateral_value,
                    mlc.description AS collateral_description,
                    NULL AS collateral_full_address,
                    'Perfected' AS collateral_status,
                    NULL AS "guarantor_name",
                    NULL AS "guarantor_BVN",
                    NULL AS "guarantor_ID",
                    NULL AS "guarantor_address",
                    NULL AS "guarantor_phone_no",
                    e.total_loan_balance as aggregated_loan_balance
                    from m_loan ml
                    left JOIN m_product_loan mpl ON mpl.id = ml.product_id
                    LEFT JOIN m_loan_collateral mlc ON mlc.loan_id = ml.id
                    left join 
                        (SELECT
                        ml.client_id,
                        mlt.loan_id,
                        mlt.amount,
                        MAX(mlt.transaction_date) as last_repayment_date,
                        ml.disbursedon_date AS disbursement_date,
                        ml.expected_maturedon_date AS maturity_date,
                        ml.loan_status_id AS loan_status
                        FROM m_loan_transaction mlt
                        JOIN m_loan ml ON ml.id = mlt.loan_id
                        WHERE mlt.transaction_type_enum IN (2,5)
                        AND mlt.is_reversed = 0
                        AND ml.loan_status_id = 300
                        GROUP BY mlt.loan_id) lt on lt.loan_id = ml.id
                    left join
                        (select
                        lo.client_id,
                        sum(lo.total_outstanding_derived) as 'total_loan_balance'
                        FROM m_loan lo
                        where lo.loan_status_id = 300
                        GROUP BY lo.client_id) e ON e.client_id = ml.client_id
                        
                    where ml.loan_status_id = 300
                    GROUP BY ml.account_no) l ON l.client_id = mc.id

                WHERE !ISNULL(mc.image_id)
                and msa.product_id != 26
                and msa.account_type_enum in (1,2,3)
                and msa.status_enum = 300
                GROUP BY msa.account_no
        '''

        df_ndic = pd.read_sql_query(ndic, Mifosdb)
        df_ndic['client_id'] = df_ndic['client_id'].astype(str)


        # cleaning date columns
        print('cleaning date columns')

        pattern = re.compile(r'^19\d\d')

        new_date = [] 
        for date in df_ndic['D.O.B']:    #runs 458k+ loop
            if date == None:
                date = '1900-01-01'
                new_date.append(date)
            else:
                if not re.search(pattern, date.strftime('%Y-%m-%d')) and date != None:
                    date = '1900-01-01'
                    new_date.append(date)
                else:
                    new_date.append(date.strftime('%Y-%m-%d'))

        clean_date = []
        for date in pd.to_datetime(new_date):    #runs 458k+ loop
            if date.strftime('%Y-%m-%d') == '0080-11-26':
                try:
                    clean_date.append(date.strftime('%m/%d/%Y'))
                except ValueError:
                    clean_date.append('01/01/1900')
                    
            else:
                try:
                    clean_date.append(date.strftime('%m/%d/%Y'))
                except ValueError:
                    clean_date.append('01/01/1900')
                
        df_ndic['D.O.B'] = clean_date


        new_date = [] 
        for date in pd.to_datetime(df_ndic['last_repayment_date']):  #runs 458k+ loop
            try:
                new_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                new_date.append('01/01/1900')
                
        df_ndic['last_repayment_date'] = new_date


        new_date = [] 
        for date in pd.to_datetime(df_ndic['maturity_date']):  #runs 458k+ loop
            try:
                new_date.append(date.strftime('%m/%d/%Y'))
            except ValueError:
                new_date.append('01/01/1900')
                
        df_ndic['maturity_date'] = new_date



        # cleaning phone columns
        print('cleaning phone columns')

        new_ph = []
        for txt in df_ndic['Phone_no']:   #runs 458k+ loop
            try:
                x = re.findall("[0-9]", txt)
                rc = ''.join([str(i) for i in x])
                new_ph.append(rc)
            except TypeError:
                new_ph.append('0')

        df_ndic['Phone_no'] = new_ph

        new_ph = []
        for no in df_ndic['Phone_no']:   #runs 458k+ loop
            if len(str(no)) > 11:
                no = float(str(no)[:11] + '.' + str(no)[11:])
                new_ph.append(no)
            else:
                new_ph.append(no)
        df_ndic['Phone_no'] = new_ph



        # cleaning email columns
        print('cleaning email columns')


        email_regex = r'''(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])'''

        pattern = re.compile(email_regex)


        val = []
        counter = 1
        for email in df_ndic['email'][:]:
            try:
                match = re.search(pattern, email)
                if match:
                    val.append(email)
                if not match:
                    val.append(f'iclienthasnoemail{counter}@gmail.com')
                    counter += 1
            except Exception as e:
                # print(email, e)
                val.append(f'iclienthasnoemail{counter}@gmail.com')
                counter += 1
                
                
        df_ndic['email'] = val



        # #
        print('Begin data conversion for freshsales load up')
        try:
            
            # call batch utils to figure things out.
            batch_data = contacts_utils.time_checker.batcher(batch_no=batch)
            test_data = df_ndic[batch_data.get('start'): batch_data.get('end')]
           
            return_data = []
            count = 0
            for ind in test_data.index:
                print('load data at', count)
                return_data.append({

                    
                    'first_name': str(test_data.get("client_name")[ind]),  # client_name is system first_name
                    'last_name': ' ',
                    'mobile_number':str(test_data.get("Phone_no")[ind]),
                    'emails':str(test_data.get("email")[ind]),

                    'custom_field':{
                        'cf_client_id': str(test_data.get("client_id")[ind]), 
                        'cf_loan_value':str(test_data.get("loan_value")[ind]),
                        'cf_product_name':str(test_data.get("product_name")[ind]),  
                        'cf_account_balance':str(test_data.get("account_balance")[ind]),
                        # 'cf_customer_contact_address':str(test_data.get("customer_contact_address'")[ind]),
                        'cf_account_officer':str(test_data.get("account_officer")[ind]), 
                        'cf_account_type':str(test_data.get("account_type")[ind]),
                        'cf_account_no':str(test_data.get("account_no")[ind]),
                        'cf_maturity_date':str(test_data.get("maturity_date")[ind]),
                        'cf_total_credit_amount':str(test_data.get("total_credit_amount")[ind]),
                        'cf_total_balance_all_accounts':str(test_data.get("total_balance_all_accounts")[ind]),
                        'cf_debit_balance':str(test_data.get("debit_balance")[ind]),

                        ## newly
                        'cf_last_repayment_date':str(test_data.get("last_repayment_date")[ind]),
                        # 'cf_gender':str(test_data.get("gender")[ind]),
                        'cf_occupation':str(test_data.get("occupation")[ind]),
                        'cf_date_of_birth':str(test_data.get("D.O.B")[ind]),
                        'outstanding_loan_amount':str(test_data.get("outstanding_loan_amount")[ind]),

                    }

                })

                count += 1

            print('done prepping data')

            # return_data = dummy_data.dummydata.generate_individual_client_data()

            print(sys.getsizeof(return_data))

            return True, return_data
        except Exception as e:
            print(e)
            return False , { 'status':'FAILED', 'message':'Data cleaning error'}
            pass       



if __name__ == "__main__":
    data_service = DataExtraction()