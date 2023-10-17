# This script defines functions to interact with data in MYSQL database.

import mysql.connector as mcs
from tabulate import tabulate
from src.database.db_cnx import *


def show_query():
    print('_'* 150, "\nFunctional Requirements 2 - Front-End Console to See/Display Data")
    print("Get transaction details by customer zip code, month, and year.")
    zip = input("Enter zip code (try 19438): ")
    mo = input("Enter month 'mm' (try 02): ")
    yr = input("Enter year 'yyyy' (try 2018): ")
    get_trxn_by_zip_mo_yr(zip, mo, yr)

    print("Get transaction totals by type.")
    type = input("Enter transaction type (try gas): ").lower()
    get_trxn_by_type(type)

    print("Get transaction totals by branch state.")
    state = input("Enter state name (try CA): ").lower()
    get_trxn_by_branch(state)

    print("Get existing account details by customer SSN.")
    ssn = input("Enter customer SSN (try 123459988): ")
    get_acct_by_cust(ssn)

    print("Modify existing account details by customer SSN.")
    ssn = input("Enter customer SSN (try 123459988): ")
    item = input("Select the item you want to modify (SSN excluded, try last_name): ").upper() 
    update = input("Enter new content (try newchange): ") 
    modify_acct(ssn, item, update)

    print("Get monthly bill by credit card number, month, and year.")
    cc_no = input("Enter credit card number (try 4210653349028689): ")
    mo = input("Enter month 'mm' (try 12): ")
    yr = input("Enter month 'yyyy' (try 2018): ")
    generate_bill_by_ccno_mo_yr(cc_no, mo, yr)

    print("Get transaction details by customer SSN and date range.")
    ssn = input("Enter customer SSN (123459988): ")
    d1 = input("From 'yyyy-mm-dd' (try 2018-12-04): ")
    d2 = input("To 'yyyy-mm-dd' (try 2018-12-24): ")
    get_trxn_by_ssn_and_date(ssn, d1, d2)


def get_trxn_by_zip_mo_yr(zip, mo, yr):
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            # use inner join here to exclude null values from both tables
            query = f'''select cust.CUST_ZIP, cc.* 
                    from cdw_sapp_customer as cust 
                    inner join cdw_sapp_credit_card as cc
                    on cc.cust_ssn = cust.ssn  
                    where cust.cust_zip = {zip} and month(cc.timeid) = {mo} and year(cc.timeid) = {yr}
                    order by cc.timeid desc'''
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                headers = cursor.column_names
                table = tabulate(data, headers=headers,
                                 tablefmt="pretty", stralign="left")
                print(table)
            else:
                print("Record not found.")

    except msc.Error as e:
        print(e)


def get_trxn_by_type(type):
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            query = f'''select transaction_type as TRXN_TYPE, count(*) as TRXN_COUNT, round(sum(transaction_value),2) as TOTAL_TRXN_VAL
                    from cdw_sapp_credit_card 
                    where transaction_type = '{type}'
                    group by transaction_type'''
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                headers = cursor.column_names
                table = tabulate(data, headers=headers,
                                 tablefmt="pretty", stralign="left")
                print(table)
            else:
                print("Record not found.")

    except msc.Error as e:
        print(e)


def get_trxn_by_branch(state):
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            # use inner join here to exclude null values from both tables
            query = f'''select branch_state as BRANCH_STATE, br.branch_code as BRANCH_CODE, branch_name as BRANCH_NAME, count(br.branch_code) as TRXN_COUNT, round(sum(transaction_value),2) as TOTAL_TRXN_VAL 
                    from cdw_sapp_branch as br
                    inner join cdw_sapp_credit_card as cc
                    on br.branch_code = cc.branch_code
                    where branch_state = '{state}'
                    group by br.branch_code, branch_name
                    order by TOTAL_TRXN_VAL desc'''
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                headers = cursor.column_names
                table = tabulate(data, headers=headers,
                                 tablefmt="pretty", stralign="left")
                print(table)
            else:
                print("Record not found.")

    except msc.Error as e:
        print(e)


def get_acct_by_cust(ssn):
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            query = f'''select * from cdw_sapp_credit_card where cust_ssn = '{ssn}' order by timeid desc'''
            cursor.execute(query)
            trxn = cursor.fetchall()
            if trxn:
                # print acct trxn details
                t_headers = cursor.column_names
                trxn_table = tabulate(
                    trxn, headers=t_headers, tablefmt="pretty", stralign="left")
                print(trxn_table)
                # print acct holder info
                query = f'''select * from cdw_sapp_customer where ssn = '{ssn}' order by last_updated desc'''
                cursor.execute(query)
                cust = cursor.fetchall()
                c_headers = cursor.column_names
                cust_table = tabulate(
                    cust, headers=c_headers, tablefmt="pretty", stralign="left")
                print(cust_table)
            else:
                print("Record not found.")

    except msc.Error as e:
        print(e)


# trxn data is usually auto-captured and non-editable, hence we'll only modify acct holder info here
def modify_acct(ssn, item, update):
    try:
        with connect_to_mysql() as cnx:
            # need to add buffered=True here, otherwise commit will fail and output 'unread result found'
            cursor = cnx.cursor(buffered=True)
            cursor.execute("use creditcard_capstone")
            cursor.execute("select * from cdw_sapp_customer limit 1")
            headers = cursor.column_names  
            if item in headers and item != 'SSN':
                query = f'''update cdw_sapp_customer set {item} = '{update}' where ssn = '{ssn}' '''
                print(query)
                cursor.execute(query)
                # commit change to db
                cnx.commit()
                print(cursor.rowcount, "record(s) affected")
                cursor.execute(f'''select * from cdw_sapp_customer where ssn = '{ssn}' ''')
                data = cursor.fetchall()
                table = tabulate(data, headers=headers, tablefmt="pretty", stralign="left")
                print(table)
            else:
                print("Commit failed. Item selected not found.")

    except msc.Error as e:
        # rollback in case there is any error
        cnx.rollback()
        print(e)


def generate_bill_by_ccno_mo_yr(cc_no, mo, yr):
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            query = f'''select * from cdw_sapp_credit_card 
                    where cust_cc_no = {cc_no} and month(timeid) = {mo} and year(timeid) = {yr} 
                    order by timeid desc'''
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                # print monthly bill
                headers = cursor.column_names
                table = tabulate(data, headers=headers,
                                 tablefmt="pretty", stralign="left")
                print(table)
                # print customer info
                query = f'''select distinct * from cdw_sapp_customer where credit_card_no = {cc_no}'''
                cursor.execute(query)
                cust_info = cursor.fetchall()
                headers = cursor.column_names
                cust_table = tabulate(
                    cust_info, headers=headers, tablefmt="pretty", stralign="left")
                print(cust_table)
            else:
                print("Record not found.")

    except msc.Error as e:
        print(e)


def get_trxn_by_ssn_and_date(ssn, d1, d2):
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            query = f'''select * from cdw_sapp_credit_card 
                    where cust_ssn = {ssn} and timeid between '{d1}' and '{d2}'
                    order by timeid desc'''
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                headers = cursor.column_names
                table = tabulate(data, headers=headers,
                                 tablefmt="pretty", stralign="left")
                print(table)
            else:
                print("Record not found.")

    except msc.Error as e:
        print(e)
