# This script defines functions to display and visualize data in MYSQL database.

from src.database.db_cnx import *
from tabulate import tabulate
import matplotlib.pyplot as plt


def show_visualization():
    print('_'* 150, "\nFunctional Requirements 3 - Data Analysis and Visualization")
    print("Find and plot which transaction type has the highest transaction count.")
    view_trxn_type()

    print("Find and plot which state has a high number of customers.")
    view_client_volume()

    print("Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.")
    view_top10_trxns()

    print('_'* 150, "\nFunctional Requirements 5 - Data Analysis and Visualization for Loan Application")
    print("Find and plot the percentage of applications approved for self-employed applicants.")
    view_appstatus_by_emp()

    print("Find the percentage of rejection for married male applicants.")
    view_appstatus_by_marital_gender()

    print("Find and plot the top three months with the largest volume of transaction data.")
    view_trxn_vol_by_mo()

    print("Find and plot which branch processed the highest total dollar value of healthcare transactions.")
    view_trxn_amt_by_br()


def view_trxn_type():
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            query = '''select transaction_type as TRXN_TYPE, count(distinct transaction_id) as NUM_TRXNS
                    from cdw_sapp_credit_card
                    group by TRXN_TYPE
                    order by NUM_TRXNS desc'''
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                headers = cursor.column_names
                table = tabulate(data, headers=headers,
                                 tablefmt="pretty", stralign="left")
                print(table)
                # create data visualization
                trxn_type = [row[0] for row in data]  # pie labels
                trxn_count = [row[1] for row in data]  # pie sizes
                plt.style.use('ggplot')
                plt.figure(3.1, figsize=(6, 4.5), layout='tight')
                # make a slice pop out
                explode = (0.1, 0, 0, 0, 0, 0, 0)
                plt.pie(trxn_count, explode=explode, labels=trxn_type,
                        autopct='%1.1f%%', shadow=True)
                plt.title("Trends in Credit Card Spending 2018",
                          fontsize=16, fontweight='bold', pad=25)
                plt.axis('equal')
                plt.show()  

    except msc.Error as e:
        print(e)


def view_client_volume():
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            query = '''select br.branch_state as BRANCH_STATE, count(distinct cc.cust_ssn) as NUM_CUST
                    from cdw_sapp_branch br
                    right join cdw_sapp_credit_card cc on br.branch_code = cc.branch_code
                    group by BRANCH_STATE
                    order by NUM_CUST desc'''
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                headers = cursor.column_names
                table = tabulate(data, headers=headers,
                                 tablefmt="pretty", stralign="left")
                print(table)
                # create data visualization
                state = [row[0] for row in data]  # y-axis
                cust = [row[1] for row in data]  # x-axis
                plt.style.use('ggplot')
                plt.figure(3.2, figsize=(8.5, 6), layout='tight')
                plt.barh(state, cust, height=0.6)
                plt.title("Number of Bank Customers by State 2018",
                          fontsize=16, fontweight='bold', pad=15)
                plt.xlabel("Client Volume", fontsize=11)
                plt.xlim(0, 1000)
                plt.yticks(fontsize=10)
                # Invert the y-axis for ascending order from bottom to top
                plt.gca().invert_yaxis()
                # add customer count as labels on top of the bars
                for index, value in enumerate(cust):
                    plt.text(value, index, str(value), va='center', fontsize=8)
                plt.show()

    except msc.Error as e:
        print(e)


def view_top10_trxns():
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            query = '''select CUST_SSN, concat(left(first_name, 1), left(last_name, 1)) as CUST, count(*) as NUM_TRXNS, round(sum(transaction_value),2) as TOTAL_TRXN_AMT
                    from cdw_sapp_credit_card cc
                    left join cdw_sapp_customer cust
                    on cc.cust_ssn = cust.ssn
                    group by CUST_SSN, CUST
                    order by TOTAL_TRXN_AMT desc, NUM_TRXNS desc
                    limit 10'''
            cursor.execute(query)
            data = cursor.fetchall()
            if data:
                headers = cursor.column_names
                table = tabulate(data, headers=headers,
                                 tablefmt="pretty", stralign="left")
                print(table)
                # create data visualization
                cust = [row[1] for row in data]  # x-axis
                trxn_amt = [row[3] for row in data]  # y-axis
                plt.style.use('ggplot')
                plt.figure(3.3, figsize=(9, 5.5))
                plt.bar(cust, trxn_amt, width=0.6)
                plt.title("Top 10 Clients with Highest Credit Card Spending in 2018",
                          fontsize=16, fontweight='bold', pad=18)
                plt.ylabel("Annual Spending", fontsize=10)
                plt.ylim(0, 7000)
                # add spending amounts as labels on top of the bars
                for x, y in zip(cust, trxn_amt):
                    plt.text(x, y, f"${round(y)}",
                             ha="center", va="bottom", fontsize=9)
                plt.show()

    except msc.Error as e:
        print(e)


def view_appstatus_by_emp():
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            q1_ct = '''select self_employed, round(sum(case when application_status='Y' then 1 else 0 end), 2) as approved,
                round(sum(case when application_status='N' then 1 else 0 end), 2) as rejected
                from cdw_sapp_loan_application 
                group by self_employed'''
            cursor.execute(q1_ct)
            ct = cursor.fetchall()
            headers = cursor.column_names
            table_ct = tabulate(ct, headers=headers, tablefmt="pretty", stralign="left")
            print(table_ct)
            # create plot
            type = ['Others', 'Self_Employed']
            approved = [row[1] for row in ct]
            rejected = [row[2] for row in ct]
            plt.style.use('ggplot')
            plt.figure(5.1, figsize=(6, 5), layout='tight')
            plt.bar(type, approved, label='Approved', width=0.4, color='green', alpha=0.7)
            plt.bar(type, rejected, label='Rejected', width=0.4, color='red', alpha=0.7, bottom=approved)
            plt.title("Loan Application Study by\nEmployment Category 2018", fontsize=16, fontweight='bold', pad=20)
            plt.xlim(-1, len(type))
            plt.ylabel("Number of Application Processed", fontsize=10)
            plt.ylim(0, max(approved)+max(rejected)+40)
            plt.legend()
            caption = "Figure 5.1. Loan approval rate is about the same for both employment \ntypes and is moderately positive(>65%). Compared to others, self-employed \nindividuals make up only a small portion of the total number of applicants. "
            plt.figtext(0.13, -0.05, caption, ha='left', va='center', fontsize=9.5)
            # convert count to %
            q1_per = '''select self_employed, round(sum(case when application_status='Y' then 1 else 0 end)/count(*)*100, 2) as 'approved %',
                round(sum(case when application_status='N' then 1 else 0 end)/count(*)*100, 2) as 'rejected %'
                from cdw_sapp_loan_application 
                group by self_employed'''
            cursor.execute(q1_per)
            per = cursor.fetchall()
            headers = cursor.column_names
            table_per = tabulate(per, headers=headers, tablefmt="pretty", stralign="left")
            print(table_per)
            type = ['Others', 'Self_Employed']
            approved_per = [row[1] for row in per]
            rejected_per = [row[2] for row in per]
            # add approval and rejction rate as labels onto the bars
            for i, (app_ct, rej_ct, app_per, rej_per) in enumerate(zip(approved, rejected, approved_per, rejected_per)):
                plt.text(i, app_ct/2, f'{app_per}%', ha='center', va='center', fontsize=9)
                plt.text(i, app_ct + rej_ct/2, f'{rej_per}%',
                        ha='center', va='center', fontsize=9)
            plt.show()

    except msc.Error as e:
        print(e)


def view_appstatus_by_marital_gender():
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            q2_ct = '''select concat(married, (","), gender) as married_gender, round(sum(case when application_status='Y' then 1 else 0 end), 2) as approved,
                round(sum(case when application_status='N' then 1 else 0 end), 2) as rejected
                from cdw_sapp_loan_application 
                group by married_gender
                order by married_gender'''
            cursor.execute(q2_ct)
            ct = cursor.fetchall()
            headers = cursor.column_names
            table_ct = tabulate(ct, headers=headers, tablefmt="pretty", stralign="left")
            print(table_ct)
            # create a horizontal bar graph
            type = ['Single Female', 'Single Male', 'Married Female', 'Married Male']
            approved = [row[1] for row in ct]
            rejected = [row[2] for row in ct]
            plt.style.use('ggplot')
            plt.figure(5.2, figsize=(9, 6))
            plt.barh(type, approved, label='Approved',
                    height=0.5, color='green', alpha=0.7)
            plt.barh(type, rejected, label='Rejected', height=0.5,
                    color='red', alpha=0.7, left=approved)
            # customize the plot
            plt.xlabel('Number of Application Processed')
            plt.xlim(-0, max(approved)+max(rejected)+40)
            plt.ylim(-1, len(type))
            plt.title("Loan Application Study by\nGender & Marital Status 2018",
                    fontsize=16, fontweight='bold', pad=20)
            plt.legend(loc='lower right')
            caption = "Figure 5.2. Loan application number is dramatically high for married males and low for married females, \nwhich possibly suggests that most married males are the breadwinners in the family. The other groups \nalso seem more less likely to apply for loans compared to married males, which makes married males \nthe target customers. But again, loan approval rate is quite positive(>62%) for all."
            plt.figtext(0.13, -0.04, caption, ha='left', va='center', fontsize=9.5)

            q2_percentage = '''select concat(married, (","), gender) as married_gender, round(sum(case when application_status='Y' then 1 else 0 end)/count(*)*100, 2) as 'approved %',
                round(sum(case when application_status='N' then 1 else 0 end)/count(*)*100, 2) as 'rejected %'
                from cdw_sapp_loan_application 
                group by married_gender
                order by married_gender'''
            cursor.execute(q2_percentage)
            percentage = cursor.fetchall()
            headers = cursor.column_names
            table_percentage = tabulate(
                percentage, headers=headers, tablefmt="pretty", stralign="left")
            print(table_percentage)
            approved_percentage = [row[1] for row in percentage]
            rejected_percentage = [row[2] for row in percentage]
            # add data values to the bars
            for i, (app_ct, rej_ct, app_per, rej_per) in enumerate(zip(approved, rejected, approved_percentage, rejected_percentage)):
                # conditionally add percentage labels if the bar width is greater than the minimum width
                if app_ct < 40 and rej_ct < 40:
                    intersection = app_ct + rej_ct
                    plt.text(intersection, i, f'{app_per}% / {rej_per}%',
                            ha='left', va='center', color='black')
                else:
                    plt.text(app_ct/2, i, f'{app_per}%',
                            ha='center', va='center', fontsize=9)
                    plt.text(app_ct + rej_ct/2, i,
                            f'{rej_per}%', ha='center', va='center', fontsize=9)
            # show the plot
            plt.show()

    except msc.Error as e:
        print(e)


def view_trxn_vol_by_mo():
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            q3 = '''select date_format(timeid, '%b') as MONTH, count(distinct transaction_id) as TRXN_VOLUME 
                from cdw_sapp_credit_card 
                group by MONTH 
                order by TRXN_VOLUME desc'''
            cursor.execute(q3)
            data3 = cursor.fetchall()
            headers = cursor.column_names
            table = tabulate(data3, headers=headers,
                             tablefmt="pretty", stralign="left")
            print(table)
            month = [row[0] for row in data3]  # x-axis
            trxn_vol = [row[1] for row in data3]  # y-axis
            plt.style.use('ggplot')
            plt.figure(5.3, figsize=(10, 6))
            plt.bar(month, trxn_vol, width=0.6)
            plt.title("Annual Transaction Volume of Banks 2018",
                    fontsize=16, fontweight='bold', pad=18)
            plt.ylabel("Transaction Volume", fontsize=10)
            plt.ylim(0, 5000)
            caption = "Figure 5.3. Annual transaction volume is very stable year round, no obvious growth in particular month."
            plt.figtext(0.14, 0.015, caption, ha='left', va='center', fontsize=10)
            # add spending amounts as labels on top of the bars
            for x, y in zip(month, trxn_vol):
                plt.text(x, y, str(y), ha="center", va="bottom", fontsize=9)
            plt.show()

    except msc.Error as e:
        print(e)


def view_trxn_amt_by_br():
    try:
        with connect_to_mysql() as cnx:
            cursor = cnx.cursor()
            cursor.execute("use creditcard_capstone")
            q4 = '''select cc.branch_code as BRANCH_CODE, concat(br.branch_name, ' ', br.branch_code, ' in ', br.branch_state) as BRANCH, round(sum(transaction_value),2) as TOTAL_TRXN_VAL
                from cdw_sapp_credit_card cc
                inner join cdw_sapp_branch br
                on cc.branch_code = br.branch_code
                where cc.transaction_type = 'Healthcare'
                group by cc.branch_code, BRANCH
                order by TOTAL_TRXN_VAL desc limit 10'''
            cursor.execute(q4)
            data4 = cursor.fetchall()
            headers = cursor.column_names
            table = tabulate(data4, headers=headers,
                             tablefmt="pretty", stralign="left")
            print(table)
            branch = [row[1] for row in data4]  # y-axis
            trxn_val = [row[2] for row in data4]  # x-axis
            plt.style.use('ggplot')
            plt.figure(5.4, figsize=(10, 6.5), layout='tight')
            plt.barh(branch, trxn_val, height=0.45)
            plt.title("Top 10 Banks with Highest Transaction \nSpending in Healthcare 2018",
                      loc='center', fontsize=16, fontweight='bold', pad=18)
            plt.xlabel("Transaction Value ($)", fontsize=10)
            plt.xlim(0, 6000)
            plt.yticks(range(len(branch)), fontsize=9.5)
            plt.ylim(-1, len(branch))
            # Invert the y-axis for ascending order from bottom to top
            plt.gca().invert_yaxis()
            # add customer count as labels on top of the bars
            for index, value in enumerate(trxn_val):
                plt.text(value, index, f"${value}", va='center', fontsize=9)
            plt.show()

    except msc.Error as e:
        print(e)
