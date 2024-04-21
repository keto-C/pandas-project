from functools import reduce
import pandas as pd
import xlrd


affiliateRates = pd.read_excel(r'af-rates.xlsx')
orders = pd.read_excel(r'orders.xlsx')
currencyRates = pd.read_excel(r'cur-rates.xlsx')

#i decided to copy the orders dataframe and work on the copied one to avoid any casualties
orders_mod = orders


#check and handle typos
#to check for the typos i used:
# print(orders_mod['Currency'].unique())
orders_mod['Currency'] = orders_mod['Currency'].replace(['EURO'], 'EUR')

#check and handle duplicates
orders_mod = orders_mod.drop_duplicates()
affiliateRates = affiliateRates.drop_duplicates()
currencyRates = currencyRates.drop_duplicates()

#check for inconsistency with order numbers and order prices
order_nums = pd.DataFrame(orders_mod['Order Number'].value_counts())
print(order_nums['count'].value_counts())
#there are none so we proceed


# handling null and different values in orders
#we drop the ones who have none or NaN as ids
specific_values = affiliateRates['Affiliate ID'].unique()
orders_mod = orders_mod[orders_mod.isin(specific_values).any(axis=1)]
#reseting indices
orders_mod = orders_mod.reset_index()


#Changing currencies
merged_df = pd.merge(orders_mod, currencyRates, left_on='Order Date', right_on='date', how='inner')
for index, row in merged_df.iterrows():
    orders_mod.at[index, 'Currency'] = "EUR"
    if row['Currency'] == "USD":
        orders_mod.at[index, 'Order Amount'] = row['Order Amount']*row['USD']
    elif row['Currency'] == "GBP":
        orders_mod.at[index, 'Order Amount'] = row['Order Amount']*row['GBP']
    else:
        pass


#calculating processing, refund and chargeback fees
orders_mod.insert(7, 'Processing fee', '')
orders_mod.insert(8, 'Refund fee', '')
orders_mod.insert(9, 'Chargeback fee', '')

#method for finding the correct processing rate, chargeback fee and refund fee according to the date
#and affiliate id
def findAff(rowDate, rowID):
    ProcRate = 0
    CBFee = 0
    RefFee = 0
    for index, row in affiliateRates.iterrows():
        if rowID == row['Affiliate ID']:
            if rowDate > row['Start Date']:
                ProcRate = row['Processing Rate']
                CBFee = row['Chargeback Fee']
                RefFee = row['Refund Fee']

    return ProcRate, CBFee, RefFee

#
for index, row in orders_mod.iterrows():
    date = row['Order Date']
    id = row['Affiliate ID']
    (ProcRate, CBFee, RefFee) = findAff(date, id)
    orders_mod.at[index, 'Processing fee'] = row['Order Amount']*ProcRate
    orders_mod.at[index, 'Refund fee'] = 0
    orders_mod.at[index, 'Chargeback fee'] = 0
    if row['Order Status'] == "Refunded":
        orders_mod.at[index, 'Refund fee'] = RefFee
    elif row['Order Status'] == "Chargeback":
        orders_mod.at[index, 'Chargeback fee'] = CBFee


#to test orders_mod
# print(orders_mod)
orders_mod.to_excel("Modified Orders.xlsx")


#creating the separate excel files for each affiliate
#first one is john
john = orders_mod.loc[orders_mod['Affiliate ID'] == 1]
john = john.reset_index()

john['Order Date'] = pd.to_datetime(john['Order Date']) - pd.to_timedelta(6, unit='d')

#calculate sum of values, grouped by week
numOfOrdersJ = john.groupby([pd.Grouper(key='Order Date', freq='W')])['Order Number'].count()
numOfOrdersJ = numOfOrdersJ.to_frame()

totalOrderAmJ = john.groupby([pd.Grouper(key='Order Date', freq='W')])['Order Amount'].sum()
totalOrderAmJ = totalOrderAmJ.to_frame()
totalOrderAmJ = totalOrderAmJ['Order Amount'].round(decimals=2)

totalProcJ = john.groupby([pd.Grouper(key='Order Date', freq='W')])['Processing fee'].sum()
totalProcJ = totalProcJ.to_frame()
totalProcJ = totalProcJ['Processing fee'].round(decimals=2)

totalRefJ = john.groupby([pd.Grouper(key='Order Date', freq='W')])['Refund fee'].sum()
totalRefJ = totalRefJ.to_frame()

totalCharJ = john.groupby([pd.Grouper(key='Order Date', freq='W')])['Chargeback fee'].sum()
totalCharJ = totalCharJ.to_frame()


data_frames = [numOfOrdersJ, totalOrderAmJ, totalProcJ, totalRefJ, totalCharJ]
johnEx = reduce(lambda left, right: pd.merge(left, right, on=['Order Date'], how='outer'), data_frames)
johnEx.rename(columns={'Order Date': 'Week', 'Order Number': 'Number of Orders', 'Order Amount': 'Total Order Amount (EUR)',
                       'Processing fee': 'Total Processing Fee', 'Refund fee': 'Total Refund Fee',
                       'Chargeback fee': 'Total Chargeback Fee'}, inplace=True)

johnEx.to_excel('John.xlsx')

#mary
mary = orders_mod.loc[orders_mod['Affiliate ID'] == 2]
mary = mary.reset_index()

mary['Order Date'] = pd.to_datetime(mary['Order Date']) - pd.to_timedelta(6, unit='d')

#calculate sum of values, grouped by week
numOfOrdersM = mary.groupby([pd.Grouper(key='Order Date', freq='W')])['Order Number'].count()
numOfOrdersM = numOfOrdersM.to_frame()

totalOrderAmM = mary.groupby([pd.Grouper(key='Order Date', freq='W')])['Order Amount'].sum()
totalOrderAmM = totalOrderAmM.to_frame()
totalOrderAmM = totalOrderAmM['Order Amount'].round(decimals=2)

totalProcM = mary.groupby([pd.Grouper(key='Order Date', freq='W')])['Processing fee'].sum()
totalProcM = totalProcM.to_frame()
totalProcM = totalProcM['Processing fee'].round(decimals=2)

totalRefM = mary.groupby([pd.Grouper(key='Order Date', freq='W')])['Refund fee'].sum()
totalRefM = totalRefM.to_frame()

totalCharM = mary.groupby([pd.Grouper(key='Order Date', freq='W')])['Chargeback fee'].sum()
totalCharM = totalCharM.to_frame()


data_frames = [numOfOrdersM, totalOrderAmM, totalProcM, totalRefM, totalCharM]
maryEx = reduce(lambda left, right: pd.merge(left, right, on=['Order Date'], how='outer'), data_frames)
maryEx.rename(columns={'Order Date': 'Week', 'Order Number': 'Number of Orders', 'Order Amount': 'Total Order Amount (EUR)',
                       'Processing fee': 'Total Processing Fee', 'Refund fee': 'Total Refund Fee',
                       'Chargeback fee': 'Total Chargeback Fee'}, inplace=True)

maryEx.to_excel('Mary.xlsx')

#luke
luke = orders_mod.loc[orders_mod['Affiliate ID'] == 3]
luke = luke.reset_index()

luke['Order Date'] = pd.to_datetime(luke['Order Date']) - pd.to_timedelta(6, unit='d')

#calculate sum of values, grouped by week
numOfOrdersL = luke.groupby([pd.Grouper(key='Order Date', freq='W')])['Order Number'].count()
numOfOrdersL = numOfOrdersL.to_frame()

totalOrderAmL = luke.groupby([pd.Grouper(key='Order Date', freq='W')])['Order Amount'].sum()
totalOrderAmL = totalOrderAmL.to_frame()
totalOrderAmL = totalOrderAmL['Order Amount'].round(decimals=2)

totalProcL = luke.groupby([pd.Grouper(key='Order Date', freq='W')])['Processing fee'].sum()
totalProcL = totalProcL.to_frame()
totalProcL = totalProcL['Processing fee'].round(decimals=2)

totalRefL = luke.groupby([pd.Grouper(key='Order Date', freq='W')])['Refund fee'].sum()
totalRefL = totalRefL.to_frame()

totalCharL = luke.groupby([pd.Grouper(key='Order Date', freq='W')])['Chargeback fee'].sum()
totalCharL = totalCharL.to_frame()


data_frames = [numOfOrdersM, totalOrderAmM, totalProcM, totalRefM, totalCharM]
lukeEx = reduce(lambda left, right: pd.merge(left, right, on=['Order Date'], how='outer'), data_frames)
lukeEx.rename(columns={'Order Date': 'Week', 'Order Number': 'Number of Orders', 'Order Amount': 'Total Order Amount (EUR)',
                       'Processing fee': 'Total Processing Fee', 'Refund fee': 'Total Refund Fee',
                       'Chargeback fee': 'Total Chargeback Fee'}, inplace=True)

lukeEx.to_excel('Luke.xlsx')
