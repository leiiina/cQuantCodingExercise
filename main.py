import numpy as np
import pandas as pd

contracts = pd.read_csv('Contracts.csv')

fuelPricesHenryHub = pd.read_csv('Henry Hub.csv')
fuelPricesGDA = pd.read_csv('GDA_TETSTX.csv')
fuelPrices = pd.concat([fuelPricesHenryHub, fuelPricesGDA])

plantParameters = pd.read_csv('Plant_Parameters.csv')

powerPrices2016 = pd.read_csv('ERCOT_DA_Prices_2016.csv')
powerPrices2017 = pd.read_csv('ERCOT_DA_Prices_2017.csv')
powerPrices2018 = pd.read_csv('ERCOT_DA_Prices_2018.csv')
powerPrices2019 = pd.read_csv('ERCOT_DA_Prices_2019.csv')
powerPrices = pd.concat([powerPrices2016, powerPrices2017, powerPrices2018, powerPrices2019])

''' Power Price Statistics

Excluding Task 3'''

monthlyPowerPrices = pd.DataFrame(columns=['SettlementPoint', 'Year', 'Month', 'Mean', 'Min', 'Max', 'SD', 'Volatility'])

for settlement in powerPrices['SettlementPoint'].unique():
    settlementPrices = powerPrices[powerPrices['SettlementPoint'] == settlement].loc[:, ['Date', 'Price']]
    settlementPrices.loc[:, 'Date'] = settlementPrices['Date'].apply(lambda x: x[:7])
    years = [x[:4] for x in settlementPrices['Date'].unique()]
    months = [x[5:] for x in settlementPrices['Date'].unique()]

    means = settlementPrices.groupby('Date').mean()['Price'].values
    mins = settlementPrices.groupby('Date').min()['Price'].values
    maxes = settlementPrices.groupby('Date').max()['Price'].values
    stds = settlementPrices.groupby('Date').std()['Price'].values
    new_df = pd.DataFrame(data={'SettlementPoint': settlement, 'Year': years, 'Month': months, 'Mean': means, 'Min': mins, \
                                'Max': maxes, 'SD': stds})
    monthlyPowerPrices = pd.concat([monthlyPowerPrices, new_df]).reset_index()
    del monthlyPowerPrices['index']

monthlyPowerPrices.to_csv('MonthlyPowerPriceStatistics.csv', index=False)



''' Contract Valuation

Complete'''

contracts['StartDate'] = pd.to_datetime(contracts['StartDate'])
contracts['EndDate'] = pd.to_datetime(contracts['EndDate'])

dContracts = contracts[contracts['Granularity'] == 'Daily']
hContracts = contracts[contracts['Granularity'] == 'Hourly']

dailyContracts = pd.DataFrame(columns=['ContractName', 'DealType', 'Date', 'Volume', 'StrikePrice', 'Premium', 'PriceName'])

for idx, contract in dContracts.iterrows():
    dDates = pd.date_range(contract['StartDate'], contract['EndDate'], freq='d').strftime('%Y-%m-%d')
    new_df = pd.DataFrame(data={'ContractName': contract['ContractName'], 'DealType': contract['DealType'], 'Date': dDates, 'Volume': contract['Volume'], \
                               'StrikePrice': contract['StrikePrice'], 'Premium': contract['Premium'], 'PriceName': contract['PriceName']})
    dailyContracts = pd.concat([dailyContracts, new_df]).reset_index()
    del dailyContracts['index']

hourlyContracts = pd.DataFrame(columns=['ContractName', 'DealType', 'Date', 'Volume', 'StrikePrice', 'Premium', 'PriceName'])

for idx, contract in hContracts.iterrows():
    hDates = pd.date_range(contract['StartDate'], contract['EndDate'], freq='h').strftime('%Y-%m-%d %X')
    new_df = pd.DataFrame(data={'ContractName': contract['ContractName'], 'DealType': contract['DealType'], 'Date': hDates, 'Volume': contract['Volume'], \
                               'StrikePrice': contract['StrikePrice'], 'Premium': contract['Premium'], 'PriceName': contract['PriceName']})
    hourlyContracts = pd.concat([hourlyContracts, new_df]).reset_index()
    del hourlyContracts['index']

mergedDailyContracts = pd.merge(dailyContracts, fuelPrices, left_on=['PriceName', 'Date'], right_on=['Variable', 'Date'], how='left')
mergedHourlyContracts = pd.merge(hourlyContracts, powerPrices, left_on=['PriceName', 'Date'], right_on=['SettlementPoint', 'Date'], how='left')
del mergedDailyContracts['Variable']
del mergedHourlyContracts['SettlementPoint']

f = lambda x: (x['Price'] - x['StrikePrice'])*x['Volume'] if (x['DealType'] == 'Swap') else (((max(x['Price'] - x['StrikePrice'], 0)) - x['Premium'])*x['Volume'])
mergedDailyContracts['Payoffs'] = mergedDailyContracts.apply(f, axis=1)
mergedHourlyContracts['Payoffs'] = mergedHourlyContracts.apply(f, axis=1)

mergedContracts = pd.concat([mergedDailyContracts, mergedHourlyContracts]).reset_index()
del mergedContracts['index']

MonthlyContractPayoffs = pd.DataFrame(columns=['ContractName', 'Year', 'Month', 'TotalPayoff'])

for contractName in mergedContracts['ContractName'].unique():
    payoffs = mergedContracts[mergedContracts['ContractName'] == contractName].loc[:, ['Date', 'Payoffs']]
    payoffs.loc[:, 'Date'] = payoffs['Date'].apply(lambda x: x[:7])
    years = [x[:4] for x in payoffs['Date'].unique()]
    months = [x[5:] for x in payoffs['Date'].unique()]


    sums = payoffs.groupby('Date').sum()['Payoffs'].values
    #print(sums)

    new_df = pd.DataFrame(data={'ContractName': contractName, 'Year': years, 'Month': months, 'TotalPayoff': sums})

    MonthlyContractPayoffs = pd.concat([MonthlyContractPayoffs, new_df]).reset_index()
    del MonthlyContractPayoffs['index']

MonthlyContractPayoffs.to_csv('MonthlyContractPayoffs.csv', index=False)

''' Plant Dispatch Modeling

Up to Task 11 '''

fuelPrices['Year'] = [int(x[:4]) for x in fuelPrices['Date']]
fuelPrices['Month'] = [int(x[5:7]) for x in fuelPrices['Date']]

mergedPlant = pd.merge(plantParameters, fuelPrices, left_on=['FuelPriceName', 'Year', 'Month'], right_on=['Variable', 'Year', 'Month'], how='left')
mergedPlant['RunningCost'] = ((mergedPlant['Price'] + mergedPlant['FuelTransportationCost'])* mergedPlant['HeatRate']) + mergedPlant['VOM']

houstonPrices = powerPrices[powerPrices['SettlementPoint'] == 'HB_HOUSTON']
houstonPrices = houstonPrices[(houstonPrices['Date'].str[:4] == '2017') | (houstonPrices['Date'].str[:4] == '2018')]

southPrices = powerPrices[powerPrices['SettlementPoint'] == 'HB_SOUTH']
southPrices = southPrices[(southPrices['Date'].str[:4] == '2017') | (southPrices['Date'].str[:4] == '2018')]

plantHourPrices = pd.concat([houstonPrices, southPrices]).reset_index()
del plantHourPrices['index']
plantHourPrices['Year'] = [int(x[:4]) for x in plantHourPrices['Date']]
plantHourPrices['Month'] = [int(x[5:7]) for x in plantHourPrices['Date']]
plantHourPrices

plantHourPrices = pd.merge(plantHourPrices, mergedPlant, left_on=['Year', 'Month', 'SettlementPoint'], right_on=['Year', 'Month', 'PowerPriceName'], how='left')
