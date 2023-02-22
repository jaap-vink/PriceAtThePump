# this is 'raw' code. No optimization or cleaning has been done.
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 20 12:17:26 2023

@author: zenoa
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

'Import cbsodata to read CBS (Dutch Central Buereau of Statistics) data into pandas'
import cbsodata as cbs

'import pandassmx to read ECB data into pandas'
import pandasdmx as sdmx

""" Import ECB USD-EUR daily reference exchange rates starting on Jan 1, 2006. Results to Panda DF df_exch_rate"""
ecb = sdmx.Request('ECB')
key = dict(CURRENCY='USD', CURRENCY_DENOM='EUR', FREQ='D', EXR_SUFFIX='A')
params = dict(startPeriod='2005-12-30')
data = ecb.data('EXR', key=key, params=params).data[0]
df_exch_rate = sdmx.to_pandas(data)
df_exch_rate = df_exch_rate.reset_index()
df_exch_rate = df_exch_rate.iloc[:,5:]
df_exch_rate = df_exch_rate.reset_index()
df_exch_rate = df_exch_rate.rename(columns={'value': "USD_RATE"})
df_exch_rate['TIME_PERIOD'] = pd.to_datetime(df_exch_rate['TIME_PERIOD'])
df_exch_rate.set_index('TIME_PERIOD',inplace=True)
df_exch_rate.drop(labels='index', axis=1, inplace=True)
'print(df_exch_rate.head())'


'Read daily european brent oil prices from eia.gov'
df_oil_price = pd.read_excel(r'https://www.eia.gov/dnav/pet/hist_xls/RBRTEd.xls', sheet_name= 1, header=2)
df_oil_price = df_oil_price.rename(columns={"Date": "TIME_PERIOD","Europe Brent Spot Price FOB (Dollars per Barrel)": "EuroBrent_Barrel"})
df_oil_price.set_index('TIME_PERIOD',inplace=True)
df_oil_price = df_oil_price["2005-12-30":]
'calculate Euro Brent litere price (1 barrel = 158.987 litres)'
df_oil_price["EuroBrent_Litre"]=df_oil_price['EuroBrent_Barrel'] / 158.987
'join exchange rates and claculate liter price in Euros'
df_oil_price = df_oil_price.join(df_exch_rate)
df_oil_price['EuroBrent_Litre_Euros']=df_oil_price['EuroBrent_Litre']/df_oil_price['USD_RATE']
df_oil_price_euro=df_oil_price.drop(labels=['EuroBrent_Barrel','EuroBrent_Litre','USD_RATE'],axis=1)


'Import CBS daily fuel prices'
df_fuel_price = pd.DataFrame(cbs.get_data('80416ned'))
df_fuel_price['TIME_PERIOD']=pd.date_range(start='2006-01-01',periods=len(df_fuel_price),freq='D')
df_fuel_price.drop(labels=['ID', 'Perioden'], axis=1, inplace=True)
df_fuel_price = df_fuel_price.rename(columns={"BenzineEuro95_1": "EURO95","Diesel_2": "Diesel",'Lpg_3':'LPG'})
df_fuel_price.set_index('TIME_PERIOD',inplace=True)


'merge daily fuel prices with euro brent in euros for analysis'
df_price_analysis=df_fuel_price.join(df_oil_price_euro,how='outer')
df_price_analysis['EuroBrent_Litre_Euros'] = df_price_analysis['EuroBrent_Litre_Euros'].ffill()
df_price_analysis=df_price_analysis["2006-01-01":]


'read excise tarif data for fuel (LPG converted to litres using 2.028397566 litres/kg) and join to df_proce_analysis'
df_excise=pd.read_csv('accijns 2006-2023.csv')
df_excise['start_date'] = pd.to_datetime(df_excise['start_date'])
df_excise['end_date'] = pd.to_datetime(df_excise['end_date'])

df_price_analysis = df_price_analysis.reset_index()
df_excise.index = pd.IntervalIndex.from_arrays(df_excise['start_date'],df_excise['end_date'],closed='both')
df_price_analysis['EURO95_acc'] = df_price_analysis['TIME_PERIOD'].apply(lambda x : df_excise.iloc[df_excise.index.get_loc(x)]['EURO95_acc'])
df_price_analysis['Diesel_acc'] = df_price_analysis['TIME_PERIOD'].apply(lambda x : df_excise.iloc[df_excise.index.get_loc(x)]['Diesel_acc'])
df_price_analysis['LPG_acc'] = df_price_analysis['TIME_PERIOD'].apply(lambda x : df_excise.iloc[df_excise.index.get_loc(x)]['LPG_acc'])
df_price_analysis.set_index('TIME_PERIOD',inplace=True)

'deduct VAT from the prices. 19% before Oct 1 2012, 21% from Oct 1 2012'
df_price_analysis['EURO95_exVAT'] = np.where(df_price_analysis.index<pd.to_datetime('2012-10-01'),df_price_analysis['EURO95']/1.19,df_price_analysis['EURO95']/1.21)
df_price_analysis['Diesel_exVAT'] = np.where(df_price_analysis.index<pd.to_datetime('2012-10-01'),df_price_analysis['Diesel']/1.19,df_price_analysis['Diesel']/1.21)
df_price_analysis['LPG_exVAT'] = np.where(df_price_analysis.index<pd.to_datetime('2012-10-01'),df_price_analysis['LPG']/1.19,df_price_analysis['LPG']/1.21)


'Calculate net price by deducting excise'
df_price_analysis['EURO95_Net']=df_price_analysis['EURO95_exVAT']-df_price_analysis['EURO95_acc']
df_price_analysis['Diesel_Net']=df_price_analysis['Diesel_exVAT']-df_price_analysis['Diesel_acc']
df_price_analysis['LPG_Net']=df_price_analysis['LPG_exVAT']-df_price_analysis['LPG_acc']


'Calculate gross margin on Euro Brent price (absolute and fraction'
df_price_analysis['EURO95_GrMarg']=df_price_analysis['EURO95_Net']-df_price_analysis['EuroBrent_Litre_Euros']
df_price_analysis['Diesel_GrMarg']=df_price_analysis['Diesel_Net']-df_price_analysis['EuroBrent_Litre_Euros']
df_price_analysis['LPG_GrMarg']=df_price_analysis['LPG_Net']-df_price_analysis['EuroBrent_Litre_Euros']

df_price_analysis['EURO95_GrMargFr']=df_price_analysis['EURO95_GrMarg']/df_price_analysis['EURO95_Net']
df_price_analysis['Diesel_GrMargFr']=df_price_analysis['Diesel_GrMarg']/df_price_analysis['Diesel_Net']
df_price_analysis['LPG_GrMargFr']=df_price_analysis['LPG_GrMarg']/df_price_analysis['LPG_Net']



df_price_analysis['EuroBrent_Litre_Euros'].plot(label="Euro Brent")
df_price_analysis['EURO95_GrMarg'].plot(label="EURO95")
df_price_analysis['Diesel_GrMarg'].plot(label="Diesel")
plt.xlabel('Date')
plt.ylabel('Amount in Euro')
plt.title('Euro Brent and Gross Margin')
plt.legend(loc="upper left")
plt.show()

df_price_analysis['EURO95_GrMargFr'].plot()
df_price_analysis['Diesel_GrMargFr'].plot()
plt.xlabel('Date')
plt.ylabel('Gross Margin (fraction)')
plt.title('Gross Margin over Time')
plt.show()


df_price_anal_1923=df_price_analysis['2019-10-01':]

df_price_anal_1923['EuroBrent_Litre_Euros'].plot(label="Euro Brent",color='k')
df_price_anal_1923['EURO95_GrMarg'].plot(label="EURO95", color='b')
df_price_anal_1923['Diesel_GrMarg'].plot(label="Diesel", color='c')
plt.xlabel('Date')
plt.ylabel('Amount in Euro')
plt.title('Euro Brent and Gross Margin 2019-2023')
plt.vlines(pd.to_datetime('2022-04-01'),0,1,linestyles='dashed',color='b',label='Temporary excise decrease')

plt.vlines(pd.to_datetime('2020-02-27'),0,1,linestyles='dotted',color='r',label='1st Covid patient')
plt.vlines(pd.to_datetime('2020-03-23'),0,1,linestyles='dashed',color='r',label='Lockdown')
plt.vlines(pd.to_datetime('2020-06-01'),0,1,linestyles='dashed',color='g',label='Lockdown release')
plt.vlines(pd.to_datetime('2021-12-19'),0,1,linestyles='solid',color='r',label='Hard Lockdown')
plt.vlines(pd.to_datetime('2022-02-24'),0,1,linestyles='dashed',color='k',label='Ukrain Invasion')
plt.vlines(pd.to_datetime('2022-03-31'),0,1,linestyles='solid',color='g',label='End Covid regulations')

plt.legend(loc="best", ncols=3, fontsize='x-small')
plt.vlines(pd.to_datetime('2020-10-14'),0,1,linestyles='dotted',color='r')
plt.vlines(pd.to_datetime('2021-06-05'),0,1,linestyles='dotted',color='g')
plt.vlines(pd.to_datetime('2022-01-26'),0,1,linestyles='dotted',color='g')
plt.show()
