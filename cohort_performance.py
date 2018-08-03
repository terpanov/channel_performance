import pandas as pd
import os
import numpy as np
from gspread_pandas import Spread
video_channel_output = Spread('dt_wb', 'Performance_Analysis_Video') #authorizing Google Sheets/Drive APIs

#adding path to CSV file with iOS raw data
file_path_iOS = os.path.abspath('Game of Thrones_ Conquest iOS Cohorts 2018-07-23 - 2018-07-29.csv')
print(file_path_iOS)
dir_path = os.path.dirname(file_path_iOS)
print(dir_path)
csv_path_iOS = os.path.join(dir_path, 'Game of Thrones_ Conquest iOS Cohorts 2018-07-23 - 2018-07-29.csv')

#adding path to CSV file with Android raw data
file_path_android = os.path.abspath('Game of Thrones_ Conquest Android Cohorts 2018-07-23 - 2018-07-29.csv')
print(file_path_android)
dir_path = os.path.dirname(file_path_android)
print(dir_path)
csv_path_android = os.path.join(dir_path, 'Game of Thrones_ Conquest Android Cohorts 2018-07-23 - 2018-07-29.csv')

#adding path to CSV file with Singular raw data
file_path_singular = os.path.abspath('Advertiser daily report 2018-07-23-2018-07-29 (3).csv')
print(file_path_singular)
dir_path = os.path.dirname(file_path_singular)
print(dir_path)
singular_path = os.path.join(dir_path, 'Advertiser daily report 2018-07-23-2018-07-29 (3).csv')

#adding path to CSV file with Singular raw data
file_path_country = os.path.abspath('Country Mapping (Adjust to Singular).csv')
print(file_path_singular)
dir_path = os.path.dirname(file_path_country)
print(dir_path)
country_path = os.path.join(dir_path, 'Country Mapping (Adjust to Singular).csv')

#creating dataframes for iOS and Android from CSVs
cohorts_iOS = pd.read_csv(csv_path_iOS)
cohorts_android = pd.read_csv(csv_path_android)
cohorts_singular = pd.read_csv(singular_path)
country_codes = pd.read_csv(country_path)

#add unified country column to Singular data and map, add date and country level
cohorts_singular['Country Full'] = cohorts_singular['Country']
cohorts_singular = cohorts_singular.drop('Country',1)
cohorts_singular = pd.merge(cohorts_singular,country_codes,left_on='Country Full',right_on='Singular',how='left')


#make android lowercase in Singular for future merging with Adjust
cohorts_singular['OS'] = cohorts_singular['OS'].str.replace('A','a')

#add OS columns to both dataframes to identify each
cohorts_iOS.info()
cohorts_iOS['OS'] = 'iOS'
cohorts_android.info()
cohorts_android['OS'] = 'android'

#full outer merge of dataframes
cohorts = pd.concat([cohorts_iOS, cohorts_android], ignore_index=True)

#converting date column to datetime format
cohorts['Date'] = pd.to_datetime(cohorts['Date'])
cohorts_singular['Date'] = pd.to_datetime(cohorts_singular['Date'])

cohorts['Days after Install'].max()
cohorts.info()

#selecting columns to keep
channels = cohorts[['Date','Tracker','Network','Campaign','Adgroup','Creative','Days after Install','Cohort Size',
				'Paying Users','Sessions','Revenue','Revenue Total','Time Spent','Lifetime Value','Country','OS',
					'Retained Users', 'Paying User Size']]

#drop channels
to_drop = ['Untrusted Devices', 'Organic', 'Off-Facebook Installs', 'Facebook Installs', 'Instagram Installs',
				'Facebook Messenger Installs','Owned:Web', 'Owned:HBO','Earned:Social','Google Organic Search',
		   		'Adwords UAC Installs','Apple Search Ads','Paid:Video:AppOnBoard']

channels = channels[~channels['Network'].isin(to_drop)]

#check that networks were dropped successfully
channels['Network'].nunique()
network_names = channels['Network'].unique()
sorted(network_names)

#add Weeks after Install column and create first/last date dataframe
channels['Weeks after Install'] = (channels['Days after Install'] / 7).round()
channels = channels.reset_index()
date_start = channels['Date'].min()
date_end = channels['Date'].max()
dates = pd.DataFrame([date_start,date_end])

#fill blank values with zeroes
channels = channels.fillna(0)
channels['Network'].unique()
cohorts_singular['Source'].unique()

def network_name(x):
	if x['Source'] == 'Vungle':
		return 'Paid:Video:Vungle'
	elif x['Source'] == 'Unity Ads':
		return 'Paid:Video:Unity'
	elif x['Source'] == 'AdColony':
		return 'Paid:Video:AdColony'
	elif x['Source'] == 'SupersonicAds':
		return 'Paid:Video:Supersonic'
	elif x['Source'] == 'AppLovin':
		return 'Paid:Video:AppLovin'
	elif x['Source'] == 'TapJoy':
		return 'Paid:Video:TapJoy'

cohorts_singular['Network'] = cohorts_singular.apply(network_name, axis=1)
singular_grouped = cohorts_singular

#add Campaigns Uni column to adjust for Singular names
def campaign_name(x):
	if x['Network'] == 'Paid:Video:Vungle':
		if 'Forge of Empires' in x['Campaign']:
			return x['Campaign'].rsplit('_',1)[0] #cut one value from the right
		else:
			return x['Campaign'].split('_',1)[0] #cut the numbers from the second part of Vungle names
	elif x['Network'] == 'Paid:Video:Unity':
		return x['Campaign']
	elif x['Network'] == 'Paid:Video:AdColony' and x['OS'] == 'iOS':
		return 'Game of Thrones: Conquest iOS  ' + x['Campaign']    #note the double space after iOS
	elif x['Network'] == 'Paid:Video:AdColony' and x['OS'] == 'android':
		return 'Game of Thrones: Conquest Android ' + x['Campaign']
	elif x['Network'] == 'Paid:Video:Supersonic':
		return x['Campaign']
	elif x['Network'] == 'Paid:Video:AppLovin':
		return x['Campaign']
	else:
		return x['Campaign']

channels['Campaign Uni'] = channels.apply(campaign_name, axis=1)

#add Campaign Uni column and Average CPI column to Singular data
singular_grouped['Campaign Uni'] = singular_grouped['Campaign']
singular_grouped['Average CPI'] = singular_grouped['Cost'] / singular_grouped['Installs']

adjust_camp = pd.DataFrame(channels[['Network','Campaign Uni']])
singular_camp = pd.DataFrame(singular_grouped[['Network','Campaign Uni','Cost']])

channels['Campaign Uni'].unique()
singular_grouped.info()
#outer merge Adjus# t and Singular data on 'Date','Network','Campaign Uni','OS','Country' columns #,
channels = pd.merge(channels,singular_grouped,left_on=['Date','Network','OS','Country','Campaign Uni'],
			right_on=['Date','Network','OS','Country ','Campaign Uni'],how='outer')

#check the number and names of the unique Campaign Uni values
channels['Campaign Uni'].nunique()
campaign_names = channels['Campaign Uni'].unique()
sorted(network_names)

#add net revenue, ARPUs, Purchase, Bid %, Status, Greylist, and Bucket
channels.info()

#cohort size
channels['Cohort Day 0'] = np.where(channels['Days after Install'] == 0,channels['Cohort Size'],0)
channels['Cohort Day 3'] = np.where(channels['Days after Install'] == 3,channels['Cohort Size'],0)
channels['Cohort Day 7'] = np.where(channels['Days after Install'] == 7,channels['Cohort Size'],0)

#retained users
channels.info()
channels['Retained Users 0'] = np.where(channels['Days after Install'] == 0,channels['Retained Users'],0)
channels['Retained Users 3'] = np.where(channels['Days after Install'] == 3,channels['Retained Users'],0)
channels['Retained Users 7'] = np.where(channels['Days after Install'] == 7,channels['Retained Users'],0)

#paying users
channels['Paying User Size 0'] = np.where(channels['Days after Install'] == 0,channels['Paying User Size'],0)
channels['Paying User Size 3'] = np.where(channels['Days after Install'] == 3,channels['Paying User Size'],0)
channels['Paying User Size 7'] = np.where(channels['Days after Install'] == 7,channels['Paying User Size'],0)

channels['D1 Net Revenue'] = np.where(channels['Days after Install'] <= 1,channels['Revenue'] * 0.7,0)
channels['D1 ARPU'] = channels['D1 Net Revenue'] / channels['Cohort Day 0']

channels['D3 Net Revenue'] = np.where(channels['Days after Install'] <= 3,channels['Revenue'] * 0.7,0)
channels['D3 ARPU'] = channels['D3 Net Revenue'] / channels['Cohort Day 3']

channels['D7 Net Revenue'] = np.where(channels['Days after Install'] <= 7,channels['Revenue'] * 0.7,0)
channels['D7 ARPU'] = channels['D7 Net Revenue'] / channels['Cohort Day 7']  #double check

channels['D180 ARPU'] = channels['D7 ARPU'] / 0.08
channels['Purchase ?'] = np.where((channels['Cohort Size'] > 50) & (channels['D7 ARPU'] == 0),0,1)

channels['Greater 75% of Bid'] = np.where((channels['Cohort Size'] > 100) & (channels['D180 ARPU'] < (channels['Average CPI'] * 0.75)),0,1)
channels['Greater 125% of Bid'] = np.where((channels['Cohort Size'] > 100) & (channels['D180 ARPU'] >= (channels['Average CPI'] * 1.25)),1,0)
channels['Status'] = np.where((channels['Purchase ?'] == 0) | (channels['Greater 75% of Bid'] == 0),'Pause','Live')
channels['Greylist'] = np.where((channels['Purchase ?'] == 1) | (channels['Greater 125% of Bid'] == 1),1,0)

#channel bucket function
def channel_bucket(x):
	if x['Status'] == 'Pause':
		return 'Blacklist'
	elif x['Cohort Size'] < 50:
		return 'RON'
	elif x['Greylist'] == 1:
		return 'Greylist'
	else:
		return 'RON'

#apply channel_bucket function
channels['Bucket'] = channels.apply(channel_bucket, axis=1)

#drop infinite values as result of 0 cohorts with revenue
channels = channels.replace([np.inf, -np.inf], np.nan)

#group by Network, Campaign, Adgroup, OS, Country  #'Adgroup','Creative','Campaign Uni'
grouped_by_date = channels.groupby(['Date','Network','OS','Country','Campaign Uni'
									#'Impressions','Clicks','Installs','Cost','eCPI'
		]).agg({'Cost':np.max,'Impressions':np.max,'Clicks':np.max,'Installs':np.max,'eCPI':np.mean,
				'D1 ARPU':np.mean,'D3 ARPU':np.mean,'D7 ARPU':np.mean,'D180 ARPU':np.mean,
                'Retained Users 0':np.sum,'Retained Users 3':np.sum,'Retained Users 7':np.sum,
                'Cohort Day 0':np.sum,'Cohort Day 3':np.sum, 'Cohort Day 7':np.sum,
                'D1 Net Revenue':np.sum,'D3 Net Revenue':np.sum,'D7 Net Revenue':np.sum,
                'Paying User Size 0':np.sum,'Paying User Size 3':np.sum,'Paying User Size 7':np.sum,'Revenue Total':np.max,
                'Days after Install':np.max,'CTR':np.mean,'CVR':np.mean,'Lifetime Value':np.mean,'Cohort Size':np.max}).reset_index()

grouped_by_date['Average CPI'] = grouped_by_date['Cost'] / grouped_by_date['Cohort Day 0']
grouped_by_date['Average eCPI (check)'] = grouped_by_date['Cost'] / grouped_by_date['Installs']

#filter channels by partner network by date
vungle = grouped_by_date[grouped_by_date['Network'] == 'Paid:Video:Vungle'].reset_index(drop=True)
unity = grouped_by_date[grouped_by_date['Network'] == 'Paid:Video:Unity'].reset_index(drop=True)
adcolony = grouped_by_date[grouped_by_date['Network'] == 'Paid:Video:AdColony'].reset_index(drop=True)
ironsourse = grouped_by_date[grouped_by_date['Network'] == 'Paid:Video:Supersonic'].reset_index(drop=True)

#aggregate all channels
agg_video_by_date = pd.concat([vungle,unity,adcolony,ironsourse],ignore_index=True)
agg_video_by_date = agg_video_by_date.fillna(0)

#output to Google Sheets
video_channel_output.df_to_sheet(grouped_by_date, sheet='sort by date')

#output start and end date
video_channel_output.df_to_sheet(dates, sheet='dates')



