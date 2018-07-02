import pandas as pd
import os
import numpy as np
from gspread_pandas import Spread
channels_output = Spread('wb', 'Performance_Analysis') #authorizing Google Sheets/Drive APIs

#adding path to CSV file with iOS raw data
file_path_iOS = os.path.abspath('Game of Thrones_ Conquest iOS Cohorts 2018-05-14 - 2018-06-24_with impressions.csv')
print(file_path_iOS)
dir_path = os.path.dirname(file_path_iOS)
print(dir_path)
csv_path_iOS = os.path.join(dir_path, 'Game of Thrones_ Conquest iOS Cohorts 2018-05-14 - 2018-06-24_with impressions.csv')

#adding path to CSV file with iOS raw data
file_path_android = os.path.abspath('Game of Thrones_ Conquest Android Cohorts 2018-05-14 - 2018-06-24_with impressions.csv')
print(file_path_android)
dir_path = os.path.dirname(file_path_android)
print(dir_path)
csv_path_android = os.path.join(dir_path, 'Game of Thrones_ Conquest Android Cohorts 2018-05-14 - 2018-06-24_with impressions.csv')

#adding path to CSV file with Singular raw data
file_path_singular = os.path.abspath('Advertiser daily report 2018-05-14-2018-06-24_impressions.csv')
print(file_path_singular)
dir_path = os.path.dirname(file_path_singular)
print(dir_path)
singular_path = os.path.join(dir_path, 'Advertiser daily report 2018-05-14-2018-06-24_impressions.csv')

#creating dataframes for iOS and Android from CSVs
cohorts_iOS = pd.read_csv(csv_path_iOS)
cohorts_android = pd.read_csv(csv_path_android)
cohorts_singular = pd.read_csv(singular_path)

#add OS columns to both dataframes to identify each
cohorts_iOS.info()
cohorts_iOS['OS'] = 'iOS'
cohorts_android.info()
cohorts_android['OS'] = 'android'

#full outer merge of dataframes
cohorts = pd.concat([cohorts_iOS, cohorts_android], ignore_index=True)

#converting date column to datetime format
cohorts['Date'] = pd.to_datetime(cohorts['Date'])
cohorts['Days after Install'].max()
cohorts_singular.info()

#selecting columns to keep
channels = cohorts[['Date','Tracker','Network','Campaign','Adgroup','Creative','Days after Install','Cohort Size',
	'Retained Users','Paying Users','Sessions','Revenue','Revenue Total','Time Spent','Lifetime Value','Country','OS']]

#drop channels
to_drop = ['Untrusted Devices', 'Organic', 'Off-Facebook Installs', 'Facebook Installs', 'Instagram Installs',
								'Facebook Messenger Installs','Owned:Web', 'Owned:HBO','Earned:Social',
								'Adwords UAC Installs', 'Google Organic Search']

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

#group Singular data by Campaign and Source (can go more granular, if necessary)
singular_grouped = cohorts_singular.groupby(['Campaign','Source']).agg({'eCPI':np.mean,'Installs':np.sum,'Cost':np.sum,
												'Impressions':np.sum,'Clicks':np.sum}).reset_index() #add date level
#get map and add date and country level

#add Campaigns Uni column to adjust for Singular names
def campaign_name(x):
	if x['Network'] == 'Paid:Video:Vungle':
		return x['Campaign'].split('_',1)[0] #cut the numbers from the second part of Vungle names
	elif x['Network'] == 'Paid:Video:Unity':
		return x['Campaign']
	elif x['Network'] == 'Paid:Video:AdColony' and x['OS'] == 'iOS':
		return 'Game of Thrones: Conquest iOS  ' + x['Campaign']    #note the double space after iOS
	elif x['Network'] == 'Paid:Video:AdColony' and x['OS'] == 'android':
		return 'Game of Thrones: Conquest Android ' + x['Campaign']
	elif x['Network'] == 'Paid:Video:Supersonic':
		return x['Campaign']
	else:
		return x['Campaign']

channels['Campaign Uni'] = channels.apply(campaign_name, axis=1)

#add Campaign Uni column and Average CPI column to Singular data
singular_grouped['Campaign Uni'] = singular_grouped['Campaign']
singular_grouped['Average CPI'] = singular_grouped['Cost'] / singular_grouped['Installs']

#outer merge Adjust and Singular data on Campaign Uni column
channels = pd.merge(channels,singular_grouped,on='Campaign Uni',how='outer')

#check the number and names of the unique Campaign Uni values
channels['Campaign Uni'].nunique()
campaign_names = channels['Campaign Uni'].unique()
sorted(network_names)
all_campaigns = pd.DataFrame(sorted(campaign_names))

#add net revenue, ARPUs, Purchase, Bid %, Status, Greylist, and Bucket
channels['D1 Net Revenue'] = np.where(channels['Days after Install'] <= 1,channels['Revenue'] * 0.7,0)
channels['D1 ARPU'] = channels['D1 Net Revenue'] / channels['Cohort Size']

channels['D3 Net Revenue'] = np.where(channels['Days after Install'] <= 3,channels['Revenue'] * 0.7,0)
channels['D3 ARPU'] = channels['D3 Net Revenue'] / channels['Cohort Size']

channels['D7 Net Revenue'] = np.where(channels['Days after Install'] <= 7,channels['Revenue'] * 0.7,0)
channels['D7 ARPU'] = channels['D7 Net Revenue'] / channels['Cohort Size'] #double check

channels['D180 ARPU'] = channels['D7 ARPU'] / 0.08
channels['Purchase ?'] = np.where((channels['Cohort Size'] > 50) & (channels['D7 ARPU'] == 0),0,1)
channels['Cohort Unique'] = np.where(channels['Days after Install'] == 0,channels['Cohort Size'],0)

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

#group by Network, Campaign, Adgroup, OS, Country
channels_grouped = channels.groupby(['Date','Network','Campaign Uni','Adgroup','OS','Status','Bucket','Country']).agg({
		'Days after Install':np.max,'Cohort Unique':np.sum,'Sessions':np.sum,'Revenue':np.sum,
		'D7 Net Revenue':np.sum,'D7 ARPU':np.mean,'D180 ARPU':np.mean,'eCPI':np.mean,'Revenue Total':np.max,
		'D3 Net Revenue':np.sum, 'D3 ARPU':np.mean,'D1 Net Revenue':np.sum,'D1 ARPU':np.mean,
		'Impressions':np.sum,'Clicks':np.sum}).reset_index()

#fix UTF format before exporting to Google Sheets
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#filter by partner network
vungle = channels_grouped[channels_grouped['Network'] == 'Paid:Video:Vungle'].reset_index(drop=True)
unity = channels_grouped[channels_grouped['Network'] == 'Paid:Video:Unity'].reset_index(drop=True)
adcolony = channels_grouped[channels_grouped['Network'] == 'Paid:Video:AdColony'].reset_index(drop=True)
ironsourse = channels_grouped[channels_grouped['Network'] == 'Paid:Video:Supersonic'].reset_index(drop=True)

#aggregate all channels
agg_channels = pd.concat([vungle,unity,adcolony,ironsourse],ignore_index=True)

#reset sheets
#channels_output.clear_sheet(sheet='Vungle')
#channels_output.clear_sheet(sheet='Unity')
#channels_output.clear_sheet(sheet='AdColony')
#channels_output.clear_sheet(sheet='IronSource')
#clear All Channels and Singular tabs

#output channel raw data to Google Sheets
#channels_output.df_to_sheet(vungle, sheet='Vungle')
#channels_output.df_to_sheet(unity, sheet='Unity')
#channels_output.df_to_sheet(adcolony, sheet='AdColony')
#channels_output.df_to_sheet(ironsourse, sheet='IronSource')
channels_output.df_to_sheet(agg_channels, sheet='All Channels')
channels_output.df_to_sheet(singular_grouped, sheet='Singular')
#channels_output.df_to_sheet(all_campaigns, sheet='Campaign Names')

#output start and end date
channels_output.df_to_sheet(dates, sheet='dates')




