import pandas as pd
import os
import numpy as np
from gspread_pandas import Spread
channels_output = Spread('new', 'Performance_Analysis')


#adding path to CSV file with iOS raw data
file_path_iOS = os.path.abspath('Game of Thrones_ Conquest iOS Cohorts 2018-06-12 - 2018-06-19.csv')
print(file_path_iOS)
dir_path = os.path.dirname(file_path_iOS)
print(dir_path)
csv_path_iOS = os.path.join(dir_path, 'Game of Thrones_ Conquest iOS Cohorts 2018-06-12 - 2018-06-19.csv')

#adding path to CSV file with iOS raw data
file_path_android = os.path.abspath('Game of Thrones_ Conquest Android Cohorts 2018-06-12 - 2018-06-19.csv')
print(file_path_android)
dir_path = os.path.dirname(file_path_android)
print(dir_path)
csv_path_android = os.path.join(dir_path, 'Game of Thrones_ Conquest Android Cohorts 2018-06-12 - 2018-06-19.csv')

#creating dataframes for iOS and Android from CSVs
cohorts_iOS = pd.read_csv(csv_path_iOS)
cohorts_android = pd.read_csv(csv_path_android)

#add OS columns to both dataframes
cohorts_iOS.info()
cohorts_iOS['OS'] = 'iOS'
cohorts_android.info()
cohorts_android['OS'] = 'android'

#full outer merge of dataframes
cohorts = pd.concat([cohorts_iOS, cohorts_android], ignore_index=True)

#converting date column to datetime format
cohorts['Date'] = pd.to_datetime(cohorts['Date'])
cohorts['Days after Install'].max()

#selecting columns to keep
channels = cohorts[['Date','Tracker','Network','Campaign','Adgroup','Creative','Days after Install','Cohort Size',
	'Retained Users','Paying Users','Sessions','Revenue','Revenue Total','Time Spent','Lifetime Value','Country','OS']]

#drop channels
to_drop = ['Untrusted Devices', 'Organic', 'Off-Facebook Installs', 'Facebook Installs', 'Instagram Installs',
								'Facebook Messenger Installs','Owned:Web', 'Owned:HBO','Earned:Social',
								'Adwords UAC Installs', 'Google Organic Search']
channels = channels[~channels['Network'].isin(to_drop)]

#check dropped networks
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

#extract active campaigns
vungle_data = channels[channels['Network'] == 'Paid:Video:Vungle']
vungle_campaigns = pd.DataFrame(vungle_data['Campaign'].unique())
vungle_campaigns['Network'] = 'Vungle'

unity_data = channels[channels['Network'] == 'Paid:Video:Unity']
unity_campaigns = pd.DataFrame(unity_data['Campaign'].unique())
unity_campaigns['Network'] = 'Unity'

adcolony_data = channels[channels['Network'] == 'Paid:Video:AdColony']
adcolony_campaigns = pd.DataFrame(adcolony_data['Campaign'].unique())
adcolony_campaigns['Network'] = 'AdColony'

ironsourse_data = channels[channels['Network'] == 'Paid:Video:Supersonic']
ironsourse_campaigns = pd.DataFrame(ironsourse_data['Campaign'].unique())
ironsourse_campaigns['Network'] = 'IronSource'

live_campaigns = pd.concat([vungle_campaigns,unity_campaigns,adcolony_campaigns,ironsourse_campaigns],ignore_index=True)
live_campaigns.columns.values[0] = 'Campaign'

#output active campaigns
channels_output.df_to_sheet(live_campaigns, sheet='Live Campaigns')

#get target CPI bids for each network
CPI_GoogleSheets = Spread('new','Performance_Analysis')
bids_data = CPI_GoogleSheets.sheet_to_df(index=1,header_rows=1, start_row=1,sheet='Live Campaigns')
bids = pd.DataFrame(bids_data)
channels = pd.merge(channels,bids,on='Campaign')

#change Bids format to float
channels['Bids'] = pd.to_numeric(channels['Bids'])
type(channels['Bids'])

#add net revenue, ARPU, Purchase, Bid, Status, Bucket
channels['D7 Net Revenue'] = channels['Revenue'] * 0.7
channels['D7 ARPU'] = channels['D7 Net Revenue'] / channels['Cohort Size']
channels['D180 ARPU'] = channels['D7 ARPU'] / 0.08
channels['Purchase ?'] = np.where((channels['Cohort Size'] > 50) & (channels['D7 ARPU'] == 0),0,1)
channels['Cohort Unique'] = np.where(channels['Days after Install'] == 0,channels['Cohort Size'],0)

channels['Greater 75% of Bid'] = np.where((channels['Cohort Size'] > 100) & (channels['D180 ARPU'] < (channels['Bids'] * 0.75)),0,1)
channels['Greater 125% of Bid'] = np.where((channels['Cohort Size'] > 100) & (channels['D180 ARPU'] >= (channels['Bids'] * 1.25)),1,0)
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

channels['Bucket'] = channels.apply(channel_bucket, axis=1)

#drop infinite values as result of 0 cohorts with revenue
channels = channels.replace([np.inf, -np.inf], np.nan).reset_index()
channels.info()

#group by Network, Campaign, Adgroup, OS, Country
channels_grouped = channels.groupby(['Network_x','Campaign','Adgroup','OS','Status','Bucket','Country']).agg({
		'Days after Install':np.max,'Cohort Unique':np.sum,'Sessions':np.sum,'Revenue':np.sum,
		'D7 Net Revenue':np.sum,'D7 ARPU':np.mean,'D180 ARPU':np.mean,'Bids':np.mean},).reset_index()

#fix UTF format before exporting to Google Sheets
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

#filter by partner network
vungle = channels_grouped[channels_grouped['Network_x'] == 'Paid:Video:Vungle'].reset_index(drop=True)
unity = channels_grouped[channels_grouped['Network_x'] == 'Paid:Video:Unity'].reset_index(drop=True)
adcolony = channels_grouped[channels_grouped['Network_x'] == 'Paid:Video:AdColony'].reset_index(drop=True)
ironsourse = channels_grouped[channels_grouped['Network_x'] == 'Paid:Video:Supersonic'].reset_index(drop=True)

agg_channels = pd.concat([vungle,unity,adcolony,ironsourse],ignore_index=True)

#reset sheets
channels_output.clear_sheet(sheet='Vungle')
channels_output.clear_sheet(sheet='Unity')
channels_output.clear_sheet(sheet='AdColony')
channels_output.clear_sheet(sheet='IronSource')

#output channel raw data to Google Sheets
channels_output.df_to_sheet(vungle, sheet='Vungle')
channels_output.df_to_sheet(unity, sheet='Unity')
channels_output.df_to_sheet(adcolony, sheet='AdColony')
channels_output.df_to_sheet(ironsourse, sheet='IronSource')
channels_output.df_to_sheet(agg_channels, sheet='All Channels')

#output start and end date
channels_output.df_to_sheet(dates, sheet='dates')

