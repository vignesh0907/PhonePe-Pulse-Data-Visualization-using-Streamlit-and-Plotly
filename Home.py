import streamlit as st
import pandas as pd
import plotly.express as px
import folium
import requests
import os
import subprocess
import json
import geopandas as gpd
import mysql.connector
from sqlalchemy import create_engine

st.set_page_config(layout='wide')
st.title('Phonepe Pulse Data - Data Visualization')
#State Name Pre-Processing...
state_name_mapping = {
        'andaman & nicobar islands': "Andaman and Nicobar Islands",
        'andhra pradesh': "Andhra Pradesh",
        'arunachal pradesh': "Arunachal Pradesh",
        'assam': "Assam",
        'bihar': "Bihar",
        'chandigarh': "Chandigarh",
        'chhattisgarh': "Chhattisgarh",
        'dadra & nagar haveli & daman & diu': "Daman and Diu",
        'delhi': "Delhi",
        'goa': "Goa",
        'gujarat' : "Gujarat",
        'haryana': "Haryana",
        'himachal pradesh': "Himachal Pradesh",
        'jammu & kashmir': "Jammu and Kashmir",
        'jharkhand': "Jharkhand",
        'karnataka': "Karnataka",
        'kerala': "Kerala",
        'ladakh': "Ladakh",
        'lakshadweep': "Lakshadweep",
        'madhya pradesh': "Madhya Pradesh",
        'maharashtra': "Maharashtra",
        'manipur': "Manipur",
        'meghalaya': "Meghalaya",
        'mizoram': "Mizoram",
        'nagaland': "Nagaland",
        'odisha': "Odisha",
        'puducherry': "Puducherry",
        'punjab': "Punjab",
        'rajasthan': 'Rajasthan',
        'sikkim': "Sikkim",
        'tamil nadu': "Tamil Nadu",
        'telangana': "Telangana",
        'tripura': "Tripura",
        'uttar pradesh': "Uttar Pradesh",
        'uttarakhand': "Uttarakhand",
        'west bengal': "West Bengal"
        }
def format_state_name(state_name):
    return state_name_mapping.get(state_name.lower(), state_name)

tab1, tab2, tab3 = st.tabs(['Data Extraction - Transformation - DB Insertion','Aggregated Data Visualization', 'Map Data Visualization'])
agg_state_user_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/user/country/india/state"
connect_mysql = mysql.connector.connect(
                host = '127.0.0.1',
                port = 3303,
                user = 'root',
                password = 'root',
                )              
mycursor = connect_mysql.cursor()
#Create DB...
mycursor.execute('CREATE DATABASE IF NOT EXISTS phonepe_db')
mycursor.execute('USE phonepe_db')

with tab1:
    col1,col2 = st.columns(2, gap="small")
    with col1:
        st.write('To Clone the Github Data click on the below button.')
    #Cloning of the GitHub repository
        clone_btn = st.button('Clone Data')
        if clone_btn:
            response = requests.get('https://api.github.com/repos/PhonePe/pulse')
            if response.status_code ==200:
                repo = response.json()
                clone_url = repo['clone_url']
    #Specify the local directory path and clone the repository
                os.makedirs("F:/Capstone Projects/PhonePe Project/phonepe_pulse")
                clone_dir= "F:/Capstone Projects/PhonePe Project/phonepe_pulse"
                try:
                    subprocess.run(["git", "clone", clone_url, clone_dir], check=True,capture_output=True,text=True)
                    st.write(f'Repository cloned to {clone_dir}')
                except subprocess.CalledProcessError as e:
                    st.write(f"Error:{e}")
            else:
                st.write(f'Failed to fetch repository details. Status code: {response.status_code}')
    with col2:
        st.write('To extract the data from the Github cloned file click on the below button')
        extract_btn = st.button('Extract Data and Insert to DB')
        if extract_btn:
            #Aggregated - Transaction Data Extraction...
            agg_2018_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/transaction/country/india/2018"
            agg_2019_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/transaction/country/india/2019"
            agg_2020_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/transaction/country/india/2020"
            agg_2021_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/transaction/country/india/2021"
            agg_2022_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/transaction/country/india/2022"
            agg_2023_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/transaction/country/india/2023"
            agg_state_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/transaction/country/india/state"
            
            json_agg_2018 = [os.path.join(agg_2018_pt, file) for file in os.listdir(agg_2018_pt) if file.endswith('.json')]
            json_agg_2019 = [os.path.join(agg_2019_pt, file) for file in os.listdir(agg_2019_pt) if file.endswith('.json')]
            json_agg_2020 = [os.path.join(agg_2020_pt, file) for file in os.listdir(agg_2020_pt) if file.endswith('.json')]
            json_agg_2021 = [os.path.join(agg_2021_pt, file) for file in os.listdir(agg_2021_pt) if file.endswith('.json')]
            json_agg_2022 = [os.path.join(agg_2022_pt, file) for file in os.listdir(agg_2022_pt) if file.endswith('.json')]
            json_agg_2023 = [os.path.join(agg_2023_pt, file) for file in os.listdir(agg_2023_pt) if file.endswith('.json')]
            
            names_2018 = []
            counts_2018 = []
            amounts_2018 = []
            year_2018 = []
            for foldername, subfolders,filenames in os.walk(agg_2018_pt):
                year = os.path.basename(foldername)
                for json_file in json_agg_2018:
                    with open(json_file, 'r') as file:
                        data_2018 = json.load(file)
                        agg_2018_transaction_data = data_2018['data']['transactionData']
                    
                        for entry in agg_2018_transaction_data:
                            names_2018.append(entry['name'])
                            year_2018.append(year)
                            counts_2018.append(entry["paymentInstruments"][0]["count"])
                            amounts_2018.append(entry["paymentInstruments"][0]["amount"])
                        df_agg_2018 = pd.DataFrame({
                        "Transaction_Name": names_2018,
                        "Year": year_2018,
                        "Count": counts_2018,
                        "Amount": amounts_2018
                        })

                        df_agg_2018_grp = df_agg_2018.groupby(["Transaction_Name","Year"]).agg({"Count": "sum", "Amount": "sum"}).reset_index()
                        csv_file_2018_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_trans_2018_data.csv"
                        df_agg_2018_grp.to_csv(csv_file_2018_path, index=False)
                        mycursor.execute('DROP TABLE IF EXISTS aggregate_trans_2018')
                        mycursor.execute('CREATE TABLE aggregate_trans_2018(Transaction_Name varchar(30), Year INT, Count BIGINT, Amount DECIMAL(20,2))')
                        engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                        df_agg_2018_grp.to_sql('aggregate_trans_2018', con=engine, if_exists = 'append', index=False)


            names_2019 = []
            counts_2019 = []
            amounts_2019 = []
            year_2019 = []
            for foldername, subfolders,filenames in os.walk(agg_2019_pt):
                year = os.path.basename(foldername)
                for json_file in json_agg_2019:
                    with open(json_file, 'r') as file:
                        data_2019 = json.load(file)
                        agg_2019_transaction_data = data_2019['data']['transactionData']
                        for entry in agg_2019_transaction_data:
                            names_2019.append(entry['name'])
                            year_2019.append(year)
                            counts_2019.append(entry["paymentInstruments"][0]["count"])
                            amounts_2019.append(entry["paymentInstruments"][0]["amount"])
                        df_agg_2019 = pd.DataFrame({
                            "Transaction_Name": names_2019,
                            "Year": year_2019,
                            "Count": counts_2019,
                            "Amount": amounts_2019
                        })
                        df_agg_2019_grp = df_agg_2019.groupby(["Transaction_Name", "Year"]).agg({"Count": "sum", "Amount": "sum"}).reset_index()
                        csv_file_2019_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_trans_2019_data.csv"
                        df_agg_2019_grp.to_csv(csv_file_2019_path, index=False)
                        mycursor.execute('DROP TABLE IF EXISTS aggregate_trans_2019')
                        mycursor.execute('CREATE TABLE aggregate_trans_2019(Transaction_Name varchar(30), Year INT, Count BIGINT, Amount DECIMAL(20,2))')
                        engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                        df_agg_2019_grp.to_sql('aggregate_trans_2019', con=engine, if_exists = 'append', index=False)

            names_2020 = []
            counts_2020 = []
            amounts_2020 = []
            year_2020 = []
            for foldername, subfolders,filenames in os.walk(agg_2020_pt):
                year = os.path.basename(foldername)
                for json_file in json_agg_2020:
                    with open(json_file, 'r') as file:
                        data_2020 = json.load(file)
                        agg_2020_transaction_data = data_2020['data']['transactionData']
                        for entry in agg_2020_transaction_data:
                            names_2020.append(entry['name'])
                            year_2020.append(year)
                            counts_2020.append(entry["paymentInstruments"][0]["count"])
                            amounts_2020.append(entry["paymentInstruments"][0]["amount"])
                        df_agg_2020 = pd.DataFrame({
                            "Transaction_Name": names_2020,
                            "Year": year_2020,
                            "Count": counts_2020,
                            "Amount": amounts_2020
                        })
                        df_agg_2020_grp = df_agg_2020.groupby(["Transaction_Name","Year"]).agg({"Count": "sum", "Amount": "sum"}).reset_index()
                        csv_file_2020_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_trans_2020_data.csv"
                        df_agg_2020_grp.to_csv(csv_file_2020_path, index=False)
                        mycursor.execute('DROP TABLE IF EXISTS aggregate_trans_2020')
                        mycursor.execute('CREATE TABLE aggregate_trans_2020(Transaction_Name varchar(30), Year INT, Count BIGINT, Amount DECIMAL(20,2))')
                        engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                        df_agg_2020_grp.to_sql('aggregate_trans_2020', con=engine, if_exists = 'append', index=False)

            names_2021 = []
            counts_2021 = []
            amounts_2021 = []
            year_2021 = []
            for foldername, subfolders,filenames in os.walk(agg_2021_pt):
                year = os.path.basename(foldername)
                for json_file in json_agg_2021:
                    with open(json_file, 'r') as file:
                        data_2021 = json.load(file)
                        agg_2021_transaction_data = data_2021['data']['transactionData']
                        for entry in agg_2021_transaction_data:
                            names_2021.append(entry['name'])
                            year_2021.append(year)
                            counts_2021.append(entry["paymentInstruments"][0]["count"])
                            amounts_2021.append(entry["paymentInstruments"][0]["amount"])
                        df_agg_2021 = pd.DataFrame({
                            "Transaction_Name": names_2021,
                            "Year": year_2021,
                            "Count": counts_2021,
                            "Amount": amounts_2021
                        })
                        df_agg_2021_grp = df_agg_2021.groupby(["Transaction_Name","Year"]).agg({"Count": "sum", "Amount": "sum"}).reset_index()
                        csv_file_2021_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_trans_2021_data.csv"
                        df_agg_2021_grp.to_csv(csv_file_2021_path, index=False)
                        mycursor.execute('DROP TABLE IF EXISTS aggregate_trans_2021')
                        mycursor.execute('CREATE TABLE aggregate_trans_2021(Transaction_Name varchar(30), Year INT, Count BIGINT, Amount DECIMAL(20,2))')
                        engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                        df_agg_2021_grp.to_sql('aggregate_trans_2021', con=engine, if_exists = 'append', index=False)

            names_2022 = []
            counts_2022 = []
            amounts_2022 = []
            year_2022 = []
            for foldername, subfolders,filenames in os.walk(agg_2022_pt):
                year = os.path.basename(foldername)
                for json_file in json_agg_2022:
                    with open(json_file, 'r') as file:
                        data_2022 = json.load(file)
                        agg_2022_transaction_data = data_2022['data']['transactionData']
                        for entry in agg_2022_transaction_data:
                            names_2022.append(entry['name'])
                            year_2022.append(year)
                            counts_2022.append(entry["paymentInstruments"][0]["count"])
                            amounts_2022.append(entry["paymentInstruments"][0]["amount"])
                        df_agg_2022 = pd.DataFrame({
                            "Transaction_Name": names_2022,
                            "Year": year_2022,
                            "Count": counts_2022,
                            "Amount": amounts_2022
                        })
                        df_agg_2022_grp = df_agg_2022.groupby(["Transaction_Name", "Year"]).agg({"Count": "sum", "Amount": "sum"}).reset_index()
                        csv_file_2022_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_trans_2022_data.csv"
                        df_agg_2022_grp.to_csv(csv_file_2022_path, index=False)
                        mycursor.execute('DROP TABLE IF EXISTS aggregate_trans_2022')
                        mycursor.execute('CREATE TABLE aggregate_trans_2022(Transaction_Name varchar(30), Year INT, Count BIGINT, Amount DECIMAL(20,2))')
                        engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                        df_agg_2022_grp.to_sql('aggregate_trans_2022', con=engine, if_exists = 'append', index=False)

            names_2023 = []
            counts_2023 = []
            amounts_2023 = []
            year_2023 = []
            for foldername, subfolders,filenames in os.walk(agg_2023_pt):
                year = os.path.basename(foldername)
                for json_file in json_agg_2023:
                    with open(json_file, 'r') as file:
                        data_2023 = json.load(file)
                        agg_2023_transaction_data = data_2023['data']['transactionData']
                        for entry in agg_2023_transaction_data:
                            names_2023.append(entry['name'])
                            year_2023.append(year)
                            counts_2023.append(entry["paymentInstruments"][0]["count"])
                            amounts_2023.append(entry["paymentInstruments"][0]["amount"])
                        df_agg_2023 = pd.DataFrame({
                            "Transaction_Name": names_2023,
                            "Year": year_2023,
                            "Count": counts_2023,
                            "Amount": amounts_2023
                        })
                        df_agg_2023_grp = df_agg_2023.groupby(["Transaction_Name","Year"]).agg({"Count": "sum", "Amount": "sum"}).reset_index()
                        csv_file_2023_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_trans_2023_data.csv"
                        df_agg_2023_grp.to_csv(csv_file_2023_path, index=False)
                        mycursor.execute('DROP TABLE IF EXISTS aggregate_trans_2023')
                        mycursor.execute('CREATE TABLE aggregate_trans_2023(Transaction_Name varchar(30), Year INT, Count BIGINT, Amount DECIMAL(20,2))')
                        engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                        df_agg_2023_grp.to_sql('aggregate_trans_2023', con=engine, if_exists = 'append', index=False)

            state_data = {'State': [], 'Year': [], 'Transaction_Name': [], 'Transaction_count': [], 'Transaction_amount': []}
            for foldername, subfolders, filenames in os.walk(agg_state_pt):
                year = os.path.basename(foldername)
                state_name = os.path.basename(os.path.dirname(foldername))
                state_name = state_name.title()
                state_name = state_name.replace("-"," ")
                state_name = state_name.replace("&","and")
                for filename in filenames:
                    if filename.endswith('.json'):
                        file_path = os.path.join(foldername, filename)
                        with open(file_path, 'r') as file:
                            data = json.load(file)
                    for entry in data.get('data', {}).get('transactionData', []):
                        name = entry.get('name', '')
                        count = entry.get('paymentInstruments', [{}])[0].get('count', 0)
                        amount = entry.get('paymentInstruments', [{}])[0].get('amount', 0)
                        quarter = int(os.path.splitext(filename)[0])  
                        state_data['State'].append(state_name)
                        state_data['Year'].append(year)
                        state_data['Transaction_Name'].append(name)
                        state_data['Transaction_count'].append(count)
                        state_data['Transaction_amount'].append(amount)
            df_state_trans = pd.DataFrame(state_data)
            df_state_trans_grp = df_state_trans.groupby(['State','Year', 'Transaction_Name']).agg({'Transaction_count': 'sum', 'Transaction_amount': 'sum'}).reset_index()
            csv_file_statetrans_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_trans_state_data.csv"
            df_state_trans_grp.to_csv(csv_file_statetrans_path, index=False)
            mycursor.execute('DROP TABLE IF EXISTS aggregate_trans_state')
            mycursor.execute('CREATE TABLE aggregate_trans_state(State varchar(100), Year INT, Transaction_Name varchar(30), Transaction_count BIGINT, Transaction_amount DECIMAL(20,2))')
            engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
            df_state_trans_grp.to_sql('aggregate_trans_state', con=engine, if_exists = 'append', index=False)
            # Aggregated - User Data Extraction...
            agg_trans_user_2018_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/user/country/india/2018"
            agg_trans_user_2019_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/user/country/india/2019"
            agg_trans_user_2020_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/user/country/india/2020"
            agg_trans_user_2021_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/user/country/india/2021"
            agg_trans_user_2022_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/user/country/india/2022"
            agg_trans_user_2023_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/aggregated/user/country/india/2023"
            json_agg_trans_user_2018 = [os.path.join(agg_trans_user_2018_pt, file) for file in os.listdir(agg_trans_user_2018_pt) if file.endswith('.json')]
            json_agg_trans_user_2019 = [os.path.join(agg_trans_user_2019_pt, file) for file in os.listdir(agg_trans_user_2019_pt) if file.endswith('.json')]
            json_agg_trans_user_2020 = [os.path.join(agg_trans_user_2020_pt, file) for file in os.listdir(agg_trans_user_2020_pt) if file.endswith('.json')]
            json_agg_trans_user_2021 = [os.path.join(agg_trans_user_2021_pt, file) for file in os.listdir(agg_trans_user_2021_pt) if file.endswith('.json')]
            json_agg_trans_user_2022 = [os.path.join(agg_trans_user_2022_pt, file) for file in os.listdir(agg_trans_user_2022_pt) if file.endswith('.json')]
            json_agg_trans_user_2023 = [os.path.join(agg_trans_user_2023_pt, file) for file in os.listdir(agg_trans_user_2023_pt) if file.endswith('.json')]

            brands_agg_trans_2018 = []
            counts_agg_trans_2018 = []
            percentages_agg_trans_2018 = []
            year_agg_trans_2018 = []
            for foldername, subfolders, filenames in os.walk(agg_trans_user_2018_pt):
                year = os.path.basename(foldername)
            for json_file in json_agg_trans_user_2018:
                with open(json_file, 'r') as file:
                    data_2018 = json.load(file)
                    usersByDevice = data_2018['data']['usersByDevice']
                    if usersByDevice is None:
                        # st.write(f"There is no data{year}")
                        continue
                    for entry in usersByDevice:
                        year_agg_trans_2018.append(year)
                        brands_agg_trans_2018.append(entry['brand'])
                        counts_agg_trans_2018.append(entry["count"])
                        percentages_agg_trans_2018.append(entry["percentage"])
            df_agg_trans_2018 = pd.DataFrame({
                "Year": year_agg_trans_2018,
                "Brand": brands_agg_trans_2018,
                "Count": counts_agg_trans_2018,
                "Percentage": percentages_agg_trans_2018
                })
            df_agg_tranusr_2018_grp = df_agg_trans_2018.groupby(['Year', 'Brand']).agg({"Count": "sum", "Percentage": "sum"}).reset_index()
            csv_file_transusr_2018_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_user_2018_data.csv"
            df_agg_tranusr_2018_grp.to_csv(csv_file_transusr_2018_path, index=False)
            mycursor.execute('DROP TABLE IF EXISTS aggregate_user_2018')
            mycursor.execute('CREATE TABLE aggregate_user_2018(Year INT, Brand varchar(30), Count BIGINT, Percentage DECIMAL(20,2))')
            engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
            df_agg_tranusr_2018_grp.to_sql('aggregate_user_2018', con=engine, if_exists = 'append', index=False)



            brands_agg_trans_2019 = []
            counts_agg_trans_2019 = []
            percentages_agg_trans_2019 = []
            year_agg_trans_2019 = []
            for foldername, subfolders, filenames in os.walk(agg_trans_user_2019_pt):
                year = os.path.basename(foldername)
            for json_file in json_agg_trans_user_2019:
                with open(json_file, 'r') as file:
                    data_2019 = json.load(file)
                    usersByDevice = data_2019['data']['usersByDevice']
                    if usersByDevice is None:
                        # st.write(f"There is no data{year}")
                        continue
                    for entry in usersByDevice:
                        year_agg_trans_2019.append(year)
                        brands_agg_trans_2019.append(entry['brand'])
                        counts_agg_trans_2019.append(entry["count"])
                        percentages_agg_trans_2019.append(entry["percentage"])
            df_agg_trans_2019 = pd.DataFrame({
                "Year": year_agg_trans_2019,
                "Brand": brands_agg_trans_2019,
                "Count": counts_agg_trans_2019,
                "Percentage": percentages_agg_trans_2019
                }) 
            df_agg_tranusr_2019_grp = df_agg_trans_2019.groupby(['Year', 'Brand']).agg({"Count": "sum", "Percentage": "sum"}).reset_index()
            csv_file_transusr_2019_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_user_2019_data.csv"
            df_agg_tranusr_2019_grp.to_csv(csv_file_transusr_2019_path, index=False)
            mycursor.execute('DROP TABLE IF EXISTS aggregate_user_2019')
            mycursor.execute('CREATE TABLE aggregate_user_2019(Year INT, Brand varchar(30), Count BIGINT, Percentage DECIMAL(20,2))')
            engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
            df_agg_tranusr_2019_grp.to_sql('aggregate_user_2019', con=engine, if_exists = 'append', index=False)


            brands_agg_trans_2020 = []
            counts_agg_trans_2020 = []
            percentages_agg_trans_2020 = []
            year_agg_trans_2020 = []
            for foldername, subfolders, filenames in os.walk(agg_trans_user_2020_pt):
                year = os.path.basename(foldername)
            for json_file in json_agg_trans_user_2020:
                with open(json_file, 'r') as file:
                    data_2020 = json.load(file)
                    usersByDevice = data_2020['data']['usersByDevice']
                    if usersByDevice is None:
                        # st.write(f"There is no data{year}")
                        continue
                    for entry in usersByDevice:
                        year_agg_trans_2020.append(year)
                        brands_agg_trans_2020.append(entry['brand'])
                        counts_agg_trans_2020.append(entry["count"])
                        percentages_agg_trans_2020.append(entry["percentage"])
            df_agg_trans_2020 = pd.DataFrame({
                "Year": year_agg_trans_2020,
                "Brand": brands_agg_trans_2020,
                "Count": counts_agg_trans_2020,
                "Percentage": percentages_agg_trans_2020
                })
            df_agg_tranusr_2020_grp = df_agg_trans_2020.groupby(['Year', 'Brand']).agg({"Count": "sum", "Percentage": "sum"}).reset_index()
            csv_file_transusr_2020_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_user_2020_data.csv"
            df_agg_tranusr_2020_grp.to_csv(csv_file_transusr_2020_path, index=False)
            mycursor.execute('DROP TABLE IF EXISTS aggregate_user_2020')
            mycursor.execute('CREATE TABLE aggregate_user_2020(Year INT, Brand varchar(30), Count BIGINT, Percentage DECIMAL(20,2))')
            engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
            df_agg_tranusr_2020_grp.to_sql('aggregate_user_2020', con=engine, if_exists = 'append', index=False)


            brands_agg_trans_2021 = []
            counts_agg_trans_2021 = []
            percentages_agg_trans_2021 = []
            year_agg_trans_2021 = []
            for foldername, subfolders, filenames in os.walk(agg_trans_user_2021_pt):
                year = os.path.basename(foldername)
            for json_file in json_agg_trans_user_2021:
                with open(json_file, 'r') as file:
                    data_2021 = json.load(file)
                    usersByDevice = data_2021['data']['usersByDevice']
                    if usersByDevice is None:
                        # st.write(f"There is no data{year}")
                        continue
                    for entry in usersByDevice:
                        year_agg_trans_2021.append(year)
                        brands_agg_trans_2021.append(entry['brand'])
                        counts_agg_trans_2021.append(entry["count"])
                        percentages_agg_trans_2021.append(entry["percentage"])
            df_agg_trans_2021 = pd.DataFrame({
                "Year": year_agg_trans_2021,
                "Brand": brands_agg_trans_2021,
                "Count": counts_agg_trans_2021,
                "Percentage": percentages_agg_trans_2021
                })
            df_agg_tranusr_2021_grp = df_agg_trans_2021.groupby(['Year', 'Brand']).agg({"Count": "sum", "Percentage": "sum"}).reset_index()
            csv_file_transusr_2021_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_user_2021_data.csv"
            df_agg_tranusr_2021_grp.to_csv(csv_file_transusr_2021_path, index=False)
            mycursor.execute('DROP TABLE IF EXISTS aggregate_user_2021')
            mycursor.execute('CREATE TABLE aggregate_user_2021(Year INT, Brand varchar(30), Count BIGINT, Percentage DECIMAL(20,2))')
            engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
            df_agg_tranusr_2021_grp.to_sql('aggregate_user_2021', con=engine, if_exists = 'append', index=False)

            brands_agg_trans_2022 = []
            counts_agg_trans_2022 = []
            percentages_agg_trans_2022 = []
            year_agg_trans_2022 = []
            for foldername, subfolders, filenames in os.walk(agg_trans_user_2022_pt):
                year = os.path.basename(foldername)
            for json_file in json_agg_trans_user_2022:
                with open(json_file, 'r') as file:
                    data_2022 = json.load(file)
                    usersByDevice = data_2022['data']['usersByDevice']
                    if usersByDevice is None:
                        # st.write(f"There is no data{year}")
                        continue
                    for entry in usersByDevice:
                        year_agg_trans_2022.append(year)
                        brands_agg_trans_2022.append(entry['brand'])
                        counts_agg_trans_2022.append(entry["count"])
                        percentages_agg_trans_2022.append(entry["percentage"])
            df_agg_trans_2022 = pd.DataFrame({
                "Year": year_agg_trans_2022,
                "Brand": brands_agg_trans_2022,
                "Count": counts_agg_trans_2022,
                "Percentage": percentages_agg_trans_2022
                })
            df_agg_tranusr_2022_grp = df_agg_trans_2022.groupby(['Year', 'Brand']).agg({"Count": "sum", "Percentage": "sum"}).reset_index()
            csv_file_transusr_2022_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_user_2022_data.csv"
            df_agg_tranusr_2022_grp.to_csv(csv_file_transusr_2022_path, index=False)
            mycursor.execute('DROP TABLE IF EXISTS aggregate_user_2022')
            mycursor.execute('CREATE TABLE aggregate_user_2022(Year INT, Brand varchar(30), Count BIGINT, Percentage DECIMAL(20,2))')
            engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
            df_agg_tranusr_2022_grp.to_sql('aggregate_user_2022', con=engine, if_exists = 'append', index=False)


            brands_agg_trans_2023 = []
            counts_agg_trans_2023 = []
            percentages_agg_trans_2023 = []
            year_agg_trans_2023 = []
            for foldername, subfolders, filenames in os.walk(agg_trans_user_2023_pt):
                year = os.path.basename(foldername)
            for json_file in json_agg_trans_user_2023:
                with open(json_file, 'r') as file:
                    data_2023 = json.load(file)
                    usersByDevice = data_2023['data']['usersByDevice']
                    if usersByDevice is None:
                        # st.write(f"There is no data{year}")
                        continue
                    for entry in usersByDevice:
                        year_agg_trans_2023.append(year)
                        brands_agg_trans_2023.append(entry['brand'])
                        counts_agg_trans_2023.append(entry["count"])
                        percentages_agg_trans_2023.append(entry["percentage"])
            df_agg_trans_2023 = pd.DataFrame({
                "Year": year_agg_trans_2023,
                "Brand": brands_agg_trans_2023,
                "Count": counts_agg_trans_2023,
                "Percentage": percentages_agg_trans_2023
                })
            df_agg_tranusr_2023_grp = df_agg_trans_2023.groupby(['Year', 'Brand']).agg({"Count": "sum", "Percentage": "sum"}).reset_index()
            csv_file_transusr_2023_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_user_2023_data.csv"
            df_agg_tranusr_2023_grp.to_csv(csv_file_transusr_2023_path, index=False)
            mycursor.execute('DROP TABLE IF EXISTS aggregate_user_2023')
            mycursor.execute('CREATE TABLE aggregate_user_2023(Year INT, Brand varchar(30), Count BIGINT, Percentage DECIMAL(20,2))')
            engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
            df_agg_tranusr_2023_grp.to_sql('aggregate_user_2023', con=engine, if_exists = 'append', index=False)

            user_state_data = {'State': [], 'Year': [], 'Brand': [], 'Count': [], 'Percentage': []}
            for foldername, subfolders, filenames in os.walk(agg_state_user_pt):
                year = os.path.basename(foldername)
                state_name = os.path.basename(os.path.dirname(foldername))
                state_name = state_name.title()
                state_name = state_name.replace("-"," ")
                state_name = state_name.replace("&","and")
                for filename in filenames:
                    if filename.endswith('.json'):
                        file_path = os.path.join(foldername, filename)
                        with open(file_path, 'r') as file:
                            data = json.load(file)
                    users_by_device = data.get('data', {}).get('usersByDevice')
                    if users_by_device is not None:
                        for entry in users_by_device:
                            brand = entry.get('brand', '')
                            count = entry.get('count', 0)
                            percentage = entry.get('percentage', 0)
                            user_state_data['State'].append(state_name)
                            user_state_data['Year'].append(year)
                            user_state_data['Brand'].append(brand)
                            user_state_data['Count'].append(count)  
                            user_state_data['Percentage'].append(percentage)
    
            df_state_trans = pd.DataFrame(user_state_data)
            df_state_trans_grp = df_state_trans.groupby(['State', 'Year', 'Brand']).agg({'Count': 'sum', 'Percentage': 'sum'}).reset_index()
            csv_file_statetrans_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Aggregated Data/agg_user_state_data.csv"
            df_state_trans_grp.to_csv(csv_file_statetrans_path, index=False)
            mycursor.execute('DROP TABLE IF EXISTS aggregate_user_state')
            mycursor.execute('CREATE TABLE aggregate_user_state(State varchar(100), Year INT, Brand varchar(30), Count BIGINT, Percentage DECIMAL(20,2))')
            engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
            df_state_trans_grp.to_sql('aggregate_user_state', con=engine, if_exists = 'append', index=False)

            #Map - Transaction Data Extraction...
            map_trans_2018_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/transaction/hover/country/india/2018"
            map_trans_2019_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/transaction/hover/country/india/2019"
            map_trans_2020_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/transaction/hover/country/india/2020"
            map_trans_2021_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/transaction/hover/country/india/2021"
            map_trans_2022_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/transaction/hover/country/india/2022"
            map_trans_2023_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/transaction/hover/country/india/2023"
            map_trans_state_pt= "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/transaction/hover/country/india/state"

            json_map_trans_2018 = [os.path.join(map_trans_2018_pt, file) for file in os.listdir(map_trans_2018_pt) if file.endswith('.json')]
            json_map_trans_2019 = [os.path.join(map_trans_2019_pt, file) for file in os.listdir(map_trans_2019_pt) if file.endswith('.json')]
            json_map_trans_2020 = [os.path.join(map_trans_2020_pt, file) for file in os.listdir(map_trans_2020_pt) if file.endswith('.json')]
            json_map_trans_2021 = [os.path.join(map_trans_2021_pt, file) for file in os.listdir(map_trans_2021_pt) if file.endswith('.json')]
            json_map_trans_2022 = [os.path.join(map_trans_2022_pt, file) for file in os.listdir(map_trans_2022_pt) if file.endswith('.json')]
            json_map_trans_2023 = [os.path.join(map_trans_2023_pt, file) for file in os.listdir(map_trans_2023_pt) if file.endswith('.json')]

            st_name_2018 = []
            st_count_2018 = []
            st_amount_2018 = []
            st_year_2018 = []
            for foldername, subfolders,filenames in os.walk(map_trans_2018_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_trans_2018:
                with open(json_file, 'r') as file:
                    data_2018 = json.load(file)
                    map_transactions_2018 = data_2018['data']['hoverDataList']
                    for entry in  map_transactions_2018:
                        name = entry['name']
                        count = entry['metric'][0]['count']
                        amount = entry['metric'][0]['amount']
                        formatted_name = format_state_name(name)
                        st_name_2018.append(formatted_name)
                        st_year_2018.append(year)
                        st_count_2018.append(count)
                        st_amount_2018.append(amount)
                    df_map_trans_2018 = pd.DataFrame({
                    "State_Name": st_name_2018,
                    "Year": st_year_2018,
                    "Total_Count": st_count_2018,
                    "Amount": st_amount_2018
                    })
                    df_map_trans_grp_2018 = df_map_trans_2018.groupby(["State_Name", "Year"]).agg({"Total_Count": "sum", "Amount": "sum"}).reset_index()
                    csv_map_trans_2018_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_trans_2018_data.csv"
                    df_map_trans_grp_2018.to_csv(csv_map_trans_2018_path, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_trans_2018')
                    mycursor.execute('CREATE TABLE map_trans_2018(State_Name varchar(100), Year INT, Total_Count BIGINT, Amount DECIMAL(20,2))')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_trans_grp_2018.to_sql('map_trans_2018', con=engine, if_exists = 'append', index=False)

            st_name_2019 = []
            st_count_2019 = []
            st_amount_2019 = []
            st_year_2019 = []
            for foldername, subfolders,filenames in os.walk(map_trans_2019_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_trans_2019:
                with open(json_file, 'r') as file:
                    data_2019 = json.load(file)
                    map_transactions_2019 = data_2019['data']['hoverDataList']
                    for entry in  map_transactions_2019:
                        name = entry['name']
                        count = entry['metric'][0]['count']
                        amount = entry['metric'][0]['amount']
                        formatted_name = format_state_name(name)
                        st_name_2019.append(formatted_name)
                        st_year_2019.append(year)
                        st_count_2019.append(count)
                        st_amount_2019.append(amount)
                    df_map_trans_2019 = pd.DataFrame({
                    "State_Name": st_name_2019,
                    "Year": st_year_2019,
                    "Total_Count": st_count_2019,
                    "Amount": st_amount_2019
                    })
                    df_map_trans_grp_2019 = df_map_trans_2019.groupby(["State_Name", 'Year']).agg({"Total_Count": "sum", "Amount": "sum"}).reset_index()
                    csv_map_trans_2019_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_trans_2019_data.csv"
                    df_map_trans_grp_2019.to_csv(csv_map_trans_2019_path, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_trans_2019')
                    mycursor.execute('CREATE TABLE map_trans_2019(State_Name varchar(100), Year INT, Total_Count BIGINT, Amount DECIMAL(20,2))')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_trans_grp_2019.to_sql('map_trans_2019', con=engine, if_exists = 'append', index=False)

            st_name_2020 = []
            st_count_2020 = []
            st_amount_2020 = []
            st_year_2020 = []
            for foldername, subfolders,filenames in os.walk(map_trans_2020_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_trans_2020:
                with open(json_file, 'r') as file:
                    data_2020 = json.load(file)
                    map_transactions_2020 = data_2020['data']['hoverDataList']
                    for entry in  map_transactions_2020:
                        name = entry['name']
                        count = entry['metric'][0]['count']
                        amount = entry['metric'][0]['amount']
                        formatted_name = format_state_name(name)
                        st_name_2020.append(formatted_name)
                        st_year_2020.append(year)
                        st_count_2020.append(count)
                        st_amount_2020.append(amount)
                    df_map_trans_2020 = pd.DataFrame({
                    "State_Name": st_name_2020,
                    "Year": st_year_2020,
                    "Total_Count": st_count_2020,
                    "Amount": st_amount_2020
                    })
                    df_map_trans_grp_2020 = df_map_trans_2020.groupby(["State_Name", "Year"]).agg({"Total_Count": "sum", "Amount": "sum"}).reset_index()
                    csv_map_trans_2020_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_trans_2020_data.csv"
                    df_map_trans_grp_2020.to_csv(csv_map_trans_2020_path, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_trans_2020')
                    mycursor.execute('CREATE TABLE map_trans_2020(State_Name varchar(100), Year INT, Total_Count BIGINT, Amount DECIMAL(20,2))')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_trans_grp_2020.to_sql('map_trans_2020', con=engine, if_exists = 'append', index=False)

            st_name_2021 = []
            st_count_2021 = []
            st_amount_2021 = []
            st_year_2021 = []
            for foldername, subfolders,filenames in os.walk(map_trans_2021_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_trans_2021:
                with open(json_file, 'r') as file:
                    data_2021 = json.load(file)
                    map_transactions_2021 = data_2021['data']['hoverDataList']
                    for entry in  map_transactions_2021:
                        name = entry['name']
                        count = entry['metric'][0]['count']
                        amount = entry['metric'][0]['amount']
                        formatted_name = format_state_name(name)
                        st_name_2021.append(formatted_name)
                        st_year_2021.append(year)
                        st_count_2021.append(count)
                        st_amount_2021.append(amount)
                    df_map_trans_2021 = pd.DataFrame({
                    "State_Name": st_name_2021,
                    "Year": st_year_2021,
                    "Total_Count": st_count_2021,
                    "Amount": st_amount_2021
                    })
                    df_map_trans_grp_2021 = df_map_trans_2021.groupby(["State_Name", "Year"]).agg({"Total_Count": "sum", "Amount": "sum"}).reset_index()
                    csv_map_trans_2021_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_trans_2021_data.csv"
                    df_map_trans_grp_2021.to_csv(csv_map_trans_2021_path, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_trans_2021')
                    mycursor.execute('CREATE TABLE map_trans_2021(State_Name varchar(100), Year INT, Total_Count BIGINT, Amount DECIMAL(20,2))')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_trans_grp_2021.to_sql('map_trans_2021', con=engine, if_exists = 'append', index=False)

            st_name_2022 = []
            st_count_2022 = []
            st_amount_2022 = []
            st_year_2022 = []
            for foldername, subfolders,filenames in os.walk(map_trans_2022_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_trans_2022:
                with open(json_file, 'r') as file:
                    data_2022 = json.load(file)
                    map_transactions_2022 = data_2022['data']['hoverDataList']
                    for entry in  map_transactions_2022:
                        name = entry['name']
                        count = entry['metric'][0]['count']
                        amount = entry['metric'][0]['amount']
                        formatted_name = format_state_name(name)
                        st_name_2022.append(formatted_name)
                        st_year_2022.append(year)
                        st_count_2022.append(count)
                        st_amount_2022.append(amount)
                    df_map_trans_2022 = pd.DataFrame({
                    "State_Name": st_name_2022,
                    "Year": st_year_2022,
                    "Total_Count": st_count_2022,
                    "Amount": st_amount_2022
                    })
                    df_map_trans_grp_2022 = df_map_trans_2022.groupby(["State_Name", "Year"]).agg({"Total_Count": "sum", "Amount": "sum"}).reset_index()
                    csv_map_trans_2022_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_trans_2022_data.csv"
                    df_map_trans_grp_2022.to_csv(csv_map_trans_2022_path, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_trans_2022')
                    mycursor.execute('CREATE TABLE map_trans_2022(State_Name varchar(100), Year INT, Total_Count BIGINT, Amount DECIMAL(20,2))')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_trans_grp_2022.to_sql('map_trans_2022', con=engine, if_exists = 'append', index=False)

            st_name_2023 = []
            st_count_2023 = []
            st_amount_2023 = []
            st_year_2023 = []
            for foldername, subfolders,filenames in os.walk(map_trans_2023_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_trans_2023:
                with open(json_file, 'r') as file:
                    data_2023 = json.load(file)
                    map_transactions_2023 = data_2023['data']['hoverDataList']
                    for entry in  map_transactions_2023:
                        name = entry['name']
                        count = entry['metric'][0]['count']
                        amount = entry['metric'][0]['amount']
                        formatted_name = format_state_name(name)
                        st_name_2023.append(formatted_name)
                        st_year_2023.append(year)
                        st_count_2023.append(count)
                        st_amount_2023.append(amount)
                    df_map_trans_2023 = pd.DataFrame({
                    "State_Name": st_name_2023,
                    "Year": st_year_2023,
                    "Total_Count": st_count_2023,
                    "Amount": st_amount_2023
                    })
                    df_map_trans_grp_2023 = df_map_trans_2023.groupby(["State_Name", "Year"]).agg({"Total_Count": "sum", "Amount": "sum"}).reset_index()
                    csv_map_trans_2023_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_trans_2023_data.csv"
                    df_map_trans_grp_2023.to_csv(csv_map_trans_2023_path, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_trans_2023')
                    mycursor.execute('CREATE TABLE map_trans_2023(State_Name varchar(100), Year INT, Total_Count BIGINT, Amount DECIMAL(20,2))')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_trans_grp_2023.to_sql('map_trans_2023', con=engine, if_exists = 'append', index=False)

            
            map_trans_state_dt = {'State_Name':[], 'Year':[], 'District':[], 'Total_Count':[], 'Amount': []}
            for foldername, subfolders,filenames in os.walk(map_trans_state_pt):
                year = os.path.basename(foldername)
                statename = os.path.basename(os.path.dirname(foldername))
                statename = statename.title()
                statename = statename.replace("-"," ")
                statename = statename.replace("&","and")
                for filename in filenames:
                    if filename.endswith('.json'):
                        file_path = os.path.join(foldername, filename)
                        with open(file_path, 'r') as file:
                            data = json.load(file)
                            map_trans_dt = data['data']['hoverDataList']
                    for entry in map_trans_dt:
                        name = entry.get('name', '')
                        count = entry['metric'][0]['count']
                        amount = entry['metric'][0]['amount']
                        map_trans_state_dt['State_Name'].append(statename)
                        map_trans_state_dt['Year'].append(year)
                        map_trans_state_dt['District'].append(name)
                        map_trans_state_dt['Total_Count'].append(count)
                        map_trans_state_dt['Amount'].append(amount)
                    df_map_trans_state = pd.DataFrame(map_trans_state_dt)
                    df_map_trans_state_grp = df_map_trans_state.groupby(['State_Name', 'Year', 'District']).agg({'Total_Count': 'sum', 'Amount': 'sum'}).reset_index()
                    csv_map_trans_state_path = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_trans_state_data.csv"
                    df_map_trans_state_grp.to_csv(csv_map_trans_state_path, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_trans_state')
                    mycursor.execute('CREATE TABLE map_trans_state(State_Name varchar(100), Year INT, District varchar(100), Total_Count BIGINT, Amount DECIMAL(20,2))')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_trans_state_grp.to_sql('map_trans_state', con=engine, if_exists = 'replace', index=False)

            #Map - User Data Extraction...
            map_user_2018_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/user/hover/country/india/2018"
            map_user_2019_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/user/hover/country/india/2019"
            map_user_2020_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/user/hover/country/india/2020"
            map_user_2021_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/user/hover/country/india/2021"
            map_user_2022_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/user/hover/country/india/2022"
            map_user_2023_pt = "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/user/hover/country/india/2023"
            map_user_state_pt= "F:/Capstone Projects/PhonePe Project/phonepe_pulse/data/map/user/hover/country/india/state"

            json_map_user_2018 = [os.path.join(map_user_2018_pt, file) for file in os.listdir(map_user_2018_pt) if file.endswith('.json')]
            json_map_user_2019 = [os.path.join(map_user_2019_pt, file) for file in os.listdir(map_user_2019_pt) if file.endswith('.json')]
            json_map_user_2020 = [os.path.join(map_user_2020_pt, file) for file in os.listdir(map_user_2020_pt) if file.endswith('.json')]
            json_map_user_2021 = [os.path.join(map_user_2021_pt, file) for file in os.listdir(map_user_2021_pt) if file.endswith('.json')]
            json_map_user_2022 = [os.path.join(map_user_2022_pt, file) for file in os.listdir(map_user_2022_pt) if file.endswith('.json')] 
            json_map_user_2023 = [os.path.join(map_user_2023_pt, file) for file in os.listdir(map_user_2023_pt) if file.endswith('.json')]

            usr_state_name_2018 = []
            usr_reg_usr_2018 = []
            usr_year_2018 = []
            for foldername, subfolders,filenames in os.walk(map_user_state_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_user_2018:
                with open(json_file, 'r') as file:
                    data_usr_2018 = json.load(file)
                    map_user_2018 = data_usr_2018['data']['hoverData']
                    for state, state_data in map_user_2018.items():  
                        name = state
                        reg_count = state_data['registeredUsers']
                        formatted_name = format_state_name(name)
                        usr_state_name_2018.append(formatted_name)
                        usr_reg_usr_2018.append(reg_count)
                        usr_year_2018.append(year)
                    df_map_usr_2018 = pd.DataFrame({    
                        "State_Name": usr_state_name_2018,
                        "Year": usr_year_2018,
                        "Registered_Users": usr_reg_usr_2018
                    })
                    df_map_usr_2018_grp = df_map_usr_2018.groupby(['State_Name', 'Year']).agg({"Registered_Users": "sum"}).reset_index()
                    csv_map_usr_2018_pt = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_user_2018_data.csv"
                    df_map_usr_2018_grp.to_csv(csv_map_usr_2018_pt, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_user_2018')
                    mycursor.execute('CREATE TABLE map_user_2018(State_Name varchar(100), Year INT, Registered_Users BIGINT)')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_usr_2018_grp.to_sql('map_user_2018', con=engine, if_exists = 'append', index=False)

            usr_state_name_2019 = []
            usr_reg_usr_2019 = []
            usr_year_2019 = []
            for foldername, subfolders,filenames in os.walk(map_user_state_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_user_2019:
                with open(json_file, 'r') as file:
                    data_usr_2019 = json.load(file)
                    map_user_2019 = data_usr_2019['data']['hoverData']
                    for state, state_data in map_user_2019.items(): 
                        name = state
                        reg_count = state_data['registeredUsers']
                        formatted_name = format_state_name(name)
                        usr_state_name_2019.append(formatted_name)
                        usr_reg_usr_2019.append(reg_count)
                        usr_year_2019.append(year)

                    df_map_usr_2019 = pd.DataFrame({
                        "State_Name": usr_state_name_2019,
                        "Year": usr_year_2019,
                        "Registered_Users": usr_reg_usr_2019
                    })
                    df_map_usr_2019_grp = df_map_usr_2019.groupby(['State_Name', 'Year']).agg({"Registered_Users": "sum"}).reset_index()
                    csv_map_usr_2019_pt = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_user_2019_data.csv"
                    df_map_usr_2019_grp.to_csv(csv_map_usr_2019_pt, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_user_2019')
                    mycursor.execute('CREATE TABLE map_user_2019(State_Name varchar(100), Year INT, Registered_Users BIGINT)')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_usr_2019_grp.to_sql('map_user_2019', con=engine, if_exists = 'append', index=False)

            usr_state_name_2020 = []
            usr_reg_usr_2020 = []
            usr_year_2020 = []
            for foldername, subfolders,filenames in os.walk(map_user_state_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_user_2020:
                with open(json_file, 'r') as file:
                    data_usr_2020 = json.load(file)
                    map_user_2020 = data_usr_2020['data']['hoverData']
                    for state, state_data in map_user_2020.items():  
                        name = state
                        reg_count = state_data['registeredUsers']
                        formatted_name = format_state_name(name)
                        usr_state_name_2020.append(formatted_name)
                        usr_reg_usr_2020.append(reg_count)
                        usr_year_2020.append(year)

                    df_map_usr_2020 = pd.DataFrame({
                        "State_Name": usr_state_name_2020,
                        "Year": usr_year_2020,
                        "Registered_Users": usr_reg_usr_2020
                    })
                    df_map_usr_2020_grp = df_map_usr_2020.groupby(['State_Name', 'Year']).agg({"Registered_Users": "sum"}).reset_index()
                    csv_map_usr_2020_pt = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_user_2020_data.csv"
                    df_map_usr_2020_grp.to_csv(csv_map_usr_2020_pt, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_user_2020')
                    mycursor.execute('CREATE TABLE map_user_2020(State_Name varchar(100), Year INT, Registered_Users BIGINT)')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_usr_2020_grp.to_sql('map_user_2020', con=engine, if_exists = 'append', index=False)

            usr_state_name_2021 = []
            usr_reg_usr_2021 = []
            usr_year_2021 = []
            for foldername, subfolders,filenames in os.walk(map_user_state_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_user_2021:
                with open(json_file, 'r') as file:
                    data_usr_2021 = json.load(file)
                    map_user_2021 = data_usr_2021['data']['hoverData']
                    for state, state_data in map_user_2021.items(): 
                        name = state
                        reg_count = state_data['registeredUsers']
                        formatted_name = format_state_name(name)
                        usr_state_name_2021.append(formatted_name)
                        usr_reg_usr_2021.append(reg_count)
                        usr_year_2021.append(year)                        

                    df_map_usr_2021 = pd.DataFrame({
                        "State_Name": usr_state_name_2021,
                        "Year": usr_year_2021,
                        "Registered_Users": usr_reg_usr_2021
                    })
                    df_map_usr_2021_grp = df_map_usr_2021.groupby(['State_Name', 'Year']).agg({"Registered_Users": "sum"}).reset_index()
                    csv_map_usr_2021_pt = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_user_2021_data.csv"
                    df_map_usr_2021_grp.to_csv(csv_map_usr_2021_pt, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_user_2021')
                    mycursor.execute('CREATE TABLE map_user_2021(State_Name varchar(100), Year INT, Registered_Users BIGINT)')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_usr_2021_grp.to_sql('map_user_2021', con=engine, if_exists = 'append', index=False)

            usr_state_name_2022 = []
            usr_reg_usr_2022 = []
            usr_year_2022 = []
            for foldername, subfolders,filenames in os.walk(map_user_state_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_user_2022:
                with open(json_file, 'r') as file:
                    data_usr_2022 = json.load(file)
                    map_user_2022 = data_usr_2022['data']['hoverData']
                    for state, state_data in map_user_2022.items():  
                        name = state
                        reg_count = state_data['registeredUsers']
                        formatted_name = format_state_name(name)
                        usr_state_name_2022.append(formatted_name)
                        usr_reg_usr_2022.append(reg_count)
                        usr_year_2022.append(year)

                    df_map_usr_2022 = pd.DataFrame({
                        "State_Name": usr_state_name_2022,
                        "Year": usr_year_2022,
                        "Registered_Users": usr_reg_usr_2022
                    })
                    df_map_usr_2022_grp = df_map_usr_2022.groupby(['State_Name', 'Year']).agg({"Registered_Users": "sum"}).reset_index()
                    csv_map_usr_2022_pt = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_user_2022_data.csv"
                    df_map_usr_2022_grp.to_csv(csv_map_usr_2022_pt, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_user_2022')
                    mycursor.execute('CREATE TABLE map_user_2022(State_Name varchar(100), Year INT, Registered_Users BIGINT)')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_usr_2022_grp.to_sql('map_user_2022', con=engine, if_exists = 'append', index=False)

            usr_state_name_2023 = []
            usr_reg_usr_2023 = []
            usr_year_2023 = []
            for foldername, subfolders,filenames in os.walk(map_user_state_pt):
                year = os.path.basename(foldername)
            for json_file in json_map_user_2023:
                with open(json_file, 'r') as file:
                    data_usr_2023 = json.load(file)
                    map_user_2023 = data_usr_2023['data']['hoverData']
                    for state, state_data in map_user_2023.items():  
                        name = state
                        reg_count = state_data['registeredUsers']
                        formatted_name = format_state_name(name)
                        usr_state_name_2023.append(formatted_name)
                        usr_reg_usr_2023.append(reg_count)
                        usr_year_2023.append(year)

                    df_map_usr_2023 = pd.DataFrame({
                        "State_Name": usr_state_name_2023,
                        "Year": usr_year_2023,
                        "Registered_Users": usr_reg_usr_2023
                    })
                    df_map_usr_2023_grp = df_map_usr_2023.groupby(['State_Name','Year']).agg({"Registered_Users": "sum"}).reset_index()
                    csv_map_usr_2023_pt = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_user_2023_data.csv"
                    df_map_usr_2023_grp.to_csv(csv_map_usr_2023_pt, index=False)
                    mycursor.execute('DROP TABLE IF EXISTS map_user_2023')
                    mycursor.execute('CREATE TABLE map_user_2023(State_Name varchar(100),Year INT, Registered_Users BIGINT)')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_usr_2023_grp.to_sql('map_user_2023', con=engine, if_exists = 'append', index=False)


            map_user_state_dt = {'State_Name':[], 'Year':[], 'District':[], 'Total_Count':[]}
            for foldername, subfolders,filenames in os.walk(map_user_state_pt):
                year = os.path.basename(foldername)
                mapstatename = os.path.basename(os.path.dirname(foldername))
                mapstatename = mapstatename.title()
                mapstatename = mapstatename.replace("-"," ")
                mapstatename = mapstatename.replace("&","and")
                for filename in filenames:
                    if filename.endswith('.json'):
                        file_path = os.path.join(foldername, filename)
                        with open(file_path, 'r') as file:
                            data = json.load(file)
                            map_user_dt = data['data']['hoverData']
                    for district, district_data in map_user_dt.items():
                        name = district
                        count = district_data.get('registeredUsers',0)
                        map_user_state_dt['State_Name'].append(mapstatename)  
                        map_user_state_dt['Year'].append(year)
                        map_user_state_dt['District'].append(name)
                        map_user_state_dt['Total_Count'].append(count)

                    df_map_user_state = pd.DataFrame(map_user_state_dt)
                    df_map_user_state_grp = df_map_user_state.groupby(['State_Name', 'Year', 'District',]).agg({'Total_Count': 'sum'}).reset_index()
                    csv_map_user_state_pt = "F:/Capstone Projects/PhonePe Project/CSV Files/Map Data/map_user_state_data.csv"
                    df_map_user_state_grp.to_csv(csv_map_user_state_pt, index = False)
                    mycursor.execute('DROP TABLE IF EXISTS map_user_state')
                    mycursor.execute('CREATE TABLE map_user_state(State_Name varchar(100), Year INT, District varchar(100), Total_Count BIGINT)')
                    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                    df_map_user_state_grp.to_sql('map_user_state', con=engine, if_exists = 'append', index=False)
            st.write('Data has been extracted')
            st.write('Extracted Data has been saved as CSV')
            st.write('Data has been Inserted into MySQL DB')

with tab2:
    st.write('Aggregated Data Visualization')
    drpdown1 = st.selectbox('Year',('Select Option','2018','2019','2020','2021','2022','2023'))
    drpdown3 = st.selectbox("Select the Plot Graph",('Select Option', 'Bar','Pie'))
    col1, col2 = st.columns(2, gap = "small")
    engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
    
    with col1:
        st.write('Aggregated - Transaction Data Visualization')
        drpdown2 = st.selectbox('Plot Based On', ('Select Option', 'Transaction Count','Amount'))
        
        if drpdown1 != 'Select Option':
            query = f"SELECT * FROM aggregate_trans_{drpdown1}"
            df = pd.read_sql(query, engine)
            df.rename(columns={'Count': 'Transaction_Count', 'Amount': 'Transaction_Amount'}, inplace=True)
            if drpdown2 == 'Transaction Count':
                if drpdown3 == 'Bar':
                    fig = px.bar(df, x='Transaction_Count', y='Transaction_Name', color='Transaction_Amount',
                                labels={'Transaction_Name': 'Transaction Name', 'Transaction_Count': 'Transaction Count'},
                                title='Aggregated Transactions Count', height=500)
                    st.plotly_chart(fig)
                elif drpdown3 == 'Pie':
                    fig = px.pie(df, values='Transaction_Count', names='Transaction_Name', color='Transaction_Amount',
                                labels={'Transaction_Name': 'Transaction Name', 'Transaction_Count': 'Transaction Count'},
                                title='Aggregated Transactions Count', height=500)
                    st.plotly_chart(fig)

            elif drpdown2 == 'Amount':
                if drpdown3 == 'Bar':
                    fig = px.bar(df, x='Transaction_Amount', y='Transaction_Name', color='Transaction_Count',
                                labels={'Transaction_Name': 'Transaction Name', 'Transaction_Amount': 'Transaction Amount'},
                                title='Aggregated Transactions Amount', height=500)

                    st.plotly_chart(fig)
                elif drpdown3 == 'Pie':
                    fig = px.pie(df, values='Transaction_Amount', names='Transaction_Name', color='Transaction_Count',
                                labels={'Transaction_Name': 'Transaction Name', 'Transaction_Amount': 'Transaction Amount'},
                                title='Aggregated Transactions Amount', height=500)
                    st.plotly_chart(fig)
    with col2:
        st.write('Aggregated - User Data Visualization')
        drpdown4 = st.selectbox("Plot Based on",('Select Option','Percentage','Count'))
        if drpdown1 != 'Select Option':
            query = f"SELECT * FROM aggregate_user_{drpdown1}"
            df1 = pd.read_sql(query, engine)
            if drpdown4 == 'Percentage':
                if drpdown3 == 'Bar':
                    fig = px.bar(df1, x='Percentage', y='Brand', color = 'Count',
                                labels ={'Brand':'Brand','Percentage':'Percentage'},
                                title = 'Aggregated User Percentage',  height=500)
                    st.plotly_chart(fig)
                elif drpdown3 == 'Pie':
                    fig = px.pie(df1, values='Percentage', names='Brand', color = 'Count',
                                 labels= {'Brand':'Brand','Percentage':'Percentage'},
                                 title = 'Aggregated User Percentage',height=500)
                    st.plotly_chart(fig)
            if drpdown4 == 'Count':
                if drpdown3 == 'Bar':
                    fig = px.bar(df1,  x='Brand', y='Count',color = 'Brand',
                                labels={'Count': 'Count', 'Brand': 'Brand'},
                                title='Aggregated User Data Count', height=500)
                    st.plotly_chart(fig)
                elif drpdown3 == 'Pie':
                    fig = px.pie(df1, values='Count', names='Brand', color = 'Brand',
                                 labels= {'Count': 'Count', 'Brand': 'Brand'},
                                 title = 'Aggregated User Data Count',height=500)
                    st.plotly_chart(fig)

with tab3:
    st.write("Map Data Visualization")
    geo_json_path = "F:/Capstone Projects/PhonePe Project/GeoJSON.json"
    geo_data = gpd.read_file(geo_json_path)
    drpdown5 = st.selectbox('Select a Year',('Select Option','2018','2019','2020','2021','2022','2023'))
    col1, col2 = st.columns(2, gap='medium')
    with col1:
        st.header('Map - Transaction Data Visualization')
        drpdown6 = st.selectbox('Based On',('Select an Option','Transaction_Count', 'Transaction_Amount'))

        if drpdown5 != 'Select Option':
                engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
                query = f"SELECT * FROM map_trans_{drpdown5}"
                df = pd.read_sql(query, engine)
                if drpdown6 == 'Transaction_Count':
                    merged_data = geo_data.set_index('st_nm').join(df.set_index('State_Name'))
                    fig = px.choropleth(merged_data, geojson=merged_data.geometry,locations=merged_data.index,color='Total_Count',
                        color_continuous_scale='Viridis', labels={'Total_Count':'Total Count'})
                    fig.update_geos(center={'lat':23.47,'lon':78}, scope='asia')
                    fig.update_layout(width=800,height=800)
                    st.plotly_chart(fig)
                elif drpdown6 == 'Transaction_Amount':
                    merged_data = geo_data.set_index('st_nm').join(df.set_index('State_Name'))
                    fig = px.choropleth(merged_data, geojson=merged_data.geometry,locations=merged_data.index,color='Amount',
                                        color_continuous_scale='Viridis', labels={'Amount': 'Total Amount'})
                    fig.update_geos(center={'lat':23.47,'lon':78}, scope='asia')
                    fig.update_layout(width=800,height=800)
                    st.plotly_chart(fig)

    with col2:
        st.header('Map - User Data Visualization')
        st.write('Map Transaction Data delivers the Insights of Transaction Count and Total Amount Transacted happened in each state whereas Map User Data gives us the Insight on the Registered User count in each State')
        geo_json_path = "F:/Capstone Projects/PhonePe Project/GeoJSON.json"
        geo_data = gpd.read_file(geo_json_path)
        if drpdown5 != 'Select Option':
            engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3303/phonepe_db', echo=False)
            query = f"SELECT * FROM map_user_{drpdown5}"
            df = pd.read_sql(query, engine)
            merged_data = geo_data.set_index('st_nm').join(df.set_index('State_Name'))
            fig = px.choropleth(merged_data, geojson=merged_data.geometry,locations=merged_data.index,color='Registered_Users',
                                color_continuous_scale='Viridis', labels={'Registered_Users':'Registered Users'})
            fig.update_geos(center={'lat':23.47,'lon':78}, scope='asia')
            fig.update_layout(width=800,height=800)
            st.plotly_chart(fig)

   


        




    

        



    