
'''
Crunchbase data downloader

@File   : cbapi.py
@Time   : 08-27-2020
@Author : Zixi Chen

'''

import requests
import json
import concurrent.futures
import pandas as pd
from datetime import datetime, date, time, timedelta, timezone


def get_data(key, client_settings, method):

    '''
    The function is used to get the previous day's organizations or people data from Crunchbase.
    The inputs should be a string (rapidapi key), a dictionary (client_settings) and a string (method).
    In client_settings, the value of key "objects" should be either "organizations" or "people";
                        the value of key "sort_order_setted" should be either "DESC" or "ASC";
                        the value of key "items_per_page_setted" should be an int.
    The method should be either "threads", which means using multithreading to get all the data.
                        or "generator", which means using generator to generate data by pages.

    '''

    assert method == "threads" or method == "generator", "method must be either threads or generator."


    # Define a function for triggering Crunchbase API
    def trigger_api(rapid_api_key, settings,since_time):   #settings is a dic that contains clients settings
    
        # Different url and querystrings for different settings
        url = "https://crunchbase-crunchbase-v1.p.rapidapi.com/odm-{}".format(settings["object"])
        querystring = {"updated_since":str(since_time), 
                       "sort_order":"created_at {}".format(settings["sort_order_setted"]),
                       "items_per_page":settings["items_per_page_setted"],
                       "page":1} 
    
        # Get the first page of data. (Then get the number of pages to iterate)
        headers = { 'x-rapidapi-host': "crunchbase-crunchbase-v1.p.rapidapi.com",
                    'x-rapidapi-key': rapid_api_key}
        response1 = requests.request("GET", url, headers=headers, params=querystring)
        api_response1 = json.loads(response1.text)   #json form with initial attributes
    
        # Integrate the first page data Into Dataframe
        if settings["object"] == "organizations":
            data = pd.DataFrame(columns = ["Name","Homepage", "Update Timestamp"])     
            for org in api_response1["data"]["items"]:
                org_name   = org["properties"]["name"]
                org_url    = org["properties"]["homepage_url"]
                org_update = str(org["properties"]["updated_at"])
                # print("Adding Company: " + org_name)
                a = {"Name":org_name, "Homepage": org_url, "Update Timestamp": org_update}
                data = data.append(a, ignore_index = True)
            # print(data.head())

        else:
            data = pd.DataFrame(columns = ["First Name","Last Name", "Gender", "Title", "Country", "Profile", "Linkedin", "Update Timestamp"])     
            for people in api_response1["data"]["items"]:
                people_first_name   = people["properties"]["first_name"]
                people_last_name    = people["properties"]["last_name"]
                people_gender  = people["properties"]["gender"]
                people_title  = people["properties"]["title"]
                people_country  = people["properties"]["country_code"]
                people_profile  = people["properties"]["profile_image_url"]
                people_linkedin  = people["properties"]["linkedin_url"]
                people_update = str(people["properties"]["updated_at"])
                # print("Adding People: " + people_name)
                a = {"First Name":people_first_name, "Last Name": people_last_name, 
                     "Gender":people_gender, "Title":people_title, 
                     "Country":people_country, "Profile":people_profile, 
                     "Linkedin":people_linkedin, "Update Timestamp": people_update}
                data = data.append(a, ignore_index = True)
            # print(data.head())

        
        pages = api_response1["data"]["paging"]["number_of_pages"]
        print("Pages: {}".format(pages))
    
        if(200 == response1.status_code):
            return [data,pages]    #return a list
        else:
            return None


    # Get the rest of pages of data and put in the df (used for multithreading)
    def get_rest_pages(rapid_api_key, settings, since_time, i):  
    
        for i in range(i*10+2,(i+1)*10+2):     # 10 pages for each thread  
            url = "https://crunchbase-crunchbase-v1.p.rapidapi.com/odm-{}".format(settings["object"])
            querystring = {"updated_since":str(since_time), 
                           "sort_order":"created_at {}".format(settings["sort_order_setted"]),
                           "items_per_page":settings["items_per_page_setted"],
                           "page":i} 

            headers = { 'x-rapidapi-host': "crunchbase-crunchbase-v1.p.rapidapi.com",
                        'x-rapidapi-key': rapid_api_key}
            response = requests.request("GET", url, headers=headers, params=querystring)
            api_response = json.loads(response.text)
   
             # Integrate the rest of the pages of data Into Dataframe
            if settings["object"] == "organizations":
                data = pd.DataFrame(columns = ["Name","Homepage", "Update Timestamp"])     
                for org in api_response["data"]["items"]:
                    org_name   = org["properties"]["name"]
                    org_url    = org["properties"]["homepage_url"]
                    org_update = str(org["properties"]["updated_at"])
                    # print("Adding Company: " + org_name)
                    a = {"Name":org_name, "Homepage": org_url, "Update Timestamp": org_update}
                    data = data.append(a, ignore_index = True)
                # print(data.head())

            else:
                data = pd.DataFrame(columns = ["First Name","Last Name", "Gender", "Title", 
                                               "Country", "Profile", "Linkedin", "Update Timestamp"])     
                for people in api_response["data"]["items"]:
                    people_first_name   = people["properties"]["first_name"]
                    people_last_name    = people["properties"]["last_name"]
                    people_gender  = people["properties"]["gender"]
                    people_title  = people["properties"]["title"]
                    people_country  = people["properties"]["country_code"]
                    people_profile  = people["properties"]["profile_image_url"]
                    people_linkedin  = people["properties"]["linkedin_url"]
                    people_update = str(people["properties"]["updated_at"])
                    # print("Adding People: " + people_name)
                    a = {"First Name":people_first_name, "Last Name": people_last_name, 
                         "Gender":people_gender, "Title":people_title, 
                         "Country":people_country, "Profile":people_profile, 
                         "Linkedin":people_linkedin, "Update Timestamp": people_update}
                    data = data.append(a, ignore_index = True)
                # print(data.head())
        
        if(200 == response.status_code):
            return data
        else:
            return None
            

    # Get the rest of pages of data and put in the df (used for generator)
    def get_rest_pages_gen(rapid_api_key, settings,since_time, pages):  
    
        # For all the rest pages, no spliting anymore
        for i in range(2,pages+1):   
            
            url = "https://crunchbase-crunchbase-v1.p.rapidapi.com/odm-{}".format(settings["object"])
            querystring = {"updated_since":str(since_time), 
                           "sort_order": "created_at {}".format(settings["sort_order_setted"]),
                           "items_per_page":settings["items_per_page_setted"],
                           "page":i}    
            headers = { 'x-rapidapi-host': "crunchbase-crunchbase-v1.p.rapidapi.com",
                        'x-rapidapi-key': rapid_api_key}
            response = requests.request("GET", url, headers=headers, params=querystring)
            api_response = json.loads(response.text)

            # Integrate the rest of the pages of data Into Dataframe
            if settings["object"] == "organizations":
                data = pd.DataFrame(columns = ["Name","Homepage", "Update Timestamp"])     
                for org in api_response["data"]["items"]:
                    org_name   = org["properties"]["name"]
                    org_url    = org["properties"]["homepage_url"]
                    org_update = str(org["properties"]["updated_at"])
                    # print("Adding Company: " + org_name)
                    a = {"Name":org_name, "Homepage": org_url, "Update Timestamp": org_update}
                    data = data.append(a, ignore_index = True)
                # print(data.head())
            
            else:
                data = pd.DataFrame(columns = ["First Name","Last Name", "Gender", "Title", 
                                               "Country", "Profile", "Linkedin", "Update Timestamp"])     
                for people in api_response["data"]["items"]:
                    people_first_name   = people["properties"]["first_name"]
                    people_last_name    = people["properties"]["last_name"]
                    people_gender  = people["properties"]["gender"]
                    people_title  = people["properties"]["title"]
                    people_country  = people["properties"]["country_code"]
                    people_profile  = people["properties"]["profile_image_url"]
                    people_linkedin  = people["properties"]["linkedin_url"]
                    people_update = str(people["properties"]["updated_at"])
                    # print("Adding People: " + people_name)
                    a = {"First Name":people_first_name, "Last Name": people_last_name, 
                         "Gender":people_gender, "Title":people_title, 
                         "Country":people_country, "Profile":people_profile, 
                         "Linkedin":people_linkedin, "Update Timestamp": people_update}
                    data = data.append(a, ignore_index = True)
                # print(data.head())
        
            yield data


    # Method1: using multithreading
    def result_by_threads(rapid_api_key, settings):
   
        # Make sure that settings = ["object","sort_order_setted","items_per_page_setted"]
        assert settings["object"] == "organizations" or settings["object"] == "people", "objects should be either organizations or people."
        assert settings["sort_order_setted"] == "DESC" or settings["sort_order_setted"] == "ASC", "sort_order_setted should be either DESC or ASC."
        assert type(settings["items_per_page_setted"]) == int, "items_per_page_setted should be an int."
    
        try: 
            # Initialize the Script with Starting Data/Time
            current_date = datetime.combine(date.today(), time(0, 0, 0))
            yesterday_date = current_date - timedelta(days=1)
            yday_timestamp_utc = int(yesterday_date.replace(tzinfo=timezone.utc).timestamp())  # find the time in UTC
            print("Scanning Crunchbase API for company updates on " + yesterday_date.strftime("%m/%d/%YYYY"))
            # print(yday_timestamp_utc)
        
            [data,pages] = trigger_api(rapid_api_key, settings,yday_timestamp_utc)  
        
            # Use multithreading to integrate the dataframe
            with concurrent.futures.ThreadPoolExecutor() as executor:
                threads = [executor.submit(get_rest_pages,rapid_api_key,settings,yday_timestamp_utc,i) for i in range(pages//10+1)]
            for thread in threads:
                data = data.append(thread.result(), ignore_index = True)
            # print(data.count())
        
            return data
    
        except Exception as e:
            print("Major exception ... aborting")
            sys.exit(e)
    

    # Method2: using generator
    def result_by_generator(rapid_api_key, settings):
   
        # Make sure that settings = ["object","sort_order_setted","items_per_page_setted"]
        assert settings["object"] == "organizations" or settings["object"] == "people","Objects should be either organizations or people."
        assert settings["sort_order_setted"] == "DESC" or settings["sort_order_setted"] == "ASC", "sort_order_setted should be either DESC or ASC."
        assert type(settings["items_per_page_setted"]) == int, "items_per_page_setted should be an int."
    
        try:
            # Initialize the Script with Starting Data/Time
            current_date = datetime.combine(date.today(), time(0, 0, 0))
            yesterday_date = current_date - timedelta(days=1)
            yday_timestamp_utc = int(yesterday_date.replace(tzinfo=timezone.utc).timestamp())  # find the time in UTC
            print("Scanning Crunchbase API for company updates on " + yesterday_date.strftime("%m/%d/%YYYY"))
            # print(yday_timestamp_utc)
        
            [first_page,pages] = trigger_api(rapid_api_key,settings, yday_timestamp_utc)    
        
            # Use generator to integrate the dataframe
            rest_pages = get_rest_pages_gen(rapid_api_key,settings, yday_timestamp_utc, pages)
        
            return [first_page,rest_pages]  #df,gen
        
        except Exception as e:
            print("Major exception ... aborting")
            sys.exit(e)


    # Call the methods to get results
    if method == "threads":
        data = result_by_threads(key,client_settings)
        print("The result is in one dataframe.")
        return data    # a dataframe
    else:
        [first_page,rest_pages] = result_by_generator(key,client_settings)
        print("The result is in a list: [first_page,rest_pages], where rest_pages is a generator of df.")
        return [first_page,rest_pages]   # a list of df and generator of df


