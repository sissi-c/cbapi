# cbapi
Crunchbase data retriever

cbapi aimes to offer a reliable, threaded, and Pythonic way to retrieve previous day's organizations or people data from Crunchbase. 

## Quick start

This file serves as a packed function. By installing the package and call the function, with some customized inputs, one can easily get the organizations and people data from Crunchbase.  
  
You can choose to get either the organizations data or the people data.  
You can choose the sorting order of the data as by DESC or ASC.  
The data would be retrieved page by page, you can define the number of items shown in each page.  
To speed it up, you can also choose the format of retrieving the data between multithreading and generator.  

### Installation
  
Step 1: Download the zip file and unzip the file;  
Step 2: Open the command line/terminal, under the path of the file, run: ```python setup.py build```, then: ```python setup.py install```;  
Step 3: Now you can ```import cbapi as cp``` in your own pthon file.  

### Get data  
  
Call the function in the package by:  
```  
data = cp.get_data(settings, method)  
```  
  
The **settings** should be a dictionary with following inputs:  
1) "object": "organizations" or "people". This is the data you want to retrieve.  
2) "sort_order_setted": "DESC" or "ASC". This is the order in which you want to get your data.  
3) "items_per_page_setted": enter a integer. This is the number of items you want to retrieve in one page.  
  
The **method** should be "threads" or "generator".  
This implies the way you want to retrieve your data.   
If "threads", a multithreading way with 10 threads would be implemented to get all data.  
If "generator", you will get the data one page at a time.  

An example of inputs would be:  
```
settings = {"object":"organizations","sort_order_setted":"ASC","items_per_page_setted":80 }
method = "generator"
```  
  
The **data** would be the return of the function. Note that it can be in different type. If multithreading method is used, the data would be a single dataframe. If generator method is used, the data would be a list, with the first element a df (page1) and the second element a generator of df (rest of pages).  
  
In generator method, you can get the 2rd page by:  
```  
next(data[1])  
```  
  
### Requirements  
  
1) Python >= 2.7, 3.4+  
2) pandas  
3) requests  
4) configparser  
5) json  
6) concurrent  
7) datetime  
  
### Note  
  
This is 1.0.0 version.  
Please leave me a not if you have any feedback.  
  
***Zixi Chen***
