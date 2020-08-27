
'''
@Time   : 08-27-2020 
@Author : Zixi Chen
'''


from setuptools import setup


setup(

   name="cbapi",
   version="1.0",
   description="Crunchbase data retriever",
   author="Zixi Chen",
   packages=["cbapi"], 
   install_requires=['json', 'requests','configparser','pandas','concurrent.futures','datetime'] 

   )