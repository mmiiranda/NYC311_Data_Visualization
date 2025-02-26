# This code make a HTTPS request from Cityofnewyork API 
import requests
import pandas as pd

# Variables of Search
offset=0
limit = 10

data = []

while offset < limit:
    # City Of New York API then accepts 1000 requests at a time, this logic solves this problem
    aux = min(1000, limit - offset)
    
    url = f"https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit={aux}&$offset={offset}"
    response = requests.get(url)
    
    if response.status_code == 200:
        chunk_data = response.json()  
        data.extend(chunk_data)
    else:
        print(f"Erro na requisição: {response.status_code}")

    
    offset += aux

print(data[0])

# Transform in CSV file and download
df = pd.DataFrame(data)
df.to_csv("nyc311_data.csv", index=False)