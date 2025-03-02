import requests
import pandas as pd
from urllib.parse import urlencode

def fetch_nyc311_data(
    total_records=2000,
    complaint_type=None,
    borough=None,
    start_date=None,
    end_date=None,
    select_cols=None
):
    
    base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    fetched_data = []
    
    where_clauses = []
    
    if complaint_type:
        where_clauses.append(f"complaint_type = '{complaint_type}'")
    if borough:
        where_clauses.append(f"borough = '{borough.upper()}'")
    if start_date and end_date:
        where_clauses.append(
            f"created_date >= '{start_date}T00:00:00' AND created_date <= '{end_date}T23:59:59'"
        )
    elif start_date:
        where_clauses.append(f"created_date >= '{start_date}T00:00:00'")
    elif end_date:
        where_clauses.append(f"created_date <= '{end_date}T23:59:59'")
    
    where_query = " AND ".join(where_clauses) if where_clauses else None
    
    select_query = ", ".join(select_cols) if select_cols else None
    
    offset = 0
    batch_size = 1000
    registros_buscados = 0
    
    while registros_buscados < total_records:
        remaining = total_records - registros_buscados
        limit = min(batch_size, remaining)
        
        params = {
            "$limit": limit,
            "$offset": offset
        }
        
        if where_query:
            params["$where"] = where_query
        if select_query:
            params["$select"] = select_query
        
        query_string = urlencode(params)
        url = f"{base_url}?{query_string}"
        
        response = requests.get(url)
        if response.status_code == 200:
            chunk_data = response.json()
            if len(chunk_data) == 0:
                break
            fetched_data.extend(chunk_data)
            registros_buscados += len(chunk_data)
            offset += len(chunk_data)
        else:
            print(f"Erro na requisição (status {response.status_code}): {response.text}")
            break
    
    df = pd.DataFrame(fetched_data)
    return df
