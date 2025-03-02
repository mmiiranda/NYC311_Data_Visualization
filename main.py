from nyc_api.fetchData import fetch_nyc311_data

def main():
    df_311 = fetch_nyc311_data(
        total_records=10,
        complaint_type="Noise - Residential",
        borough="MANHATTAN",
        start_date="2023-01-01",
        end_date="2023-01-31",
        select_cols=["unique_key", "created_date", "complaint_type", "descriptor", "borough"]
    )
    
    print(df_311.head())
    print(len(df_311))

if __name__ == "__main__":
    main()