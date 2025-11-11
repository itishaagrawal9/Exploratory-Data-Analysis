import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db



logging.basicConfig(
filename = "logs/get_vendor_summary.log",
level = logging.debug,
format = "%(asctime)s-%(levelname)s-%(message)s"
filemode = "a"
)

def create_vendor_summary (conn):
    '''this function will merge all 3 tables in 1 table to get te overall vendor summary and adding new columns in the data'''
    vendor_sales_summary = pd.read_sql_query('''WITH
    FreightSummary AS (                         
    select 
    vendornumber, sum(freight) as FreightCost
    from Vendor_Invoice
    group by vendornumber
    ),


    PurchaseSummary AS (
    select 
    p.vendornumber, 
    p.vendorname, 
    p.brand,
    p.description,
    p.purchaseprice,
    pp.volume, 
    pp.price as ActualPrice,
    sum(p.quantity) as TotalPurchaseQuantity,
    sum(p.dollars) as TotalPurchaseDollars
    From purchases p
    join purchase_prices pp
    on
    p.brand = pp.brand
    where p.purchaseprice > 0
    group by p.vendornumber, p.vendorname, p.brand, p.description, p.purchaseprice, pp.price, pp.volume
    ),


    SalesSummary AS(
    select 
    vendornumber,
    brand,
    sum(salesdollars) as TotalSalesDollars,
    sum(salesprice) as TotalSalesPrice,
    sum(salesquantity) as TotalSalesQuantity,
    sum(excisetax) as TotalExciseTax
    from sales
    group by vendornumber, brand
    )


    Select
    ps.vendornumber, 
    ps.vendorname, 
    ps.brand,
    ps.description,
    ps.purchaseprice,
    ps.ActualPrice,
    ps.volume, 
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesDollars,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.FreightCost
    FROM purchasesummary ps
    left join salessummary ss
    on ps.vendornumber = ss.vendornumber
    and ps.brand = ss.brand
    left join freightSummary fs
    on ps.vendornumber = fs.vendornumber
    order by ps.totalpurchasedollars desc''',conn)
    
    return vendor_sales_summary
    
    
def clean_data(df):
    '''This function will clean the data'''
    # Change datatype to float
    df['volume'] = df['volume'].astype('float')
    
    # Fill missing cells with fillna()
    df.fillna(0, inplace = True)
    
    # Remove unnecessary spaces from the columns
    df["vendorname"] = df["vendorname"].str.strip()
    df["description"] = df["description"].str.strip()
    
    
    # Creating new columns for better analysis
    vendor_sales_summary["Gross Profit"] = vendor_sales_summary["TotalSalesDollars"] - vendor_sales_summary["TotalPurchaseDollars"]
    vendor_sales_summary["Profit Margin"] = vendor_sales_summary["Gross Profit"] / vendor_sales_summary["TotalSalesDollars"] * 100
    vendor_sales_summary["StockTurnOver"] = vendor_sales_summary["TotalSalesQuantity"]/vendor_sales_summary["TotalPurchaseQuantity"]
    vendor_sales_summary["SalesToPurchaseRatio"] = vendor_sales_summary["TotalSalesDollars"] / vendor_sales_summary["TotalPurchaseDollars"]
    
    
    return df
    
if __name == '__main__':
    
    # Creating database connection
    conn = sqlite3.connect(r'E:\Vendor Performance Analysis\inventory.db')
    
    logging.info("Creating Vendor Summary Table......")
    summary_df = create_vendor_summary(conn)
    logging.info(summary.df.head())
    
    logging.info("Cleaning Data.....")
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())
    
    logging.info("Ingesting Data....")
    ingest_db(clean_df, "vendor_sales_summary", conn)
    logging.info("Completed")