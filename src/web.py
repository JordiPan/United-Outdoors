from flask import Flask, render_template, request
import plotly.graph_objs as go
import pandas as pd
import pyodbc
import warnings
warnings.simplefilter('ignore')
from settings import settings

app = Flask(__name__)

UnitedOutdoors = pyodbc.connect("DRIVER={SQL SERVER};SERVER="+settings.servername + ";DATABASE="+settings.database+";Trusted_Connection=yes")
export_cursor = UnitedOutdoors.cursor()

order = pd.read_sql_query('SELECT * FROM order_details', UnitedOutdoors)
product = pd.read_sql_query('SELECT * FROM product', UnitedOutdoors)
employee = pd.read_sql_query('SELECT * FROM employee', UnitedOutdoors)
customer = pd.read_sql_query('SELECT * FROM customer', UnitedOutdoors)

order['TotalQuantity'] = order.groupby('product_productid')['product_quantity'].transform('sum')
order['TotalWinst'] = order['TotalQuantity'] * order['product_listprice']
order['date'] = pd.to_datetime(order['order_date'])
order['Year'] = order['date'].dt.year 

order_product = pd.merge(order,product,on="product_sk",how='left')
op = order_product.loc[:,["order_id","product_name","product_category","TotalQuantity","TotalWinst","Year"]]
opsort = op.sort_values(by="TotalQuantity", ascending=False)
unique_product_names = opsort.drop_duplicates(subset=["product_name"])
op_top10 = unique_product_names.head(10)
print(op_top10)

@app.route('/')
def index():
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=op_top10["product_name"], y=op_top10['TotalQuantity'], mode='lines+markers', name='Verkocht product'))
    graphJSON = fig.to_json()
    return render_template('index.html', graphJSON=graphJSON)

@app.route('/sort/<sort_type>')
def sort_data(sort_type):
    if sort_type == 'year':
        df_sorted = op_top10.sort_values(by='Year')
    elif sort_type == 'quantity':
        df_sorted = op_top10.sort_values(by='TotalQuantity')
    else:
        df_sorted = op_top10

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_sorted['product_name'], y=df_sorted['TotalQuantity'], mode='lines+markers', name='Sorted Data'))
    graphJSON = fig.to_json()
    return render_template('index.html', graphJSON=graphJSON)

if __name__ == '__main__':
    app.run(debug=True)