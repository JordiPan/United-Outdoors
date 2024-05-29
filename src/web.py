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
years = op['Year'].unique()

def tekenGrafiek(arr):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=arr["product_name"], y=arr['TotalWinst'], mode='lines+markers', name='profit product',yaxis='y2'))
    fig.add_trace(go.Bar(x=arr["product_name"], y=arr['TotalQuantity'], name='Sold product',yaxis='y1'))
    fig.update_layout(
        yaxis=dict(
            title='Total Quantity',
            side='left' 
        ),
        yaxis2=dict(
            title='Total Winst',
            overlaying='y',
            side='right'
        )
    )
    graphJSON = fig.to_json()
    return graphJSON

def tekenPie(arr):
    category_counts = arr['product_category'].value_counts().nlargest(10)
    labels = category_counts.index.tolist()
    values = category_counts.values.tolist()

    print("Labels:", labels)
    print("Values:", values)
    fig_pie = go.Figure(data=[go.Pie(labels=labels, values=values)])
    graphJSON_pie = fig_pie.to_json()
    return graphJSON_pie

@app.route('/')
def index():
    graphJSON = tekenGrafiek(op_top10)
    return render_template('index.html', graphJSON=graphJSON, years =years)

@app.route('/sort/<sort_type>')
def sort_data(sort_type):
    if sort_type == 'quantity':
        df_sorted = op_top10.sort_values(by='TotalQuantity')
    elif sort_type == 'winst':
        df_sorted = op_top10.sort_values(by='TotalWinst')
    else:
        df_sorted = op_top10

    graphJSON = tekenGrafiek(df_sorted)
    graphJSON_pie = tekenPie(op)
    return render_template('index.html', graphJSON=graphJSON, years =years )

@app.route('/filter', methods=['POST'])
def filter_data():
    selected_year = request.form.get('year')
    graphName = request.form.get('graph')
    filtered_data = op[op['Year'] == int(selected_year)]
    if graphName == 'pie':
        graphJSON_pie = tekenPie(filtered_data)
        return render_template('pie.html', years =years, selected_year=selected_year,graphJSON_pie=graphJSON_pie)
    else:
        data=filtered_data.sort_values(by="TotalQuantity", ascending=False)
        data_top10 = data.drop_duplicates(subset=["product_name"]).head(10)
        graphJSON = tekenGrafiek(data_top10)
        return render_template('index.html', graphJSON=graphJSON,  years =years, selected_year=selected_year)


@app.route('/graphe_pie')
def piePagina():
    graphJSON_pie = tekenPie(op)
    return render_template('pie.html',  years =years, graphJSON_pie=graphJSON_pie)

if __name__ == '__main__':
    app.run(debug=True)