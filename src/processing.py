import pandas as pd
import pyodbc
import sqlite3
import warnings
warnings.simplefilter('ignore')
from settings import settings
from datetime import datetime
def process():
    
    # servername verander naar jouw eigen
    servername = 'LAPTOP-C1FMPSTV\\SQLEXPRESS01'

    # Northwind, Adventureworks， Aenc en datawarehouse connectie
    northwind_conn = pyodbc.connect('DRIVER={SQL SERVER};SERVER='+servername+';DATABASE=NorthWind; Trusted_Connection=yes')
    nwCursor = northwind_conn.cursor()
    adventureworks_conn = pyodbc.connect('DRIVER={SQL SERVER};SERVER='+servername+';DATABASE=AdventureWorks2019; Trusted_Connection=yes')
    awCursor = adventureworks_conn.cursor()
    aenc_fild = str(settings.processeddir /'aenc.accdb')
    aenc_conn = pyodbc.connect('DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+aenc_fild)
    aencCursor = aenc_conn.cursor()
    database = "UnitedOutdoors"
    export_conn = pyodbc.connect("DRIVER={SQL SERVER};SERVER="+servername + ";DATABASE="+database+";Trusted_Connection=yes")
    export_cursor = export_conn.cursor()

    #Product data input naar datawarehouse
    # Stap1: Producten data uit drie bedrijven extraheren.

    #Product data uit  Adventureworks bedrijf
    product = pd.read_sql_query('SELECT * FROM Production.Product', adventureworks_conn)
    ProductInventory = pd.read_sql_query('SELECT * FROM Production.ProductInventory', adventureworks_conn)
    ProductSubCategory = pd.read_sql_query('SELECT * FROM Production.ProductSubCategory', adventureworks_conn)
    ProductCategory=pd.read_sql_query('SELECT * FROM Production.ProductCategory', adventureworks_conn)
    category = pd.merge(ProductSubCategory,ProductCategory,on="ProductCategoryID",how='inner')
    product_quantity = pd.merge(product,ProductInventory,on="ProductID",how="left")
    quantity_groupby = product_quantity.groupby(["ProductID","Name","StandardCost","ListPrice"])['Quantity'].sum().reset_index()
    product_sumQuantity = pd.merge(product[["ProductID","ProductSubcategoryID","Color"]],quantity_groupby,on="ProductID",how="left")
    product_category = pd.merge(category,product_sumQuantity,on="ProductSubcategoryID",how="right")
    result = product_category.loc[:,["ProductID","Name","StandardCost","ListPrice","Quantity",'Name_x','Name_y',"Color"]]
    result = result.rename(columns={'Name_x': 'Sub_category'})
    result = result.rename(columns={'Name_y': 'Category'})
    result = result.rename(columns={'Quantity': 'UnitsInStock'})
    aw_product = result
    aw_product['Source']= "AW"
    aw_product

    #Product data uit  Northwind bedrijf
    ProductNW = pd.read_sql_query('SELECT * FROM Products', northwind_conn)
    categorynw = pd.read_sql_query('SELECT * FROM Categories', northwind_conn)
    product_category = pd.merge(ProductNW,categorynw,on="CategoryID",how="left")
    nw_product = product_category.loc[:,["ProductID","ProductName","UnitPrice","UnitsInStock","CategoryName"]]
    nw_product = nw_product.rename(columns={'ProductName':'Name'})
    nw_product = nw_product.rename(columns={'UnitPrice':'ListPrice'})
    nw_product = nw_product.rename(columns={'CategoryName':'Category'})
    nw_product.UnitsInStock = nw_product.UnitsInStock.astype(float)
    nw_product['Source']= "NW"
    nw_product

    #Product data uit  Aenc bedrijf
    ProductAenc = pd.read_sql_query('SELECT * FROM Product', aenc_conn)
    ProductAenc = ProductAenc.rename(columns={'name':'Name'})
    ProductAenc = ProductAenc.rename(columns={'id':'ProductID'})
    ProductAenc = ProductAenc.rename(columns={'color':'Color'})
    ProductAenc = ProductAenc.rename(columns={'quantity':'UnitsInStock'})
    ProductAenc = ProductAenc.rename(columns={'unit_price':'ListPrice'})
    aencProdcut = ProductAenc.loc[:,["ProductID","Name","ListPrice","UnitsInStock","Category","Color"]]
    aencProdcut.UnitsInStock=aencProdcut.UnitsInStock.astype(float)
    aencProdcut['Source']='AC'
    aencProdcut

    #Samenvoegen tot één productgegevensbestand
    datawarehouseProduct = pd.concat([aw_product,nw_product,aencProdcut],ignore_index=True)
    datawarehouseProduct = datawarehouseProduct.where(pd.notna(datawarehouseProduct), None)
    datawarehouseProduct['StandardCost'] = datawarehouseProduct['StandardCost'].fillna(0)
    datawarehouseProduct.UnitsInStock=datawarehouseProduct.UnitsInStock.astype(int)
    datawarehouseProduct.ListPrice=datawarehouseProduct.ListPrice.astype(float)
    datawarehouseProduct.ProductID=datawarehouseProduct.ProductID.astype(int)
    datawarehouseProduct.dtypes


    #Stap 2: Uploaden naar datawarehouse 
    for index, row in datawarehouseProduct.iterrows():
        try:
            query = """INSERT INTO product (
                product_productid,
                product_name, 
                product_color, 
                product_standardcost,
                product_listprice,
                product_category,
                product_subcategory,
                product_quantity,
                product_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""  
            
            values = (
                row['ProductID'],
                row['Name'],
                row['Color'],
                row['StandardCost'],
                row['ListPrice'],
                row['Category'],
                row['Sub_category'],
                row['UnitsInStock'],
                row['Source']

            )

            export_cursor.execute(query, values)

        except pyodbc.IntegrityError as e:
            continue
        except pyodbc.Error as e:
            print("Er is een fout opgetreden: ", e)
            print("Foutieve query: ", query)
        
    export_conn.commit()

    print("Product Data is succesvol geïmporteerd in de UnitedOutdoors database")


    #Customer data input naar datawarehouse
    # Stap1: Customer data uit drie bedrijven extraheren.

    #Customer adventureworks
    df_customer =  pd.read_sql_query('SELECT * FROM Sales.Customer', adventureworks_conn)
    df_company = pd.read_sql_query('SELECT * FROM Sales.Store', adventureworks_conn)
    df_person_address = pd.read_sql_query('SELECT * FROM Person.Address', adventureworks_conn)
    df_business_entity_address = pd.read_sql_query('SELECT * FROM Person.BusinessEntityAddress', adventureworks_conn)
    df_person_phone = pd.read_sql_query('SELECT * FROM Person.PersonPhone', adventureworks_conn)

    df_company = df_company.rename(columns={"BusinessEntityID":"StoreID","Name":"company_name","SalesPersonID":"BusinessEntityID"})

    aw_CC = pd.merge(df_customer[['CustomerID','StoreID','PersonID']], df_company[['BusinessEntityID','StoreID','company_name']], on='StoreID', how='left')
    aw_cc_pisna = aw_CC.loc[aw_CC['PersonID'].isna(),['CustomerID','BusinessEntityID','company_name']]
    aw_cp = aw_CC.loc[~aw_CC['PersonID'].isna(),['PersonID','CustomerID','company_name']]

    aw_cc_pisna = aw_cc_pisna.rename(columns={"BusinessEntityID":"PersonID"})
    aw_PC = pd.concat([aw_cc_pisna,aw_cp])
    aw_custoemer = pd.merge(aw_PC, df_person_phone[['BusinessEntityID','PhoneNumber']],left_on='PersonID',right_on='BusinessEntityID', how='left')
    aw_custoemer_adres = pd.merge(aw_custoemer,df_business_entity_address[['BusinessEntityID','AddressID']],on='BusinessEntityID', how='left')
    aw_ca = pd.merge(aw_custoemer_adres,df_person_address,on='AddressID', how='left')
    merged_df_aw = aw_ca

    #Aenc customer
    df_aenc_customer = pd.read_sql_query('SELECT * FROM customer', aenc_conn)
    df_aenc_customer = df_aenc_customer.rename(columns={"id":"CustomerID"})
    df_aenc_customer

    #Northwind
    df_northwind_customers = pd.read_sql_query('SELECT * FROM customers', northwind_conn)


    #Samenvoegen in Customer DF
    df_aw_customers_selected = merged_df_aw.loc[:,['CustomerID', 'AddressLine1', 'City', 'PostalCode', 'PhoneNumber', 'company_name']]
    df_northwind_customers_selected = df_northwind_customers.loc[:,['CustomerID', 'CompanyName', 'Address', 'City', 'PostalCode', 'Phone']]
    df_aenc_customer_selected = df_aenc_customer.loc[:,['CustomerID', 'address', 'city', 'zip', 'phone', 'company_name']]

    df_aw_customers_selected['Source'] = "AW"
    df_northwind_customers_selected['Source'] = "NW"
    df_aenc_customer_selected['Source'] = "AC"

    customer = pd.concat([df_northwind_customers_selected, df_aenc_customer_selected, df_aw_customers_selected], axis=0)
    customer['customer_id'] = customer['CustomerID']
    customer['customer_company_name'] = customer['company_name'].combine_first(customer['CompanyName'])
    customer['customer_address'] = customer['address'].combine_first(customer['Address']).combine_first(customer['AddressLine1'])
    customer['customer_city'] = customer['city'].combine_first(customer['City'])
    customer['customer_zip'] = customer['PostalCode'].combine_first(customer['zip'])
    customer['customer_phone'] = customer['phone'].combine_first(customer['PhoneNumber']).combine_first(customer['Phone'])
    customer['source'] = customer['Source']
    customer.drop(['CustomerID', 'CompanyName', 'Address', 'AddressLine1', 'City', 'PostalCode', 'PhoneNumber', 'Phone'], axis=1, inplace=True)
    customer = customer[['customer_id', 'customer_company_name', 'customer_address', 'customer_city', 'customer_zip', 'customer_phone','source']]
    customer=customer.where(pd.notna(customer), None)
    customer

    #Stap 2: Uploaden naar datawarehouse
    for index, row in customer.iterrows():
        try:
            query = """INSERT INTO customer (
                customer_id,
                customer_company_name, 
                customer_address, 
                customer_city,
                customer_zip,
                customer_phone,
                customer_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?)"""  
            
            values = (
                row['customer_id'],
                row['customer_company_name'],
                row['customer_address'],
                row['customer_city'],
                row['customer_zip'],
                row['customer_phone'],
                row['source']
            )

            export_cursor.execute(query, values)

        except pyodbc.IntegrityError as e:
            continue
        except pyodbc.Error as e:
            print("Er is een fout opgetreden: ", e)
            print("Foutieve query: ", query)
        
    export_cursor.commit()

    print("Customer Data is succesvol geïmporteerd in de UnitedOutdoors database")
    
    #Employee data input naar datawarehouse
    # Stap1: Employee data uit drie bedrijven extraheren.

    #Aenc
    aenctest1 = pd.read_sql_query('SELECT emp_ID, city, state, start_date, sex, birth_date FROM employee', aenc_conn)
    aenctest2 = pd.read_sql_query('SELECT state_id, country FROM state', aenc_conn)
    aencmerged_df = pd.merge(aenctest1, aenctest2, left_on= 'state', right_on= 'state_id')
    aencmerged_df.drop(columns=['state_id'], inplace=True)
    aencmerged_df = aencmerged_df.rename(columns={'emp_ID':'employeeid'})
    aencmerged_df.employeeid = aencmerged_df.employeeid.astype(int)
    aencmerged_df['Source'] = "AC"

    #AdventureWorks
    awtest1 = pd.read_sql_query('SELECT businessEntityID, JobTitle, BirthDate, Gender, HireDate FROM HumanResources.Employee', adventureworks_conn)
    awtest2 = pd.read_sql_query('SELECT BusinessEntityID, AddressID FROM Person.BusinessEntityAddress', adventureworks_conn)
    awtest3 = pd.read_sql_query('SELECT AddressID, StateProvinceID, City FROM Person.Address', adventureworks_conn)
    awtest4 = pd.read_sql_query('SELECT StateProvinceID, CountryRegionCode FROM Person.StateProvince', adventureworks_conn)

    awmerged_df1 = pd.merge(awtest1, awtest2, left_on='businessEntityID', right_on='BusinessEntityID').drop(columns=['BusinessEntityID'])
    awmerged_df2 = pd.merge(awmerged_df1, awtest3, on='AddressID')
    awmerged_df3 = pd.merge(awmerged_df2, awtest4, on='StateProvinceID')
    awmerged_df3 = awmerged_df3.rename(columns={'businessEntityID':'employeeid'})
    awmerged_df3 = awmerged_df3.rename(columns={'CountryRegionCode':'country'})

    awmerged_df3['Source'] = "AW"
    
    #NorthWind
    nwtest = pd.read_sql_query('SELECT EmployeeID, Title, HireDate, City, Region, Country, BirthDate FROM employees', northwind_conn)
    nwtest = nwtest.rename(columns={'EmployeeID':'employeeid'})
    nwtest['Source'] = "NW"
    nwtest
    
    #Samenvoegen
    employee = pd.concat([ nwtest, awmerged_df3, aencmerged_df], axis =0)
    employee['row_id'] = range(len(employee))
    employee['title'] = employee['Title'].combine_first(employee['JobTitle'])
    employee['gender'] = employee['Gender'].combine_first(employee['sex'])
    employee['city'] = employee['City'].combine_first(employee['city'])
    employee['region'] = employee['Region']
    employee['country'] = employee['Country'].combine_first(employee['country'])
    employee['hiredate'] = employee['HireDate'].combine_first(employee['start_date'])
    employee['birthdate'] = employee['BirthDate'].combine_first(employee['birth_date'])
    employee['source'] = employee['Source']
    final_columns = [
        'employeeid', 'title', 'gender', 'city', 'region', 'country', 
        'hiredate', 'birthdate', 'row_id', 'source'
    ]
    final_employee_df = employee[final_columns]
    employee = final_employee_df

    #Stap 2: Uploaden naar datawarehouse

    # Convert all date columns to a uniform datetime format
    employee['hiredate'] = pd.to_datetime(employee['hiredate'])
    employee['birthdate'] = pd.to_datetime(employee['birthdate'])
    date_format = '%d-%m-%Y'
    employee['hiredate'] = employee['hiredate'].dt.strftime(date_format)
    employee['birthdate'] = employee['birthdate'].dt.strftime(date_format)
    employee = employee.where(pd.notna(employee), None)

    #datetime

    for index, row in employee.iterrows():
        date_string = [row['hiredate'],row['birthdate']]
        for dateString in date_string:
            date_parts = dateString.split("-")
            date_obj = datetime.strptime(dateString, '%d-%m-%Y')
            date_value = date_obj.strftime('%Y-%m-%d')
            # Extract year, month, and day
            year = int(date_parts[2])
            month = int(date_parts[1])
            day = int(date_parts[0])
            try:
                query = """INSERT INTO date_table (
                    DATE_date,
                    DATE_year_nr, 
                    DATE_month_nr, 
                    DATE_day_nr
                ) VALUES (?, ?, ?, ?)"""  
                
                values = (
                    date_value, 
                    year,
                    month,
                    day  
                )

                export_cursor.execute(query, values)

            except pyodbc.IntegrityError as e:
                continue
            except pyodbc.Error as e:
                print("Er is een fout opgetreden: ", e)
                print("Foutieve query: ", query)
            
            
    export_cursor.commit()

    #data
    employee.where(pd.notna(employee), None)
    for index, row in employee.iterrows():
        date_string = row['hiredate']
        date_obj = datetime.strptime(date_string, '%d-%m-%Y')
        date_value = date_obj.strftime('%Y-%m-%d')
        date_string2 = row['birthdate']
        date_obj2 = datetime.strptime(date_string2, '%d-%m-%Y')
        date_value2 = date_obj2.strftime('%Y-%m-%d')
        try:
            query = """INSERT INTO employee (
                employee_employeeid,
                employee_title,
                employee_gender, 
                employee_city,
                employee_region,
                employee_country,
                employee_hiredate,
                employee_birthdate,
                employee_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""  
            
            values = (
                row['employeeid'],
                row['title'],
                row['gender'],
                row['city'],
                row['region'],
                row['country'],
                date_value,
                date_value2,
                row['source']
            )

            export_cursor.execute(query, values)

        except pyodbc.IntegrityError as e:
            continue
        except pyodbc.Error as e:
            print("Er is een fout opgetreden: ", e)
            print("Foutieve query: ", query)
        
    export_cursor.commit()
    print("Employee Data is succesvol geïmporteerd in de UnitedOutdoors database")


    #Order details data input naar datawarehouse
    # Stap1: Order details uit drie bedrijven extraheren.

    #Northwind orders    
    nw_order = pd.read_sql_query('SELECT * FROM Orders', northwind_conn)
    nw_orderdetails = pd.read_sql_query('SELECT * FROM [Order Details]', northwind_conn)
    nw_order_combo = pd.merge(nw_order, nw_orderdetails, on='OrderID', how='left')
    nw_order_combo_trim = nw_order_combo[['OrderID', 'Quantity', 'UnitPrice', 'ProductID', 'EmployeeID', 'CustomerID', 'OrderDate', 'ShippedDate']]
    nw_order_combo_trim = nw_order_combo_trim.rename(columns={'OrderID': 'order_id', 'Quantity': 'product_quantity', 'UnitPrice': 'product_listprice', 'ProductID': 'product_productid','EmployeeID': 'employee_employeeid', 'CustomerID': 'customer_id', 'OrderDate': 'order_date', 'ShippedDate': 'shipping_date'})
    nw_order_combo_trim['source'] = 'NW'

    # aenc orders
    aenc_order = pd.read_sql_query('SELECT * FROM sales_order', aenc_conn)
    aenc_orderdetails = pd.read_sql_query('SELECT * FROM sales_order_item', aenc_conn)
    aenc_products = pd.read_sql_query('SELECT id, unit_price FROM product', aenc_conn)
    aenc_order_combo = pd.merge(aenc_order, aenc_orderdetails, on='id', how='left')
    aenc_order_combo = pd.merge(aenc_order_combo, aenc_products, left_on='prod_id', right_on='id', how='inner')
    aenc_order_combo_trim = aenc_order_combo[['id_x', 'quantity', 'unit_price', 'prod_id', 'sales_rep', 'cust_id', 'order_date', 'ship_date']]
    aenc_order_combo_trim = aenc_order_combo_trim.rename(columns={'id_x': 'order_id', 'quantity': 'product_quantity', 'unit_price': 'product_listprice', 'prod_id': 'product_productid', 'sales_rep': 'employee_employeeid', 'cust_id': 'customer_id', 'ship_date': 'shipping_date'})
    aenc_order_combo_trim['order_date'] = pd.to_datetime(aenc_order_combo_trim['order_date'], format='%d-%b-%Y %I:%M:%S %p')
    aenc_order_combo_trim['shipping_date'] = pd.to_datetime(aenc_order_combo_trim['shipping_date'], format='%d-%b-%Y %I:%M:%S %p')
    aenc_order_combo_trim['source'] = 'AC'
    
    # adventureworks orders
    aw_order = pd.read_sql_query('SELECT * FROM Sales.SalesOrderHeader', adventureworks_conn)
    aw_orderdetails = pd.read_sql_query('SELECT * FROM Sales.SalesOrderDetail', adventureworks_conn)
    aw_order_combo = pd.merge(aw_order, aw_orderdetails, on='SalesOrderID', how='left')
    aw_order_combo_trim = aw_order_combo[['SalesOrderID', 'OrderQty', 'UnitPrice', 'ProductID', 'SalesPersonID', 'CustomerID', 'OrderDate', 'ShipDate']]
    aw_order_combo_trim = aw_order_combo_trim.rename(columns={'SalesOrderID': 'order_id', 'OrderQty': 'product_quantity', 'UnitPrice': 'product_listprice', 'ProductID': 'product_productid', 'SalesPersonID': 'employee_employeeid', 'CustomerID': 'customer_id', 'OrderDate': 'order_date', 'ShipDate': 'shipping_date'})
    aw_order_combo_trim['source'] = 'AW'

    #Samenvoegen
    aenc_en_nw_orders = pd.concat([aenc_order_combo_trim, nw_order_combo_trim])
    alle_orders = pd.concat([aenc_en_nw_orders, aw_order_combo_trim])
    alle_orders = alle_orders.where(pd.notna(alle_orders), None)
    alle_orders.shipping_date = alle_orders.shipping_date.astype(object).where(alle_orders.shipping_date.notnull(), None)

    #Stap 2: Uploaden naar datawarehouse
    #datetime 
    for index, row in alle_orders.iterrows():
        if row.shipping_date != None: 
            date_string = [row['order_date'], row['shipping_date']]
        else:
            date_string = [row['order_date']]
        for dateString in date_string:
            dateString = dateString.strftime('%Y-%m-%d')
            date_parts = dateString.split("-")
            # date_obj = datetime.strptime(dateString, '%d-%m-%Y')
            # date_value = date_obj.strftime('%Y-%m-%d')
            # Extract year, month, and day
            year = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            try:
                query = """INSERT INTO date_table (
                    DATE_date,
                    DATE_year_nr, 
                    DATE_month_nr, 
                    DATE_day_nr
                ) VALUES (?, ?, ?, ?)"""

                values = (
                    dateString, 
                    year,
                    month,
                    day
                )

                export_conn.execute(query, values)

            except pyodbc.IntegrityError as e:
                continue
            except pyodbc.Error as e:
                print("Er is een fout opgetreden: ", e)
                print("Foutieve query: ", query)

    export_conn.commit()

    #data
    for index, row in alle_orders.iterrows():
        try:
            query = """INSERT INTO order_details (
                order_id,
                product_quantity, 
                product_listprice,
                order_date,
                shipping_date,
                product_productid,
                employee_employeeid,
                customer_id,
                source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""  
            
            values = (
                row['order_id'],
                row['product_quantity'],
                row['product_listprice'],
                row['order_date'],
                row['shipping_date'],
                row['product_productid'],
                row['employee_employeeid'],
                row['customer_id'],
                row['source'],
            )

            export_cursor.execute(query, values)

        except pyodbc.IntegrityError as e:
            continue
        except pyodbc.Error as e:
            print("row: ", row)
            print("Er is een fout opgetreden: ", e)
            print("Foutieve query: ", query)
        
    export_conn.commit()
    print("Order details Data is succesvol geïmporteerd in de UnitedOutdoors database")