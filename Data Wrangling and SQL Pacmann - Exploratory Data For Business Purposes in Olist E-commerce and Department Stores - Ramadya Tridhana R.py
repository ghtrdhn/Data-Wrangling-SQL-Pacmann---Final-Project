#Import fungsi yang dibutuhkan
import mysql.connector 
from mysql.connector import Error
import pandas as pd
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
sns.set_theme(style="whitegrid")

host = #input host SQL
user = #input user SQL
pw = #input password SQL
db = #input datase, (menggunakan olist.db)

#fungsi koneksi ke server sql
def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: {err}")
    return connection

#fungsi koneksi ke database SQL
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db,
            auth_plugin='mysql_native_password')
        print("MySQL database connection successfull")
    except Error as err:
        print(f"Error: {err}")
    return connection

# Output koneksi ke serverd dan database
connection = create_server_connection(host, user, pw)
connection = create_db_connection(host, user, pw, db)

#Fungsi read query SQL
def read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return pd.DataFrame(result)
    except Error as err:
        print(f"Error: {err}")
        
#fungsi eksekusi query SQL
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query berhasil dieksekusi")
    except Error as err:
        print(f"Error: {err}")

# membuat koneksi ke database dan membuat object cursor
nama_host = host
user = user 
password = pw 
db = db
mydb = mysql.connector.connect(host=nama_host, user=user, passwd=password, database=db)
mycursor = mydb.cursor()


###SQL Query
##Customer Segmentation
# Question 1: Temukan asal kota customer yang melakukan order lebih dari satu kali daalam satu bulan!
query_customer_segmentation_q1 = """
SELECT
    ocd.customer_unique_id,
    ocd.customer_city,
    ocd.customer_state,
    MONTH(od.order_delivered_customer_date) AS monthly,
    YEAR(od.order_delivered_customer_date) AS yearly,
    EXTRACT(YEAR_MONTH FROM od.order_delivered_customer_date) AS yearmonth,
    COUNT(ocd.customer_id) AS amountOrdered
FROM order_dataset AS od
INNER JOIN order_customer_dataset AS ocd
    ON od.customer_id = ocd.customer_id
GROUP BY 2,4
    HAVING COUNT(od.customer_id) > 1
ORDER BY 7 DESC
;"""

#Output hasil eksekusi query SQL
pd.set_option('display.width', 1000)
customer_segmentation_q1 = pd.read_sql_query(query_customer_segmentation_q1, connection)
customer_segmentation_q1

#Fungsi merubah format data menjadi datetime
customer_segmentation_q1['yearly'] = pd.to_datetime(customer_segmentation_q1['yearly'], format = '%Y')
customer_segmentation_q1['yearmonth'] = pd.to_datetime(customer_segmentation_q1['yearmonth'], format = '%Y%m')
customer_segmentation_q1.info()

#Cek data duplikat
customer_segmentation_q1.duplicated().any()
# Tidak ada data yang duplikat


#Cek data missing value
nan_col = customer_segmentation_q1.isna().sum().sort_values(ascending = False)
nan_col
# ada 875 data missing value dalam 3 kolom


#Imputasi data missing value
mode1 = customer_segmentation_q1['yearmonth'].mode()[0]
mode2 = customer_segmentation_q1['yearly'].mode()[0]
mode3 = customer_segmentation_q1['monthly'].mode()[0]
customer_segmentation_q1['yearmonth'] = customer_segmentation_q1['yearmonth'].fillna(mode1)
customer_segmentation_q1['yearly'] = customer_segmentation_q1['yearly'].fillna(mode2)
customer_segmentation_q1['monthly'] = customer_segmentation_q1['monthly'].fillna(mode3)
print(customer_segmentation_q1['yearmonth'].isna().sum())
print('--'*10)
print(customer_segmentation_q1['yearly'].isna().sum())
print('--'*10)
print(customer_segmentation_q1['monthly'].isna().sum())
#memformat data menjadi tipe integer
customer_segmentation_q1['monthly'] = customer_segmentation_q1['monthly'].astype(int)


#Fungsi visualisasi grouped barchart
fig, ax = plt.subplots(figsize=(16, 8))
custom_palette = sns.color_palette("coolwarm",12)
# membuat plot
sns.barplot(data=customer_segmentation_q1, x="customer_city", y="amountOrdered",hue="monthly", ax=ax, palette=custom_palette)
batas_x = [-0.5, 9.5]
ax.set_xlim(batas_x)
# menambahkan judul
ax.set_title("Monthly Number of Customer Order by City", fontsize=18)
# menambahkan label
ax.set_xlabel("City", fontsize=12)
ax.set_ylabel("Number or order", fontsize=12)
plt.show()


###SQL Query
##Customer Segmentation
# Question 2: Temukan Top 10 buyer dari customer city, order delivered berdasarkan asal customer_state
query_customer_segmentation_q2 = """
SELECT
ocd.customer_city,
ocd.customer_state,
od.order_status,
COUNT(order_status) AS amountOrderDelivered,
MONTH(od.order_delivered_customer_date) AS monthly,
EXTRACT(YEAR_MONTH FROM od.order_delivered_customer_date) AS yearmonth
FROM order_customer_dataset AS ocd
INNER JOIN order_dataset AS od
USING(customer_id)
WHERE od.order_status IN ('delivered')
GROUP BY 2,3,5
ORDER BY amountOrderDelivered DESC;
"""

#Output hasil eksekusi query SQL
pd.set_option('display.width', 1000)
customer_segmentation_q2 = pd.read_sql_query(query_customer_segmentation_q2, connection)
customer_segmentation_q2

#Fungsi merubah format data menjadi datetime
customer_segmentation_q2['yearmonth'] = pd.to_datetime(customer_segmentation_q2['yearmonth'], format = '%Y%m')
customer_segmentation_q2.info()


#Cek data duplikat
print(customer_segmentation_q2.duplicated().any())
print('--'*10)
# Tidak ada data yang duplikat

#Cek data missing value
nan_col = customer_segmentation_q2.isna().sum().sort_values(ascending = False)
print(nan_col)
print('--'*10)
# ada 2 kolom yang punya missing value

#Imputasi data missing value
mode1 = customer_segmentation_q2['yearmonth'].mode()[0]
mode2 = customer_segmentation_q2['monthly'].mode()[0]
customer_segmentation_q2['yearmonth'] = customer_segmentation_q2['yearmonth'].fillna(mode1)
customer_segmentation_q2['monthly'] = customer_segmentation_q2['monthly'].fillna(mode2)
print(customer_segmentation_q2['yearmonth'].isna().sum())
print('--'*10)
print(customer_segmentation_q2['monthly'].isna().sum())

#Fungsi limitasi data
limit = customer_segmentation_q2["amountOrderDelivered"]
limit_blw300 = limit[limit <300].index
limit_blw300
df = customer_segmentation_q2.drop(limit_blw300, axis = 0)
df.head()

#Fungsi visualisasi
fig, ax = plt.subplots(figsize=(14, 20))
custom_palette = sns.color_palette("icefire",7)
# membuat plot
sns.barplot(data=df, x="amountOrderDelivered", y="customer_city", ax=ax, orient = "h",hue = 'customer_state', palette=custom_palette, dodge=False)
# menambahkan judul
ax.set_title("Amount of Successfull Delivered", fontsize=18)
# menambahkan label
ax.set_xlabel("Number of Delivered Order", fontsize=12)
ax.set_ylabel("States", fontsize=12)
plt.show()

###SQl Query
##Product Segmentation
#Question 1: Produk yang paling banyak di order berdasarkan tahun
query_product_segmentation_q1 = """
SELECT
id.order_item_id,
pd.product_category_name,
COUNT(pd.product_category_name) AS amountOrdered,
id.freight_value,
YEAR(od.order_delivered_customer_date) AS yearly,
EXTRACT(YEAR_MONTH FROM od.order_delivered_customer_date) AS yearmonth
FROM order_items_dataset AS id
JOIN products_dataset AS pd
USING (product_id)
JOIN order_dataset AS od
USING(order_id)
GROUP BY 2,5
ORDER BY 1,3 DESC;
"""

#Output hasil eksekusi query SQL
pd.set_option('display.width', 1000)
product_segmentation_q1 = pd.read_sql_query(query_product_segmentation_q1, connection)
product_segmentation_q1

#Fungsi merubah format data menjadi date
product_segmentation_q1['yearly'] = pd.to_datetime(product_segmentation_q1['yearly'], format = '%Y')
product_segmentation_q1['yearmonth'] = pd.to_datetime(product_segmentation_q1['yearmonth'], format = '%Y%m')
print(product_segmentation_q1.info())
product_segmentation_q1

#Cek data duplikat
print(product_segmentation_q1.duplicated().any())
# Tidak ada data yang duplikat
print('--'*10)

#Cek data missing value
nan_col = product_segmentation_q1.isna().sum().sort_values(ascending = False)
print(nan_col)
# ada 65 data dari 2 kolom yang punya missing value
print('--'*10)

#Imputasi data missing value
mode1 = product_segmentation_q1['yearmonth'].mode()[0]
mode2 = product_segmentation_q1['yearly'].mode()[0]
product_segmentation_q1['yearmonth'] = product_segmentation_q1['yearmonth'].fillna(mode1)
product_segmentation_q1['yearly'] = product_segmentation_q1['yearly'].fillna(mode2)
print(product_segmentation_q1['yearmonth'].isna().sum())
print('--'*10)
print(product_segmentation_q1['yearly'].isna().sum())
product_segmentation_q1

#Fungsi merubah inkonsisten format
product_segmentation_q1.product_category_name = product_segmentation_q1.product_category_name.str.replace("_", " ")

#Fungsi limitasi data
limit = product_segmentation_q1["amountOrdered"]
limit_blw300 = limit[limit <300].index
limit_blw300
df = product_segmentation_q1.drop(limit_blw300, axis = 0)
df.head()

#Fungsi visualisasi
fig, ax = plt.subplots(figsize=(20, 16))
# membuat plot
sns.barplot(data=product_segmentation_q1, x="product_category_name", y="amountOrdered",hue="yearly", ax=ax, palette=['steelblue', 'crimson', 'seagreen'], width=1)
# menambahkan judul
ax.set_title("Number of Product Ordered", fontsize=18)
# menambahkan label
ax.set_xlabel("Products", fontsize=12)
plt.xticks(rotation=90)
ax.set_ylabel("Amount of order", fontsize=12)
batas_x = [-1, 30.5]
ax.set_xlim(batas_x)
plt.show()

###SQl Query
##Product Segmentation
#Question 2: Temukan lokasi kota, dan state dari produk yang terkirim berdasarkan bulan
query_product_segmentation_q2 = """
SELECT
ocd.customer_city,
ocd.customer_state,
pd.product_category_name,
COUNT(pd.product_category_name) AS amountOrdered,
MONTH(od.order_delivered_customer_date) AS monthly,
EXTRACT(YEAR_MONTH FROM od.order_delivered_customer_date) AS yearmonth
FROM order_items_dataset AS id
JOIN products_dataset AS pd ON id.product_id = pd.product_id
JOIN order_dataset AS od ON od.order_id = id.order_id
JOIN order_customer_dataset AS ocd ON ocd.customer_id = od.customer_id
GROUP BY 2,3,5
ORDER BY 4 DESC;
"""
pd.set_option('display.width', 1000)
product_segmentation_q2 = pd.read_sql_query(query_product_segmentation_q2, connection)
product_segmentation_q2

#Fungsi merubah format data menjadi date
print(product_segmentation_q2.info())
product_segmentation_q2['yearmonth'] = pd.to_datetime(product_segmentation_q2['yearmonth'], format = '%Y%m')
print(product_segmentation_q2.info())
product_segmentation_q2

#Cek data duplikat
print(product_segmentation_q2.duplicated().any())
# Tidak ada data yang duplikat
print('--'*10)

#Cek data missing value
nan_col = product_segmentation_q2.isna().sum().sort_values(ascending = False)
print(nan_col)
# ada 478 data dari 2 kolom yang punya missing value
print('--'*10)

#Imputasi data missing value
mode = product_segmentation_q2['yearmonth'].mode()[0]
mode1 = product_segmentation_q2['monthly'].mode()[0]
product_segmentation_q2['yearmonth'] = product_segmentation_q2['yearmonth'].fillna(mode)
product_segmentation_q2['monthly'] = product_segmentation_q2['monthly'].fillna(mode)
print(product_segmentation_q2['yearmonth'].isna().sum())
print('--'*10)
print(product_segmentation_q2['monthly'].isna().sum())

#Fungsi merubah inkonsisten format
product_segmentation_q2.product_category_name = product_segmentation_q2.product_category_name.str.replace("_", " ")

#Fungsi visualisasi
fig, ax = plt.subplots(figsize=(18, 20))
custom_palette = sns.color_palette("coolwarm",27)
# membuat plot
sns.barplot(data=product_segmentation_q2, x="amountOrdered", y="product_category_name", ax=ax, hue = "customer_state", palette=custom_palette,width=1)
batas_y = [6.5, -0.5]
ax.set_ylim(batas_y)
# menambahkan judul
ax.set_title("Amount of Product Ordered by States", fontsize=18)
# menambahkan label
ax.set_xlabel("Number of Delivered Order", fontsize=12)
ax.set_ylabel("States", fontsize=12)
plt.show()

###SQL Query
##Revenue Segmentation
#Question 1: #Hitung total payment value, total price, dan total profit dari tiap kategori produk setiap tahun dan bulannya.
query_revenue_segmentation_q1 = """
WITH total_productValue AS (
SELECT
	prd.product_category_name,       
    COUNT(prd.product_category_name) AS quantityProduct,    
    SUM(id.price) AS totalPrice,
    SUM(pd.payment_value) AS totalRevenue,
    MONTH(id.shipping_limit_date) AS monthly,
    YEAR(id.shipping_limit_date) AS yearly,
    EXTRACT(YEAR_MONTH FROM id.shipping_limit_date) AS yearmonth
FROM payments_dataset AS pd
JOIN order_items_dataset AS id
USING (order_id)
JOIN order_dataset AS od
USING(order_id)
JOIN products_dataset AS prd
USING (product_id)
GROUP BY 1,5)
SELECT
	product_category_name,       
    quantityProduct,    
    totalPrice,
    totalRevenue,
    totalRevenue-totalPrice AS totalProfit,
    monthly,
    yearly,
	yearmonth
FROM total_productValue
ORDER BY totalProfit DESC;
"""
pd.set_option('display.width', 1000)
revenue_segmentation_q1 = pd.read_sql_query(query_revenue_segmentation_q1, connection)
revenue_segmentation_q1

#Fungsi merubah format data menjadi date
print(revenue_segmentation_q1.info())
revenue_segmentation_q1['yearmonth'] = pd.to_datetime(revenue_segmentation_q1['yearmonth'], format = '%Y%m')
print(revenue_segmentation_q1.info())
revenue_segmentation_q1

#Cek data duplikat
print(revenue_segmentation_q1.duplicated().any())
# Tidak ada data yang duplikat
print('--'*10)

#Cek data missing value
nan_col = revenue_segmentation_q1.isna().sum().sort_values(ascending = False)
print(nan_col)
# Tidak ada kolom yang punya missing value
print('--'*10)

#Fungsi merubah inkonsisten format
revenue_segmentation_q1.product_category_name = revenue_segmentation_q1.product_category_name.str.replace("_", " ")

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah price, revenue, dan profit.
fig, ax = plt.subplots(ncols = 3, nrows=1, figsize = (20, 16),sharex = True, sharey = True)
# Menambahkan title pada figure
fig.suptitle('Boxplot of Total Price, Total Revenue, and Total Profit', fontsize = 20)
custom_palette = sns.color_palette("coolwarm")
# Buat boxplot dengan seaborn untuk total price
sns.boxplot(data = revenue_segmentation_q1,
            x = "totalPrice",
            y = "product_category_name",
            ax = ax[0], palette=custom_palette)
ax[0].set_title("Boxplot Total Price", fontsize=18)
ax[0].set_xlabel("Total Price", fontsize=12)
# Buat boxplot dengan seaborn untuk total revenue
sns.boxplot(data = revenue_segmentation_q1,
            x = "totalRevenue",
            y = "product_category_name",
            ax = ax[1], palette=custom_palette)
ax[1].set_title("Boxplot Total Revenue", fontsize=18)
ax[1].set_xlabel("Total Revenue", fontsize=12)
# Buat boxplot dengan seaborn untuk total profit
sns.boxplot(data = revenue_segmentation_q1,
            x = "totalProfit",
            y = "product_category_name",
            ax = ax[2], palette=custom_palette)
ax[2].set_title("Boxplot Total Profit", fontsize=18)
ax[2].set_xlabel("Total Profit", fontsize=12)
plt.show()

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah total price, total revenue, dan total profit.
fig, ax = plt.subplots(ncols = 1, nrows=3, figsize = (14, 8),sharex = True, sharey = True)
# Menambahkan title pada figure
fig.suptitle('Histogram of Price, Revenue, and Total Profit', fontsize = 20)
# Plot histogram untuk total price
sns.histplot(data = revenue_segmentation_q1, 
             x = "totalPrice", 
             ax = ax[0],
             bins=100,
             stat="density")
ax[0].set_title("Total Price")
ax[0].set_xlabel("Total Price", fontsize=12)
# Plot histogram untuk total revenue
sns.histplot(data = revenue_segmentation_q1, 
             x = "totalRevenue", 
             ax = ax[1],
             bins=100,
             stat="density")
ax[1].set_title("Total Revenue")
ax[1].set_xlabel("Total Revenue", fontsize=12)
# Plot histogram untuk total profit
sns.histplot(data = revenue_segmentation_q1, 
             x = "totalProfit", 
             ax = ax[2],
             bins=100,
             stat="density")
ax[2].set_title("Total Profit")
ax[2].set_xlabel("Total Profit", fontsize=12)
plt.show()

# Deskripsi statistik dari kolom total price
print(revenue_segmentation_q1["totalPrice"].describe())
print('--'*10)
# Deskripsi statistik dari kolom total revenue
print(revenue_segmentation_q1["totalRevenue"].describe())
print('--'*10)
# Deskripsi statistik dari kolom total profit.
print(revenue_segmentation_q1["totalProfit"].describe())
# Cari Q1 & Q3 untuk masing-masing total price, total revenue, dan total profit.
Q1totalPrice = revenue_segmentation_q1.totalPrice.quantile(0.25)
Q3totalPrice = revenue_segmentation_q1.totalPrice.quantile(0.75)
print(f"Q1totalPrice : {Q1totalPrice:.2f}")
print(f"Q3totalPrice : {Q3totalPrice:.2f}")
print('--'*10)
Q1totalRevenue = revenue_segmentation_q1.totalRevenue.quantile(0.25)
Q3totalRevenue = revenue_segmentation_q1.totalRevenue.quantile(0.75)
print(f"Q1totalRevenue : {Q1totalRevenue:.2f}")
print(f"Q3totalRevenue : {Q3totalRevenue:.2f}")
print('--'*10)
Q1totalProfit = revenue_segmentation_q1.totalProfit.quantile(0.25)
Q3totalProfit = revenue_segmentation_q1.totalProfit.quantile(0.75)
print(f"Q1totalProfit : {Q1totalProfit:.2f}")
print(f"Q3totalProfit : {Q3totalProfit:.2f}")

# Cari IQR & BATAS MAXIMUM total price
IQRtotalPrice = Q3totalPrice - Q1totalPrice
max_boundtotalPrice = Q3totalPrice + 1.5*IQRtotalPrice
min_boundtotalPrice = Q1totalPrice - 1.5*IQRtotalPrice
print(f"IQRtotalPrice : {IQRtotalPrice:.2f}")
print(f"Maximum Boundary : {max_boundtotalPrice:.2f}")
print(f"Minimum Boundary : {min_boundtotalPrice: 2f}")
print('--'*10)
# Cari IQR & BATAS MAXIMUM total revenue
IQRtotalRevenue = Q3totalRevenue - Q1totalRevenue
max_boundtotalRevenue = Q3totalRevenue + 1.5*IQRtotalRevenue
min_boundtotalRevenue = Q1totalRevenue - 1.5*IQRtotalRevenue
print(f"IQRtotalRevenue : {IQRtotalRevenue:.2f}")
print(f"Maximum Boundary : {max_boundtotalRevenue:.2f}")
print(f"Minimum Boundary : {min_boundtotalRevenue: 2f}")
print('--'*10)
# Cari IQR & BATAS MAXIMUM total profit
IQRtotalProfit = Q3totalProfit - Q1totalProfit
max_boundtotalProfit = Q3totalProfit + 1.5*IQRtotalProfit
min_boundtotalProfit = Q1totalProfit - 1.5*IQRtotalProfit
print(f"IQRtotalProfit : {IQRtotalProfit:.2f}")
print(f"Maximum Boundary : {max_boundtotalProfit:.2f}")
print(f"Minimum Boundary : {min_boundtotalProfit: 2f}")

# Filter data tanpa outlier dengan lebih dari minimum boundary dan maksimum boundary
revenue_segmentation_q1 = revenue_segmentation_q1[revenue_segmentation_q1["totalPrice"] > min_boundtotalPrice]
revenue_segmentation_q1 = revenue_segmentation_q1[revenue_segmentation_q1["totalRevenue"] > min_boundtotalRevenue]
revenue_segmentation_q1 = revenue_segmentation_q1[revenue_segmentation_q1["totalProfit"] > min_boundtotalProfit]
# Filter data tanpa outlier dengan lebih dari maksimum boundary
revenue_segmentation_q1 = revenue_segmentation_q1[revenue_segmentation_q1["totalPrice"] < max_boundtotalPrice] 
revenue_segmentation_q1 = revenue_segmentation_q1[revenue_segmentation_q1["totalRevenue"] < max_boundtotalRevenue]
revenue_segmentation_q1 = revenue_segmentation_q1[revenue_segmentation_q1["totalProfit"] < max_boundtotalProfit]

# Deskripsi statistik dari kolom price setelah filter outlier
print(revenue_segmentation_q1["totalPrice"].describe())
print('--'*10)
# Deskripsi statistik dari kolom revenue setelah filter outlier
print(revenue_segmentation_q1["totalRevenue"].describe())
print('--'*10)
# Deskripsi statistik dari kolom profit setelah filter outlier
print(revenue_segmentation_q1["totalProfit"].describe())

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah total price, total revenue, dan total profit.
fig, ax = plt.subplots(ncols = 1, nrows=3, figsize = (14, 8),sharex = True, sharey = True)
# Menambahkan title pada figure
fig.suptitle('Density Plot of Price, Revenue, and Total Profit', fontsize = 20)
# Plot histogram untuk total price setelah filter oulier
sns.histplot(data = revenue_segmentation_q1, 
             x = "totalPrice", 
             ax = ax[0],
             bins=100,
             stat="density")
ax[0].set_title("Total Price")
ax[0].set_xlabel("Total Price", fontsize=12)
# Plot histogram untuk total revenue setelah filter oulier
sns.histplot(data = revenue_segmentation_q1, 
             x = "totalRevenue", 
             ax = ax[1],
             bins=100,
             stat="density")
ax[1].set_title("Total Revenue")
ax[1].set_xlabel("Total Revenue", fontsize=12)
# Plot histogram untuk total profit setelah filter oulier
sns.histplot(data = revenue_segmentation_q1, 
             x = "totalProfit", 
             ax = ax[2],
             bins=100,
             stat="density")
ax[2].set_title("Total Profit")
ax[2].set_xlabel("Total Profit", fontsize=12)
plt.show()

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah price, revenue, dan profit.
fig, ax = plt.subplots(ncols = 3, nrows=1, figsize = (20, 16),sharex = True, sharey = True)
# Menambahkan title pada figure
fig.suptitle('Boxplot After Removing Outlier', fontsize = 20)
custom_palette = sns.color_palette("coolwarm")
# Buat boxplot dengan seaborn untuk total price
sns.boxplot(data = revenue_segmentation_q1,
            x = "totalPrice",
            y = "product_category_name",
            ax = ax[0], palette=custom_palette)
ax[0].set_title("Boxplot Total Price No Outlier", fontsize=18)
ax[0].set_xlabel("Total Price", fontsize=12)
# Buat boxplot dengan seaborn untuk total revenue
sns.boxplot(data = revenue_segmentation_q1,
            x = "totalRevenue",
            y = "product_category_name",
            ax = ax[1], palette=custom_palette)
ax[1].set_title("Boxplot Total Revenue No Outlier", fontsize=18)
ax[1].set_xlabel("Total Revenue", fontsize=12)

# Buat boxplot dengan seaborn untuk total profit
sns.boxplot(data = revenue_segmentation_q1,
            x = "totalProfit",
            y = "product_category_name",
            ax = ax[2], palette=custom_palette)
ax[2].set_title("Boxplot Total Profit No Outlier", fontsize=18)
ax[2].set_xlabel("Total Profit", fontsize=12)
plt.show()

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah price, revenue, dan profit.
fig, ax = plt.subplots(ncols = 1, nrows=3, figsize = (14, 8),sharex = True)
custom_palette = sns.color_palette("icefire",3)
# Menambahkan title pada figure
fig.suptitle('Line Plot of Price, Revenue, and Total Profit', fontsize = 20)
# Buat lineplot dengan seaborn untuk total price
sns.lineplot(
    data=revenue_segmentation_q1,
    x="monthly", y="totalPrice", hue="yearly", style="yearly",palette=custom_palette, ax=ax[0],
    markers=True, dashes=False
)
# menambahkan judul
ax[0].set_title("Lineplot of Total Price", fontsize=18)
# menambahkan label
ax[0].set_xlabel("Month", fontsize=12)
ax[0].set_ylabel("Total Price", fontsize=12)
# Buat lineplot dengan seaborn untuk total revenue
sns.lineplot(
    data=revenue_segmentation_q1,
    x="monthly", y="totalRevenue", hue="yearly", style="yearly",palette=custom_palette, ax=ax[1],
    markers=True, dashes=False
)
# menambahkan judul
ax[1].set_title("Lineplot of Total Revenue", fontsize=18)
# menambahkan label
ax[1].set_xlabel("Month", fontsize=12)
ax[1].set_ylabel("Total Revenue", fontsize=12)
# Buat lineplot dengan seaborn untuk total profit
sns.lineplot(
    data=revenue_segmentation_q1,
    x="monthly", y="totalProfit", hue="yearly", style="yearly",palette=custom_palette, ax=ax[2],
    markers=True, dashes=False
)
# menambahkan judul
ax[2].set_title("Lineplot of Total Profit", fontsize=18)
# menambahkan label
ax[2].set_xlabel("Month", fontsize=12)
ax[2].set_ylabel("Total Profit", fontsize=12)
plt.show()

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah total price, total revenue, dan total profit.
fig, ax = plt.subplots(ncols = 1, nrows=3, figsize = (14, 8),sharex = True, sharey = True)
#memfilter kolom total price, kolom total revenue, dan kolom total profit menjadi log
revenue_segmentation_q1["totalPrice"] = np.log10(revenue_segmentation_q1["totalPrice"])
revenue_segmentation_q1["totalRevenue"] = np.log10(revenue_segmentation_q1["totalRevenue"])
revenue_segmentation_q1["totalProfit"] = np.log10(revenue_segmentation_q1["totalProfit"])
# Menambahkan title pada figure
fig.suptitle('Densityplot of Log of Total Price, Log of Total Revenue, and Log of Total Profit', fontsize = 20)
# Plot histogram untuk log total price
sns.histplot(data = revenue_segmentation_q1, 
             x = "totalPrice", 
             ax = ax[0],
             bins=100,
             stat="density")
ax[0].set_title("Log of Total Price")
ax[0].set_xlabel("Log of Total Price", fontsize=12)
ax[0].set_ylabel("Density", fontsize=12)
# Plot histogram untuk log total revenue
sns.histplot(data = revenue_segmentation_q1, 
             x = "totalRevenue", 
             ax = ax[1],
             bins=100,
             stat="density")
ax[1].set_title("Log of Total Revenue")
ax[1].set_xlabel("Log of Total Revenue", fontsize=12)
ax[1].set_ylabel("Density", fontsize=12)
# Plot histogram untuk log total profit
sns.histplot(data = revenue_segmentation_q1, 
             x = "totalProfit", 
             ax = ax[2],
             bins=100,
             stat="density")
ax[2].set_title("Log of Total Profit")
ax[2].set_xlabel("Log of Total Profit", fontsize=12)
ax[2].set_ylabel("Density", fontsize=12)
plt.show()

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah price, revenue, dan profit.
fig, ax = plt.subplots(ncols = 3, nrows=1, figsize = (20, 16),sharex = True, sharey = True)
# Menambahkan title pada figure
fig.suptitle('Boxplot in Log10', fontsize = 20)
custom_palette = sns.color_palette("coolwarm")
# Buat boxplot dengan seaborn untuk log total price
sns.boxplot(data = revenue_segmentation_q1,
            x = "totalPrice",
            y = "product_category_name",
            ax = ax[0], palette=custom_palette)
ax[0].set_title("Log of Boxplot Total Price", fontsize=18)
ax[0].set_xlabel("Log of Total Price", fontsize=12)
# Buat boxplot dengan seaborn untuk log total revenue
sns.boxplot(data = revenue_segmentation_q1,
            x = "totalRevenue",
            y = "product_category_name",
            ax = ax[1], palette=custom_palette)
ax[1].set_title("Log of Boxplot Total Revenue", fontsize=18)
ax[1].set_xlabel("Log of Total Revenue", fontsize=12)
# Buat boxplot dengan seaborn untuk log total profit
sns.boxplot(data = revenue_segmentation_q1,
            x = "totalProfit",
            y = "product_category_name",
            ax = ax[2], palette=custom_palette)
ax[2].set_title("Log of Boxplot Total Profit", fontsize=18)
ax[2].set_xlabel("Log of Total Profit", fontsize=12)
plt.show()

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah price, revenue, dan profit.
fig, ax = plt.subplots(ncols = 1, nrows=3, figsize = (14, 8),sharex = True)
custom_palette = sns.color_palette("icefire",3)
# Menambahkan title pada figure
fig.suptitle('Line Plot of Log Total Price, Log Total Revenue, and Log Total Profit', fontsize = 20)
# Buat lineplot dengan seaborn untuk log total price
sns.lineplot(
    data=revenue_segmentation_q1,
    x="monthly", y="totalPrice", hue="yearly", style="yearly",palette=custom_palette, ax=ax[0],
    markers=True, dashes=False
)
# menambahkan judul
ax[0].set_title("Lineplot of Log Total Price", fontsize=18)
# menambahkan label
ax[0].set_xlabel("Month", fontsize=12)
ax[0].set_ylabel("Log of Total Price", fontsize=12)
# Buat lineplot dengan seaborn untuk log total revenue
sns.lineplot(
    data=revenue_segmentation_q1,
    x="monthly", y="totalRevenue", hue="yearly", style="yearly",palette=custom_palette, ax=ax[1],
    markers=True, dashes=False
)
# menambahkan judul
ax[1].set_title("Lineplot of Log Total Revenue", fontsize=18)
# menambahkan label
ax[1].set_xlabel("Month", fontsize=12)
ax[1].set_ylabel("Log of Total Revenue", fontsize=12)
# Buat lineplot dengan seaborn untuk log total profit
sns.lineplot(
    data=revenue_segmentation_q1,
    x="monthly", y="totalProfit", hue="yearly", style="yearly",palette=custom_palette, ax=ax[2],
    markers=True, dashes=False
)
# menambahkan judul
ax[2].set_title("Lineplot of Log Total Profit", fontsize=18)
# menambahkan label
ax[2].set_xlabel("Month", fontsize=12)
ax[2].set_ylabel("Log of Total Profit", fontsize=12)
plt.show()

###SQL Query
##Revenue Segmentation
#Question 2: #Estimasi trend harian dari total profit dari top 7 produk ordered 
query_revenue_segmentation_q2 = """
WITH trend_accumulation AS (
SELECT
DATE_FORMAT((od.order_delivered_customer_date), '%Y-%m-%d')AS daily,
prd.product_category_name AS productName,
SUM(id.price) AS totalPrice,
SUM(pd.payment_value) AS totalRevenue
FROM products_dataset AS prd
JOIN order_items_dataset AS id
USING (product_id)
JOIN order_dataset AS od
USING(order_id)
JOIN payments_dataset AS pd
USING(order_id)
GROUP BY daily, productName
ORDER BY daily ASC)
SELECT
daily,
productName,
(totalRevenue - totalPrice) AS totalProfit
FROM trend_accumulation
GROUP BY daily, productName
HAVING productName IN ('bed_bath_table','health_beauty','computers_accessories','sports_leisure','furniture_decor','housewares','watches_gifts')
ORDER BY totalProfit DESC;
"""
pd.set_option('display.width', 1000)
revenue_segmentation_q2 = pd.read_sql_query(query_revenue_segmentation_q2, connection)
revenue_segmentation_q2

#Fungsi merubah format data menjadi date
print(revenue_segmentation_q2.info())
revenue_segmentation_q2['daily'] = pd.to_datetime(revenue_segmentation_q2['daily'])
print(revenue_segmentation_q2.info())
revenue_segmentation_q2

#Cek data duplikat
print(revenue_segmentation_q2.duplicated().any())
# Tidak ada data yang duplikat
print('--'*10)
#Cek data missing value
nan_col = revenue_segmentation_q2.isna().sum().sort_values(ascending = False)
print(nan_col)
# ada 7 data dari 1 kolom yang punya missing value
print('--'*10)
#Imputasi data missing value
median = revenue_segmentation_q2['daily'].median()
revenue_segmentation_q2['daily'] = revenue_segmentation_q2['daily'].fillna(median)
revenue_segmentation_q2['daily'].isna().sum()
print(revenue_segmentation_q2.info())
revenue_segmentation_q2

#Fungsi merubah inkonsisten format
revenue_segmentation_q2.productName = revenue_segmentation_q2.productName.str.replace("_", " ")

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah profit.
fig, ax = plt.subplots(figsize=(14, 7))
custom_palette = sns.color_palette("mako",7)
# Buat boxplot dengan seaborn untuk total profit
sns.boxplot(data = revenue_segmentation_q2,
            x = "productName",
            y = "totalProfit",
            ax = ax, palette=custom_palette)
# Menambahkan title pada figure
ax.set_title("Boxplot of Top 10 Product Ordered by Total Profit", fontsize=18)
plt.xticks(rotation=90)
ax.set_xlabel("Product Name", fontsize=12)
ax.set_ylabel("Total Profit", fontsize=12)
plt.show()

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah total profit.
fig, ax = plt.subplots(figsize=(14, 7))
sns.histplot(data = revenue_segmentation_q2, 
             x = "totalProfit", 
             ax = ax,
             bins=100,
             stat="density")
# Menambahkan judul
ax.set_title("Histogram of Top 10 Product Ordered by Total Profit")

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah total profit.
fig, ax = plt.subplots(figsize=(14, 8))
custom_palette = sns.color_palette("viridis",7)
sns.lineplot(
    data=revenue_segmentation_q2,
    x="daily", y="totalProfit", hue="productName", style="productName",palette=custom_palette, ax=ax, markers=False, dashes=False)
# menambahkan judul
ax.set_title("Lineplot of Top 10 Product Ordered by Total Profit", fontsize=18)
# menambahkan label
ax.set_xlabel("Daily", fontsize=12)
ax.set_ylabel("Total Profit", fontsize=12)
plt.show()

#Fungsi visualisasi

# Buat base figure & axes sesuai dengan jumlah price, revenue, dan profit.
fig, axes = plt.subplots(ncols = 3, nrows=1, figsize = (24, 22))
custom_palette = sns.color_palette("viridis",7)
for ax in axes.flatten():
    plt.sca(ax)
    plt.xticks(rotation = 90)

# Deskripsi statistik dari kolom price
print(revenue_segmentation_q2["totalProfit"].describe())
print('--'*10)

# Cari Q1 & Q3 untuk masing-masing total profit.
Q1totalProfit = revenue_segmentation_q2.totalProfit.quantile(0.25)
Q3totalProfit = revenue_segmentation_q2.totalProfit.quantile(0.75)
print(f"Q1totalProfit : {Q1totalProfit:.2f}")
print(f"Q3totalProfit : {Q3totalProfit:.2f}")

# Cari IQR & BATAS MAXIMUM total profit
IQRtotalProfit = Q3totalProfit - Q1totalProfit
max_boundtotalProfit = Q3totalProfit + 1.5*IQRtotalProfit
min_boundtotalProfit = Q1totalProfit - 1.5*IQRtotalProfit
print(f"IQRtotalProfit : {IQRtotalProfit:.2f}")
print(f"Maximum Boundary : {max_boundtotalProfit:.2f}")
print(f"Minimum Boundary : {min_boundtotalProfit: 2f}")

# Filter data tanpa outlier dengan lebih dari minimum boundary dan maksimum boundary
revenue_segmentation_q2 = revenue_segmentation_q2[revenue_segmentation_q2["totalProfit"] > min_boundtotalProfit]
# Filter data tanpa outlier dengan lebih dari maksimum boundary
revenue_segmentation_q2 = revenue_segmentation_q2[revenue_segmentation_q2["totalProfit"] < max_boundtotalProfit]

# Deskripsi statistik dari kolom profit setelah filter outlier
print(revenue_segmentation_q2["totalProfit"].describe())

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah total profit setelah filter outlier.
fig, ax = plt.subplots(figsize=(14, 7))
custom_palette = sns.color_palette("mako",7)

# Buat boxplot dengan seaborn untuk total profit setelah filter outlier
sns.boxplot(data = revenue_segmentation_q2,
            x = "productName",
            y = "totalProfit",
            ax = ax, palette=custom_palette)
# Menambahkan title pada figure
ax.set_title("Boxplot of Top 10 Product Ordered by Total Profit No Outlier", fontsize=18)
plt.xticks(rotation=90)
ax.set_xlabel("Product Name", fontsize=12)
ax.set_ylabel("Total Profit", fontsize=12)
plt.show()

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah dan total profit.
fig, ax = plt.subplots(figsize=(14, 7))
# Plot histogram untuk total profit setelah filter oulier
sns.histplot(data = revenue_segmentation_q2, 
             x = "totalProfit", 
             ax = ax,
             bins=100,
             stat="density")
ax.set_title("Histogram of Top 10 Product Ordered by Total Profit No Outlier")

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah dan total profit.
fig, ax = plt.subplots(figsize=(14, 8))
custom_palette = sns.color_palette("viridis",7)
# Buat lineplot dengan seaborn untuk total profit setelah filter outlier
sns.lineplot(
    data=revenue_segmentation_q2,
    x="daily", y="totalProfit", hue="productName", style="productName",palette=custom_palette, ax=ax, markers=False, dashes=False)
# menambahkan judul
ax.set_title("Lineplot of Top 10 Product Ordered by Total Profit No Outlier", fontsize=18)
# menambahkan label
ax.set_xlabel("Daily", fontsize=12)
ax.set_ylabel("Total Profit", fontsize=12)
plt.show()

#Membuat variable baru dan mereset index dari dataset yang telah dilakukan filterisasi outlier
revenue_segmentation_q2_filteredoutlier=revenue_segmentation_q2.reset_index()
revenue_segmentation_q2_filteredoutlier.drop(["index"], axis=1, inplace = True)
revenue_segmentation_q2_filteredoutlier

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah log total profit .
fig, ax = plt.subplots(figsize=(14, 7))
#memfilter kolom total profit menjadi log
revenue_segmentation_q2_filteredoutlier["totalProfit"] = np.log10(revenue_segmentation_q2_filteredoutlier["totalProfit"])
sns.histplot(data = revenue_segmentation_q2_filteredoutlier, 
             x = "totalProfit", 
             ax = ax,
             bins=100,
             stat="density")
# Menambahkan judul
ax.set_title("Histogram of Top 10 Product Ordered by Log Total Profit")

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah total price, total revenue, dan total profit.
fig, ax = plt.subplots(figsize=(14, 7))
custom_palette = sns.color_palette("mako",7)
# Buat boxplot dengan seaborn untuk log total profit
sns.boxplot(data = revenue_segmentation_q2_filteredoutlier,
            x = "productName",
            y = "totalProfit",
            ax = ax, palette=custom_palette)
ax.set_title("Boxplot of Top 10 Product Ordered by Log Total Profit", fontsize=18)
plt.xticks(rotation=90)
ax.set_xlabel("Product Name", fontsize=12)
ax.set_ylabel("Total Profit", fontsize=12)
plt.show()

#Fungsi visualisasi
# Buat base figure & axes sesuai dengan jumlah price, revenue, dan profit.
fig, ax = plt.subplots(figsize=(14, 8))
custom_palette = sns.color_palette("viridis",7)
# Buat lineplot dengan seaborn untuk log total profit
sns.lineplot(
    data=revenue_segmentation_q2_filteredoutlier,
    x="daily", y="totalProfit", hue="productName", style="productName",palette=custom_palette, ax=ax, markers=False, dashes=False)
# menambahkan judul
ax.set_title("Lineplot of Top 10 Product Ordered by Log Total Profit", fontsize=18)
# menambahkan label
ax.set_xlabel("Daily", fontsize=12)
ax.set_ylabel("Total Profit", fontsize=12)
plt.show()