# MacShop

<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/f838f1af-ebc0-4d91-8684-18b4199564a7" />
1. 從 PTT Macshop 版進行爬蟲，使用 Redis 避免存取重複資料，將有更新的文章寫入 PostgreSQL，形成 raw data。
2. 將 raw data 運用 Pyspark 擷取出細節資訊，如產品品項，價格，顏色，容量等，存回 PostgreSQL 形成 Data Warehouse.
3. 運用 DBT 從 Data Warehouse 整理出每日交易量，平均成交價格等資訊，存回 PostgreSQL 形成 Data Mart.
4. 上傳 Data Mart 資訊至 Google Sheet 儲存
5. Looker Studio 串接 Goolge Sheet 每 15分鐘更新資料，製作視覺化儀表板

<a href="https://lookerstudio.google.com/reporting/354ecdfc-95b0-47cc-8d26-65cbc7fd7338">
  <img width="1033" height="674" alt="Loooker Dashboard" src="https://github.com/user-attachments/assets/be8526b6-5e33-4b18-89cc-357dcf1e3f4c" />
</a>
