1.Applied for a Azure cloud account, got a storage accont ,container.
2.There are three folders on the container: bronze, silver, and golden, which store raw files, general cleaning files and aggregated business logic files respectively. powerbI is to get data from golden for display.
3.There are three folders under golden: last24Month, last30Days, top10MostSteadilySold, which store the corresponding business logic data.
4.AzureStorageUploader is to load the raw files to the bronze.
5.The logic is to load the raw file to the Bronze, read the raw data from the Bronze, and do the cleaning and aggregation operation to load data to the silver and golden respective
6.The main application OrdersMainApp is the entry application.
7.OrdersProcessor is used to get last24Month and last30Days aggregated data.
8.ProductsProcessor is used to get top10MostSteadilySold aggregated data.
9.EmailProcessor is for sending emails.
10.Order.pbix is the powerbI report.
