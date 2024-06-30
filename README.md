Applied for a Azure cloud account, got a storage accont ,container.
There are three folders on the container: bronze, silver, and golden, which store raw files, general cleaning files and aggregated business logic files respectively. powerbI is to get data from golden for display.
There are three folders under golden: last24Month, last30Days, top10MostSteadilySold, which store the corresponding business logic data.
AzureStorageUploader is to load the raw files to the bronze.
The logic is to load the raw file to the Bronze, read the raw data from the Bronze, and do the cleaning and aggregation operation to load data to the silver and golden respective
The main application OrdersMainApp is the entry application.
OrdersProcessor is used to get last24Month and last30Days aggregated data.
ProductsProcessor is used to get top10MostSteadilySold aggregated data.
EmailProcessor is for sending emails.
Order.pbix is the powerbI report.
