package order

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.{CloudBlobClient, CloudBlobContainer, CloudBlockBlob, BlobContainerPublicAccessType}

class AzureStorageUploader {

  def uploadFileToStorage(storageConnectionString: String, containerName: String): Unit = {

    val bronzeFolderName = "bronze"
    val bronzeFileName="sourceFile.csv"
    val localFilePath = "C:/order_file/sourceFile/order_list-1(1).csv"


    val storageAccount = CloudStorageAccount.parse(storageConnectionString)
    val blobClient: CloudBlobClient = storageAccount.createCloudBlobClient
    val container: CloudBlobContainer = blobClient.getContainerReference(containerName)

    container.createIfNotExists()

    val blob: CloudBlockBlob = container.getBlockBlobReference(s"$bronzeFolderName/$bronzeFileName")
    val file = new java.io.File(localFilePath)
    blob.uploadFromFile(file.getAbsolutePath)

    println(s"Local file  uploaded to '$bronzeFolderName' folder in container '$containerName'.")
  }




}
