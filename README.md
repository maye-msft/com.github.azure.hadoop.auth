# MSIBasedAccessTokenProvider

This Repo is to provide a custom authentication provider for Azure Data Lake Storage Gen2 (ADLS Gen2) for Apache Hadoop, which is base on  [MSI](https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview). The default MSI authentication provider "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider" has an issue  caused by the [rate limiting](https://learn.microsoft.com/en-us/azure/virtual-machines/windows/instance-metadata-service?tabs=windows#rate-limiting) of Azure Instance Metadata Service.

So the custom AccessToken Provider "com.github.azure.hadoop.custom.auth.MSIBasedAccessTokenProvider" is to add retry when the request get [HTTP 429 error](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429).

The retry count can be configured by the property "fs.azure.custom.token.fetch.retry.count" in core-site.xml. The default retry count is 3.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>Custom</value>
  <description>
  Custom Authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>com.github.azure.hadoop.custom.auth.MSIBasedAccessTokenProvider</value>
  <description>
  classname of Custom Authentication Provider
  </description>
</property>
<property>
  <name>fs.azure.custom.token.fetch.retry.count</name>
  <value>10</value>
</property>
```

The config of MSI is applicable to this custom authentication provider.

```xml
<property>
  <name>fs.azure.account.oauth2.msi.tenant</name>
  <value></value>
  <description>
  Optional MSI Tenant ID
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.msi.endpoint</name>
  <value></value>
  <description>
   Optional MSI endpoint
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value></value>
  <description>
  Optional Client ID
  </description>
</property>
```

## Demo

Run a Java Demo, run this command in Azure VM with MSI enabled.

```bash
mvn exec:java -Dexec.mainClass="com.github.azure.hadoop.custom.auth.DFSCustomMSIApp" -Dexec.args="wasbs://<container-name>@<storage-account>.blob.core.windows.net/<file-name>"
```

## Deploy

Build the jar file and copy it to the "$HADOOP_HOME/share/hadoop/tools/lib",

```bash
mvn clean package
cp bin/com.github.azure.hadoop.custom.auth-1.0.jar $HADOOP_HOME/share/hadoop/tools/lib
```

Add the following configuration to core-site.xml.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>Custom</value>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>com.github.azure.hadoop.custom.auth.MSIBasedAccessTokenProvider</value>
</property>
<property>
  <name>fs.azure.custom.token.fetch.retry.count</name>
  <value>10</value>
</property>
```

Run hadoop command to test.

```bash
hadoop fs -ls abfss://<container-name>@<storage-account>.blob.core.windows.net/<file-name>
```
