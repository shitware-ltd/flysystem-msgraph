# flysystem-msgraph
A flysystem 3.0 adapter for Sharepoint 365 / OneDrive using Microsoft Graph API with support for uploading large files

# Installation
```composer require shitware-ltd/flysystem-msgraph```

# Usage
Instantiate the adapter by passing in an instance of `\Microsoft\Graph\Graph` and the DriveId of the drive you want to use. 

The third optional parameter allows you to override the timeout and chunk size values which are used for uploading large files (writeStream).


You must set the access token on the graph instance before using the adapter. A guide on acquiring an access token is available at https://github.com/microsoftgraph/msgraph-sdk-php#readme

```php
$graph = new \Microsoft\Graph\Graph();
$graph->setAccessToken($your_access_token);
$adapter = new \ShitwareLtd\FlysystemMsGraph\Adapter($graph, $your_drive_id);
```

# FAQ
Q: Do we really need another flysystem adapter?

A: I couldn't find one for flysystem 3.0 that implemented `writeStream` properly. This adapter allows you to upload files larger than your memory buffer.

Q: How do I find my drive ID?

A: [Out of scope for this project.](https://learn.microsoft.com/en-us/answers/questions/730575/how-to-find-site-id-and-drive-id-for-graph-api)

Q: What happens if my access token expires?

A: The adapter stops working.

