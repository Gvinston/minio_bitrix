Библиотека позволяет подключить s3 minio в bitrix

Требования:
php >= 7.4

Для подключения в init.php вставьте:
<pre>
    $eventManager = \Bitrix\Main\EventManage::getInstance();
    
    $eventManager->addEventHandler(
    'clouds',
    'OnGetStorageService', [
    '\Gvinston\Storage\CCloudStorageServiceMinio',
    'GetObjectInstance',
    ]
    );
</pre>