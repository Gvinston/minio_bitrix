Библиотека позволяет подключить s3 minio в bitrix

Для подключения в init.php вставьте:

$eventManager = \Bitrix\Main\EventManage::getInstance();

$eventManager->addEventHandler(
'clouds',
'OnGetStorageService', [
'\Gvinston\Storage\CCloudStorageServiceMinio',
'GetObjectInstance',
]
);