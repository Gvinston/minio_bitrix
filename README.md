Библиотека позволяет подключить s3 minio в bitrix

Требования:
php >= 7.4

Установка:
composer require gvinston/minio_bitrix dev-master

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

Чтобы в облако загружать файлы из определенного инфоблока, нужно в названия файлов
добавить строку "to-minio-s3-". Лучше всего это сделать через событие "OnBeforeIBlockElementUpdate".

Если облако будет недоступно, то оно отключится автоматически, что позволит сайту работать и дальше.
Логи о недоступности облака запишутся по пути от корня сайта: "/upload/logs/minio_s3/"

 
