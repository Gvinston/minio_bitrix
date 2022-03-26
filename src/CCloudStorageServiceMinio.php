<?

namespace Gvinston\Storage;

use \Gvinston\Storage\CCloudStorageService;
use \Aws\S3\S3Client;
use \GuzzleHttp\Exception\ClientException;
use \GuzzleHttp\Exception\ConnectException;
use \Monolog\Handler\StreamHandler;
use \Monolog\Logger;

class CCloudStorageServiceMinio extends CCloudStorageService
{
    private int $connect_timeout = 5;

    private int $timeout = 10;

    private string $policy = '{
      "Version": "2012-10-17",
      "Statement": [
        {
          "Action": [
            "s3:GetBucketLocation",
            "s3:ListBucket"
          ],
          "Effect": "Allow",
          "Principal": {
            "AWS": [
              "*"
            ]
          },
          "Resource": [
            "arn:aws:s3:::%s"
          ],
          "Sid": ""
        },
        {
          "Action": [
            "s3:GetObject"
          ],
          "Effect": "Allow",
          "Principal": {
            "AWS": [
              "*"
            ]
          },
          "Resource": [
            "arn:aws:s3:::%s/*"
          ],
          "Sid": ""
        }
      ]
    }
    ';

    public static string $fileKeyForUpload = 'to-minio-s3-';

    /**
     * @return CCloudStorageService
     */
    public function GetObject()
    {
        return new CCloudStorageServiceMinio();
    }

    /**
     * @return string
     */
    public function GetID()
    {
        return "minio_s3";
    }

    /**
     * @return string
     */
    public function GetName()
    {
        return "Minio Object Storage";
    }

    /**
     * @return array
     */
    public function GetLocationList()
    {
        return [
            '' => '',
            'us-east-1' => 'us-east-1',
        ];
    }

    /**
     * @param $arBucket
     * @param $bServiceSet
     * @param $cur_SERVICE_ID
     * @param $bVarsFromForm
     * @return string
     */
    public function GetSettingsHTML($arBucket, $bServiceSet, $cur_SERVICE_ID, $bVarsFromForm)
    {
        if ($bVarsFromForm)
            $arSettings = $_POST["SETTINGS"][$this->GetID()];
        else
            $arSettings = unserialize($arBucket["SETTINGS"]);

        if (!is_array($arSettings)) {
            $arSettings = array(
                "HOST" => "",
                "ACCESS_KEY" => "",
                "SECRET_KEY" => "",
            );
        }

        $htmlID = htmlspecialcharsbx($this->GetID());

        $result = '
		<tr id="SETTINGS_0_' . $htmlID . '" style="display:' . ($cur_SERVICE_ID === $this->GetID() || !$bServiceSet ? '' : 'none') . '" class="settings-tr adm-detail-required-field">
			<td>' . GetMessage("CLO_STORAGE_S3_EDIT_HOST") . ':</td>
			<td><input type="hidden" name="SETTINGS[' . $htmlID . '][HOST]" id="' . $htmlID . 'HOST" value="' . htmlspecialcharsbx($arSettings['HOST']) . '"><input type="text" size="55" name="' . $htmlID . 'INP_HOST" id="' . $htmlID . 'INP_HOST" value="' . htmlspecialcharsbx($arSettings['HOST']) . '" onchange="BX(\'' . $htmlID . 'HOST\').value = this.value"></td>
		</tr>
		<tr id="SETTINGS_1_' . $htmlID . '" style="display:' . ($cur_SERVICE_ID === $this->GetID() || !$bServiceSet ? '' : 'none') . '" class="settings-tr adm-detail-required-field">
			<td>' . GetMessage("CLO_STORAGE_S3_EDIT_ACCESS_KEY") . ':</td>
			<td><input type="hidden" name="SETTINGS[' . $htmlID . '][ACCESS_KEY]" id="' . $htmlID . 'ACCESS_KEY" value="' . htmlspecialcharsbx($arSettings['ACCESS_KEY']) . '"><input type="text" size="55" name="' . $htmlID . 'INP_ACCESS_KEY" id="' . $htmlID . 'INP_ACCESS_KEY" value="' . htmlspecialcharsbx($arSettings['ACCESS_KEY']) . '" onchange="BX(\'' . $htmlID . 'ACCESS_KEY\').value = this.value"></td>
		</tr>
		<tr id="SETTINGS_2_' . $htmlID . '" style="display:' . ($cur_SERVICE_ID === $this->GetID() || !$bServiceSet ? '' : 'none') . '" class="settings-tr adm-detail-required-field">
			<td>' . GetMessage("CLO_STORAGE_S3_EDIT_SECRET_KEY") . ':</td>
			<td><input type="hidden" name="SETTINGS[' . $htmlID . '][SECRET_KEY]" id="' . $htmlID . 'SECRET_KEY" value="' . htmlspecialcharsbx($arSettings['SECRET_KEY']) . '"><input type="text" size="55" name="' . $htmlID . 'INP_SECRET_KEY" id="' . $htmlID . 'INP_SECRET_KEY" value="' . htmlspecialcharsbx($arSettings['SECRET_KEY']) . '" autocomplete="off" onchange="BX(\'' . $htmlID . 'SECRET_KEY\').value = this.value"></td>
		</tr>

		';
        return $result;
    }

    /**
     * @param $arBucket
     * @param $arSettings
     * @return bool
     */
    public function CheckSettings($arBucket, &$arSettings)
    {
        global $APPLICATION;
        $aMsg =/*.(array[int][string]string).*/
            array();

        $result = array(
            "HOST" => is_array($arSettings) ? trim($arSettings["HOST"]) : '',
            "ACCESS_KEY" => is_array($arSettings) ? trim($arSettings["ACCESS_KEY"]) : '',
            "SECRET_KEY" => is_array($arSettings) ? trim($arSettings["SECRET_KEY"]) : '',
        );

        if ($result["HOST"] === '') {
            $aMsg[] = array(
                "id" => $this->GetID() . "INP_HOST",
                "text" => GetMessage("CLO_STORAGE_S3_EMPTY_HOST"),
            );
        }

        if ($result["ACCESS_KEY"] === '') {
            $aMsg[] = array(
                "id" => $this->GetID() . "INP_ACCESS_KEY",
                "text" => GetMessage("CLO_STORAGE_S3_EMPTY_ACCESS_KEY"),
            );
        }

        if ($result["SECRET_KEY"] === '') {
            $aMsg[] = array(
                "id" => $this->GetID() . "INP_SECRET_KEY",
                "text" => GetMessage("CLO_STORAGE_S3_EMPTY_SECRET_KEY"),
            );
        }

        if (!empty($aMsg)) {
            $e = new \CAdminException($aMsg);
            $APPLICATION->ThrowException($e);
            return false;
        } else {
            $arSettings = $result;
        }

        return true;
    }

    /**
     * @param $arBucket
     * @return bool
     */
    public function CreateBucket($arBucket)
    {
        try {
            $s3 = $this->getS3Client($arBucket);

            $s3->createBucket([
                'Bucket' => $arBucket['BUCKET'],
            ]);

            $s3->putBucketPolicy([
                'Bucket' => $arBucket['BUCKET'],
                'Policy' => sprintf($this->policy, $arBucket['BUCKET'], $arBucket['BUCKET']),
            ]);

            return $s3->doesBucketExist($arBucket['BUCKET']);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $arBucket
     * @return bool
     */
    public function DeleteBucket($arBucket)
    {
        try {
            $s3 = $this->getS3Client($arBucket);

            $s3->deleteBucket(['Bucket' => $arBucket['BUCKET']]);

            return !$s3->doesBucketExist($arBucket['BUCKET']);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $arBucket
     * @return bool
     */
    public function IsEmptyBucket($arBucket)
    {
        try {
            $s3 = $this->getS3Client($arBucket);

            $iterator = $s3->getIterator('ListObjects', [
                'Bucket' => $arBucket['BUCKET'],
            ]);

            $count = iterator_count($iterator);

            return empty($count);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $arBucket
     * @param $arFile
     * @return string
     */
    public function GetFileSRC($arBucket, $arFile)
    {
        try {
            if (empty($arFile)) {
                return '';
            }

            if ($this->FileExists($arBucket, $arFile)) {
                $key = $this->getKey($arFile);
                $s3 = $this->getS3Client($arBucket);

                return $s3->getObjectUrl($arBucket['BUCKET'], $key);
            }
        } catch (\Exception $e) {
            return '';
        }
    }

    /**
     * @param $arBucket
     * @param $filePath
     * @return bool
     */
    public function FileExists($arBucket, $filePath)
    {
        try {
            if (empty($filePath)) {
                return false;
            }

            $key = $this->getKey($filePath);
            if (empty($key)) {
                return false;
            }

            $s3 = $this->getS3Client($arBucket);

            $exist = $s3->doesObjectExist(
                $arBucket['BUCKET'],
                $key
            );

            return $exist;

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $arBucket
     * @param $arFile
     * @param $filePath
     * @return bool
     */
    public function FileCopy($arBucket, $arFile, $filePath)
    {
        return false;
    }

    /**
     * @param $arBucket
     * @param $arFile
     * @param $filePath
     * @return bool
     */
    public function DownloadToFile($arBucket, $arFile, $filePath)
    {
        $io = \CBXVirtualIo::GetInstance();
        $obRequest = new \CHTTP;
        $obRequest->follow_redirect = true;
        return $obRequest->Download($this->GetFileSRC($arBucket, $arFile), $io->GetPhysicalName($filePath));
    }

    /**
     * @param $arBucket
     * @param $filePath
     * @return bool
     */
    public function DeleteFile($arBucket, $filePath)
    {
        try {
            $s3 = $this->getS3Client($arBucket);
            $parsedKey = array_diff(explode('/', $filePath), ['']);

            // Удаление вместе с resize_cache
            $regex = "/(iblock\/{$parsedKey['2']})/";
            $s3->deleteMatchingObjects($arBucket['BUCKET'], '', $regex);

            return $s3->doesObjectExist($arBucket['BUCKET'], $filePath);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $arBucket
     * @param $filePath
     * @param $arFile
     * @return bool
     */
    public function SaveFile($arBucket, $filePath, $arFile)
    {
        try {
            if (!empty($this->fileKeyForUpload)) {
                if (stripos($arFile['name'], $this->fileKeyForUpload) === false) {
                    return false;
                }
            }

            if (!file_exists($arFile['tmp_name'])) {
                return false;
            }

            $s3 = $this->getS3Client($arBucket);

            $params = [
                'Bucket' => $arBucket['BUCKET'],
                'Key' => $this->getKey($filePath),
                'Body' => fopen($arFile['tmp_name'], 'r'),
                'ContentType' => $arFile['type']
            ];

            $s3->putObject($params);

            return $s3->doesObjectExist($params['Bucket'], $params['Key']);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $arBucket
     * @param $filePath
     * @param $bRecursive
     * @return array
     */
    public function ListFiles($arBucket, $filePath, $bRecursive = false)
    {
        try {
            $result = [
                "dir" => [],
                "file" => [],
                "file_size" => [],
                "file_mtime" => [],
                "file_hash" => [],
            ];
            $s3 = $this->getS3Client($arBucket);

            $iterator = $s3->getIterator('ListObjects', [
                'Bucket' => $arBucket['BUCKET'],
            ]);

            foreach ($iterator as $object) {
                $parsedKey = explode('/', $object['Key']);
                $fileName = end($parsedKey);
                $result['dir'][] = str_replace($fileName, '', $object['Key']);
                $result['file'][] = $fileName;
                $result['file_size'][] = $object['Size'];
                $result['file_mtime'][] = $object['LastModified'];
                $result['file_hash'][] = $object['ETag'];
            }

            return $result;

        } catch (\Exception $e) {
            return [];
        }
    }

    /**
     * @param $arBucket
     * @param $NS
     * @param $filePath
     * @param $fileSize
     * @param $ContentType
     * @return bool
     */
    public function InitiateMultipartUpload($arBucket, &$NS, $filePath, $fileSize, $ContentType)
    {
        return false;
    }

    /**
     * @return float
     */
    public function GetMinUploadPartSize()
    {
        return 5 * 1024 * 1024;
    }

    /**
     * @param $arBucket
     * @param $NS
     * @param $data
     * @return bool
     */
    public function UploadPart($arBucket, &$NS, $data)
    {
        return false;
    }

    /**
     * @param $arBucket
     * @param $NS
     * @return bool
     */
    public function CompleteMultipartUpload($arBucket, &$NS)
    {
        return false;
    }

    private function getS3Client(array $arBucket): S3Client
    {
        $this->checkConnection($arBucket);

        if ($arBucket['ACTIVE'] != 'Y') {
            throw new \Exception('Bucket disabled');
        }

        $s3 = new S3Client([
            'version' => 'latest',
            'region' => $arBucket['LOCATION'],
            'endpoint' => $arBucket['SETTINGS']['HOST'],
            'use_path_style_endpoint' => true,
            'credentials' => [
                'key' => $arBucket['SETTINGS']['ACCESS_KEY'],
                'secret' => $arBucket['SETTINGS']['SECRET_KEY'],
            ],
            'http' => [
                'connect_timeout' => $this->connect_timeout,
                'timeout' => $this->timeout,
            ],
        ]);

        return $s3;
    }

    private function getKey($filePath): string
    {
        $key = '';
        if (empty($filePath)) {
            $key = '';
        } else {
            if (is_array($filePath)) {
                $key = "{$filePath['SUBDIR']}/{$filePath['FILE_NAME']}";
            } else {
                $key = substr($filePath, 1);
            }
        }

        return $key;
    }

    private function checkConnection(array &$arBucket)
    {
        try {
            $client = new \GuzzleHttp\Client();

            $client->request(
                'GET',
                $arBucket["SETTINGS"]["HOST"],
                [
                    'connect_timeout' => $this->connect_timeout,
                    'timeout' => $this->timeout
                ]
            );
        } catch (ConnectException $e) {
            $log = new Logger('CCloudStorageServiceMinio::checkConnection');
            $log->pushHandler(new StreamHandler($_SERVER['DOCUMENT_ROOT'] . '/upload/logs/minio_s3/' . date('Y-m') . '.log', Logger::ERROR));
            $log->error('ConnectException:  ' . $e->getMessage());

            $this->disableCloud($arBucket);
        }
        catch (ClientException $e) {
            return;
        }
    }

    private function disableCloud(&$arBucket)
    {
        $bucket = new \CCloudStorageBucket($arBucket['ID']);
        $bucket->Update(['ACTIVE' => 'N']);
        $arBucket['ACTIVE'] = 'N';
    }
}
