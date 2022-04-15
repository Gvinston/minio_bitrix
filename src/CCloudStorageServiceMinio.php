<?

namespace Gvinston\Storage;

use \Aws\S3\S3Client;
use \GuzzleHttp\Exception\ClientException;
use \GuzzleHttp\Exception\ConnectException;
use \Monolog\Handler\StreamHandler;
use \Monolog\Logger;

class CCloudStorageServiceMinio extends \CCloudStorageService
{
    private int $connectTimeout = 5;

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
     * @param $bucket
     * @param $bServiceSet
     * @param $curServiceId
     * @param $bVarsFromForm
     * @return string
     */
    public function GetSettingsHTML($bucket, $bServiceSet, $curServiceId, $bVarsFromForm)
    {
        if ($bVarsFromForm)
            $settings = $_POST["SETTINGS"][$this->GetID()];
        else
            $settings = unserialize($bucket["SETTINGS"]);

        if (!is_array($settings)) {
            $settings = array(
                "HOST" => "",
                "ACCESS_KEY" => "",
                "SECRET_KEY" => "",
            );
        }

        $htmlId = htmlspecialcharsbx($this->GetID());

        $result = '
		<tr id="SETTINGS_0_' . $htmlId . '" style="display:' . ($curServiceId === $this->GetID() || !$bServiceSet ? '' : 'none') . '" class="settings-tr adm-detail-required-field">
			<td>' . GetMessage("CLO_STORAGE_S3_EDIT_HOST") . ':</td>
			<td><input type="hidden" name="SETTINGS[' . $htmlId . '][HOST]" id="' . $htmlId . 'HOST" value="' . htmlspecialcharsbx($settings['HOST']) . '"><input type="text" size="55" name="' . $htmlId . 'INP_HOST" id="' . $htmlId . 'INP_HOST" value="' . htmlspecialcharsbx($settings['HOST']) . '" onchange="BX(\'' . $htmlId . 'HOST\').value = this.value"></td>
		</tr>
		<tr id="SETTINGS_1_' . $htmlId . '" style="display:' . ($curServiceId === $this->GetID() || !$bServiceSet ? '' : 'none') . '" class="settings-tr adm-detail-required-field">
			<td>' . GetMessage("CLO_STORAGE_S3_EDIT_ACCESS_KEY") . ':</td>
			<td><input type="hidden" name="SETTINGS[' . $htmlId . '][ACCESS_KEY]" id="' . $htmlId . 'ACCESS_KEY" value="' . htmlspecialcharsbx($settings['ACCESS_KEY']) . '"><input type="text" size="55" name="' . $htmlId . 'INP_ACCESS_KEY" id="' . $htmlId . 'INP_ACCESS_KEY" value="' . htmlspecialcharsbx($settings['ACCESS_KEY']) . '" onchange="BX(\'' . $htmlId . 'ACCESS_KEY\').value = this.value"></td>
		</tr>
		<tr id="SETTINGS_2_' . $htmlId . '" style="display:' . ($curServiceId === $this->GetID() || !$bServiceSet ? '' : 'none') . '" class="settings-tr adm-detail-required-field">
			<td>' . GetMessage("CLO_STORAGE_S3_EDIT_SECRET_KEY") . ':</td>
			<td><input type="hidden" name="SETTINGS[' . $htmlId . '][SECRET_KEY]" id="' . $htmlId . 'SECRET_KEY" value="' . htmlspecialcharsbx($settings['SECRET_KEY']) . '"><input type="text" size="55" name="' . $htmlId . 'INP_SECRET_KEY" id="' . $htmlId . 'INP_SECRET_KEY" value="' . htmlspecialcharsbx($settings['SECRET_KEY']) . '" autocomplete="off" onchange="BX(\'' . $htmlId . 'SECRET_KEY\').value = this.value"></td>
		</tr>

		';
        return $result;
    }

    /**
     * @param $bucket
     * @param $settings
     * @return bool
     */
    public function CheckSettings($bucket, &$settings)
    {
        global $APPLICATION;
        $aMsg =/*.(array[int][string]string).*/
            array();

        $result = array(
            "HOST" => is_array($settings) ? trim($settings["HOST"]) : '',
            "ACCESS_KEY" => is_array($settings) ? trim($settings["ACCESS_KEY"]) : '',
            "SECRET_KEY" => is_array($settings) ? trim($settings["SECRET_KEY"]) : '',
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
            $settings = $result;
        }

        return true;
    }

    /**
     * @param $bucket
     * @return bool
     */
    public function CreateBucket($bucket)
    {
        try {
            $s3 = $this->getS3Client($bucket);

            $s3->createBucket([
                'Bucket' => $bucket['BUCKET'],
            ]);

            $s3->putBucketPolicy([
                'Bucket' => $bucket['BUCKET'],
                'Policy' => sprintf($this->policy, $bucket['BUCKET'], $bucket['BUCKET']),
            ]);

            return $s3->doesBucketExist($bucket['BUCKET']);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $bucket
     * @return bool
     */
    public function DeleteBucket($bucket)
    {
        try {
            $s3 = $this->getS3Client($bucket);

            $s3->deleteBucket(['Bucket' => $bucket['BUCKET']]);

            return !$s3->doesBucketExist($bucket['BUCKET']);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $bucket
     * @return bool
     */
    public function IsEmptyBucket($bucket)
    {
        try {
            $s3 = $this->getS3Client($bucket);

            $iterator = $s3->getIterator('ListObjects', [
                'Bucket' => $bucket['BUCKET'],
            ]);

            $count = iterator_count($iterator);

            return empty($count);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $bucket
     * @param $file
     * @return string
     */
    public function GetFileSRC($bucket, $file)
    {
        try {
            if (empty($file)) {
                return '';
            }

            if ($this->FileExists($bucket, $file)) {
                $key = $this->getKey($file);
                $s3 = $this->getS3Client($bucket);

                return $s3->getObjectUrl($bucket['BUCKET'], $key);
            }
        } catch (\Exception $e) {
            return '';
        }
    }

    /**
     * @param $bucket
     * @param $filePath
     * @return bool
     */
    public function FileExists($bucket, $filePath)
    {
        try {
            if (empty($filePath)) {
                return false;
            }

            $key = $this->getKey($filePath);
            if (empty($key)) {
                return false;
            }

            $s3 = $this->getS3Client($bucket);

            $exist = $s3->doesObjectExist(
                $bucket['BUCKET'],
                $key
            );

            return $exist;

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $bucket
     * @param $file
     * @param $filePath
     * @return bool
     */
    public function FileCopy($bucket, $file, $filePath)
    {
        return false;
    }

    /**
     * @param $bucket
     * @param $file
     * @param $filePath
     * @return bool
     */
    public function DownloadToFile($bucket, $file, $filePath)
    {
        $io = \CBXVirtualIo::GetInstance();
        $obRequest = new \CHTTP;
        $obRequest->follow_redirect = true;
        return $obRequest->Download($this->GetFileSRC($bucket, $file), $io->GetPhysicalName($filePath));
    }

    /**
     * @param $bucket
     * @param $filePath
     * @return bool
     */
    public function DeleteFile($bucket, $filePath)
    {
        try {
            $s3 = $this->getS3Client($bucket);
            $parsedKey = array_diff(explode('/', $filePath), ['']);

            // Удаление вместе с resize_cache
            $regex = "/(iblock\/{$parsedKey['2']})/";
            $s3->deleteMatchingObjects($bucket['BUCKET'], '', $regex);

            return $s3->doesObjectExist($bucket['BUCKET'], $filePath);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $bucket
     * @param $filePath
     * @param $file
     * @return bool
     */
    public function SaveFile($bucket, $filePath, $file)
    {
        try {
            if (!empty(self::$fileKeyForUpload) &&
                stripos($filePath, '/resize_cache/') === false) {
                if (stripos($file['name'], self::$fileKeyForUpload) === false) {
                    return false;
                }
            }

            if (!file_exists($file['tmp_name'])) {
                return false;
            }

            $s3 = $this->getS3Client($bucket);

            $params = [
                'Bucket' => $bucket['BUCKET'],
                'Key' => $this->getKey($filePath),
                'Body' => fopen($file['tmp_name'], 'r'),
                'ContentType' => $file['type']
            ];

            $s3->putObject($params);

            return $s3->doesObjectExist($params['Bucket'], $params['Key']);

        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * @param $bucket
     * @param $filePath
     * @param $bRecursive
     * @return array
     */
    public function ListFiles($bucket, $filePath, $bRecursive = false)
    {
        try {
            $result = [
                "dir" => [],
                "file" => [],
                "file_size" => [],
                "file_mtime" => [],
                "file_hash" => [],
            ];
            $s3 = $this->getS3Client($bucket);

            $iterator = $s3->getIterator('ListObjects', [
                'Bucket' => $bucket['BUCKET'],
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
     * @param $bucket
     * @param $ns
     * @param $filePath
     * @param $fileSize
     * @param $contentType
     * @return bool
     */
    public function InitiateMultipartUpload($bucket, &$ns, $filePath, $fileSize, $contentType)
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
     * @param $bucket
     * @param $ns
     * @param $data
     * @return bool
     */
    public function UploadPart($bucket, &$ns, $data)
    {
        return false;
    }

    /**
     * @param $bucket
     * @param $ns
     * @return bool
     */
    public function CompleteMultipartUpload($bucket, &$ns)
    {
        return false;
    }

    private function getS3Client(array $bucket): S3Client
    {
        $this->checkConnection($bucket);

        if ($bucket['ACTIVE'] != 'Y') {
            throw new \Exception('Bucket disabled');
        }

        $s3 = new S3Client([
            'version' => 'latest',
            'region' => $bucket['LOCATION'],
            'endpoint' => $bucket['SETTINGS']['HOST'],
            'use_path_style_endpoint' => true,
            'credentials' => [
                'key' => $bucket['SETTINGS']['ACCESS_KEY'],
                'secret' => $bucket['SETTINGS']['SECRET_KEY'],
            ],
            'http' => [
                'connect_timeout' => $this->connectTimeout,
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

    private function checkConnection(array &$bucket)
    {
        try {
            $client = new \GuzzleHttp\Client();

            $client->request(
                'GET',
                $bucket["SETTINGS"]["HOST"],
                [
                    'connect_timeout' => $this->connectTimeout,
                    'timeout' => $this->timeout
                ]
            );
        } catch (ConnectException $e) {
            $log = new Logger('CCloudStorageServiceMinio::checkConnection');
            $log->pushHandler(new StreamHandler($_SERVER['DOCUMENT_ROOT'] . '/upload/logs/minio_s3/' . date('Y-m') . '.log', Logger::ERROR));
            $log->error('ConnectException:  ' . $e->getMessage());

            $this->disableCloud($bucket);
        }
        catch (ClientException $e) {
            return;
        }
    }

    private function disableCloud(&$bucket)
    {
        $bucket = new \CCloudStorageBucket($bucket['ID']);
        $bucket->Update(['ACTIVE' => 'N']);
        $bucket['ACTIVE'] = 'N';
    }
}
