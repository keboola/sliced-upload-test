<?php

namespace Keboola\SlicedUpload;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\FileUploadTransferOptions;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class Client extends \Keboola\StorageApi\Client
{

    /**
     * Upload a sliced file to file uploads
     *
     * @param array $slices list of slices that make the file
     * @param FileUploadOptions $options
     * @param FileUploadTransferOptions $transferOptions
     * @return int created file id
     * @throws ClientException
     */
    public function uploadSlicedFile(array $slices, FileUploadOptions $options, FileUploadTransferOptions $transferOptions = null)
    {
        if (!$options->getIsSliced()) {
            throw new ClientException("File is not sliced.");
        }
        if (!$options->getFileName()) {
            throw new ClientException("File name for sliced file upload not set.");
        }
        if (!$transferOptions) {
            $transferOptions = new FileUploadTransferOptions();
        }

        $fileSize = 0;
        foreach ($slices as $filePath) {
            if (!is_readable($filePath)) {
                throw new ClientException("File is not readable: " . $filePath, null, null, 'fileNotReadable');
            }
            $fileSize += filesize($filePath);
        }
        $newOptions = clone $options;
        $fs = null;
        $currentUploadDir = null;
        $fs = new Filesystem();
        $sapiClientTempDir = sys_get_temp_dir() . '/sapi-php-client';
        if (!$fs->exists($sapiClientTempDir)) {
            $fs->mkdir($sapiClientTempDir);
        }
        $currentUploadDir = $sapiClientTempDir . '/' . uniqid('file-upload');
        $fs->mkdir($currentUploadDir);

        if ($newOptions->getCompress()) {
            foreach ($slices as $key => $filePath) {
                if (!in_array(strtolower(pathinfo($filePath, PATHINFO_EXTENSION)), array("gzip", "gz", "zip"))) {
                    // gzip file and preserve it's base name
                    $gzFilePath = $currentUploadDir . '/' . basename($filePath) . '.gz';
                    $command = sprintf("gzip -c %s > %s", escapeshellarg($filePath), escapeshellarg($gzFilePath));
                    $process = new Process($command);
                    $process->setTimeout(null);
                    if (0 !== $process->run()) {
                        $error = sprintf(
                            'The command "%s" failed.' . "\nExit Code: %s(%s)",
                            $process->getCommandLine(),
                            $process->getExitCode(),
                            $process->getExitCodeText()
                        );
                        throw new ClientException("Failed to gzip file. " . $error);
                    }
                    $slices[$key] = $gzFilePath;
                }
            }
        }

        $newOptions
            ->setSizeBytes($fileSize)
            ->setFederationToken(true)
            ->setIsSliced(true);

        // 1. prepare resource
        $preparedFileResult = $this->prepareFileUpload($newOptions);

        // 2. upload directly do S3 using returned credentials
        // using federation token
        $uploadParams = $preparedFileResult['uploadParams'];

        $options = [
            'version' => '2006-03-01',
            'retries' => $this->getAwsRetries(),
            'region' => $preparedFileResult['region'],
            'debug' => false,
            'credentials' => [
                'key' => $uploadParams['credentials']['AccessKeyId'],
                'secret' => $uploadParams['credentials']['SecretAccessKey'],
                'token' => $uploadParams['credentials']['SessionToken'],
            ]
        ];

        if ($this->isAwsDebug()) {
            $logfn = function ($message) {
                if (trim($message) != '') {
                    // print_r($message, ['source' => 'AWS SDK PHP debug']);
                }
            };
            $options['debug'] = [
                'logfn' => function ($message) use ($logfn) {
                    call_user_func($logfn, $message);
                },
                'stream_size' => 0,
                'scrub_auth' => true,
                'http' => true
            ];
        }

        $s3Client = new \Aws\S3\S3Client($options);

        // prepare manifest object
        $manifest = [
            'entries' => []
        ];
        // split all slices into batch chunks and upload them separately
        $chunks = ceil(count($slices) / $transferOptions->getChunkSize());
        for ($i = 0; $i < $chunks; $i++) {
            $slicesChunk = array_slice($slices, $i * $transferOptions->getChunkSize(), $transferOptions->getChunkSize());
            $finished = false;
            $promises = [];
            /**
             * @var $splitFile \Keboola\Csv\CsvFile
             */
            foreach ($slicesChunk as $key => $splitFile) {
                $uploaderOptions = [
                    'bucket' => $uploadParams['bucket'],
                    'key' => $uploadParams['key'] . basename($filePath) . $splitFile->getBasename(),
                    'acl' => $uploadParams['acl'],
                ];
                if ($newOptions->getIsEncrypted()) {
                    $uploaderOptions['ServerSideEncryption'] = $uploadParams['x-amz-server-side-encryption'];
                }
                $uploader = new \Aws\S3\MultipartUploader($s3Client, $splitFile->getPathname(), $uploaderOptions);
                $promises[$splitFile->getPathname()] = $uploader->promise();
            }
            do {
                try {
                    \GuzzleHttp\Promise\unwrap($promises);
                    $finished = true;
                } catch (\Aws\Exception\MultipartUploadException $e) {
                    print "Retrying upload: " . $e->getMessage() . "\n";
                    //var_dump($e->getState());
                    //var_dump($promises);
                    /**
                     * @var $promise \GuzzleHttp\Promise\Promise
                     */
                    foreach($promises as $filePath => $promise) {
                        print "{$filePath} - {$promise->getState()}\n";
                        if ($promise->getState() == 'rejected') {
                            print "Resuming upload of {$filePath}\n";
                            $uploader = new \Aws\S3\MultipartUploader($s3Client, $filePath, [
                                'state' => $e->getState()
                            ]);
                            $promises[] = $uploader->promise();
                        }
                    }
                }
            } while (!$finished);
        }

        $manifestUploadOptions = [
            'Bucket' => $uploadParams['bucket'],
            'Key' => $uploadParams['key'] . 'manifest',
            'Body' => json_encode($manifest)
        ];
        if ($newOptions->getIsEncrypted()) {
            $manifestUploadOptions['ServerSideEncryption'] = $uploadParams['x-amz-server-side-encryption'];
        }
        $s3Client->putObject($manifestUploadOptions);

        if ($fs) {
            $fs->remove($currentUploadDir);
        }

        return $preparedFileResult['id'];
    }
}
