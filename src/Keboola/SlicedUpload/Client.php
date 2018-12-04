<?php

namespace Keboola\SlicedUpload;

use Aws\Exception\AwsException;
use Aws\Exception\MultipartUploadException;
use Aws\Multipart\UploadState;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\FileUploadTransferOptions;
use Psr\Log\LoggerInterface;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Process\Process;

class Client extends \Keboola\StorageApi\Client
{
    /**
     * @var LoggerInterface
     *
     */
    private $logger;

    public function __construct(array $config = array())
    {
        parent::__construct($config);

        if (isset($config['logger'])) {
            $this->logger = $config['logger'];
        }
    }

    private function log($message, $context = array())
    {
        if ($this->logger) {
            $this->logger->info($message, $context);
        }
    }


    /**
     * Upload a file to file uploads
     *
     * @param string $filePath
     * @param FileUploadOptions $options
     * @return int - created file id
     * @throws ClientException
     */
    public function uploadFile($filePath, FileUploadOptions $options)
    {
        if (!is_readable($filePath)) {
            throw new ClientException("File is not readable: " . $filePath, null, null, 'fileNotReadable');
        }
        $newOptions = clone $options;
        $fs = null;
        $currentUploadDir = null;
        if ($newOptions->getCompress() && !in_array(strtolower(pathinfo($filePath, PATHINFO_EXTENSION)), array("gzip", "gz", "zip"))) {
            $fs = new Filesystem();
            $sapiClientTempDir = sys_get_temp_dir() . '/sapi-php-client';
            if (!$fs->exists($sapiClientTempDir)) {
                $fs->mkdir($sapiClientTempDir);
            }

            $currentUploadDir = $sapiClientTempDir . '/' . uniqid('file-upload');
            $fs->mkdir($currentUploadDir);

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

            $filePath = $gzFilePath;
        }
        $newOptions
            ->setFileName(basename($filePath))
            ->setSizeBytes(filesize($filePath))
            ->setFederationToken(true);

        // 1. prepare resource
        $result = $this->prepareFileUpload($newOptions);

        // 2. upload directly do S3 using returned credentials
        // using federation token
        $uploadParams = $result['uploadParams'];


        $s3options = [
            'version' => '2006-03-01',
            'retries' => $this->getAwsRetries(),
            'region' => $result['region'],
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
                    $this->log($message, ['source' => 'AWS SDK PHP debug']);
                }
            };
            $s3options['debug'] = [
                'logfn' => function ($message) use ($logfn) {
                    call_user_func($logfn, $message);
                },
                'stream_size' => 0,
                'scrub_auth' => true,
                'http' => true
            ];
        }

        $s3Client = new \Aws\S3\S3Client($s3options);

        $fh = @fopen($filePath, 'r');
        if ($fh === false) {
            throw new ClientException("Error on file upload to S3: " . $filePath, null, null, 'fileNotReadable');
        }

        // Use MultipartUpload if file size great than threshold
        if (filesize($filePath) > $newOptions->getMultipartUploadThreshold()) {
            $uploader = $this->multipartUploaderFactory(
                $s3Client,
                $filePath,
                $uploadParams['bucket'],
                $uploadParams['key'],
                $uploadParams['acl'],
                $newOptions->getIsEncrypted() ? $uploadParams['x-amz-server-side-encryption'] : null,
                $result['name']
            );
            $uploadCount = 0;
            do {
                $uploadCount++;
                var_dump($uploadCount);
                try {
                    $s3result = $uploader->upload();
                } catch (MultipartUploadException $e) {
                    $uploader = $this->multipartUploaderFactory(
                        $s3Client,
                        $filePath,
                        $uploadParams['bucket'],
                        $uploadParams['key'],
                        $uploadParams['acl'],
                        $newOptions->getIsEncrypted() ? $uploadParams['x-amz-server-side-encryption'] : null,
                        $result['name'],
                        $e->getState()
                    );
                }
            } while (!isset($s3result));
        } else {
            $putParams = array(
                'Bucket' => $uploadParams['bucket'],
                'Key' => $uploadParams['key'],
                'ACL' => $uploadParams['acl'],
                'Body' => $fh,
                'ContentDisposition' => sprintf('attachment; filename=%s;', $result['name']),
            );

            if ($newOptions->getIsEncrypted()) {
                $putParams['ServerSideEncryption'] = $uploadParams['x-amz-server-side-encryption'];
            }

            $s3Client->putObject($putParams);
        }

        if (is_resource($fh)) {
            fclose($fh);
        }

        if ($fs) {
            $fs->remove($currentUploadDir);
        }

        return $result['id'];
    }

    /**
     * Upload a sliced file to file uploads. This method ignores FileUploadOption->getMultipartThreshold().
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

        $s3Client = new \Aws\S3\S3Client($options);

        // prepare manifest object
        $manifest = [
            'entries' => []
        ];
        // split all slices into batch chunks and upload them separately
        $chunks = ceil(count($slices) / $transferOptions->getChunkSize());

        for ($i = 0; $i < $chunks; $i++) {
            $slicesChunk = array_slice($slices, $i * $transferOptions->getChunkSize(), $transferOptions->getChunkSize());

            // Initialize promises
            $promises = [];
            foreach ($slicesChunk as $filePath) {
                $manifest['entries'][] = [
                    "url" => "s3://" . $uploadParams['bucket'] . "/" . $uploadParams['key'] . basename($filePath)
                ];
                /*
                 * Cannot upload empty files using multipart: https://github.com/aws/aws-sdk-php/issues/1429
                 * Upload them directly immediately and continue to next part in the chunk.
                 */
                if (filesize($filePath) === 0) {
                    $fh = fopen($filePath, 'r');
                    $putParams = array(
                        'Bucket' => $uploadParams['bucket'],
                        'Key' => $uploadParams['key'] . basename($filePath),
                        'ACL' => $uploadParams['acl'],
                        'Body' => $fh,
                        'ContentDisposition' => sprintf('attachment; filename=%s;', basename($filePath)),
                    );

                    if ($newOptions->getIsEncrypted()) {
                        $putParams['ServerSideEncryption'] = $uploadParams['x-amz-server-side-encryption'];
                    }
                    $s3Client->putObject($putParams);
                    continue;
                }
                $uploader = $this->multipartUploaderFactory(
                    $s3Client,
                    $filePath,
                    $uploadParams['bucket'],
                    $uploadParams['key'] . basename($filePath),
                    $uploadParams['acl'],
                    $newOptions->getIsEncrypted() ?  $uploadParams['x-amz-server-side-encryption'] : null
                );
                $promises[$filePath] = $uploader->promise();
            }

            var_dump("memory_get_peak_usage()", memory_get_peak_usage());
            var_dump("memory_get_peak_usage(true)", memory_get_peak_usage(true));

            /*
             * In case of an upload failure (\Aws\Exception\MultipartUploadException) there is no sane way of resuming
             * failed uploads, the exception returns state for a single failed upload and I don't know which one it is
             * So I need to iterate over all promises and retry all rejected promises from scratch
             */
            $finished = false;
            $retries = 0;
            do {
                try {
                    \GuzzleHttp\Promise\unwrap($promises);
                    $finished = true;
                } catch (\Aws\Exception\MultipartUploadException $e) {
                    $retries++;
                    var_dump($retries);
                    var_dump('$e->getState()->getId()', $e->getState()->getId());
                    /** @var AwsException $prev */
                    $prev = $e->getPrevious();
                    if ($prev) {
                        var_dump('$prev->getTransferInfo()', $prev->getTransferInfo());
                    }

                    $this->log("Exception: " . $e->getMessage());
                    if ($retries >= $transferOptions->getMaxRetriesPerChunk()) {
                        throw new ClientException('Exceeded maximum number of retries per chunk upload');
                    }
                    $unwrappedPromises = $promises;
                    $promises = [];
                    /**
                     * @var $promise \GuzzleHttp\Promise\Promise
                     */
                    foreach ($unwrappedPromises as $filePath => $promise) {
                        var_dump('$filePath', $filePath);
                        if ($promise->getState() == 'rejected') {
                            $uploader = $this->multipartUploaderFactory(
                                $s3Client,
                                $filePath,
                                $uploadParams['bucket'],
                                $uploadParams['key'] . basename($filePath),
                                $uploadParams['acl'],
                                $newOptions->getIsEncrypted() ?  $uploadParams['x-amz-server-side-encryption'] : null
                            );
                            $promises[$filePath] = $uploader->promise();
                        }
                    }
                    var_dump("memory_get_peak_usage()", memory_get_peak_usage());
                    var_dump("memory_get_peak_usage(true)", memory_get_peak_usage(true));
                }
            } while (!$finished);
        }

        // Upload manifest
        $manifestUploadOptions = [
            'Bucket' => $uploadParams['bucket'],
            'Key' => $uploadParams['key'] . 'manifest',
            'Body' => json_encode($manifest)
        ];
        if ($newOptions->getIsEncrypted()) {
            $manifestUploadOptions['ServerSideEncryption'] = $uploadParams['x-amz-server-side-encryption'];
        }
        $s3Client->putObject($manifestUploadOptions);

        // Cleanup
        if ($fs) {
            $fs->remove($currentUploadDir);
        }

        return $preparedFileResult['id'];
    }


    /**
     * @param S3Client $s3Client
     * @param string $filePath
     * @param string $bucket
     * @param string $key
     * @param string $acl
     * @param string|null $encryption
     * @param string|null $friendlyName
     * @param UploadState|null $state
     * @return \Aws\S3\MultipartUploader
     */
    private function multipartUploaderFactory($s3Client, $filePath, $bucket, $key, $acl, $encryption = null, $friendlyName = null, UploadState $state = null)
    {
        $uploaderOptions = [
            'Bucket' => $bucket,
            'Key' => $key,
            'ACL' => $acl
        ];
        if (!empty($state)) {
            $uploaderOptions['state'] = $state;
        }
        $beforeInitiateCommands = [];
        if (!empty($friendlyName)) {
            $beforeInitiateCommands['ContentDisposition'] = sprintf('attachment; filename=%s;', $friendlyName);
        }
        if (!empty($encryption)) {
            $beforeInitiateCommands['ServerSideEncryption'] = $encryption;
        }
        if (count($beforeInitiateCommands)) {
            $uploaderOptions['before_initiate'] = function ($command) use ($beforeInitiateCommands) {
                foreach ($beforeInitiateCommands as $key => $value) {
                    $command[$key] = $value;
                }
            };
        }
        return new \Aws\S3\MultipartUploader($s3Client, $filePath, $uploaderOptions);
    }

}
