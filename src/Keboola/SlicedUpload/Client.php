<?php

namespace Keboola\SlicedUpload;

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
            ],
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
                'http' => true,
            ];
        }

        $s3Client = new \Aws\S3\S3Client($s3options);
        $s3Uploader = new S3Uploader($s3Client);
        $s3Uploader->uploadFile($uploadParams['bucket'], $uploadParams['key'], $uploadParams['acl'], $filePath, $result['name'], $newOptions->getIsEncrypted() ? $uploadParams['x-amz-server-side-encryption'] : null);
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
            ],
        ];

        $s3Client = new \Aws\S3\S3Client($options);
        $s3Uploader = new S3Uploader($s3Client);
        $s3Uploader->uploadSlicedFile($uploadParams['bucket'], $uploadParams['key'], $uploadParams['acl'], $slices, $newOptions->getIsEncrypted() ? $uploadParams['x-amz-server-side-encryption'] : null);

        // Upload manifest
        $manifest = [
            'entries' => [],
        ];
        foreach ($slices as $filePath) {
            $manifest['entries'][] = [
                "url" => "s3://" . $uploadParams['bucket'] . "/" . $uploadParams['key'] . basename($filePath),
            ];
        }
        $manifestUploadOptions = [
            'Bucket' => $uploadParams['bucket'],
            'Key' => $uploadParams['key'] . 'manifest',
            'Body' => json_encode($manifest),
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
     * @param \Aws\S3\S3Client $s3Client
     * @param filePath $
     * @param string $bucket
     * @param string $key
     * @param string $acl
     * @param int $concurrency
     * @param null $encryption
     * @param null $friendlyName
     * @param UploadState|null $state
     * @return \Aws\S3\MultipartUploader
     */
    private function multipartUploaderFactory(
        $s3Client,
        $filePath,
        $bucket,
        $key,
        $acl,
        $concurrency,
        $encryption = null,
        $friendlyName = null,
        UploadState $state = null
    ) {
        $uploaderOptions = [
            'Bucket' => $bucket,
            'Key' => $key,
            'ACL' => $acl,
            'concurrency' => $concurrency,
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
