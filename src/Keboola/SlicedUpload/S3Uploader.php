<?php

namespace Keboola\SlicedUpload;

use Aws\Multipart\UploadState;
use Aws\S3\Exception\S3MultipartUploadException;
use Aws\S3\S3Client;
use Keboola\StorageApi\ClientException;

class S3Uploader
{
    const SINGLE_FILE_CONCURRENCY = 20;
    const MULTI_FILE_CONCURRENCY = 5;
    const MAX_RETRIES = 10;
    const CHUNK_SIZE = 50;
    
    protected $s3Client;

    public function __construct(S3Client $s3Client)
    {
        $this->s3Client = $s3Client;
    }

    /**
     * @param $bucket
     * @param $key
     * @param $acl
     * @param $file
     * @param $name
     * @param null $encryption
     * @throws ClientException
     */
    public function uploadFile($bucket, $key, $acl, $file, $name, $encryption = null)
    {
        $this->upload($bucket, $acl, [$file => $key], $name, $encryption);
    }

    /**
     * @param $bucket
     * @param $key
     * @param $acl
     * @param $slices
     * @param null $encryption
     * @throws ClientException
     */
    public function uploadSlicedFile($bucket, $key, $acl, $slices, $encryption = null)
    {
        // split all slices into batch chunks and upload them separately
        $chunks = ceil(count($slices) / self::CHUNK_SIZE);
        for ($i = 0; $i < $chunks; $i++) {
            $slicesChunk = array_slice(
                $slices,
                $i * self::CHUNK_SIZE,
                self::CHUNK_SIZE
            );
            $slices = [];
            foreach ($slicesChunk as $filePath) {
                $slices[$filePath] = $key . basename($filePath);
            }
            $this->upload($bucket, $acl, $slices, null, $encryption);
        }
    }

    /**
     * @param $bucket
     * @param $acl
     * @param $key
     * @param $filePath
     * @param null $name
     * @param null $encryption
     * @throws ClientException
     */
    protected function putFile($bucket, $key, $acl, $filePath, $name = null, $encryption = null)
    {
        $fh = @fopen($filePath, 'r');
        if ($fh === false) {
            throw new ClientException("Error on file upload to S3: " . $filePath, null, null, 'fileNotReadable');
        }
        $putParams = array(
            'Bucket' => $bucket,
            'Key' => $key,
            'ACL' => $acl,
            'Body' => $fh,
            'ContentDisposition' => sprintf('attachment; filename=%s;', $name ? $name : basename($filePath)),
        );

        if ($encryption) {
            $putParams['ServerSideEncryption'] = $encryption;
        }
        $this->s3Client->putObject($putParams);
        if (is_resource($fh)) {
            fclose($fh);
        }
    }

    /**
     * @param $bucket
     * @param $acl
     * @param array $files
     * @param null $name
     * @param null $encryption
     * @throws ClientException
     */
    protected function upload($bucket, $acl, $files, $name = null, $encryption = null)
    {
        var_dump($files);
        // Initialize promises
        $promises = [];
        foreach ($files as $filePath => $key) {
            /*
             * Cannot upload empty files using multipart: https://github.com/aws/aws-sdk-php/issues/1429
             * Upload them directly immediately and continue to next part in the chunk.
             */
            if (filesize($filePath) === 0) {
                $this->putFile($bucket, $key, $acl, $filePath, $name, $encryption);
                continue;
            }
            $uploader = $this->multipartUploaderFactory(
                $filePath,
                $bucket,
                $key,
                $acl,
                count($files) > 1 ? self::MULTI_FILE_CONCURRENCY : self::SINGLE_FILE_CONCURRENCY,
                $encryption ? $encryption : null,
                $name ? $name : basename($filePath)
            );
            $promises[$filePath] = $uploader->promise();
        }

        $retries = 0;
        do {
            $retries++;
            var_dump($retries);
            var_dump("memory_get_peak_usage()", memory_get_peak_usage());
            var_dump("memory_get_peak_usage(true)", memory_get_peak_usage(true));
            if ($retries >= self::MAX_RETRIES) {
                throw new ClientException('Exceeded maximum number of retries per chunk upload');
            }

            var_dump('array_keys($promises)', array_keys($promises));
            $results = \GuzzleHttp\Promise\settle($promises)->wait();
            var_dump('array_keys($results)', array_keys($results));
            var_dump('$results', array_map(function ($result) {
                $response = [
                    'state' => $result["state"],
                ];
                if ($result["state"] !== "fulfilled") {
                    var_dump($result["reason"]->getMessage());
                    if ($result["reason"] instanceof S3MultipartUploadException) {
                        $response["reason.key"] = $result["reason"]->getKey();
                    } else {
                        throw new \UnexpectedValueException("reason not an instance of S3MultipartUploadException");
                    }
                }
                return $response;
            }, $results));
            $finished = true;
            $rejected = [];
            foreach ($results as $filePath => $result) {
                if ($result["state"] === "rejected") {
                    /** @var S3MultipartUploadException $reason */
                    $rejected[$filePath] = $result["reason"];
                }
            }
            if (count($rejected) > 0) {
                $finished = false;
                /**
                 * @var string $filePath
                 * @var S3MultipartUploadException $reason
                 */
                foreach ($rejected as $filePath => $reason) {
                    $uploader = $this->multipartUploaderFactory(
                        $filePath,
                        $bucket,
                        $reason->getKey(),
                        $acl,
                        count($rejected) > 1 ? self::MULTI_FILE_CONCURRENCY : self::SINGLE_FILE_CONCURRENCY,
                        $encryption ? $encryption : null,
                        $name ? $name : basename($filePath),
                        $reason->getState()
                    );
                    $promises[$filePath] = $uploader->promise();
                }
            }
        } while (!$finished);
    }

    /**
     * @param $bucket
     * @param $key
     * @param $acl
     * @param $concurrency
     * @param null $encryption
     * @param null $name
     * @param UploadState|null $state
     * @return array
     */
    public function getMultipartUploadOptions(
        $bucket,
        $key,
        $acl,
        $concurrency,
        $encryption = null,
        $name = null,
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
        if (!empty($name)) {
            $beforeInitiateCommands['ContentDisposition'] = sprintf('attachment; filename=%s;', $name);
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
        return $uploaderOptions;
    }

    /**
     * @param string $filePath
     * @param string $bucket
     * @param string $key
     * @param string $acl
     * @param int $concurrency
     * @param null $encryption
     * @param null $name
     * @param UploadState|null $state
     * @return \Aws\S3\MultipartUploader
     */
    protected function multipartUploaderFactory(
        $filePath,
        $bucket,
        $key,
        $acl,
        $concurrency,
        $encryption = null,
        $name = null,
        UploadState $state = null
    ) {
        $uploaderOptions = $this->getMultipartUploadOptions($bucket, $key, $acl, $concurrency, $encryption, $name, $state);
        return new \Aws\S3\MultipartUploader($this->s3Client, $filePath, $uploaderOptions);
    }

}
