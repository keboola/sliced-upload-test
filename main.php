<?php

ini_set('display_errors', true);

require_once __DIR__ . '/vendor/autoload.php';

$arguments = getopt("d::", array("data::"));
$dataFolder = "/data";
if (isset($arguments["data"])) {
    $dataFolder = $arguments["data"];
}
$config = json_decode(file_get_contents($dataFolder . "/config.json"), true)["parameters"];

$chars = [
    "\t", "\n", "a", "b", "c", "d", "e", "f"
];

function generateCell($bytes, $chars) {
    $cell = "";
    for ($j = 0; $j < $bytes; $j++) {
        $cell .= $chars[mt_rand(0, count($chars) - 1)];
    }
    return $cell;
}

function generateFile(\Keboola\Csv\CsvFile $csv, $rows, $row) {
    for ($i = 0; $i < $rows; $i++) {
        $csv->writeRow($row);
    }
}

$k1row = generateCell(1000, $chars);
$k10row = generateCell(10000, $chars);
$k100row = generateCell(100000, $chars);

$matrix = $config["matrix"];
foreach ($matrix as $key => $matrixItem) {
    $newRow = [];
    foreach ($matrixItem["row"] as $rowItem) {
        switch($rowItem) {
            case "k1row":
                $newRow[] = $k1row;
                break;
            case "k10row":
                $newRow[] = $k10row;
                break;
            case "k100row":
                $newRow[] = $k100row;
                break;
            default:
                throw new \Exception("invalid row identifier");
                break;
        }
    }
    $matrix[$key]["row"] = $newRow;
}

$chunkSize = 50;
if (isset($config["chunkSize"])) {
    $chunkSize = $config["chunkSize"];
}

foreach($matrix as $parameters) {
    $temp = new Keboola\Temp\Temp();
    $source = $temp->createFile('source.csv');
    $csv = new Keboola\Csv\CsvFile($source->getPathname());

    $rowSize = 0;
    foreach ($parameters["row"] as $cell) {
        $rowSize += strlen($cell);
    }
    $sizeMB = round($parameters["rows"] * $rowSize / 1024**2);

    $time = microtime(true);

    generateFile($csv, $parameters["rows"], $parameters["row"], $chars);
    $duration = microtime(true) - $time;

    print "{$parameters["rows"]} rows with " . count($parameters["row"]) . " columns by {$rowSize} bytes ($sizeMB MB) generated in $duration seconds\n";

    $csvFiles = [];
    $fs = new \Symfony\Component\Filesystem\Filesystem();
    for ($i = 0; $i < $parameters["files"]; $i++) {
        $fileName = $dataFolder . "/out/tables/csvfile/part_{$i}.csv";
        $fs->copy($csv->getPathname(), $fileName);
        $csvFiles[] = new \Keboola\Csv\CsvFile($fileName);
    }

    print "Copied into " . count($csvFiles) . " files\n";


    // upload files to S3
    $credentials = new \Aws\Credentials\Credentials(
        $config['AWS_ACCESS_KEY_ID'],
        $config['#AWS_SECRET_ACCESS_KEY']
    );
    $s3client = new \Aws\S3\S3Client(
        [
            "credentials" => $credentials,
            "region" => $config['AWS_REGION'],
            "version" => "2006-03-01"
        ]
    );
    $chunksCount = ceil(count($csvFiles) / $chunkSize);


    // putObject
    // delete all files
    $s3client->deleteMatchingObjects($config['AWS_S3_BUCKET'], $config['S3_KEY_PREFIX'] . "/putObject");
    $time = microtime(true);
    for ($i = 0; $i < $chunksCount; $i++) {
        $csvFilesChunk = array_slice($csvFiles, $i * $chunkSize, $chunkSize);
        $promises = [];
        $handles = [];
        /**
         * @var $splitFile \Keboola\Csv\CsvFile
         */
        foreach ($csvFilesChunk as $key => $splitFile) {
            $handle = fopen($splitFile->getPathname(), "r+");
            $handles[] = $handle;
            $promises[] = $s3client->putObjectAsync(
                [
                    'Bucket' => $config['AWS_S3_BUCKET'],
                    'Key' => $config['S3_KEY_PREFIX'] . "/putObject/" . $splitFile->getBasename(),
                    'Body' => $handle,
                ]
            );
        }
        $results = GuzzleHttp\Promise\unwrap($promises);
        foreach ($handles as $handle) {
            fclose($handle);
        }
    }
    $duration = microtime(true) - $time;
    print "$sizeMB MB split into {$parameters["files"]} files ({$chunksCount} chunks) uploaded to S3 using 'putObjectAsync' method in $duration seconds\n";

    $objects = $s3client->listObjects([
        'Bucket' => $config['AWS_S3_BUCKET'],
        'Prefix' => $config['S3_KEY_PREFIX'] . "/putObject"
    ]);
    print "Uploaded " . count($objects->get('Contents')) . " objects\n";


    // Storage API
    // upload files to Storage API
    $time = microtime(true);
    $client = new \Keboola\SlicedUpload\Client(["token" => $config["#storageApiToken"]]);
    $slices = [];
    /**
     * @var $csvFile \Keboola\Csv\CsvFile
     */
    foreach ($csvFiles as $csvFile) {
        $slices[] = $csvFile->getPathname();
    }
    $fileUploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
    $fileUploadOptions
        ->setFileName("slices.csv")
        ->setTags(["sliced-upload-test"])
        ->setIsSliced(true)
    ;
    $fileUploadTransferOptions = new \Keboola\StorageApi\Options\FileUploadTransferOptions();
    $fileUploadTransferOptions->setChunkSize($chunkSize);

    $fileId = $client->uploadSlicedFile($slices, $fileUploadOptions, $fileUploadTransferOptions);
    $duration = microtime(true) - $time;
    print "$sizeMB MB split into {$parameters["files"]} files ({$chunksCount} chunks) uploaded to Storage API in $duration seconds, file id {$fileId}\n";


    // uploadDirectory
    // delete all files
    $s3client->deleteMatchingObjects($config['AWS_S3_BUCKET'], $config['S3_KEY_PREFIX'] . "/uploadDirectory");
    $time = microtime(true);
    $s3client->uploadDirectory($dataFolder . "/out/tables/csvfile", $config["AWS_S3_BUCKET"], $config["S3_KEY_PREFIX"] . "/uploadDirectory");
    $duration = microtime(true) - $time;
    print "$sizeMB MB split into {$parameters["files"]} files ({$chunksCount} chunks) uploaded to S3 using 'uploadDirectory' method in $duration seconds\n";

    $objects = $s3client->listObjects([
        'Bucket' => $config['AWS_S3_BUCKET'],
        'Prefix' => $config['S3_KEY_PREFIX'] . "/uploadDirectory"
    ]);
    print "Uploaded " . count($objects->get('Contents')) . " objects\n";


    // uploadAsync
    // delete all files
    $s3client->deleteMatchingObjects($config['AWS_S3_BUCKET'], $config['S3_KEY_PREFIX'] . "/uploadAsync");
    $time = microtime(true);
    // well, i have to rerun the whole thing again, as i have no idea which slices are done and slice failed
    // splice files into chunks
    for ($i = 0; $i < $chunksCount; $i++) {
        $csvFilesChunk = array_slice($csvFiles, $i * $chunkSize, $chunkSize);
        $finished = false;
        do {
            $handles = [];
            try {
                $promises = [];
                /**
                 * @var $splitFile \Keboola\Csv\CsvFile
                 */
                foreach ($csvFilesChunk as $key => $splitFile) {
                    $handle = fopen($splitFile->getPathname(), "r");
                    $handles[] = $handle;
                    $promises[] = $s3client->uploadAsync(
                        $config['AWS_S3_BUCKET'],
                        $config['S3_KEY_PREFIX'] . "/uploadAsync/" . $splitFile->getBasename(),
                        $handle
                    );
                }
                $results = GuzzleHttp\Promise\unwrap($promises);
                // var_dump($results);
                $finished = true;
            } catch (\Aws\Exception\MultipartUploadException $e) {
                print "Retrying upload: " . $e->getMessage() . "\n";
            } finally {
                foreach ($handles as $handle) {
                    fclose($handle);
                }
            }
        } while (!isset($finished));
    }
    $duration = microtime(true) - $time;
    print "$sizeMB MB split into {$parameters["files"]} files ({$chunksCount} chunks) uploaded to S3 using 'uploadAsync' method in $duration seconds\n";

    $objects = $s3client->listObjects([
        'Bucket' => $config['AWS_S3_BUCKET'],
        'Prefix' => $config['S3_KEY_PREFIX'] . "/uploadAsync"
    ]);
    print "Uploaded " . count($objects->get('Contents')) . " objects\n";

    
    // cleanup
    unlink($csv->getPathname());
    foreach ($csvFiles as $csvFiles) {
        unlink($csvFiles->getPathname());
    }
}

