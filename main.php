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

function generateCell($bytes, $chars)
{
    $cell = "";
    for ($j = 0; $j < $bytes; $j++) {
        $cell .= $chars[mt_rand(0, count($chars) - 1)];
    }
    return $cell;
}

function generateFile(\Keboola\Csv\CsvFile $csv, $rows, $row)
{
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
        switch ($rowItem) {
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

foreach ($matrix as $parameters) {
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

    $chunksCount = ceil(count($csvFiles) / $chunkSize);
    $client = new \Keboola\SlicedUpload\Client(["token" => $config["#storageApiToken"], "url" => $config["storageApiUrl"]]);
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
    $totalSizeMb = $sizeMB * $parameters["files"];
    $throughput = round($totalSizeMb / $duration);
    print "$totalSizeMb MB split into {$parameters["files"]} files ({$chunksCount} chunks) uploaded to Storage API in $duration seconds (~$throughput MB/s), file id {$fileId}\n";


    // cleanup
    unlink($csv->getPathname());
    foreach ($csvFiles as $csvFiles) {
        unlink($csvFiles->getPathname());
    }
}

