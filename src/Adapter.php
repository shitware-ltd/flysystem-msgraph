<?php

namespace ShitwareLtd\FlysystemMsGraph;

use Exception;
use GuzzleHttp\Client as Guzzle;
use GuzzleHttp\Psr7\StreamWrapper;
use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use Microsoft\Graph\Graph;
use Microsoft\Graph\Http\GraphResponse;
use Microsoft\Graph\Model\Directory;
use Microsoft\Graph\Model\DriveItem;
use Microsoft\Graph\Model\File;
use Microsoft\Graph\Model\UploadSession;

class Adapter implements FilesystemAdapter
{
    protected $options = [];

    protected const CONFLICT_BEHAVIOR_FAIL = 'fail';
    protected const CONFLICT_BEHAVIOR_IGNORE = 'ignore';
    protected const CONFLICT_BEHAVIOR_RENAME = 'rename';
    protected const CONFLICT_BEHAVIOR_REPLACE = 'replace';

    public function __construct(public Graph $graph, protected string $drive_id, array $options = [])
    {
        $default_options = [
            'request_timeout' => 90,        //Increase this for larger chunks / higher latency
            'chunk_size' => 320 * 1024 * 10, //Microsoft requires chunks to be multiples of 320KB
            'directory_conflict_behavior' => static::CONFLICT_BEHAVIOR_IGNORE, //ignore, rename, replace, fail
        ];

        $this->options = array_merge($default_options, $options);
        switch($this->options['directory_conflict_behavior']) {
            case static::CONFLICT_BEHAVIOR_FAIL:
            case static::CONFLICT_BEHAVIOR_IGNORE:
            case static::CONFLICT_BEHAVIOR_RENAME:
            case static::CONFLICT_BEHAVIOR_REPLACE:
                break;
            default:
                throw new Exception('Invalid directory_conflict_behavior');
        }
        
        if ($this->options['chunk_size'] % (320 * 1024)) {
            throw new Exception('Chunk size must be a multiple of 320KB');
        }
    }

    public function getDriveRootUrl(): string
    {
        return '/drives/' . $this->drive_id . '/root';
    }

    public function getUrlToPath(string $path): string
    {
        if ($path === '' || $path === '.' || $path === '/') {
            return $this->getDriveRootUrl();
        }

        return $this->getDriveRootUrl() . ':/' . $path;
    }

    protected function getDriveItemUrl(string $path): string
    {
        return '/drives/' . $this->drive_id . '/items/' . $this->getDriveItem($path)->getId();
    }

    public function fileExists(string $path): bool
    {
        $path = $this->getUrlToPath($path);
        try {
            $this->getFile($path);

            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    public function directoryExists(string $path): bool
    {
        $path = $this->getUrlToPath($path);
        try {
            $this->getDirectory($path);

            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    protected function ensureValidPath(string $path)
    {
        //If we're not writing to root we need to make sure the target directory exists
        if (str_contains($path, '/')) {
            $this->ensureDirectoryExists(dirname($path));
        }
    }

    public function write(string $path, string $contents, Config $config): void
    {
        $path = trim($path, '/');
        $this->ensureValidPath($path);
        //Files larger than 4MiB require an UploadSession
        if (strlen($contents) > 4194304) {
            $stream = fopen('php://temp', 'r+');
            fwrite($stream, $contents);
            rewind($stream);
            $this->writeStream($path, $stream, $config);

            return;
        }


        $file_name = basename($path);
        $parentItem = $this->getUrlToPath(dirname($path));
        $dirItem = $this->graph
            ->createRequest(
                'PUT',
                $this->getDriveItemUrl($parentItem) . ":/$file_name:/content"
            )
            ->addHeaders([
                'Content-Type' => 'text/plain',
            ])
            ->attachBody($contents)
            ->execute();
    }

    private function getUploadSessionUrl(string $path): string
    {
        return "/drives/$this->drive_id/items/root:/$path:/createUploadSession";
    }

    public function createUploadSession($path): UploadSession
    {
        return $this->graph->createRequest('POST', $this->getUploadSessionUrl($path))->setReturnType(UploadSession::class)->execute();
    }

    /**
     * @param  resource  $contents
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $path = trim($path, '/');
        $this->ensureValidPath($path);
        $upload_session = $this->createUploadSession($path);
        $upload_url = $upload_session->getUploadUrl();

        $meta = fstat($contents);
        $chunk_size = $config->withDefaults($this->options)->get('chunk_size');
        $offset = 0;

        //Chunks have to be uploaded without authorization headers, so we need a fresh guzzle client
        $guzzle = new Guzzle();
        while ($chunk = fread($contents, $chunk_size)) {
            $this->writeChunk($guzzle, $upload_url, $meta['size'], $chunk, $offset);
            $offset += $chunk_size;
        }
    }

    private function writeChunk(Guzzle $guzzle, string $upload_url, int $file_size, string $chunk, int $first_byte, int $retries = 0)
    {
        $last_byte_pos = $first_byte + strlen($chunk) - 1;
        $headers = [
            'Content-Range' => "bytes $first_byte-$last_byte_pos/$file_size",
            'Content-Length' => strlen($chunk),
        ];

        $response = $guzzle->request(
            'PUT',
            $upload_url,
            [
                'headers' => $headers,
                'body' => $chunk,
                'timeout' => $this->options['request_timeout'],
            ]
        );

        if ($response->getStatusCode() === 404) {
            throw new \Exception('Upload URL has expired, please create new upload session');
        }

        if ($response->getStatusCode() === 429) {
            sleep($response->getHeader('Retry-After')[0] ?? 1);
            $this->writeChunk($guzzle, $upload_url, $file_size, $chunk, $first_byte, $retries + 1);
        }

        if ($response->getStatusCode() >= 500) {
            //Server errors happen sometimes. Wait a bit and retry
            if ($retries > 9) {
                //After 10 tries we're probably not gonna get anywhere
                throw new \Exception('Upload failed after 10 attempts.');
            }
            sleep(pow(2, $retries));
            $this->writeChunk($guzzle, $upload_url, $file_size, $chunk, $first_byte, $retries + 1);
        }

        if (($file_size - 1) == $last_byte_pos) {
            if ($response->getStatusCode() === 409) {
                throw new \Exception('File name conflict. A file with the same name already exists at target destination.');
            }

            if (in_array($response->getStatusCode(), [200, 201])) {
                $response = new GraphResponse(
                    $this->graph->createRequest('', ''),
                    $response->getBody(),
                    $response->getStatusCode(),
                    $response->getHeaders()
                );

                $item = $response->getResponseAsObject(DriveItem::class);

                return $item;
            }

            throw new \Exception(
                'Unknown error occured while uploading last part of file. HTTP response code is ' . $response->getStatusCode()
            );
        }

        if ($response->getStatusCode() !== 202) {
            throw new \Exception('Unknown error occured while trying to upload file chunk. HTTP status code is ' . $response->getStatusCode());
        }

        return false;
    }

    public function read(string $path): string
    {
        if (!($stream = $this->readStream($path))) {
            throw new UnableToReadFile('Unable to read file at ' . $path);
        }

        return stream_get_contents($stream);
    }

    public function readStream(string $path)
    {
        $path = $this->getUrlToPath($path);

        $driveitem = $this->getDriveItem($path);
        //ensure we're dealing with a file
        if ($driveitem->getFile() == null) {
            throw new UnableToReadFile("Drive item at $path is not a file");
        }
        $download_url = $driveitem->getProperties()['@microsoft.graph.downloadUrl'];

        $guzzle = new Guzzle();
        $response = $guzzle->request(
            'GET',
            $download_url,
        );

        return StreamWrapper::getResource($response->getBody());
    }

    public function delete(string $path): void
    {
        $path = $this->getUrlToPath($path);

        $this->graph
            ->createRequest(
                'DELETE',
                $this->getDriveItemUrl($path)
            )
            ->execute()
            ->getBody();
    }

    public function deleteDirectory(string $path): void
    {
        $this->delete($path);
    }

    public function getChildrenUrl(string $path): string
    {
        if ($path === '' || $path === '.' || $path === '/') {
            return $this->getDriveRootUrl() . '/children';
        }

        return $this->getDriveRootUrl() . ':/' . $path . ':/children';
    }

    public function createDirectory(string $path, Config $config): void
    {
        if ($this->options['directory_conflict_behavior'] == static::CONFLICT_BEHAVIOR_IGNORE && $this->directoryExists($path)) {
            return;
        }

        $newDirPathArray = explode('/', $path);
        $newDirName = array_pop($newDirPathArray);
        $path = implode('/', $newDirPathArray);
        $body = [
            'name' => $newDirName,
            'folder' => new \stdClass(),
        ];
        if ($this->options['directory_conflict_behavior'] !== static::CONFLICT_BEHAVIOR_IGNORE) {
            $body['@microsoft.graph.conflictBehavior'] = $this->options['directory_conflict_behavior'];
        }
        $dirItem = $this->graph
            ->createRequest(
                'POST',
                $this->getChildrenUrl($path)
            )
            ->attachBody($body)
            ->setReturnType(DriveItem::class)
            ->execute();
    }

    public function setVisibility(string $path, string $visibility): void
    {
        throw UnableToSetVisibility::atLocation($path, 'Unsupported Operation');
    }

    public function visibility(string $path): FileAttributes
    {
        throw UnableToRetrieveMetadata::visibility($path, 'Unsupported Operation');
    }

    public function mimeType(string $path): FileAttributes
    {
        $item = $this->getDriveItem(
            $path = $this->getUrlToPath($path)
        );

        return FileAttributes::fromArray([
            StorageAttributes::ATTRIBUTE_PATH => $path,
            StorageAttributes::ATTRIBUTE_MIME_TYPE => $item->getFile()
                ? $item->getFile()->getMimeType()
                : null,
        ]);
    }

    public function lastModified(string $path): FileAttributes
    {
        return FileAttributes::fromArray([
            StorageAttributes::ATTRIBUTE_PATH => $path,
            StorageAttributes::ATTRIBUTE_LAST_MODIFIED => $this->getDriveItem(
                $path = $this->getUrlToPath($path)
            )
                ->getLastModifiedDateTime()
                ->getTimestamp(),
        ]);
    }

    public function file_size(string $path): FileAttributes
    {
        return FileAttributes::fromArray([
            StorageAttributes::ATTRIBUTE_PATH => $path,
            StorageAttributes::ATTRIBUTE_FILE_SIZE => $this->getDriveItem(
                $path = $this->getUrlToPath($path)
            )->getSize(),
        ]);
    }

    /**
     * @return array<string, mixed>
     *
     * @throws \Exception
     */
    public function listContents(string $directory, bool $deep): iterable
    {
        $path = $directory ? $this->getUrlToPath($directory) . ':/children' : '/drives/' . $this->drive_id . '/root/children';

        /** @var DriveItem[] $items */
        $items = [];
        $request = $this->graph
            ->createCollectionRequest('GET', $path)
            ->setReturnType(DriveItem::class);
        while (!$request->isEnd()) {
            $items = array_merge($items, $request->getPage());
        }
        if ($deep) {
            $folders = array_filter($items, fn ($item) => $item->getFolder() !== null);
            while (count($folders)) {
                $folder = array_pop($folders);
                $folder_path = $folder->getParentReference()->getPath() . DIRECTORY_SEPARATOR . $folder->getName();
                $children = $this->getChildren($folder_path);
                $items = array_merge($items, $children);
                $folders = array_merge($folders, array_filter($children, fn ($child) => $child->getFolder() !== null));
            }
        }

        return $this->convertDriveItemsToStorageAttributes($items);
    }

    private function convertDriveItemsToStorageAttributes(array $drive_items)
    {
        return array_map(function (DriveItem $item) {
            $class = $item->getFile() ? FileAttributes::class : DirectoryAttributes::class;
            $path = $item->getParentReference()->getPath() . DIRECTORY_SEPARATOR . $item->getName();
            $driveless_path = array_reverse(explode('root:', $path, 2))[0];
            return $class::fromArray([
                StorageAttributes::ATTRIBUTE_TYPE => $item->getFile() ? StorageAttributes::TYPE_FILE : StorageAttributes::TYPE_DIRECTORY,
                StorageAttributes::ATTRIBUTE_PATH => $driveless_path,
                StorageAttributes::ATTRIBUTE_LAST_MODIFIED => $item->getLastModifiedDateTime()->getTimestamp(),
                StorageAttributes::ATTRIBUTE_FILE_SIZE => $item->getSize(),
                StorageAttributes::ATTRIBUTE_MIME_TYPE => $item->getFile()
                    ? $item->getFile()->getMimeType()
                    : null,
                'visibility' => 'public',
            ]);
        }, $drive_items);
    }

    private function getChildren($directory)
    {
        $path = $directory . ':/children';
        $request = $this->graph
            ->createCollectionRequest('GET', $path)
            ->setReturnType(DriveItem::class);
        /** @var DriveItem[] $items */
        $items = [];
        while (!$request->isEnd()) {
            $items = array_merge($items, $request->getPage());
        }

        return $items;
    }

    public function move(string $source, string $destination, Config $config): void
    {
        $destination = trim($destination, '/');
        $this->ensureValidPath($destination);
        $source = $this->getUrlToPath($source);

        $newFilePathArray = explode('/', $destination);
        $newFileName = array_pop($newFilePathArray);
        $newPath = count($newFilePathArray)
            ? $this->getUrlToPath(implode('/', $newFilePathArray))
            : $this->getDriveRootUrl();

        $this->graph
            ->createRequest(
                'PATCH',
                $this->getDriveItemUrl($source)
            )
            ->attachBody([
                'parentReference' => [
                    'driveId' => $this->drive_id,
                    'id' => $this->getFile($newPath)->getId(),
                ],
                'name' => $newFileName,
            ])
            ->execute()
            ->getBody();
    }

    public function copy(string $source, string $destination, Config $config): void
    {
        $destination = trim($destination, '/');
        $this->ensureValidPath($destination);

        $source = $this->getUrlToPath($source);

        $newFilePathArray = explode('/', $destination);
        $newFileName = array_pop($newFilePathArray);
        $newPath = count($newFilePathArray)
            ? $this->getUrlToPath(implode('/', $newFilePathArray))
            : $this->getDriveRootUrl();

        $this->graph
            ->createRequest(
                'POST',
                $this->getDriveItemUrl($source) . '/copy'
            )
            ->attachBody([
                'parentReference' => [
                    'driveId' => $this->drive_id,
                    'id' => $this->getFile($newPath)->getId(),
                ],
                'name' => $newFileName,
            ])
            ->execute()
            ->getBody();
    }

    private function getFileAttributes(string $path): FileAttributes
    {
        $file = $this->getDriveItem($path);

        return new FileAttributes(
            $path,
            $file->getSize(),
            null,
            $file->getLastModifiedDateTime()->getTimestamp(),
            $file->getFile()->getMimeType(),
            $file->getFile()->getProperties(),
        );
    }

    protected function ensureDirectoryExists(string $path)
    {
        if (!$this->directoryExists($path)) {
            $this->createDirectory($path, new Config());
        }
    }

    public function fileSize(string $path): FileAttributes
    {
        $path = $this->getUrlToPath($path);

        return $this->getFileAttributes($path);
    }

    public function getFile(string $path): File
    {
        return $this->graph
            ->createRequest('GET', $path)
            ->setReturnType(File::class)
            ->execute();
    }

    public function getDirectory(string $path): Directory
    {
        return $this->graph
            ->createRequest('GET', $path)
            ->setReturnType(Directory::class)
            ->execute();
    }

    public function getDriveItem(string $path): DriveItem
    {
        return $this->graph
            ->createRequest('GET', $path)
            ->setReturnType(DriveItem::class)
            ->execute();
    }

    public function setDriveId(string $driveId)
    {
        $this->drive_id = $driveId;
    }
}
