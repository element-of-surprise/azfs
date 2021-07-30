/*
Package blob is an implementation of the io.FS for Azure blob storage. This package
foresakes all the options offered by the standard azure storage package to simplify
use. If you need options not provided here, your best solution is probably to use
the standard package.

This package supports two additional features over io.FS capabilities:
- Writing files opened with OpenFile()
- Locking files

This currently only support Block Blobs, not Append or Page. We may offer that
in the future with enough demand.

Open a Blob storage container:
	cred, err := msi.Token(msi.SystemAssigned{Resource: "https://resource"})
	if err != nil {
		panic(err)
	}

	fsys, err := NewFS("account", "container", *cred)
	if err != nil {
		// Do something
	}

Read an entire file:
	file, err := fsys.Open("users/jdoak.json")
	if err != nil {
		// Do something
	}

	b, err := io.ReadAll(file)
	if err != nil {
		// Do something
	}

	fmt.Println(string(b))

Stream a file to stdout:
	file, err := fsys.Open("users/jdoak.json")
	if err != nil {
		// Do something
	}

	if _, err := io.Copy(os.Stdout, file); err != nil {
		// Do something
	}

Copy a file:
	src, err := os.Open("path/to/some/file")
	if err != nil {
		// Do something
	}

	dst, err := fsys.OpenFile("path/to/place/content", O_WRONLY | O_CREATE)
	if err != nil {
		// Do something
	}

	if _, err := io.Copy(dst, src); err != nil {
		// Do something
	}

	// The file is not actually written until the file is closed, so it is
	// important to know if Close() had an error.
	if err := file.Close(); err != nil {
		// Do something
	}

Write a string to a file:
	file, err := fsys.OpenFile("users/jdoak.json", O_WRONLY | O_CREATE)
	if err != nil {
		// Do something
	}

	if _, err := io.WriteString(file, `{"Name":"John Doak"}`) (n int, err error); err != nil {
		// Do something
	}

	// The file is not actually written until the file is closed, so it is
	// important to know if Close() had an error.
	if err := file.Close(); err != nil {
		// Do something
	}

Walk the file system and log all directories:
	err := fs.WalkDir(
		fsys,
		".",
		func(path string, d fs.DirEntry, err error) error {
			if !d.IsDir() {
				return nil
			}
			log.Println("dir: ", path)
			return nil
		},
	)
	if err != nil {
		// Do something
	}
*/
package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/url"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"golang.org/x/sync/errgroup"
)

// File implements io.FS.File and io.Writer for blobs.
type File struct {
	flags   int
	contURL azblob.ContainerURL // Only set if File is a directory.
	u       azblob.BlockBlobURL
	fi      fileInfo
	path    string // The full path, used for directories

	mu sync.Mutex

	// For files that can be read.
	reader io.ReadCloser
	// For files that can write.
	writer io.WriteCloser
	// writeErr indicates if we have an error with writing.
	writeErr  error
	writeWait sync.WaitGroup

	transferManager azblob.TransferManager

	dirReader *dirReader // Usee when this represents a directory
}

// Read implements fs.File.Read().
func (f *File) Read(p []byte) (n int, err error) {
	if isFlagSet(f.flags, O_RDONLY) {
		return 0, fmt.Errorf("File is not set to O_RDONLY")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.reader == nil {
		if err := f.fetchReader(); err != nil {
			return 0, err
		}
	}

	return f.reader.Read(p)
}

// Write implements io.Writer.Write().
func (f *File) Write(p []byte) (n int, err error) {
	if !isFlagSet(f.flags, O_WRONLY) {
		return 0, errors.New("cannot write to file without flag O_WRONLY")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writer == nil {
		r, w := io.Pipe()
		f.writer = w

		f.writeWait.Add(1)
		go func() {
			defer f.writeWait.Done()
			_, err := azblob.UploadStreamToBlockBlob(
				context.Background(),
				r,
				f.u.ToBlockBlobURL(),
				azblob.UploadStreamToBlockBlobOptions{TransferManager: f.transferManager},
			)
			if err != nil {
				f.mu.Lock()
				defer f.mu.Unlock()
				if f.writeErr == nil {
					f.writeErr = err
				}
			}
		}()
	}
	if f.writeErr != nil {
		return 0, err
	}

	return f.writer.Write(p)
}

// Close implements fs.File.Close().
func (f *File) Close() error {
	if f.reader != nil {
		return f.reader.Close()
	}
	if f.writer != nil {
		f.writer.Close()
		f.writeWait.Wait()
		return f.writeErr
	}
	return nil
}

// Stat implements fs.File.Stat().
func (f *File) Stat() (fs.FileInfo, error) {
	return f.fi, nil
}

func (f *File) fetchReader() error {
	resp, err := f.u.Download(context.Background(), 0, 0, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return err
	}

	f.reader = resp.Body(azblob.RetryReaderOptions{})
	return nil
}

// ReadDir implements fs.ReadDirFile.ReadDir().
func (f *File) ReadDir(n int) ([]fs.DirEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.fi.dir {
		return nil, fmt.Errorf("File is not a directory")
	}

	if f.dirReader == nil {
		dr, err := newDirReader(f.path, f.contURL)
		if err != nil {
			return nil, err
		}
		f.dirReader = dr
	}
	return f.dirReader.ReadDir(n)
}

type dirReader struct {
	name    string
	path    string
	contURL azblob.ContainerURL
	items   []fs.DirEntry
	index   int
}

func newDirReader(dirPath string, contURL azblob.ContainerURL) (*dirReader, error) {
	dr := &dirReader{
		name:    path.Base(dirPath),
		path:    dirPath,
		contURL: contURL,
	}
	if err := dr.get(); err != nil {
		return nil, err
	}
	return dr, nil
}

func (d *dirReader) ReadDir(n int) ([]fs.DirEntry, error) {
	if n <= 0 {
		return d.items, nil
	}

	if d.index < len(d.items) {
		ret := d.items[d.index:]
		d.index += n
		return ret, nil
	}

	return nil, io.EOF
}

func (d *dirReader) get() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if d.path == "." {
		d.path = ""
	} else {
		// If we are the root, you can't use '/' in the prefix, but if you are
		// not the root, you MUST use '/'.
		d.path += "/"
	}

	resp, err := d.contURL.ListBlobsHierarchySegment(
		ctx,
		azblob.Marker{},
		"/",
		azblob.ListBlobsSegmentOptions{
			Prefix:     d.path,
			MaxResults: math.MaxInt32,
		},
	)
	if err != nil {
		return err
	}

	for _, prefix := range resp.Segment.BlobPrefixes {
		n := path.Base(prefix.Name)
		item := &dirEntry{
			name: n,
			fi: fileInfo{
				name: n,
				dir:  true,
			},
		}
		d.items = append(d.items, item)
	}

	mu := sync.Mutex{}
	g, ctx := errgroup.WithContext(ctx)
	limiter := make(chan struct{}, 20)
	for _, blob := range resp.Segment.BlobItems {
		blob = blob
		n := path.Base(blob.Name)

		limiter <- struct{}{}
		g.Go(func() error {
			defer func() { <-limiter }()

			u := d.contURL.NewBlobURL(blob.Name)
			resp, err := u.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
			if err == nil {
				mu.Lock()
				defer mu.Unlock()
				d.items = append(d.items, &dirEntry{name: n, fi: newFileInfo(n, resp)})
			}
			return err
		})
	}
	return g.Wait()
}

type dirEntry struct {
	name string
	fi   fs.FileInfo
}

func (d dirEntry) Name() string {
	return d.name
}

func (d dirEntry) IsDir() bool {
	return d.fi.IsDir()
}

func (d dirEntry) Type() fs.FileMode {
	return d.fi.Mode()
}

func (d dirEntry) Info() (fs.FileInfo, error) {
	return d.fi, nil
}

/*
type Notification chan struct{}

type Lock struct {
	mu     sync.Mutex
	notify Notification
}

func (l *Lock) Unlock() {

}

func (l *Lock) Renew(d *time.Duration) {

}

func (l *Lock) Notify() Notification {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.notify == nil {
		l.notify = make(Notification)
	}
	return l.notify
}
*/

// FS implements io/fs.FS
type FS struct {
	containerURL azblob.ContainerURL
}

// NewFS is the constructor for FS. It is recommended that you use blob/auth/msi to create
// the "cred".
func NewFS(account, container string, cred azblob.Credential) (*FS, error) {
	p := azblob.NewPipeline(cred, azblob.PipelineOptions{})
	blobPrimaryURL, _ := url.Parse("https://" + account + ".blob.core.windows.net/")
	bsu := azblob.NewServiceURL(*blobPrimaryURL, p)

	return &FS{
		containerURL: bsu.NewContainerURL(container),
	}, nil
}

// Open implements fs.FS.Open().
func (f *FS) Open(name string) (fs.File, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	u := f.containerURL.NewBlobURL(name)

	props, err := u.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return f.dirFile(ctx, name)
	}

	switch props.BlobType() {
	case azblob.BlobBlockBlob:
		return &File{
			contURL: f.containerURL,
			flags:   O_RDONLY,
			u:       u.ToBlockBlobURL(),
			fi:      newFileInfo(path.Base(name), props),
		}, nil
	}
	return nil, fmt.Errorf("%T type blobs are not currently supported", props.BlobType())
}

// ReadFile implements fs.ReadFileFS.ReadFile.
func (f *FS) ReadFile(name string) ([]byte, error) {
	file, err := f.Open(name)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(file)
}

func (f *FS) ReadDir(name string) ([]fs.DirEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if name == "." {
		name = ""
	}

	u := f.containerURL.NewBlobURL(name)

	_, err := u.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err == nil {
		return nil, fmt.Errorf("ReadDir(%s) does not appear to be a directory", name)
	}

	file, err := f.dirFile(ctx, name)
	if err != nil {
		return nil, err
	}

	return file.ReadDir(-1)
}

// Stat implements fs.StatFS.Stat.
func (f *FS) Stat(name string) (fs.FileInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir, err := f.dirFile(ctx, name)
	if err == nil {
		return dir.fi, nil
	}
	u := f.containerURL.NewBlobURL(name)

	props, err := u.GetProperties(ctx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
	if err != nil {
		return nil, err
	}
	return newFileInfo(name, props), nil
}

func (f *FS) dirFile(ctx context.Context, name string) (*File, error) {
	switch name {
	case ".", "":
		return &File{
			path:    ".",
			contURL: f.containerURL,
			fi: fileInfo{
				name: ".",
				dir:  true,
			},
		}, nil
	}

	resp, err := f.containerURL.ListBlobsHierarchySegment(
		ctx,
		azblob.Marker{},
		"/",
		azblob.ListBlobsSegmentOptions{Prefix: name + `/`, MaxResults: math.MaxInt32},
	)
	if err != nil {
		return nil, err
	}

	if len(resp.Segment.BlobPrefixes) > 0 || len(resp.Segment.BlobItems) > 0 {
		return &File{
			path:    name,
			contURL: f.containerURL,
			fi: fileInfo{
				name: path.Base(name),
				dir:  true,
			},
		}, nil
	}

	return nil, &fs.PathError{
		Op:   "open",
		Path: name,
		Err:  errors.New("no such file or directory"),
	}
}

type rwOptions struct {
	lock bool
	tm   azblob.TransferManager
}

// RWOption are options used for OpenRW().
type RWOption func(o *rwOptions)

// WithLock locks the file and attempts to keep it locked until the file is closed.
func WithLock() RWOption {
	return func(o *rwOptions) {
		o.lock = true
	}
}

// WithTransferManager allows you to provide one of azblob's TransferManagers or your
// own TransferManager for controlling file writes.
func WithTransferManager(tm azblob.TransferManager) RWOption {
	return func(o *rwOptions) {
		o.tm = tm
	}
}

const (
	O_RDONLY int = syscall.O_RDONLY // open the file read-only.
	O_WRONLY int = syscall.O_WRONLY // open the file write-only.
	O_RDWR   int = syscall.O_RDWR   // open the file read-write.
	// The remaining values may be or'ed in to control behavior.
	O_APPEND int = syscall.O_APPEND // append data to the file when writing.
	O_CREATE int = syscall.O_CREAT  // create a new file if none exists.
	O_EXCL   int = syscall.O_EXCL   // used with O_CREATE, file must not exist.
	O_TRUNC  int = syscall.O_TRUNC  // truncate regular writable file when opened.
)

func isFlagSet(flags int, flag int) bool {
	return flags&flag != 0
}

// OpenFile is the generalized open call that provides non-readonly options that Open()
// provides. When creating a new file, this will always be a block blob.
func (f *FS) OpenFile(name string, flags int, options ...RWOption) (*File, error) {
	opts := rwOptions{}
	for _, o := range options {
		o(&opts)
	}

	if opts.lock {
		// Do lock stuff and pass an autorenew lock to *File
		panic("add")
	}

	if isFlagSet(flags, O_RDONLY) {
		if flags > 0 {
			return nil, fmt.Errorf("cannot set any other flag if O_RDONLY is set")
		}
		file, err := f.Open(name)
		if err != nil {
			return nil, err
		}
		return file.(*File), nil
	}

	if isFlagSet(flags, O_EXCL) && !isFlagSet(flags, O_CREATE) {
		return nil, fmt.Errorf("cannot set O_EXCL without O_CREATE")
	}
	if name == "." {
		name = ""
	}

	propCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dir, err := f.dirFile(propCtx, name)
	if err == nil {
		return dir, nil
	}
	u := f.containerURL.NewBlobURL(name)
	props, err := u.GetProperties(propCtx, azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})

	// NOTE: These are not fully implemented because I have no idea what all the return
	// error codes are. So this is generally assuming that the error is that they can't
	// find the file.

	switch {
	// The user didn't specify to create the file and the file did not exist.
	case !isFlagSet(flags, O_CREATE) && err != nil:
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fmt.Errorf("(%s): no such file or directory, if you want to create the file, must pass O_CREATE", err),
		}
	case isFlagSet(flags, O_EXCL) && err == nil:
		return nil, &fs.PathError{
			Op:   "open",
			Path: name,
			Err:  fmt.Errorf("(%s)file already exists and passed O_EXCL", err),
		}
	}

	return &File{
		flags: flags,
		u:     u.ToBlockBlobURL(),
		fi:    newFileInfo(name, props),
	}, nil
}

/*
func (f *FS) Lock(name string, d *time.Duration) (*Lock, error) {
	return nil, nil
}
*/

// Sys is returned on a FileInfo.Sys() call.
type Sys struct {
	// Props holds propertis of the blobstore file.
	Props *azblob.BlobGetPropertiesResponse
}

type fileInfo struct {
	name string
	dir  bool
	size int64
	resp *azblob.BlobGetPropertiesResponse
}

func newFileInfo(name string, resp *azblob.BlobGetPropertiesResponse) fileInfo {
	return fileInfo{
		name: name,
		resp: resp,
	}
}

// Name implements fs.FileInfo.Name().
func (f fileInfo) Name() string {
	return f.name
}

// Size implements fs.FileInfo.Size().
func (f fileInfo) Size() int64 {
	if f.dir {
		return 0
	}
	return f.resp.ContentLength()
}

// Mode implements fs.FileInfo.Mode(). This always returns 0660.
func (f fileInfo) Mode() fs.FileMode {
	if f.dir {
		return 0660 | fs.ModeDir
	}
	return 0660
}

// ModTime implements fs.FileInfo.ModTime(). If the blob is a directory, this
// is always the zero time.
func (f fileInfo) ModTime() time.Time {
	if f.dir {
		return time.Time{}
	}
	return f.resp.LastModified()
}

// IsDir implements fs.FileInfo.IsDir().
func (f fileInfo) IsDir() bool {
	return f.dir
}

// Sys implements fs.FileInfo.Sys(). If this is a dir, this returns nil.
func (f fileInfo) Sys() interface{} {
	if f.dir {
		return nil
	}
	return Sys{Props: f.resp}
}
