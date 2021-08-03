<p align="right">
  ⭐ &nbsp;&nbsp;<strong>the project to show your appreciation.</strong> :arrow_upper_right:
</p>

# AZFS - io/fs Implementations for Azure

[![GoDev](https://img.shields.io/static/v1?label=godev&message=reference&color=00add8)](https://pkg.go.dev/github.com/element-of-surprise/azfs)

## Introduction

One of the newest and more interesting features to be added to the standard library in the last few years is the addition of the `io/fs` package.  The interfaces in this package will allow developers to use common tools to interact with different filesystems in the same manner package `io` has allowed us to do this with files.

The most common use of `fs.FS` that people are raving about is the use of the `embed` package to embed files at compile time into a `fs.FS` for use in a web server. I think that we are just scratching the surface of what could be done with `fs.FS`.

This package implements the interfaces defined in `fs.FS` + a few additional interfaces for file writing that use Azure Blob Storage as the underlying storage mechanism.

Note that this is NOT an official Microsoft SDK or product. Support may be limited.  This translates to **try it out before baking it into a production service**.

## Azure Blob Storage Access

The `blob/` directory contains an `io/fs` compatible version of the Azure Go Blob Storage package.  There are a few things to note here (and more details in the godoc):

- Currently we only support Block Blobs (no Pages or Append)
- This package also implements opening files for writing, which is not defined in package `fs`
- We support any of the standard token credentials, but we highly encourage use of MSI and therefore have first class support via the `blob/auth/msi` package.

The following sections will give an overview of using this implemenation with Blob Storage.

### Open a Blob storage container:
```go
cred, err := msi.Token(msi.AppID{ID: "your app ID"})
if err != nil {
    panic(err)
}

fsys, err := blob.NewFS("account", "container", *cred)
if err != nil {
    // Do something
}
```

### Read an entire file and print it out:
```go
b, err := fsys.ReadFile("users/jdoak.json")
if err != nil {
    // Do something
}

fmt.Println(string(b))
```

### Stream a file to stdout:
```go
file, err := fsys.Open("users/jdoak.json")
if err != nil {
    // Do something
}
defer file.Close()

if _, err := io.Copy(os.Stdout, file); err != nil {
    // Do something
}
```

### Copy a file from the local file system to Blob Storage:
```go
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
if err := dst.Close(); err != nil {
    // Do something
}
```

### Write a string to a file:
```go
file, err := fsys.OpenFile("users/jdoak.json", O_WRONLY | O_CREATE)
if err != nil {
    // Do something
}

if _, err := io.WriteString(file, `{"Name":"John Doak"}`); err != nil {
    // Do something
}

// The file is not actually written until the file is closed, so it is
// important to know if Close() had an error.
if err := file.Close(); err != nil {
    // Do something
}
```

### Write a file with a lock to prevent other writes:
```go
// The blob.WithLock() option will take a lock on the file when the file is opened.
// No other writer may open this file.
file, err := fsys.OpenFile("users/jdoak.json", O_WRONLY | O_CREATE, blob.WithLock())
if err != nil {
    // Do something
}

if _, err := file.Write(`{"Name":"John Doak"}`); err != nil {
    // Do something
}

// The file is not actually written until the file is closed, so it is
// important to know if Close() had an error.
if err := file.Close(); err != nil {
    // Do something
}
```

### Walk the file system using fs.WalkDir() and log all directories:
```go
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
```