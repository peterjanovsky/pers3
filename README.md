pers3 (per se)

A Scala interface for Amazon S3.  Below are examples using Play's Iteratee to support multipart uploads

S3 multipart uploads support chunk sizes of 5 MB, unless the chunk is the last chunk.
```Scala
val CHUNK_SIZE = 5242880 // 5 MB
```

Define a multipart body parser 
```Scala
val multipartBodyParser = BodyParser { request =>
  parse.multipartFormData(filePartHandler(request)).apply(request)
}
```

Define a part handler to upload the chunks
```Scala
def filePartHandler(request: RequestHeader): PartHandler[FilePart[Array[Byte]]] = {

  handleFilePart {

    case FileInfo(partName, fileName, contentType) => TO_BE_DEFINED

}
```
