package com.pjanof.tools

import java.io.ByteArrayInputStream
import java.util.ArrayList

import scala.util.control.Exception._

import scalaz._
import Scalaz._

import org.slf4j.LoggerFactory

// AWS
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException

import com.amazonaws.auth.BasicAWSCredentials

// S3
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult
import com.amazonaws.services.s3.model.DeleteObjectRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult
import com.amazonaws.services.s3.model.PartETag
import com.amazonaws.services.s3.model.UploadPartRequest
import com.amazonaws.services.s3.model.UploadPartResult

class S3(key: String, secret: String, bucket: String) {

  private val logger = LoggerFactory.getLogger(getClass)

  sealed trait Error {
    val msg: String
    val e: Throwable = new Exception(msg)
  }

  case class ServiceException(msg: String) extends Error
  case class ClientException(msg: String) extends Error
  case class GenericException(msg: String) extends Error

  type Result[+X] = Error \/ X

  implicit class ResultOps[X](x: X) {
    def s: Result[X] = \/.right(x)
  }

  implicit class ErrorOps[E <: Error](e: E) {
    def f[X]: Result[X] = \/.left(e)
  }

  def handleExceptions[T](body: => T, op: String): Result[T] = catching( classOf[AmazonServiceException], classOf[AmazonClientException], classOf[Exception] ).either( body ).fold(
    { x: Throwable => x match {
      case ase: AmazonServiceException => {
        logger.error("Amazon S3 Request Rejected with Error Message: [ %s ] | HTTP Status Code: [ %s ] | AWS Error Code: [ %s ] | Error Type: [ %s ] | Request ID: [ %s ]"
          .format(ase.getMessage()
                  , ase.getStatusCode()
                  , ase.getErrorCode()
                  , ase.getErrorType()
                  , ase.getRequestId()))
        ServiceException("Origin Rejected %s Request".format(op)).f
      }
      case ace: AmazonClientException => {
        logger.error("Amazon S3 Client Internal Communication Error with S3: [ %s ]".format(ace.getMessage()))
        ClientException("Communication Error with Origin for %s Request".format(op)).f
      }
      case ex: Exception => {
        logger.error("Amazon S3 General Exception: [ %s ]".format(ex.getMessage()))
        GenericException("Error Processing %s Request".format(op)).f
      } } }
    , x => x.s
  )

  private def getClient(): AmazonS3 = new AmazonS3Client(new BasicAWSCredentials(key, secret))

  def requestUploadId(key: String): Result[String] = {

    logger.debug("Requesting Multipart Upload Id from Amazon S3: [ key: %s ]".format(key));

    val client: AmazonS3 = getClient

    handleExceptions[String]({
      val request: InitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(bucket, key)
      val response: InitiateMultipartUploadResult = client.initiateMultipartUpload(request)
      response.getUploadId()
    }, "Multipart Upload Id")
  }

  def cancelUpload(key: String, uploadId: String): Result[Unit] = {

    logger.debug("Canceling Upload of Parts to Amazon S3: [ key: %s | uploadId: %s ]".format(key, uploadId));

    val client: AmazonS3 = getClient

    handleExceptions[Unit]({
      val abortRequest: AbortMultipartUploadRequest = new AbortMultipartUploadRequest(bucket, key, uploadId)
      client.abortMultipartUpload(abortRequest)
    }, "Upload Cancellation")
  }

  def deleteObject(key: String): Result[Unit] = {

    logger.debug("Deleting Object from Amazon S3: [ key: %s ]".format(key));

    val client: AmazonS3 = getClient

    handleExceptions[Unit]({
      val deleteRequest: DeleteObjectRequest = new DeleteObjectRequest(bucket, key)
      client.deleteObject(deleteRequest)
    }, "Asset Removal")
  }

  def uploadPart(key: String, uploadId: String, partNumber: Int, partSize: Int, bytes: Array[Byte]): Result[PartETag] = {

    logger.debug("Uploading Part to Amazon S3: [ key: %s | uploadId: %s | partNumber: %s | partSize: %s ]".format(key, uploadId, partNumber, partSize));

    val client: AmazonS3 = getClient

    handleExceptions[PartETag]({
      val bais: ByteArrayInputStream = new ByteArrayInputStream(bytes)
      val uploadRequest: UploadPartRequest = new UploadPartRequest().withBucketName(bucket)
                                                                    .withInputStream(bais)
                                                                    .withKey(key)
                                                                    .withPartNumber(partNumber)
                                                                    .withPartSize(partSize)
                                                                    .withUploadId(uploadId)
      client.uploadPart(uploadRequest).getPartETag()
    }, "Asset Chunk Upload")
  }

  def completeUpload(key: String, uploadId: String, partETags: ArrayList[PartETag]): Result[CompleteMultipartUploadResult] = {

    logger.debug("Completing Upload of Parts to Amazon S3: [ key: %s | uploadId: %s | partETags: %s ]".format(key, uploadId, partETags.toArray.mkString(",")));

    val client: AmazonS3 = getClient

    handleExceptions[CompleteMultipartUploadResult]({
      val completeRequest: CompleteMultipartUploadRequest = new CompleteMultipartUploadRequest(bucket, key, uploadId, partETags)
      client.completeMultipartUpload(completeRequest)
    }, "Asset Upload Completion")
  }
}
