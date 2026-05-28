#!/usr/bin/env node

import "dotenv/config";
import { Readable } from "node:stream";
import { list } from "@vercel/blob";
import {
  S3Client,
  PutObjectCommand,
  HeadObjectCommand,
} from "@aws-sdk/client-s3";
import { Command } from "commander";

function createS3Client(options) {
  return new S3Client({
    region: options.region || process.env.AWS_REGION,
    endpoint: options.endpoint || process.env.AWS_ENDPOINT,
    credentials: {
      accessKeyId: options.accessKeyId || process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: options.secretKey || process.env.AWS_SECRET_ACCESS_KEY,
    },
  });
}

async function backupVercelStorageToS3(options) {
  const s3Client = createS3Client(options);
  const bucketName = options.bucket || process.env.AWS_BUCKET_NAME;

  async function fileExistsInS3(key) {
    try {
      await s3Client.send(
        new HeadObjectCommand({
          Bucket: bucketName,
          Key: key,
        }),
      );
      return true;
    } catch (error) {
      if (error.name === "NotFound") return false;
      if (error.$metadata?.httpStatusCode === 404) return false;

      if (error.name === "NoSuchBucket") {
        throw new Error(`Bucket ${bucketName} does not exist`);
      }
      if (error.name === "AccessDenied") {
        throw new Error("Access denied to S3 bucket - check your credentials");
      }
      throw new Error(
        `S3 check failed: ${error.name} - ${error.message} - httpStatusCode: ${error.$metadata?.httpStatusCode}`,
      );
    }
  }

  async function uploadToS3(url, key) {
    try {
      if (await fileExistsInS3(key)) {
        return { skipped: true, key };
      }

      const blobToken = process.env.BLOB_READ_WRITE_TOKEN;
      const isPrivateBlobUrl = url.includes(
        ".private.blob.vercel-storage.com/",
      );
      const fetchHeaders = {};
      if (blobToken) {
        fetchHeaders.Authorization = `Bearer ${blobToken}`;
      } else if (isPrivateBlobUrl) {
        throw new Error(
          "BLOB_READ_WRITE_TOKEN is required to read private Vercel Blob URLs",
        );
      }

      const response = await fetch(url, { headers: fetchHeaders });
      if (!response.ok) {
        throw new Error(
          `Failed to fetch file: ${response.status} ${response.statusText}`,
        );
      }

      if (!response.body) {
        throw new Error("Failed to fetch file body stream");
      }

      const bodyStream = Readable.fromWeb(response.body);
      const contentType = response.headers.get("content-type");
      const contentLengthHeader = response.headers.get("content-length");
      const putObjectInput = {
        Bucket: bucketName,
        Key: key,
        Body: bodyStream,
      };
      if (contentType) {
        putObjectInput.ContentType = contentType;
      }
      if (contentLengthHeader) {
        const contentLength = Number(contentLengthHeader);
        if (Number.isFinite(contentLength) && contentLength >= 0) {
          putObjectInput.ContentLength = contentLength;
        }
      }

      const command = new PutObjectCommand({
        ...putObjectInput,
      });

      await s3Client.send(command);
      return { skipped: false, key };
    } catch (error) {
      if (error.name === "AccessDenied") {
        throw new Error("Access denied to S3 bucket - check your credentials");
      }
      if (error.name === "NoSuchBucket") {
        throw new Error(`Bucket ${bucketName} does not exist`);
      }
      console.error(error);
      throw new Error(`Upload failed for ${key}: ${error.message}`);
    }
  }

  let cursor;
  let totalProcessed = 0;
  let totalFiles = 0;
  const tuningBatchSizes = [10, 20, 50, 100];
  let tuningIndex = 0;
  const tuningResults = [];
  let selectedBatchSize;
  const prefix = options.prefix || "";

  console.log("Starting backup process...");
  console.log(`AWS Endpoint: ${options.endpoint || process.env.AWS_ENDPOINT}`);
  console.log(`Batch size tuning candidates: ${tuningBatchSizes.join(", ")}`);
  console.log(`Prefix: ${prefix}`);
  console.log(`Target bucket: ${bucketName}`);
  console.log(`AWS Region: ${options.region || process.env.AWS_REGION}\n\n\n`);

  console.log("Calculating total files...\n");
  const PAGE_SIZE = 1000;
  let countCursor;
  do {
    const countResult = await list({
      cursor: countCursor,
      limit: 1000,
      prefix,
    });
    totalFiles += countResult.blobs.length;
    countCursor = countResult.cursor;
  } while (countCursor);
  console.log(`Total files to process: ${totalFiles}\n`);

  let nextCursor;
  let listPromise = list({
    cursor,
    limit: PAGE_SIZE,
    prefix,
  });

  async function processBatch(batch) {
    const startedAt = Date.now();
    let uploadedInBatch = 0;
    let skippedInBatch = 0;

    const promises = batch.map((blob) =>
      uploadToS3(blob.url, blob.pathname)
        .then((result) => {
          if (!result.skipped) {
            uploadedInBatch += 1;
          } else {
            skippedInBatch += 1;
          }
        })
        .catch((error) => {
          console.error(`✗ Failed: ${blob.pathname}`, error.message);
        }),
    );

    await Promise.all(promises);
    return {
      elapsedMs: Date.now() - startedAt,
      uploadedInBatch,
      skippedInBatch,
    };
  }

  do {
    const listResult = await listPromise;
    nextCursor = listResult.cursor;
    if (nextCursor) {
      listPromise = list({
        cursor: nextCursor,
        limit: PAGE_SIZE,
        prefix,
      });
    }
    const pageTotal = listResult.blobs.length;

    if (pageTotal > 0) {
      // Process files in batches
      for (let i = 0; i < pageTotal; ) {
        let activeBatchSize = selectedBatchSize;
        let isTuningBatch = false;

        if (!activeBatchSize && tuningIndex < tuningBatchSizes.length) {
          activeBatchSize = tuningBatchSizes[tuningIndex];
          isTuningBatch = true;
          console.log(`Tuning: testing batch size ${activeBatchSize}...`);
        }
        if (!activeBatchSize) {
          const validResults = tuningResults.filter((result) =>
            Number.isFinite(result.avgMsPerUploadedFile),
          );
          if (validResults.length > 0) {
            validResults.sort(
              (a, b) => a.avgMsPerUploadedFile - b.avgMsPerUploadedFile,
            );
            selectedBatchSize = validResults[0].batchSize;
            console.log("Tuning results:");
            for (const result of tuningResults) {
              const avgText = Number.isFinite(result.avgMsPerUploadedFile)
                ? `${result.avgMsPerUploadedFile.toFixed(2)} ms/file`
                : "skipped (no uploaded files)";
              console.log(`- batch ${result.batchSize}: ${avgText}`);
            }
            console.log(`Selected batch size: ${selectedBatchSize}\n`);
          } else {
            selectedBatchSize = tuningBatchSizes[0];
            console.log(
              `Tuning: all sampled files were skipped, fallback batch size ${selectedBatchSize}\n`,
            );
          }
          activeBatchSize = selectedBatchSize;
        }

        const batch = listResult.blobs.slice(i, i + activeBatchSize);
        const { elapsedMs, uploadedInBatch, skippedInBatch } =
          await processBatch(batch);

        i += batch.length;
        totalProcessed += batch.length;

        if (isTuningBatch) {
          if (skippedInBatch > 0) {
            console.log(
              `Tuning: batch ${activeBatchSize} has ${skippedInBatch} skipped files, repeating same batch size`,
            );
          } else {
            const avgMsPerUploadedFile =
              uploadedInBatch > 0 ? elapsedMs / uploadedInBatch : Number.NaN;
            tuningResults.push({
              batchSize: activeBatchSize,
              avgMsPerUploadedFile,
            });
            if (Number.isFinite(avgMsPerUploadedFile)) {
              console.log(
                `Tuning: batch ${activeBatchSize} -> ${avgMsPerUploadedFile.toFixed(2)} ms/file (${uploadedInBatch} uploaded)`,
              );
            } else {
              console.log(
                `Tuning: batch ${activeBatchSize} skipped in metrics (all files skipped)`,
              );
            }
            tuningIndex += 1;
            if (tuningIndex === tuningBatchSizes.length) {
              const validResults = tuningResults.filter((result) =>
                Number.isFinite(result.avgMsPerUploadedFile),
              );
              if (validResults.length > 0) {
                validResults.sort(
                  (a, b) => a.avgMsPerUploadedFile - b.avgMsPerUploadedFile,
                );
                selectedBatchSize = validResults[0].batchSize;
                console.log("Tuning results:");
                for (const result of tuningResults) {
                  const avgText = Number.isFinite(result.avgMsPerUploadedFile)
                    ? `${result.avgMsPerUploadedFile.toFixed(2)} ms/file`
                    : "skipped (no uploaded files)";
                  console.log(`- batch ${result.batchSize}: ${avgText}`);
                }
                console.log(`Selected batch size: ${selectedBatchSize}\n`);
              } else {
                selectedBatchSize = tuningBatchSizes[0];
                console.log(
                  `Tuning: all sampled files were skipped, fallback batch size ${selectedBatchSize}\n`,
                );
              }
            }
          }
        }

        const totalPercent = totalFiles
          ? Math.floor((totalProcessed / totalFiles) * 100)
          : 100;
        const batchElapsedSeconds = Math.max(elapsedMs / 1000, 0.001);
        const filesPerSecond = batch.length / batchElapsedSeconds;
        console.log(
          `Progress: ${totalProcessed}/${totalFiles} (${totalPercent}%) ${filesPerSecond.toFixed(1)} files/sec`,
        );
      }
    }

    cursor = nextCursor;
  } while (nextCursor);

  console.log(
    `Backup complete. Total processed: ${totalProcessed}/${totalFiles}`,
  );
}

// Set up CLI
const program = new Command();

program
  .name("backup-vercel-storage")
  .description("Backup Vercel Blob Storage to S3")
  .version("1.0.0")
  .option(
    "-b, --batch-size <number>",
    "legacy option (ignored, tuning uses 10/20/50/100)",
    "10",
  )
  .option("-p, --prefix <string>", "prefix for files to backup")
  .option("--region <string>", "AWS region")
  .option("--endpoint <string>", "S3 endpoint URL")
  .option("--bucket <string>", "S3 bucket name")
  .option("--access-key-id <string>", "AWS access key ID")
  .option("--secret-key <string>", "AWS secret access key")
  .action(async (options) => {
    try {
      // Check for required parameters/env vars
      const requiredParams = [
        ["region", "AWS_REGION"],
        ["accessKeyId", "AWS_ACCESS_KEY_ID"],
        ["secretKey", "AWS_SECRET_ACCESS_KEY"],
        ["bucket", "AWS_BUCKET_NAME"],
      ];

      const missing = requiredParams.filter(([param, envVar]) => {
        return !options[param] && !process.env[envVar];
      });

      if (missing.length > 0) {
        console.error(
          "Missing required parameters. Please provide either command line arguments or environment variables:",
        );
        missing.forEach(([param, envVar]) => {
          console.error(
            `  --${param.replace(/[A-Z]/g, (m) => `-${m.toLowerCase()}`)} or ${envVar}`,
          );
        });
        process.exit(1);
      }

      await backupVercelStorageToS3(options);
    } catch (error) {
      console.error("\n\n\n\n\n\nBackup process failed:", error.message);
      process.exit(1);
    }
  });

program.parse();
