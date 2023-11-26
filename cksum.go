package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func listObjects(ctx context.Context, client *s3.Client) {
	// Get the first page of results for ListObjectsV2 for a bucket
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String("sheik"),
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("first page results:")
	for _, object := range output.Contents {
		log.Printf("key=%s size=%d", aws.ToString(object.Key), object.Size)
	}
} /* listObjects */

func putObject1(ctx context.Context, client *s3.Client) {

	body := fmt.Sprintf("body for %s/%s", "sheik", "fookeroo")
	fmt.Printf("body: " + body)

	poinput := &s3.PutObjectInput{
		Bucket:            aws.String("sheik"),
		Key:               aws.String("fookeroo"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		/* ChecksumAlgorithm: types.ChecksumAlgorithmCrc32c, */
		Body: strings.NewReader(body),
	}

	_, err := client.PutObject(ctx, poinput)
	consume(err)

} /* putObject1 */

func consume(e error) {
	if e != nil {
		panic(e)
	}
}

func uploadByManager(ctx context.Context, client *s3.Client) {
	uploader := manager.NewUploader(client,
		func(u *manager.Uploader) {
			u.PartSize = 5 * 1024 * 1024
			u.Concurrency = 1
		})

	filename := "initramfs-0-rescue-f7f7c386986a44ca8d033b3f84ebc0ce.img"
	f, err := os.Open(filename)
	if err == nil {
		defer f.Close()
		_, err2 := uploader.Upload(context.Background(), &s3.PutObjectInput{
			Bucket:            aws.String("sheik"),
			Key:               aws.String(filename),
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			Body:              f,
		})
		consume(err2)
	}

	// uploader := manager.NewUploader(client manager.UploadAPIClient, options ...func(*manager.Uploader))
}

func main() {

	/* one day we will use these directly */
	access_key := "0555b35654ad1656d804"
	secret_key := "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="

	fmt.Printf("start! acc: %s secret: %s\n",
		access_key, secret_key)

	/* aws-sdk-go-v2 threads a context parameter which can be empty, so
	   just create one to pass around */
	ctx := context.TODO()

	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatal(err)
	}

	/* Create an Amazon S3 service client willing to accept a
	   self-signed ssl certificate */
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	httpclient := &http.Client{Transport: tr}
	cfg.HTTPClient = httpclient

	client := s3.NewFromConfig(cfg,
		func(o *s3.Options) {
			o.UsePathStyle = true
		})

	//listObjects(ctx, client)
	putObject1(ctx, client)
	//uploadByManager(ctx, client)
} /* main */
