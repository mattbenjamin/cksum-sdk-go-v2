package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

	poinput := &s3.PutObjectInput{
		Bucket: aws.String("sheik"),
		Key:    aws.String("fookeroo"),
		Body:   strings.NewReader(body),
	}

	_, _ = client.PutObject(ctx, poinput)

	//output, err := client.PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options))

} /* putObject1 */

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

	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg,
		func(o *s3.Options) {
			o.UsePathStyle = true
		})

	listObjects(ctx, client)
	putObject1(ctx, client)
} /* main */
