package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	flags "github.com/jessevdk/go-flags"
)

const selectDatabases = "select datname from pg_database where datistemplate = false"

type reader struct {
	r io.Reader
}

func (r *reader) Read(p []byte) (int, error) {
	return r.r.Read(p)
}

func upload(bucket string, key string, r reader) {
	ctx := context.Background()

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	}))
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = 20 << 20 // 20MB
	})
	_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   &r,
	})
	if err != nil {
		panic(err)
	}
}

var options struct {
	Bucket string `short:"b" long:"bucket" description:"AWS bucket" required:"true"`
	Prefix string `short:"p" long:"prefix" description:"S3 Prefix"`
}

func uploadSchema(databaseName string, bucket string, prefix string) {
	filename := databaseName + ".sql"
	key := path.Join(prefix, filename)
	fmt.Println("Writing schema to s3://" + bucket + "/" + key)
	cmd := exec.Command("pg_dump", "-U", "postgres", "-s", "-h", "ec2-52-204-30-139.compute-1.amazonaws.com", "-d", databaseName)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	r := reader{stdout}
	upload(bucket, key, r)

	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
}

func uploadAll(bucket string, prefix string) {
	current := time.Now().Local()
	d := current.Format("2006-01-02")
	filename := "data-" + d + ".bak"
	key := path.Join(prefix, filename)
	fmt.Println("Writing schema to s3://" + bucket + "/" + key)
	cmd := exec.Command("pg_dumpall", "-U", "postgres", "-h", "ec2-52-204-30-139.compute-1.amazonaws.com")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	r := reader{stdout}
	upload(bucket, key, r)

	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	_, err := flags.Parse(&options)
	if err != nil {
		log.Fatal(err)
	}
	out, _ := exec.Command("psql", "-U", "postgres", "-h", "ec2-52-204-30-139.compute-1.amazonaws.com", "-t", "-c", selectDatabases).Output()
	databases := strings.Split(string(out), "\n")
	for _, name := range databases {
		db := strings.Trim(name, " ")
		if len(db) > 0 {
			uploadSchema(db, options.Bucket, options.Prefix)
		}
	}
	uploadAll(options.Bucket, options.Prefix)
}
