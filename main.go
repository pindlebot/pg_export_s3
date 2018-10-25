package main

import (
	"flag"
	"fmt"
	"os/exec"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const selectDatabases = "select datname from pg_database where datistemplate = false"

func putObject(bucket string, key string, data string) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(endpoints.UsEast1RegionID),
	}))
	svc := s3.New(sess)
	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   aws.ReadSeekCloser(strings.NewReader(data)),
	})
	if err != nil {
		panic(err)
	}
}

func fetchSchema(databaseName string, bucket string, prefix string) {
	filename := databaseName + ".sql"
	key := path.Join(prefix, filename)
	fmt.Println("Writing schema to s3://" + bucket + "/" + prefix)
	out, _ := exec.Command("pg_dump", "-U", "postgres", "-s", "-d", databaseName).Output()
	data := string(out)
	putObject(bucket, key, data)
}

func main() {
	bucketPtr := flag.String("bucket", "", "AWS bucket")
	prefixPtr := flag.String("prefix", "", "AWS key prefix")

	flag.Parse()
	out, _ := exec.Command("psql", "-U", "postgres", "-t", "-c", selectDatabases).Output()
	databases := strings.Split(string(out), "\n")
	for _, name := range databases {
		db := strings.Trim(name, " ")
		if len(db) > 0 {
			fmt.Println(db)
			fetchSchema(db, *bucketPtr, *prefixPtr)
		}
	}
}
