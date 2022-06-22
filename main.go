package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/minio/minio-go/v7"
	credentials2 "github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	//IscsiDB = "/tmp/storage/iscsidb"
	DBPath = "/tank/db"
	// MaximumMultipartNumber is the max multipart number.
	MaximumMultipartNumber = 10000
	// MaximumPartSize is the max part size for single part, 5GB.
	MaximumPartSize = 5 * 1024 * 1024 * 1024
	// MaximumObjectSize is the max object size for a single object, 50TB.
	MaximumObjectSize = MaximumMultipartNumber * MaximumPartSize
	// MaximumAutoMultipartSize is the size limit for auto part size detect.
	MaximumAutoMultipartSize = MaximumPartSize / 5
	// DefaultPartSize is the default part size, 128MB.
	DefaultPartSize = 128 * 1024 * 1024

	Threshold         = 0.85
	DefaultVolumeSize = 128 * 1024 * 1024 * 1024
)

type RecvSnap struct {
	object   string // s3 object (example: 'pool1/file1@snap1')
	snapshot string // after '@' (example: 'snap1')
	full     bool   // Full backup
}
type FakeWriterAt struct {
	w io.Writer
}

func (fw FakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	return fw.w.Write(p) // ignore 'offset' because we forced sequential downloads
}

func CalculatePartSize(size int64) (partSize int64, err error) {
	partSize = DefaultPartSize

	if size > MaximumObjectSize {
		return 0, errors.New("the file is too big !")
	}

	for size/partSize >= int64(MaximumMultipartNumber) {
		if partSize < MaximumAutoMultipartSize {
			partSize = partSize << 1
			continue
		}
		// Try to adjust partSize if it is too small and account for
		// integer division truncation.
		partSize = size/int64(MaximumMultipartNumber) + 1
		break
	}
	return
}

func recvSnapHeadAdd(slice []RecvSnap, elem RecvSnap) []RecvSnap {
	slice = append(slice, elem)
	index := 0
	copy(slice[index+1:], slice[index:])
	slice[index] = elem
	return slice
}

func snapAlreadyExit(snap string) bool {
	cmdList := "zfs list -H -t snapshot -o name"
	cmd := exec.Command("bash", "-c", cmdList)
	cmdout, err := cmd.CombinedOutput()
	if err != nil {
		log.Println(cmdout)
		return false
	}
	list := string(cmdout)
	list = strings.TrimSpace(list)
	lines := strings.Split(list, "\n")

	for _, line := range lines {
		if strings.Contains(line, snap) {
			log.Printf("'%s' exists\n", snap)
			return true
		}
	}
	return false
}

func ZfsRecv(ctx aws.Context, sess *session.Session, bucket, snapshot, filesystem string) (n int64, err error) {
	var s3snaps []RecvSnap
	var s3snap RecvSnap
	var out *s3.ListObjectsOutput

	// 1.Find dependency snapshot
	one := snapshot
	for {
		snaps := strings.Split(one, "@")
		if len(snaps) < 2 {
			return 0, nil
		}
		// filesystem local exist
		localsnap := filesystem + "@" + snaps[1]
		exit := snapAlreadyExit(localsnap)
		if exit {
			break
		}

		// full snapshot
		svc := s3.New(sess)
		_, err = svc.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    &one,
		})
		if err == nil {
			s3snap.object = one
			s3snap.snapshot = snaps[1]
			s3snap.full = true
			s3snaps = recvSnapHeadAdd(s3snaps, s3snap)
			log.Println(s3snap.object)
			break
		}

		// dependency snapshot
		prefix := one + "@"
		out, err = svc.ListObjectsWithContext(ctx, &s3.ListObjectsInput{
			Bucket:  aws.String(bucket),
			MaxKeys: aws.Int64(1),
			Prefix:  aws.String(prefix),
			//Delimiter: aws.String(*delimiter)
		})
		if len(out.Contents) <= 0 {
			break
		}
		for _, object := range out.Contents {
			snaps := strings.Split(*object.Key, "@")
			if len(snaps) > 2 {
				one = snaps[0] + "@" + snaps[1]
				s3snap.object = *object.Key
				s3snap.snapshot = snaps[1]
				s3snap.full = false
				s3snaps = recvSnapHeadAdd(s3snaps, s3snap)
				log.Println(s3snap.object)

				one = snaps[0] + "@" + snaps[2] // next depend
			}
			break
		}
	}

	// 2. S3 download
	var written int64 = 0
	var first bool = true
	for _, s3snap = range s3snaps {
		// The first snapshot format must be "pool/volume@snap1"
		if first {
			snaps := strings.Split(s3snap.object, "@")
			if len(snaps) > 2 {
				errStr := "First full snapshot not found:" + s3snap.object
				log.Println(errStr)
				return 0, errors.New(errStr)
			}
		}
		first = false
		written, err = zfsRecvOneCheck(ctx, sess, bucket, filesystem, &s3snap) // first download depend
		n += written
		log.Printf("size(%v)=%v,total=%v\n", s3snap.object, written, n)
	}

	return n, err
}

func zfsRecvOneCheck(ctx aws.Context, sess *session.Session, bucket, filesystem string, s3snap *RecvSnap) (n int64, err error) {

	// 1. filesystem local exist
	localsnap := filesystem + "@" + s3snap.snapshot
	exit := snapAlreadyExit(localsnap)
	if exit {
		return 0, nil
	}

	// // 2. full backup
	// if s3snap.full {
	// 	cmdString := "zfs destroy -rR " + filesystem
	// 	cmd := exec.Command("bash", "-c", cmdString)
	// 	cmd.CombinedOutput()
	// 	log.Println(cmdString)
	// }

	// // 3.  s3snap.object local exist
	// exit = snapAlreadyExit(s3snap.object)
	// if exit {
	// 	cmdString := "zfs send " + s3snap.object + " | zfs recv " + filesystem
	// 	log.Println(cmdString)
	// 	cmd := exec.Command("bash", "-c", cmdString)
	// 	out, err := cmd.CombinedOutput()
	// 	if err != nil {
	// 		log.Println(string(out))
	// 	} else {
	// 		return 0,nil // no need s3download
	// 	}
	// }

	return zfsRecvOne(ctx, sess, bucket, s3snap.object, filesystem)
}

func zfsRecvOne(ctx aws.Context, sess *session.Session, bucket, object, filesystem string) (n int64, err error) {
	cmdRecv := "zfs recv " + filesystem
	log.Println(bucket + ":" + object + " > " + cmdRecv)

	ctxcmd, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctxcmd, "bash", "-c", cmdRecv)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		fmt.Println("StdinPipe err: ", err.Error())
		return 0, err
	}
	defer stdin.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		downloader := s3manager.NewDownloader(sess)
		n, err = downloader.DownloadWithContext(ctx,
			FakeWriterAt{stdin}, // writer, awsutil.Copy and WriteAt
			&s3.GetObjectInput{
				Bucket: &bucket,
				Key:    &object,
			}, func(d *s3manager.Downloader) {
				d.Concurrency = 1 //runtime.NumCPU()
			})
		if err != nil {
			log.Println("Download err: ", err.Error())
			cancel()
			return
		}
	}()
	go func() {
		defer wg.Done()
		err = cmd.Start()
		if err != nil {
			log.Println("Start err: ", err.Error())
			return
		}
		err = cmd.Wait()
		if err != nil {
			log.Println("Wait err: ", err.Error())
			return
		}
	}()
	wg.Wait()
	return n, err
}

func ZfsSend(ctx aws.Context, sess *session.Session, bucket, snapshot, snapshot2 string, partSize int64) (n int64, err error) {
	var cmdString, s3Key string

	//1.check snapshot
	exit := snapAlreadyExit(snapshot)
	if !exit { // 本地没有snapshot
		errStr := "no such snapshot: " + snapshot + ". Please try: zfs list -t snapshot"
		return 0, errors.New(errStr)
	}

	if snapshot2 == "" {
		s3Key = snapshot
		//cmdString = "zfs send -PR " + *snapshot
		cmdString = "zfs send -LPpvc " + snapshot
	} else {
		exit := snapAlreadyExit(snapshot2)
		if !exit { // 本地没有snapshot2
			errStr := "no such snapshot: " + snapshot2 + ". Please try: zfs list -t snapshot"
			return 0, errors.New(errStr)
		}

		snap := snapshot
		comma := strings.Index(snap, "@")
		if comma < 0 {
			errStr := "snapshot: " + snapshot + " no @"
			return 0, errors.New(errStr)
		}
		pos := strings.Index(snap[comma:], "@")
		if pos < 0 {
			errStr := "snapshot: " + snapshot + " no @"
			return 0, errors.New(errStr)
		}

		s3Key = snapshot2 + snap[comma+pos:]
		//cmdString = "zfs send -PR -I " + snapshot + " " + snapshot2
		cmdString = "zfs send -LPpvc -i " + snapshot + " " + snapshot2
	}

	//2. S3 upload
	return ZfsSendOne(ctx, sess, bucket, s3Key, cmdString, partSize)
}

func ZfsSendOne(ctx aws.Context, sess *session.Session, bucket, key, stream string, partSize int64) (n int64, err error) {
	ctxcmd, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctxcmd, "bash", "-c", stream)
	reader, err := cmd.StdoutPipe()
	if err != nil {
		log.Println("StdoutPipe err: ", err.Error())
		return 0, err
	}
	defer reader.Close()
	log.Println(stream + " > " + bucket + ":" + key)

	pr, pw := io.Pipe()
	defer pr.Close()
	go func() {
		defer pw.Close()
		err = cmd.Start()
		if err != nil {
			log.Println("Start err: ", err.Error())
			return
		}
		defer cmd.Wait()

		var written int64 = 0
		written, err = io.Copy(pw, reader)
		if err != nil {
			log.Println("Copy err: ", err.Error())
			return
		}
		n += written
		log.Printf("Copy:%v,Total:%v\n", written, n)
	}()

	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.PartSize = partSize
	})
	_, err = uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   pr, // reader,  awsutil.Copy and seek
	})
	if err != nil {
		log.Println("Upload err: ", err.Error())
		cancel() // notify command exit
	}

	return n, err
}

func PutMd5File(url, accesskey, secretkey, bucket, filename, region string, virtualhost bool) error {
	var useSSL bool
	var endpoint string
	var lookup minio.BucketLookupType
	if virtualhost {
		lookup = minio.BucketLookupDNS
	} else {
		lookup = minio.BucketLookupPath
	}
	ep := url
	if strings.HasPrefix(ep, "https://") {
		useSSL = true
		endpoint = ep[8:]
	} else if strings.HasPrefix(ep, "http://") {
		useSSL = false
		endpoint = ep[7:]
	}

	// Initialize minio client object.
	s3Client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials2.NewStaticV4(accesskey, secretkey, ""),
		Secure:       useSSL,
		Region:       region,
		BucketLookup: lookup,
	})
	if err != nil {
		log.Println(err.Error())
		return err
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Println(err.Error())
		return err
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		log.Println(err.Error())
		return err
	}

	uploadInfo, err := s3Client.PutObject(context.Background(),
		bucket, filename, file, fileStat.Size(),
		minio.PutObjectOptions{
			ContentType:    "application/octet-stream",
			SendContentMd5: true,
			PartSize:       DefaultPartSize,
		})
	if err != nil {
		log.Println(err.Error())
		return err
	}
	log.Println("Successfully uploaded bytes: ", uploadInfo)
	return nil
}

func PutFile(ctx aws.Context, sess *session.Session, bucket string, filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		log.Println("Unable to open file " + filename)
		return err
	}
	defer file.Close()

	uploader := s3manager.NewUploader(sess)
	_, err = uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &filename,
		Body:   file,
	})
	return err
}

func DownloadObject(ctx aws.Context, sess *session.Session, bucket string, filename string) (n int64, err error) {
	path := filepath.Dir(filename)
	err = os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return 0, err
	}
	file, err := os.Create(filename)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	downloader := s3manager.NewDownloader(sess)
	n, err = downloader.DownloadWithContext(ctx, file,
		&s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &filename,
		})
	return n, err
}

func DeleteItem(ctx aws.Context, sess *session.Session, bucket string, item string) error {
	svc := s3.New(sess)
	_, err := svc.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(item),
	})
	if err != nil {
		return err
	}

	err = svc.WaitUntilObjectNotExistsWithContext(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &item,
	})
	return err
}

func ListObjectsPages(ctx aws.Context, sess *session.Session, bucket, prefix, delimiter string, maxkeys int64) error {
	var total int64

	svc := s3.New(sess)
	err := svc.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket:    aws.String(bucket),
		MaxKeys:   aws.Int64(maxkeys),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String(delimiter),
	}, func(p *s3.ListObjectsOutput, lastPage bool) bool {
		for _, object := range p.Contents {
			log.Println("Name:          ", *object.Key)
			log.Println("Last modified: ", *object.LastModified)
			log.Println("ETag:          ", *object.ETag)
			log.Println("Size:          ", *object.Size)
			log.Println("Storage class: ", *object.StorageClass)
			log.Println("")
			total++
		}
		log.Printf("LastPage:%v, Total:%v\n", lastPage, total)

		return true // continue paging
	})
	return err
}

func GetAllBuckets(ctx aws.Context, sess *session.Session) (*s3.ListBucketsOutput, error) {
	svc := s3.New(sess)
	result, err := svc.ListBucketsWithContext(ctx, &s3.ListBucketsInput{})
	return result, err
}

func RestoreItem(ctx aws.Context, sess *session.Session, bucket string, item string, days int64) error {
	svc := s3.New(sess)
	_, err := svc.RestoreObjectWithContext(ctx, &s3.RestoreObjectInput{
		Bucket: &bucket,
		Key:    &item,
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int64(days),
		},
	})
	return err
}

func ConfirmBucketItemExists(ctx aws.Context, sess *session.Session, bucket string, item string) error {
	svc := s3.New(sess)
	object, err := svc.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &item,
	})
	if err != nil {
		return err
	}
	log.Println(object)
	return nil
}

func ParseHeadObject(ctx aws.Context, svc *s3.S3, bucket string, filename string) (standard int, ongoing int, err error) {
	standard = -1
	ongoing = -1

	object, err := svc.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &filename,
	})
	if err != nil {
		return standard, ongoing, err
	}

	if object.StorageClass != nil {
		if strings.Contains(*object.StorageClass, "GLACIER") {
			standard = 0
			if object.Restore != nil && strings.Contains(*object.Restore, "ongoing-request=\"false\"") {
				ongoing = 0
			} else if object.Restore != nil && strings.Contains(*object.Restore, "ongoing-request=\"true\"") {
				ongoing = 1
			} else {
				ongoing = -1
			}
		} else {
			standard = 1
		}
	}
	log.Println("Standard:", standard, " ongoing-request:", ongoing)
	return standard, ongoing, err
}

func GetObject(ctx aws.Context, sess *session.Session, bucket, filename string, interval int) (n int64, err error) {
	svc := s3.New(sess)
	ok := make(chan int)

	go func() {
	ParseHeadObject:
		standard, glacier_ongoing, err := ParseHeadObject(ctx, svc, bucket, filename)
		if err != nil {
			ok <- 0
			return
		}
		if standard == 1 {
			ok <- 1
			return
		}
		if standard == 0 {
			if glacier_ongoing == -1 {
				_, err = svc.RestoreObjectWithContext(ctx, &s3.RestoreObjectInput{
					Bucket: &bucket,
					Key:    &filename,
					RestoreRequest: &s3.RestoreRequest{
						Days: aws.Int64(1),
					},
				})
				if err != nil {
					ok <- 0
					return
				}
				time.Sleep(time.Second * time.Duration(interval))
				goto ParseHeadObject
			}
			if glacier_ongoing == 1 {
				time.Sleep(time.Second * time.Duration(interval))
				goto ParseHeadObject
			}
			if glacier_ongoing == 0 {
				ok <- 1
				return
			}
		}
	}()

	restore := -1
	select {
	case restore = <-ok:
		log.Println("Restore ok:", restore)
		close(ok)
	}
	if restore == 1 {
		err = os.MkdirAll(filepath.Dir(filename), os.ModePerm)
		if err != nil {
			return 0, err
		}
		file, err := os.Create(filename)
		if err != nil {
			return 0, err
		}
		defer file.Close()

		downloader := s3manager.NewDownloader(sess)
		n, err = downloader.DownloadWithContext(ctx, file,
			&s3.GetObjectInput{
				Bucket: &bucket,
				Key:    &filename,
			})
	}
	return n, err
}

func GetSnapshotInfo(snap1, snap2 string) (size int64, err error) {
	var size1, size2 int64 = 0, 0
	if snap1 != "" {
		cmdstr := "zfs list -Hp -o refer " + snap1
		cmd := exec.Command("/bin/bash", "-c", cmdstr)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Println("get snapshot info failed, stdout:", string(out), "err:", err)
			return 0, errors.New(string(out))
		}
		slist := strings.Split(string(out), "\n")
		list := strings.Split(slist[0], "\t")
		if len(list) > 0 {
			size1, err = strconv.ParseInt(list[0], 10, 64)
		}
	}

	if snap2 != "" {
		cmdstr := "zfs list -Hp -o refer " + snap2
		cmd := exec.Command("/bin/bash", "-c", cmdstr)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Println("get snapshot info failed, stdout:", string(out), "err:", err)
			return 0, errors.New(string(out))
		}
		slist := strings.Split(string(out), "\n")
		list := strings.Split(slist[0], "\t")
		if len(list) > 0 {
			size2, err = strconv.ParseInt(list[0], 10, 64)
		}
	}
	if size1 >= size2 {
		size = size1
	} else {
		size = size2
	}
	log.Printf("max(%v:%v,%v:%v)=%v\n", snap1, size1, snap2, size2, size)

	return size, err
}

func Usage() {
	log.Fatalln(`
Please specify
  Handle(-h up|down|get|del|head|list|res|md5|cryp|zsend|zrecv|help) 
  Bucket (-b BUCKET) 
  Filename (-f FILENAME) 
  AccessKey(-ak AK) 
  SecretKey(-sk SK) 
  Endpoint(-ep ENDPOINT) 
  VirtualHostedStyle(-vs 1|0) 
  Region(-r REGION) 
  Prefix(-p PREFIX) 
  Maxkeys(-m MAXKEYS) 
  Delimiter(-d DELIMITER) 
  ZFS-Snapshot(-ss ZFS-SNAPSHOT)
  ZFS-Snapshot2(-ss2 ZFS-SNAPSHOT2) 
  ZFS-Filesystem|Volume(-z ZFS-Filesystem | ZFS-Volume )

example:
  ./s3 -h head -f pool1/file1@snap1 -b bucket1 -ep https://10.2.174.133:9000 -vs 0 -ak LTAIOJ4o2JtFMvgl -sk 4TLgEn7m2GEr6OfM6KkRaZLd5EDCQM
  ./s3 -h list -p pool1/file1@snap -b bucket1 -ep https://10.2.174.133:9000 -vs 0 -ak LTAIOJ4o2JtFMvgl -sk 4TLgEn7m2GEr6OfM6KkRaZLd5EDCQM
  ./s3 -h up -f ./dir/file.txt -b bucket1 -ep https://10.2.174.133:9000 -vs 0 -ak LTAIOJ4o2JtFMvgl -sk 4TLgEn7m2GEr6OfM6KkRaZLd5EDCQM
  ./s3 -h get -f ./dir/file.txt -b bucket1 -ep https://10.2.174.133:9000 -vs 0 -ak LTAIOJ4o2JtFMvgl -sk 4TLgEn7m2GEr6OfM6KkRaZLd5EDCQM  
  ./s3 -h zsend -ss pool1/file1@snap1 -ss2 pool1/file1@snap2 -b bucket1 -ep http://oss-cn-beijing.aliyuncs.com -ak LTAIOJ4o2JtFMvgl -sk 4TLgEn7m2GEr6OfM6KkRaZLd5EDCQM
  ./s3 -h zrecv -ss pool1/file1@snap2 -z pool1 -b bucket1 -ep http://oss-cn-beijing.aliyuncs.com -ak LTAIOJ4o2JtFMvgl -sk 4TLgEn7m2GEr6OfM6KkRaZLd5EDCQM
	`,
	)
}

func main() {
	var err error
	var n int64
	handle := flag.String("h", "list", "The handle of up|down|get|del|head|list|res|md5|cryp|zsend|zrecv|help")
	bucket := flag.String("b", "test123", "Bucket ")
	filename := flag.String("f", "", "FileName ")
	snapshot := flag.String("ss", "", "zfs snapshot(example: pool1/file1@snap1)")
	snapshot2 := flag.String("ss2", "", "zfs snapshot2, to sends all intermediary snapshots from the ss to the ss2(example: pool1/file1@snap2)")
	zfs := flag.String("z", "", "zfs recv filesystem or volume(example: pool1/volume1)")
	accesskey := flag.String("ak", "DUP7AEVZF01E8P56DO10", "AccessKey")
	secretkey := flag.String("sk", "Ka0DNRMv1VRZhsYvnc4BpeziZALpsONGc9HtFHre", "SecretKey")
	endpoint := flag.String("ep", "http://172.38.30.6:7480", "Endpoint( http(s):// )")
	region := flag.String("r", "us-east-1", "Region ")
	virtualhost := flag.Bool("vs", false, "Virtual-Hosted Style(1:true) or Path Style(0:false)")
	prefix := flag.String("p", "", "Prefix of ListObjects")
	maxkeys := flag.Int64("m", 1000, "MaxKeys of ListObjects")
	delimiter := flag.String("d", "", "Delimiter of ListObjects")
	interval := flag.Int("i", 10, "Interval(second) of HeadObject during restore")
	flag.Parse()
	if *endpoint == "" {
		log.Println("endpoint null")
		Usage()
	}
	if *bucket == "" {
		if *handle != "list" {
			log.Println("bucket null")
			Usage()
		}
	}
	if *accesskey == "" {
		log.Println("accesskey null")
		Usage()
	}
	if *secretkey == "" {
		log.Println("secretkey null")
		Usage()
	}
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(*accesskey, *secretkey, ""),
		Endpoint:         aws.String(*endpoint),
		Region:           aws.String(*region),
		HTTPClient:       &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}},
		S3ForcePathStyle: aws.Bool(!*virtualhost),
	}))

	ctx := context.Background()
	start := time.Now()

	switch *handle {
	case "help":
		Usage()
	case "list":
		if *bucket == "" { // buckets
			result, err := GetAllBuckets(ctx, sess)
			if err != nil {
				log.Println("Got error! retrieving buckets: " + err.Error())
				return
			}
			log.Println("Buckets:")
			for _, bucket := range result.Buckets {
				log.Println(*bucket.Name + ": " + bucket.CreationDate.Format("2006-01-02 15:04:05 Monday"))
			}
		} else { // objects
			err := ListObjectsPages(ctx, sess, *bucket, *prefix, *delimiter, *maxkeys)
			if err != nil {
				log.Println("Got error! retrieving list of objects: " + err.Error())
				return
			}
		}
	case "get":
		n, err = GetObject(ctx, sess, *bucket, *filename, *interval)
		if err != nil {
			log.Println("Got error! get " + *filename + ": " + err.Error())
			return
		}
		log.Printf("GetObject(%v)=%v\n", *filename, n)
	case "up":
		err = PutFile(ctx, sess, *bucket, *filename)
		if err != nil {
			log.Println("Got error! upload " + *filename + ": " + err.Error())
			return
		}
		log.Println("Uploaded " + *filename)
	case "down":
		n, err = DownloadObject(ctx, sess, *bucket, *filename)
		if err != nil {
			log.Println("Got error! download " + *filename + ": " + err.Error())
			return
		}
		log.Printf("Downloaded(%v)=%v\n", *filename, n)
	case "md5":
		err = PutMd5File(*endpoint, *accesskey, *secretkey, *bucket, *filename, *region, *virtualhost)
		if err != nil {
			log.Println("Got error! md5 upload " + *filename + ": " + err.Error())
			return
		}
		log.Println("Uploaded " + *filename)
	case "del":
		err = DeleteItem(ctx, sess, *bucket, *filename)
		if err != nil {
			log.Println("Got error! delete " + *filename + ": " + err.Error())
			return
		}
		log.Println("Deleted " + *filename)
	case "head":
		err = ConfirmBucketItemExists(ctx, sess, *bucket, *filename)
		if err != nil {
			log.Println("Got error! head " + *filename + ": " + err.Error())
			return
		}
		log.Println("Headed " + *filename)
	case "res":
		err := RestoreItem(ctx, sess, *bucket, *filename, 7)
		if err != nil {
			log.Println("Got error! restore " + *filename + ": " + err.Error())
			return
		}
		log.Println("Restored " + *filename + " to " + *bucket)
	case "zsend":
		size, err := GetSnapshotInfo(*snapshot, *snapshot2)
		if err != nil {
			log.Println("Got error! zsend " + *snapshot + " " + *snapshot2 + ": " + err.Error())
			return
		}
		partsize, err := CalculatePartSize(size)
		if err != nil {
			log.Println("Got error! zsend " + *snapshot + " " + *snapshot2 + ": " + err.Error())
			return
		}
		log.Printf("snap size=%v,partsize=%v\n", size, partsize)

		n, err = ZfsSend(ctx, sess, *bucket, *snapshot, *snapshot2, partsize)
		if err != nil {
			log.Println("Got error! zsend " + *snapshot + " " + *snapshot2 + ": " + err.Error())
			return
		}
		log.Printf("ZfsSend(%v,%v to %v)=%v\n", *snapshot, *snapshot2, *bucket, n)
	case "zrecv":
		if *snapshot == "" {
			log.Println("snapshot null")
			Usage()
		}
		if *zfs == "" {
			log.Println("zfs filesystem or volume null")
			Usage()
		}
		n, err = ZfsRecv(ctx, sess, *bucket, *snapshot, *zfs)
		if err != nil {
			log.Println("Got error! zrecv " + *snapshot + " to " + *zfs + ": " + err.Error())
			return
		}
		log.Printf("ZfsRecv(%v)=%v\n", *snapshot, n)
	default:
		log.Println("unknown handle: " + *handle)
		Usage()
	}

	elapsed := time.Since(start)
	if err == nil {
		log.Println("This function took: ", elapsed)
	}
}

