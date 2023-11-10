package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/grafov/m3u8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"time"
)

var streamMutex sync.Mutex
var client *mongo.Client
var db *mongo.Database

// create a map to store stream id and context cancel
var streamContextMap = make(map[string]context.CancelFunc)

type RecordingInfo struct {
	Id           string `json:"_id" bson:"_id"`
	RTSPURL      string `json:"rtsp_url"`
	IsRecording  bool   `json:"is_recording"`
	OutputFileID string `json:"output_file_id"`
}

func startRecording(recordingInfo *RecordingInfo) {
	log.Printf("Recording stream url %s & stream id %s\n", recordingInfo.RTSPURL, recordingInfo.Id)

	// create context with cancel
	ctx, cancel := context.WithCancel(context.Background())
	streamContextMap[recordingInfo.Id] = cancel

	_ = exec.Command("mkdir", "-p", fmt.Sprintf("temp/%s", recordingInfo.Id)).Start()
	cmd := exec.CommandContext(ctx,
		"ffmpeg",
		"-i",
		recordingInfo.RTSPURL,
		"-c:v",
		"copy",
		"-c:a",
		"copy",
		"-hls_time",
		"10",
		"-hls_list_size",
		"0",
		fmt.Sprintf("temp/%s/output.m3u8", recordingInfo.Id))
	err := cmd.Start()
	if err != nil {
		log.Printf("Error converting to HLS: %s\n", err)
		return
	}

	// goroutine wait for context cancel
	watchContext, cancel := context.WithCancel(context.Background())
	go func() {
		err = cmd.Wait()
		<-ctx.Done()
		log.Printf("Stream %s is stopped by context cancel\n", recordingInfo.Id)
		cancel()
		err := cmd.Process.Kill()
		if err != nil {
			return
		}
	}()

	log.Printf("Waiting for ffmpeg command to finish & recording...")
	time.Sleep(15 * time.Second)
	watchM3u8File(watchContext, recordingInfo)

	// remove temp folder
	// _ = exec.Command("rm", "-rf", fmt.Sprintf("temp/%s", recordingInfo.Id)).Start()
}

func watchM3u8File(ctx context.Context, recordingInfo *RecordingInfo) {
	var oldAllSegments []*m3u8.MediaSegment
	tempPath := fmt.Sprintf("temp/%s", recordingInfo.Id)
	filePath := fmt.Sprintf("%s/output.m3u8", tempPath)

	for {
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR, os.ModeAppend)
		if err != nil {
			log.Printf("Error when open %s: %s\n", filePath, err.Error())
			return
		}
		defer file.Close()

		playlist, listType, err := m3u8.DecodeFrom(io.Reader(file), true)
		if err != nil {
			log.Printf("Error when m3u8 decode %s: %s\n", filePath, err.Error())
			return
		}

		if listType == m3u8.MEDIA {
			mediaPlaylist := playlist.(*m3u8.MediaPlaylist)

			log.Printf("Media Playlist with %d segments\n", mediaPlaylist.Count())

			currentAllSegments := mediaPlaylist.GetAllSegments()
			if len(currentAllSegments) > 0 {
				storeIntoGridFS(recordingInfo, "output.m3u8")
			}

			if len(currentAllSegments) > len(oldAllSegments) {
				for i := uint(len(oldAllSegments)); i < mediaPlaylist.Count(); i++ {
					segment := mediaPlaylist.Segments[i]
					log.Printf("New Segment %d: %s\n", i, segment.URI)
					storeIntoGridFS(recordingInfo, segment.URI)
				}
			}

			log.Printf("Media Playlist closed: %v\n", mediaPlaylist.Closed)
			if mediaPlaylist.Closed {
				break
			}
		}

		if ctx.Err() == context.Canceled {
			log.Printf("Context done, stop recording stream id %s & append end character to %s\n", recordingInfo.Id, filePath)
			_, err := file.WriteString("#EXT-X-ENDLIST\n")
			if err != nil {
				log.Printf("Error appending end character to %s: %s\n", filePath, err.Error())
			}

			break
		}

		oldAllSegments = playlist.(*m3u8.MediaPlaylist).GetAllSegments()

		time.Sleep(10 * time.Second)
	}
}

func storeIntoGridFS(recordingInfo *RecordingInfo, segmentUri string) {
	log.Printf("Storing segment %s into GridFS\n", segmentUri)
	segmentPath := fmt.Sprintf("temp/%s/%s", recordingInfo.Id, segmentUri)

	fs, err := gridfs.NewBucket(
		db,
	)
	if err != nil {
		log.Printf("Error creating GridFS bucket: %s \n", err)
		return
	}

	fileId := findGridFsFile(recordingInfo, segmentUri)
	if fileId != "" {
		log.Printf("Drop exist segment %s in GridFS\n", segmentUri)
		id, _ := primitive.ObjectIDFromHex(fileId)
		if err := fs.Delete(id); err != nil {
			log.Printf("Drop exist segment %s in GridFS error: %s\n", segmentUri, err.Error())
		}
	}

	streamMutex.Lock()
	defer streamMutex.Unlock()
	if recordingInfo.IsRecording {
		file, err := os.Open(segmentPath)
		if err != nil {
			log.Printf("Error opening HLS file: %s\n", err)
			return
		}
		defer file.Close()

		log.Printf("Uploading file %s to GridFS\n", segmentUri)

		uploadOpts := options.GridFSUpload().SetMetadata(bson.D{{"stream_id", recordingInfo.Id}})
		uploadStream, err := fs.OpenUploadStream(segmentUri, uploadOpts)
		if err != nil {
			log.Printf("Error creating GridFS upload stream: %s\n", err)
			return
		}
		defer uploadStream.Close()

		fileContent, err := io.ReadAll(file)
		if err != nil {
			log.Printf("Error reading file content from upload stream: %s \n", err)
		}

		_, err = uploadStream.Write(fileContent)
		if err != nil {
			log.Printf("Error writing to GridFS: %s\n", err)
			return
		}

		fmt.Printf("File uploaded to GridFS for stream ID: %s, file id: %s \n", recordingInfo.Id, uploadStream.FileID)
	}
}

func findGridFsFile(recordingInfo *RecordingInfo, segmentUri string) string {
	type gridfsFile struct {
		Id string `bson:"_id"`
	}
	var file gridfsFile

	// find one record in mongo with filter
	filter := bson.D{{"filename", segmentUri}, {"metadata", bson.D{{"stream_id", recordingInfo.Id}}}}
	err := db.Collection("fs.files").FindOne(context.Background(), filter).Decode(&file)
	if err != nil {
		return ""
	}

	return file.Id
}

func main() {
	r := gin.Default()

	// Creat mongodb client with username password
	// clientOptions := options.Client().ApplyURI("mongodb://<username>:<password>@localhost:27017")
	clientOptions := options.Client().ApplyURI("mongodb://mongoadmin:mongoadmin@localhost:27017")
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	db = client.Database("rstp")

	r.GET("/healthcheck", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "OK!"})
	})

	r.POST("/start-recording", func(c *gin.Context) {
		var recordingInfo RecordingInfo
		// bind request body into RecordingInfo struct
		if err := c.ShouldBindJSON(&recordingInfo); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		log.Printf("Recording stream url %s\n", recordingInfo.RTSPURL)
		rtspURL := recordingInfo.RTSPURL

		streamMutex.Lock()
		defer streamMutex.Unlock()

		err := db.Collection("recordings").FindOne(context.Background(), bson.D{{"rtsp_url", rtspURL}}).Decode(&recordingInfo)
		if err != nil {
			// Stream info not found, so create a new document in MongoDB
			recordingInfo = RecordingInfo{
				RTSPURL:     rtspURL,
				IsRecording: true,
			}
			savedRecord := bson.D{
				{"rtsp_url", recordingInfo.RTSPURL},
				{"is_recording", recordingInfo.IsRecording},
			}
			re, err := db.Collection("recordings").InsertOne(context.Background(), savedRecord)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"message": "Error creating recording info" + err.Error()})
				return
			}
			log.Printf("Created new recording info: %v\n", re)

			// insert savedRecord to mongodb then bind id to recordingInfo
			err = db.Collection("recordings").FindOne(context.Background(), bson.D{{"rtsp_url", rtspURL}}).Decode(&recordingInfo)

			go startRecording(&recordingInfo)
		} else {
			if recordingInfo.IsRecording == false {
				recordingInfo.IsRecording = true
				update := bson.D{
					{"$set", bson.D{
						{"is_recording", true},
					}},
				}
				_, err := db.Collection("recordings").UpdateOne(context.Background(), bson.D{{"rtsp_url", rtspURL}}, update)
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"message": "Error updating recording info"})
					return
				}
				go startRecording(&recordingInfo)
			}
		}

		c.JSON(http.StatusOK, gin.H{"message": "Recording started for rtsp URL: " + rtspURL})
	})

	r.POST("/stop-recording", func(c *gin.Context) {
		var recordingInfo RecordingInfo
		// bind request body into RecordingInfo struct
		if err := c.ShouldBindJSON(&recordingInfo); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		log.Printf("Stop recording stream url %s\n", recordingInfo.RTSPURL)
		rtspURL := recordingInfo.RTSPURL

		streamMutex.Lock()
		defer streamMutex.Unlock()

		err := db.Collection("recordings").FindOne(context.Background(), bson.D{{"rtsp_url", rtspURL}}).Decode(&recordingInfo)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"message": "No recording in progress for rtsp url: " + rtspURL})
			return
		}

		recordingInfo.IsRecording = false
		update := bson.D{
			{"$set", bson.D{
				{"is_recording", false},
			}},
		}
		_, err = db.Collection("recordings").UpdateOne(context.Background(), bson.D{{"rtsp_url", rtspURL}}, update)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"message": "Error updating recording info"})
			return
		}

		// cancel context
		streamContextMap[recordingInfo.Id]()

		c.JSON(http.StatusOK, gin.H{"message": "Recording stopped for rtsp url: " + rtspURL})
	})

	r.Run(":8080") // Run API on port 8080
}
