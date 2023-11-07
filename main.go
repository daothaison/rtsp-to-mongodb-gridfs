package main

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo/gridfs"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var streamMutex sync.Mutex
var client *mongo.Client
var db *mongo.Database

type RecordingInfo struct {
	Id           string `json:"_id" bson:"_id"`
	RTSPURL      string `json:"rtsp_url"`
	IsRecording  bool   `json:"is_recording"`
	OutputFileID string `json:"output_file_id"`
}

func startRecording(recordingInfo *RecordingInfo) {
	fs, err := gridfs.NewBucket(
		db,
	)
	if err != nil {
		log.Printf("Error creating GridFS bucket: %s \n", err)
		return
	}

	log.Printf("Recording stream url %s & stream id %s\n", recordingInfo.RTSPURL, recordingInfo.Id)

	_ = exec.Command("mkdir", "-p", fmt.Sprintf("temp/%s", recordingInfo.Id)).Start()
	cmd := exec.Command(
		"ffmpeg",
		"-i",
		recordingInfo.RTSPURL,
		"-c:v",
		"copy",
		"-c:a",
		"copy",
		"-hls_time",
		"10",
		fmt.Sprintf("temp/%s/output.m3u8", recordingInfo.Id))
	err = cmd.Start()
	if err != nil {
		log.Printf("Error converting to HLS: %s\n", err)
		return
	}

	err = cmd.Wait()
	if err != nil {
		log.Printf("Error during conversion: %s\n", err)
		return
	}

	streamMutex.Lock()
	defer streamMutex.Unlock()
	if recordingInfo.IsRecording {
		file, err := os.Open(fmt.Sprintf("temp/%s/output.m3u8", recordingInfo.Id))
		if err != nil {
			log.Printf("Error opening HLS file: %s\n", err)
			return
		}
		defer file.Close()

		uploadStream, err := fs.OpenUploadStream(fmt.Sprintf("recorded_video_%s.m3u8", recordingInfo.Id))
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

		// Update the information in MongoDB to store the GridFS file ID
		filter := bson.D{{"rtsp_url", recordingInfo.RTSPURL}}
		update := bson.D{
			{"$set", bson.D{
				{"output_file_id", uploadStream.FileID},
				{"is_recording", false},
			}},
		}
		_, err = db.Collection("recordings").UpdateOne(context.Background(), filter, update)
		if err != nil {
			log.Printf("Error updating MongoDB document: %s\n", err)
		}
	}
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

		log.Printf("recording stream url %s\n", recordingInfo.RTSPURL)
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
		}

		c.JSON(http.StatusOK, gin.H{"message": "Recording started for Rtsp URL: " + rtspURL})
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
			// Stream not found, meaning no recording is in progress
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

		c.JSON(http.StatusOK, gin.H{"message": "Recording stopped for rtsp url: " + rtspURL})
	})

	r.Run(":8080") // Run API on port 8080
}
