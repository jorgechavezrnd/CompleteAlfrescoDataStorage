package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

// AlfContentURL ...
type AlfContentURL struct {
	ContentURL  string
	ContentSize int64
}

// Data for connection string
var server = "localhost"
var port = 1433
var user = "sa"
var password = "sa"
var database = "registry"

// Contentstore Path
var contentstorePath = "D:/wildfly-10/bin/alfdata/contentstore/"

// Global variables
var db *sql.DB
var alfContentURLList []AlfContentURL

func main() {
	createConnectionPool()
	defer closeConnection()
	readDataFromAlfContentURLTable()
	createMissingFiles()
}

func createConnectionPool() {
	var err error

	connString := fmt.Sprintf("Server=%s;user id=%s;password=%s;port=%d;database=%s",
		server, user, password, port, database)

	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: " + err.Error())
	}

	ctx := context.Background()

	err = db.PingContext(ctx)
	if err != nil {
		log.Fatal("Error connecting database: " + err.Error())
	}

	log.Println("Connected!")
}

func closeConnection() {
	log.Println("Close connection")
	db.Close()
}

func readDataFromAlfContentURLTable() {
	ctx := context.Background()

	err := db.PingContext(ctx)
	if err != nil {
		log.Fatal("Error pinging database: " + err.Error())
	}

	tsql := fmt.Sprintf("SELECT content_url, content_size FROM alf_content_url;")

	rows, err := db.QueryContext(ctx, tsql)
	if err != nil {
		log.Fatal("Error reading rows: ", err.Error())
	}

	defer rows.Close()

	log.Println("Read Alf Content URL Data Start")

	for rows.Next() {
		var contentURL string
		var contentSize int64

		err := rows.Scan(&contentURL, &contentSize)
		if err != nil {
			log.Fatal("Error reading rows: " + err.Error())
		}

		alfContentURLList = append(alfContentURLList, AlfContentURL{
			ContentURL:  contentURL[8:],
			ContentSize: contentSize,
		})
	}

	log.Println("Read Alf Content URL Data End")
}

func createMissingFiles() {
	log.Println("Create Missing Files Start")

	existingFilesCount := 0
	nonExistingFilesCount := 0

	for index, alfContentURL := range alfContentURLList {
		filePath := contentstorePath + alfContentURL.ContentURL
		_, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			log.Printf("%d.- File %s does not exist\n", (index + 1), filePath)
			nonExistingFilesCount++

			folderPath := getFolderPath(alfContentURL.ContentURL)
			createFolderPath(folderPath)
			createFile(filePath, alfContentURL.ContentSize)
		} else {
			log.Printf("%d.- File %s exists\n", (index + 1), filePath)
			existingFilesCount++
		}
	}

	log.Println("Create Missing Files End")
	log.Printf("Existing Files Count: %d", existingFilesCount)
	log.Printf("Non Existing Files Count: %d", nonExistingFilesCount)
}

func getFolderPath(contentURL string) string {
	parts := strings.Split(contentURL, "/")

	if len(parts) < 1 {
		log.Fatal("Error extracting folder path and file name to create")
	}

	folderPath := contentstorePath + strings.Join(parts[:len(parts)-1], "/")

	return folderPath
}

func createFolderPath(folderPath string) {
	err := os.MkdirAll(folderPath, os.ModePerm)
	if err != nil {
		log.Fatal("Error creating folder: " + err.Error())
	}
}

func createFile(filePath string, size int64) {
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatal("Error on create file: " + err.Error())
	}
	err = file.Truncate(size)
	if err != nil {
		log.Fatal("Error on truncate file: " + err.Error())
	}
}
