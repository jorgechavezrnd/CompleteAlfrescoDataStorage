package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

// AlfContentURL ...
type AlfContentURL struct {
	ContentURL  string
	ContentSize int64
}

// Config ...
type Config struct {
	Host                 string `json:"host"`
	Port                 int    `json:"port"`
	Username             string `json:"username"`
	Password             string `json:"password"`
	Database             string `json:"database"`
	ContentstorePath     string `json:"contentstorePath"`
	GenerateFromThisDate string `json:"generateFromThisDate"`
}

// Global variables
var db *sql.DB
var alfContentURLList []AlfContentURL
var config = Config{}

func main() {
	if setUpConfig() {
		args := os.Args[1:]
		if len(args) > 0 {
			createConnectionPool()
			defer closeConnection()
			readDataFromAlfContentURLTable()
			showValidAlfContentURLTableData()
		} else {
			createConnectionPool()
			defer closeConnection()
			readDataFromAlfContentURLTable()
			createMissingFiles()
		}
	}
}

func setUpConfig() bool {
	jsonFile, err := os.Open("config.json")
	if err != nil {
		log.Println("Error on open config.json file: " + err.Error())
		return false
	}

	defer jsonFile.Close()

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		log.Println("Error on read config.json file: " + err.Error())
		return false
	}

	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		log.Println("Error on config.json structure: " + err.Error())
		return false
	}

	if config.ContentstorePath[len(config.ContentstorePath)-1] != '/' {
		config.ContentstorePath = config.ContentstorePath + "/"
	}

	return true
}

func createConnectionPool() {
	var err error

	connString := fmt.Sprintf("Server=%s;user id=%s;password=%s;port=%d;database=%s",
		config.Host, config.Username, config.Password, config.Port, config.Database)

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

func showValidAlfContentURLTableData() {
	log.Println("Show Valid Alf Content URL Table Data Start")
	for index, alfContURL := range alfContentURLList {
		if isValidDate(alfContURL.ContentURL) {
			log.Printf("%d.- %s\n", index, alfContURL.ContentURL)
		}
	}
	log.Println("Show Valid Alf Content URL Table Data End")
}

func createMissingFiles() {
	log.Println("Create Missing Files Start")

	existingFilesCount := 0
	nonExistingFilesCount := 0
	notValidDateCount := 0

	for index, alfContentURL := range alfContentURLList {
		filePath := config.ContentstorePath + alfContentURL.ContentURL
		_, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			if isValidDate(alfContentURL.ContentURL) {
				log.Printf("%d.- File %s does not exist\n", (index + 1), filePath)
				nonExistingFilesCount++

				folderPath := getFolderPath(alfContentURL.ContentURL)
				createFolderPath(folderPath)
				// createFile(filePath, alfContentURL.ContentSize)
				createFile(filePath, 1000)
			} else {
				log.Printf("ContentURL is less than 'generateFromThisDate' configuration: " + alfContentURL.ContentURL)
				notValidDateCount++
			}
		} else {
			log.Printf("%d.- File %s exists\n", (index + 1), filePath)
			existingFilesCount++
		}
	}

	log.Println("Create Missing Files End")
	log.Printf("Existing Files Count: %d", existingFilesCount)
	log.Printf("Non Existing Files Count: %d", nonExistingFilesCount)
	log.Printf("Not Validate Date Count: %d", notValidDateCount)
}

func isValidDate(contentURL string) bool {
	fromThisDate := strings.Split(config.GenerateFromThisDate, "/")
	contentURLDate := strings.Split(contentURL, "/")

	if len(fromThisDate) == 0 && len(contentURLDate) == 0 {
		log.Println("Error on generateFromThisDate in config.js or contentURL")
		log.Fatal("generateFromThisDate: " + config.GenerateFromThisDate + ", contentURL: " + contentURL)
	}

	contentURLDate = contentURLDate[:len(contentURLDate)-1]

	if len(fromThisDate) != len(contentURLDate) {
		log.Println("FromThisDate and ContentURLDate don't have same len")
		log.Fatal(fmt.Sprintf("FromThisDate len: %d, ContentURLDate len: %d", len(fromThisDate), len(contentURLDate)))
	}

	for index := range fromThisDate {
		fromThisDateValue, err := strconv.Atoi(fromThisDate[index])
		if err != nil {
			log.Fatal("Error on convert GenerateFromThisDate to int: " + err.Error())
		}
		contentURLDateValue, err := strconv.Atoi(contentURLDate[index])
		if err != nil {
			log.Fatal("Error on convert ContentURLDate to int: " + err.Error())
		}
		if contentURLDateValue < fromThisDateValue {
			return false
		}
	}

	return true
}

func getFolderPath(contentURL string) string {
	parts := strings.Split(contentURL, "/")

	if len(parts) < 1 {
		log.Fatal("Error extracting folder path and file name to create")
	}

	folderPath := config.ContentstorePath + strings.Join(parts[:len(parts)-1], "/")

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
