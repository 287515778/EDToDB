package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tealeg/xlsx"
	"github.com/fsnotify/fsnotify"
)

// DB配置
var groupConfig =  map[string]string{
	"dbnamekey": "user:password@protocol(ip:port)/dbname?charset=utf8&parseTime=true&loc=Local",
}

// 初始化DB
func NewDataBase(group string) (*sql.DB, error) {
	dsn, ok := groupConfig[group]
	if !ok {
		return nil, errors.New(fmt.Sprintf("db组未配置[%s]", group))
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	return db, nil
}

func Update(db *sql.DB, m map[string]string) (int64, error) {
	sqlStr := fmt.Sprintf("update TableName set field1 = ? where id = ?")
	result, err := db.Exec(sqlStr, m["field1"], m["id"])
	if err != nil {
		panic(err)
	}
	return result.RowsAffected()
}

func Insert(db *sql.DB, m map[string]string) (int64, error) {
	sqlStr := fmt.Sprintf("insert into TableName(field1,field2) values (?, ?)")
	result, err := db.Exec(sqlStr, m["field1"], m["field2"])
	if err != nil {
		panic(err)
	}
	return result.LastInsertId()
}

func fileMonitor() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()
	done := make(chan bool)

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Println("event:", event)
				//if event.Op&fsnotify.Write == fsnotify.Write {
				//	log.Println("modified file:", event.Name)
				//}
				if event.Op == fsnotify.Create {
					go process(event.Name)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	curDirPath, _ := os.Getwd()
	watcherDir := curDirPath + "/excel"
	err = watcher.Add(watcherDir)
	if err != nil {
		log.Fatal(err)
	}
    //Hang
	<-done
}

func process(filePath string) {
	db, err := NewDataBase("dbnamekey")
	defer db.Close()
	if err != nil {
		log.Println(err)
	}
	time.Sleep(3 * time.Second)
	xlFile, err := xlsx.OpenFile(filePath)
	if err != nil {
		log.Println(err)
	}
	for _, sheet := range xlFile.Sheets {
		for _, row := range sheet.Rows {
			m := make(map[string]string)
			for ck, cell := range row.Cells {
				switch ck {
				case 0:
					m["field1"] = cell.String()
				case 1:
					m["field2"] = cell.String()
                case 2:
                    m["id"] = cell.String()
                }
			}

			i, err := Insert(db, m)
			if err != nil {
				log.Println(err)
			}
			fmt.Println(i)
		}
	}
}

func main() {
	fileMonitor()
}
