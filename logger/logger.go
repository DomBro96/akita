package logger

import (
	"io"
	"log"
	"os"
)

var (
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

func init() {
	file, err := os.OpenFile("errors.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open error log file: %s\n", err)
	}
	Info = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	Warning = log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(io.MultiWriter(file, os.Stderr), "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

// Infof to print the info log, call Logger.Printf()
func Infof(format string, v ...interface{}) {
	format += " \n"
	Info.Printf(format, v...)
}

// Infoln to print the info log, call Logger.Println()
func Infoln(v ...interface{}) {
	Info.Println(v...)
}

// Errorf to print the error log, call Logger.Printf()
func Errorf(format string, v ...interface{}) {
	format += " \n"
	Error.Printf(format, v...)
}

// Warningf to print the warning log, call Logger.Printf()
func Warningf(format string, v ...interface{}) {
	format += " \n"
	Warning.Panicf(format, v...)
}

// Fatalf to print the fatal log, call Logger.Fatalf()
func Fatalf(format string, v ...interface{}) {
	format += " \n"
	Error.Fatalf(format, v...)
}

// Fatalln to print the fatal log, call Logger.Fatalln()
func Fatalln(v ...interface{}) {
	Error.Fatalln(v...)
}
