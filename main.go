package main

import (
	"go.uber.org/zap"
)

func main() {
	development, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(development)

}
