go build -buildmode=plugin ../mrapps/crash.go
rm mr-out*
go run mrmaster.go pg-*.txt