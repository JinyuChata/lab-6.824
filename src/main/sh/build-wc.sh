export GO111MODULE="auto"
export GOROOT="/usr/local/go"
export GOPATH="/Users/jinyuzhu/Documents/dev/mit_6824/lab-6.824"
go build -gcflags="all=-N -l" -buildmode=plugin ../mrapps/wc.go