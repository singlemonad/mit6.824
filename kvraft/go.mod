module github.com/singlemonad/mit6.824/kvraft

go 1.13

replace (
    github.com/singlemonad/mit6.824 v0.0.0-20200427133103-9409b87cb545 => /Users/yang.qu/go/src/github.com/singlemonad/mit6.824
    github.com/singlemonad/mit6.824/raft v0.0.0-20200427133103-9409b87cb545 => /Users/yang.qu/go/src/github.com/singlemonad/mit6.824/raft
)

require (
	github.com/satori/go.uuid v1.2.0
	github.com/singlemonad/mit6.824 v0.0.0-20200427133103-9409b87cb545
	github.com/singlemonad/mit6.824/raft v0.0.0-20200427133103-9409b87cb545
	go.uber.org/zap v1.15.0
)
