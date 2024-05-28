package main

import "sync"

type ServerInfo struct {
    Master bool
}

var (
    instance *ServerInfo
    once     sync.Once
)

func GetServerInfo() *ServerInfo {
    once.Do(func() {
        instance = &ServerInfo{}
    })
    return instance
}
