package main

import (
	"fmt"
	"net"
)

type User struct {
	Conn   net.Conn
	C      chan string
	Name   string
	Addr   string
	server *Server
}

// 创建User
func NewUser(conn net.Conn, server *Server) *User {
	userAddr := conn.RemoteAddr().String()
	user := &User{
		Conn:   conn,
		C:      make(chan string),
		Name:   userAddr,
		Addr:   userAddr,
		server: server,
	}
	go user.listenMessage()
	return user
}

// 监听 User 的 channel，一旦有消息，就直接发送给客户端
func (user *User) listenMessage() {
	for {
		message := <-user.C
		user.Conn.Write([]byte(message + "\n"))
	}
}

// 用户上线
func (user *User) Online() {
	user.server.mapLock.Lock()
	defer user.server.mapLock.Unlock()
	user.server.OnlineMap[user.Name] = user
	user.SendToBroadCastChannel("已上线")
}

// 用户下线
func (user *User) Offline() {
	user.server.mapLock.Lock()
	defer user.server.mapLock.Unlock()
	delete(user.server.OnlineMap, user.Name)
	user.SendToBroadCastChannel("已下线")
}

// 发送消息到广播Channel
func (user *User) SendToBroadCastChannel(message string) {
	fmt.Printf("Broadcast channel receives message from [%s]: %s\n", user.Name, message)
	user.server.Message <- fmt.Sprintf("[%s]%s: %s", user.Addr, user.Name, message)
}

// 发送消息到user客户端
func (user *User) SendMessageToClient(message string) {
	user.C <- message
}

// 查看当前所有在线用户
func (user *User) CheckOnlineUsers() {
	user.server.mapLock.RLock()
	defer user.server.mapLock.RUnlock()
	for _, onlineUser := range user.server.OnlineMap {
		message := fmt.Sprintf("[%s]%s: 在线...", onlineUser.Addr, onlineUser.Name)
		user.SendMessageToClient(message)
	}
}
