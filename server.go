package main

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

type Server struct {
	IP        string
	Port      int
	OnlineMap map[string]*User
	mapLock   sync.RWMutex
	Message   chan string
}

const BUFFER_LEN int = 4096

// 启动服务器
func (server *Server) start() {
	fmt.Println("初始化聊天服务器...")
	// 创建socket连接
	listener, err := net.Listen("tcp4", fmt.Sprintf("%s:%d", server.IP, server.Port))
	if err != nil {
		fmt.Println("[server.start] [net.Listen] err: ", err)
		return
	}

	go server.BroadCastChannelOnMessage()

	// 关闭连接
	defer listener.Close()

	// 接收消息并调用回调函数处理消息
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("[server.start] [listener.Accept] err: ", err)
			return
		}
		fmt.Printf("已建立和[%s]的连接\n", conn.RemoteAddr().String())
		go server.ConnHandler(conn)
	}
}

// 创建服务器
func NewServer(IP string, Port int) *Server {
	server := &Server{
		IP:        IP,
		Port:      Port,
		OnlineMap: make(map[string]*User),
		Message:   make(chan string),
	}
	return server
}

// 处理连接的回调函数
func (server *Server) ConnHandler(conn net.Conn) {
	// 创建一个User连接
	user := NewUser(conn, server)

	// 用户上线
	user.Online()

	go server.ReceiveMessage(conn, user)

	select {}
}

// 接收客户端发送的消息
func (server *Server) ReceiveMessage(conn net.Conn, user *User) {
	for {
		buf := make([]byte, BUFFER_LEN)
		n, err := conn.Read(buf)

		// n == 0 连接关闭
		if n == 0 {
			user.Offline()
			return
		}

		// err = io.EOF 也表示连接关闭
		if err != nil && err != io.EOF {
			fmt.Println("Conn Read error: ", err)
			return
		}

		// 提取消息，去掉末尾的\n
		msg := string(buf[:n-1])
		if msg == "who" {
			user.CheckOnlineUsers()
		} else if len(msg) > 7 && msg[:7] == "rename|" {
			newName := strings.Split(msg, "|")[1]

			_, ok := server.OnlineMap[newName]
			if ok {
				user.SendToBroadCastChannel(fmt.Sprintf("当前用户名[%s]已经被使用", newName))
			} else {
				server.mapLock.Lock()
				delete(server.OnlineMap, user.Name)
				server.OnlineMap[newName] = user
				server.mapLock.Unlock()

				user.Name = newName
				user.SendToBroadCastChannel(fmt.Sprintf("您已修改用户名为[%s]", newName))
			}
		} else {
			user.SendToBroadCastChannel(msg)
		}
	}
}

// 广播Channel监听消息
func (server *Server) BroadCastChannelOnMessage() {
	fmt.Println("广播channel已就绪...")
	for {
		message := <-server.Message
		func() {
			server.mapLock.RLock()
			defer server.mapLock.RUnlock()
			for _, cli := range server.OnlineMap {
				cli.C <- message
			}
		}()
	}
}
