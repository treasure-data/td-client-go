//
// Treasure Data API client for Go
//
// Copyright (C) 2014 Treasure Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package td_client

import (
	"net"
	"time"
)

// TimeoutConn wraps a regular net.Conn so read / write operations on it will time out in the specified amount of time.
type TimeoutConn struct {
	Conn          net.Conn
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	readDeadline  time.Time
	writeDeadline time.Time
}

func (conn *TimeoutConn) Read(b []byte) (int, error) {
	if conn.readDeadline.IsZero() {
		if conn.ReadTimeout == 0 {
			conn.Conn.SetReadDeadline(time.Time{})
		} else {
			conn.Conn.SetReadDeadline(time.Now().Add(conn.ReadTimeout))
		}
	} else {
		conn.Conn.SetReadDeadline(conn.readDeadline)
	}
	return conn.Conn.Read(b)
}

func (conn *TimeoutConn) Write(b []byte) (int, error) {
	if conn.writeDeadline.IsZero() {
		if conn.WriteTimeout == 0 {
			conn.Conn.SetWriteDeadline(time.Time{})
		} else {
			conn.Conn.SetWriteDeadline(time.Now().Add(conn.WriteTimeout))
		}
	} else {
		conn.Conn.SetWriteDeadline(conn.writeDeadline)
	}
	return conn.Conn.Write(b)
}

func (conn *TimeoutConn) Close() error {
	return conn.Conn.Close()
}

func (conn *TimeoutConn) LocalAddr() net.Addr {
	return conn.Conn.LocalAddr()
}

func (conn *TimeoutConn) RemoteAddr() net.Addr {
	return conn.Conn.RemoteAddr()
}

func (conn *TimeoutConn) SetDeadline(t time.Time) error {
	conn.readDeadline = t
	conn.writeDeadline = t
	return nil
}

func (conn *TimeoutConn) SetReadDeadline(t time.Time) error {
	conn.readDeadline = t
	return nil
}

func (conn *TimeoutConn) SetWriteDeadline(t time.Time) error {
	conn.writeDeadline = t
	return nil
}
