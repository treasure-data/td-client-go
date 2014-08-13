package td_client

import (
	"net"
	"time"
)

type TimeoutConn struct {
	Conn net.Conn
	ReadTimeout time.Duration
	WriteTimeout time.Duration
	readDeadline time.Time
	writeDeadline time.Time
}

func (conn *TimeoutConn) Read(b []byte) (int, error) {
	if conn.readDeadline.IsZero() {
		if conn.ReadTimeout == 0 {
			conn.Conn.SetReadDeadline(time.Time {})
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
			conn.Conn.SetWriteDeadline(time.Time {})
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
